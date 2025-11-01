import httpx
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Set
import logging
import sqlite3 # Added for queue interaction
import json # Added for serializing/deserializing queue data
import os

# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set to DEBUG to capture all messages

# Create a file handler for this module's logs
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "category_normalizer.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG) # Log all messages to file

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Optionally, add a stream handler to also output to console (e.g., for INFO and above)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info(f"Category Normalizer logging to {log_file_path}")

# Import the refactored surfer prospecting module
from surfer_prospector_module import run_prospecting_async
# Import the keyword normalization utility from scoring_utils
from scoring_utils import _normalize_keyword

# Module-level cache for canonical categories and their associated arbitrage data
_arbitrage_data_cache: Dict[str, Dict[str, Any]] = {}
_cache_initialized = False

MAX_LLM_RETRIES = 2 # Max retries for LLM calls if validation fails

def push_to_surfer_queue(
    db_path: str,
    seed_keywords: List[str],
    customer_domain: str,
    avg_job_amount: float,
    avg_conversion_rate: float,
    category: str,
    state: str,
    service_radius_cities: List[str],
    target_pool_size: int,
    min_volume_filter: int,
    country: str
):
    """
    Pushes a new task to the surfer_prospector_queue table.
    """
    logger.debug(f"Attempting to push task to surfer_prospector_queue for category: '{category}'")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO surfer_prospector_queue (
                seed_keywords, customer_domain, avg_job_amount, avg_conversion_rate,
                category, state, service_radius_cities,
                target_pool_size, min_volume_filter, country
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            json.dumps(seed_keywords),
            customer_domain,
            avg_job_amount,
            avg_conversion_rate,
            category,
            state,
            json.dumps(service_radius_cities),
            target_pool_size,
            min_volume_filter,
            country
        ))
        conn.commit()
        logger.info(f"Pushed task for category '{category}' to surfer_prospector_queue. Task details: {category}, {state}, {customer_domain}")
    except sqlite3.Error as e:
        logger.error(f"Error pushing to surfer_prospector_queue for category '{category}': {e}")
    finally:
        if conn:
            conn.close()

# New function to generate city-specific keywords via LLM with proper validation
async def generate_city_specific_keywords(category: str, batch_cities: List[str], llm_model: Any) -> Dict[str, List[str]]:
    """
    Generate 10 high-quality, short-tail SEO keywords for each city in the batch for a given category.
    These keywords should represent what a human would search for with the highest probability
    when looking for services related to the category in that specific city.
    """

    keywords_prompt = f"""
You are a professional SEO keyword researcher. Your task is to generate exactly 10 high-quality, short-tail SEO keywords for each city in the batch for a given category. These keywords MUST be highly relevant and represent what a human would search for with the highest probability for services related to '{category}' in that specific city.

CRITICAL INSTRUCTIONS - READ CAREFULLY:
1. You MUST return ONLY a valid JSON object.
2. You MUST include ALL requested cities as keys in the JSON object.
3. Each city MUST have an array called "keywords" containing exactly 10 keyword strings.
4. ALL keywords MUST be short-tail (1-2 words) and mid-tail (2-4 words) only.
5. NO long phrases, questions, or conversational language.
6. ALL keywords MUST explicitly include the city name (e.g., "moving Portland", "best movers Portland"). Do NOT generate keywords without the city name.
7. AVOID generic terms that don't clearly link the service to the city.
8. AVOID keywords that are not directly related to the service or are too broad.
9. Consider the nature of the business (service vs. non-service) and return the most logical search terms.
10. If the 'existingCategories' list provided in the input is an empty array, you MUST consider this category as a new category and you MUST set the "newCategory" field, and set "categoryId" to null.

VERIFICATION CHECKLIST - CONFIRM BEFORE RESPONDING:
- [ ] Response is a valid JSON object (not array, not string).
- [ ] Each city key exists and has an array called "keywords".
- [ ] "keywords" array contains exactly 10 keyword strings.
- [ ] All keywords are 1-4 words maximum.
- [ ] No keywords contain questions or long phrases.
- [ ] ALL keywords MUST follow the format: "service city" or "adjective service city".
- [ ] ALL keywords MUST be highly relevant to the category and city.

EXPECTED JSON FORMAT:
{{
  "Portland": {{
    "keywords": [
      "{category} portland",
      "best {category} portland",
      "local {category} portland",
      "moving service portland",
      "24 hour {category} portland",
      "top rated {category} portland",
      "affordable {category} portland",
      "residential {category} portland",
      "commercial {category} portland",
      "long distance {category} portland"
    ]
  }},
  "Beaverton": {{
    "keywords": [
      "{category} beaverton",
      "best {category} beaverton",
      "local {category} beaverton",
      "moving service beaverton",
      "24 hour {category} beaverton",
      "24 hour {category} beaverton",
      "top rated {category} beaverton",
      "affordable {category} beaverton",
      "residential {category} beaverton",
      "commercial {category} beaverton",
      "long distance {category} beaverton"
    ]
  }}
}}

Cities to process: {', '.join(batch_cities)}

Return ONLY the JSON object. No explanations, no markdown, no additional text.
"""

    # Retry mechanism with validation and quota handling
    max_retries = 5 # Increased retries for quota errors
    for attempt in range(max_retries):
        try:
            logger.debug(f"LLM Input for generate_city_specific_keywords (Attempt {attempt + 1}):\n{keywords_prompt}")

            response = await llm_model.generate_content_async(keywords_prompt)
            raw_response = response.text.strip()
            logger.debug(f"LLM Raw Output for generate_city_specific_keywords (Attempt {attempt + 1}):\n{raw_response}")

            # Clean the response
            cleaned_text = raw_response.replace("```json", "").replace("```", "").strip()

            # Parse the JSON response
            city_keywords_data = json.loads(cleaned_text)

            # Validate the response structure
            is_valid_structure = True
            if not isinstance(city_keywords_data, dict):
                is_valid_structure = False
            else:
                for city, keywords_obj in city_keywords_data.items():
                    if not isinstance(keywords_obj, dict) or \
                       "keywords" not in keywords_obj or \
                       not isinstance(keywords_obj["keywords"], list) or \
                       len(keywords_obj["keywords"]) != 10:
                        is_valid_structure = False
                        break
                    for kw in keywords_obj["keywords"]:
                        if not (isinstance(kw, str) and 1 <= len(kw.split()) <= 7):
                            is_valid_structure = False
                            break
                    if not is_valid_structure:
                        break

            if is_valid_structure:
                logger.debug(f"Successfully generated keywords for batch of {len(batch_cities)} cities.")
                return city_keywords_data
            else:
                logger.warning(f"Attempt {attempt + 1}: LLM returned invalid keyword data for batch. Response: {city_keywords_data}")
                if attempt == max_retries - 1:
                    logger.error(f"Skipping batch after {max_retries} failed attempts. Returning empty dict.")
                    return {}
                continue

        except json.JSONDecodeError as e:
            logger.warning(f"Attempt {attempt + 1}: JSON decode error for batch: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Skipping batch after {max_retries} failed JSON parsing attempts. Returning empty dict.")
                return {}
        except Exception as e:
            if "429" in str(e): # Quota exceeded error
                logger.warning(f"Attempt {attempt + 1}: Quota exceeded (429) for LLM call. Pausing for 20 seconds before retrying.")
                await asyncio.sleep(20)
            else:
                logger.error(f"Attempt {attempt + 1}: Unexpected error generating keywords for batch: {e}")
            
            if attempt == max_retries - 1:
                logger.error(f"Skipping batch after {max_retries} failed attempts. Returning empty dict.")
                return {}
    return {} # Should not be reached if retries are handled correctly

async def _initialize_cache(master_db_path: str):
    """
    Loads all category-location data from the canonical_categories table into the in-memory cache.
    This function should be called only once per session.
    """
    global _arbitrage_data_cache, _cache_initialized
    if _cache_initialized:
        logger.debug("Cache already initialized.")
        return

    logger.debug("Initializing in-memory cache from DB...")
    try:
        with sqlite3.connect(master_db_path) as con:
            cur = con.cursor()
            cur.execute("SELECT category, location, json_metadata FROM canonical_categories")
            for category, location, json_data in cur.fetchall():
                try:
                    metadata = json.loads(json_data)
                    # Explicitly check for essential fields. If any are missing, log and skip.
                    required_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "avgJobAmount"]
                    # The 'id' field in metadata should correspond to the category
                    if not all(field in metadata for field in required_fields) or metadata.get("id") != category:
                        logger.error(f"Essential fields missing or 'id' mismatch for category '{category}', location '{location}'. Required: {required_fields}. Found: {list(metadata.keys())}. Skipping entry.")
                        continue
                    if not isinstance(metadata.get("aliases"), list):
                        logger.error(f"'aliases' for category '{category}', location '{location}' is not a list. Skipping entry.")
                        continue
                    if not isinstance(metadata.get("examplePhrases"), list):
                        logger.error(f"'examplePhrases' for category '{category}', location '{location}' is not a list. Skipping entry.")
                        continue
                    
                    # Check if arbitrageData is present and not just a "queued" placeholder
                    if "arbitrageData" not in metadata or (isinstance(metadata["arbitrageData"], dict) and metadata["arbitrageData"].get("status") == "queued"):
                        logger.debug(f"Arbitrage data for category '{category}', location '{location}' is missing or queued. Not adding to cache for 'up-to-date' check.")
                        continue

                    # Add category and location to metadata for consistency
                    metadata['category'] = category
                    metadata['location'] = location
                    _arbitrage_data_cache[f"{category}|{location}"] = metadata
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for category '{category}', location '{location}'. Skipping entry.")
        logger.debug(f"Loaded {len(_arbitrage_data_cache)} entries into cache.")
        _cache_initialized = True
    except sqlite3.Error as e:
        logger.error(f"Error initializing cache from DB: {e}")
        _arbitrage_data_cache = {} # Ensure cache is empty on error
        _cache_initialized = False # Mark as not initialized

async def _validate_llm_output(llm_result: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates the LLM's output for required fields from the initial categorization call.
    Returns a tuple: (is_valid, missing_fields_list).
    """
    missing_fields = []
    is_valid = True

    if llm_result.get("newCategory"):
        new_cat = llm_result["newCategory"]
        # Ensure new_cat is a dictionary and has required fields
        if not isinstance(new_cat, dict) or not new_cat.get("id") or not new_cat.get("displayName"):
            missing_fields.append("newCategory (must be a dictionary with 'id' and 'displayName')")
            is_valid = False
        else:
            required_new_cat_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "avgJobAmount"]
            for field in required_new_cat_fields:
                if field not in new_cat:
                    missing_fields.append(f"newCategory.{field}")
                    is_valid = False
            if not isinstance(new_cat.get("aliases"), list):
                missing_fields.append("newCategory.aliases (must be a list)")
                is_valid = False
            if not isinstance(new_cat.get("examplePhrases"), list):
                missing_fields.append("newCategory.example_phrases (must be a list)")
                is_valid = False
            if not isinstance(new_cat.get("avgJobAmount"), (int, float)):
                missing_fields.append("newCategory.avgJobAmount (must be a number)")
                is_valid = False
    elif llm_result.get("categoryId"):
        # Ensure categoryId is a non-empty string
        if not isinstance(llm_result.get("categoryId"), str) or not llm_result.get("categoryId"):
            missing_fields.append("categoryId (must be a non-empty string)")
            is_valid = False
        else:
            required_matched_cat_fields = ["categoryId", "matchedAlias", "confidence", "avgJobAmount"]
            for field in required_matched_cat_fields:
                if field not in llm_result:
                    missing_fields.append(field)
                    is_valid = False
            if not isinstance(llm_result.get("avgJobAmount"), (int, float)):
                missing_fields.append("avgJobAmount (must be a number)")
                is_valid = False
    else:
        missing_fields.append("categoryId or newCategory (neither found or invalid)")
        is_valid = False
    
    # Validate serviceRadiusCities
    if "serviceRadiusCities" not in llm_result or not isinstance(llm_result["serviceRadiusCities"], list):
        missing_fields.append("serviceRadiusCities (must be a list of 51 city names)")
        is_valid = False
    else:
        # Enforce 51 city names: truncate if more, pad with empty strings if less
        current_cities = llm_result["serviceRadiusCities"]
        if len(current_cities) > 51:
            llm_result["serviceRadiusCities"] = current_cities[:51]
            logger.debug(f"Truncated serviceRadiusCities to 51 cities.")
        elif len(current_cities) < 51:
            llm_result["serviceRadiusCities"].extend([""] * (51 - len(current_cities)))
            logger.debug(f"Padded serviceRadiusCities to 51 cities.")
            
    return is_valid, missing_fields

async def normalize_business_category(
    leads_batch: List[Dict[str, Any]],
    llm_model: Any,
    master_db_path: str,
    firebase_arbitrage_sync_url: str,
    category_arbitrage_update_interval_days: int
) -> List[Dict[str, Any]]:
    """
    Normalizes business categories for a batch of leads using an LLM and a local cache.
    Conditionally triggers Surfer Prospecting and pushes data to Firebase.
    Returns the updated leads batch with normalized categories.
    """
    logger.info(f"Starting normalization for batch of {len(leads_batch)} leads.")

    await _initialize_cache(master_db_path)

    # Prepare data for batch LLM call
    llm_batch_input = []
    for lead in leads_batch:
        # Use the original post body text for category classification if available, otherwise use the lead's business_name
        category_classification_text = lead.get("body_text") or lead.get("business_name", "")
        llm_batch_input.append({
            "post_id": lead.get("post_id"), # Using post_id as a unique identifier for mapping back
            "businessText": category_classification_text,
            "city": lead.get("city", ""), # Pass city to LLM input
            "state": lead.get("state", ""), # Pass state to LLM input
            "servicesRendered": lead.get("services_rendered", []), # Pass services_rendered to LLM input
            "existingCategories": [
                {"id": cat_data["id"], "displayName": cat_data["displayName"], "aliases": cat_data["aliases"]}
                for cat_data in _arbitrage_data_cache.values() # Use cache for existing categories
            ]
        })

    # Construct the LLM prompt for batch classification
    batch_prompt_template = """
You are a classifier that maps business descriptions to an existing category or proposes a new category.
You will receive a JSON array of objects, where each object has 'post_id', 'businessText', 'city', 'state', 'servicesRendered', and 'existingCategories'.
For each object, classify the 'businessText' against the 'existingCategories' or propose a 'newCategory'.

Rules:
1. For each input object, if the 'businessText' clearly fits an existing category, set "categoryId" to the matched category id (slug), include "matchedAlias" (the alias matched), "confidence" (0.0-1.0).
   Additionally, generate a list of the top 50 cities within a 30-mile radius of the lead's 'city', 'state' (including the lead's 'city' itself), ranked by population. This list should be returned in a field called "serviceRadiusCities".

2. If it does NOT fit any existing category, set "categoryId": null AND return a "newCategory" object. The "newCategory" must be a full category object with these fields: id (suggested slug), displayName, aliases (array), description (1-2 sentences), examplePhrases (array), parent (optional), confidence (0.0-1.0) representing how confident you are in the suggestion.
   Additionally, generate a list of the top 50 cities within a 30-mile radius of the lead's 'city', 'state' (including the lead's 'city' itself), ranked by population. This list should be returned in a field called "serviceRadiusCities".

3. Your response MUST be a single, valid JSON array of results, one object for each input object, maintaining the original 'post_id'. Do not include explanatory text.

**Important Rule for avgJobAmount:**
- For both new and existing categories, `avgJobAmount` must be a numerical value representing the average job amount in USD. Infer this amount based on typical industry numbers for the given category.

**Important Rule for serviceRadiusCities:**
- The list MUST contain exactly 51 city names (the base city + 50 others).
- Cities MUST be ranked by population (highest to lowest).
- Return ONLY city names (strings), no state abbreviations or other info.

**CRITICAL RULE FOR CATEGORY ASSIGNMENT:**
- If the 'existingCategories' list provided in the input is an empty array, you MUST consider this category as a new category and you MUST set the "newCategory" field, and set "categoryId" to null.

Example expected outputs (for a batch of 2 inputs):
[
  {{
    "post_id": "post123",
    "categoryId": "plumbing",
    "matchedAlias": "plumber",
    "confidence": 0.95,
    "newCategory": null,
    "avgJobAmount": 500,
    "serviceRadiusCities": ["Portland", "Vancouver", "Gresham", "Hillsboro", "Beaverton", "Happy Valley", "Lake Oswego", "Oregon City", "Milwaukie", "Tigard", "Troutdale", "Fairview", "Wood Village", "Clackamas", "Wilsonville", "Sherwood", "Canby", "Forest Grove", "Portland", "Vancouver", "Gresham", "Hillsboro", "Beaverton", "Happy Valley", "Lake Oswego", "Oregon City", "Milwaukie", "Tigard", "Troutdale", "Fairview", "Wood Village", "Clackamas", "Wilsonville", "Sherwood", "Canby", "Forest Grove", "Cornelius", "Banks", "North Plains", "Gaston", "St. Helens", "Scappoose", "Columbia City", "Rainier", "Vernonia", "Estacada", "Sandy", "Welches", "Government Camp", "Brightwood", "Rhododendron", "Zigzag", "Mount Hood Village", "Parkdale", "Hood River", "The Dalles", "White Salmon", "Bingen", "Stevenson", "Carson", "North Bonneville", "Skamania", "Underwood", "Lyle", "Goldendale", "Klickitat", "Wishram", "Dufur", "Maupin"]
  }},
  {{
    "post_id": "post456",
    "categoryId": null,
    "matchedAlias": null,
    "confidence": 0.62,
    "newCategory": {{
      "id": "water-heater-repair",
      "displayName": "Water Heater Repair",
      "aliases": ["water heater repair","hot water heater service","tankless heater repair"],
      "description": "Specialized repair & installation of residential and commercial water heaters (tank and tankless).",
      "examplePhrases": ["water heater repair","hot water heater won't heat","tankless heater service"],
      "parent": "plumbing",
      "confidence": 0.62,
      "avgJobAmount": 800
    }},
    "serviceRadiusCities": ["Portland", "Vancouver", "Gresham", "Hillsboro", "Beaverton", "Happy Valley", "Lake Oswego", "Oregon City", "Milwaukie", "Tigard", "Troutdale", "Fairview", "Wood Village", "Clackamas", "Wilsonville", "Sherwood", "Canby", "Forest Grove", "Cornelius", "Banks", "North Plains", "Gaston", "St. Helens", "Scappoose", "Columbia City", "Rainier", "Vernonia", "Estacada", "Sandy", "Welches", "Government Camp", "Brightwood", "Rhododendron", "Zigzag", "Mount Hood Village", "Parkdale", "Hood River", "The Dalles", "White Salmon", "Bingen", "Stevenson", "Carson", "North Bonneville", "Skamania", "Underwood", "Lyle", "Goldendale", "Klickitat", "Wishram", "Dufur", "Maupin"]
  }}
]

Here is the batch of leads to process: {batch_llm_input_json}
"""
    
    llm_batch_output = []
    retries = 0
    while retries <= MAX_LLM_RETRIES:
        full_batch_prompt = batch_prompt_template.format(
            batch_llm_input_json=json.dumps(llm_batch_input, separators=(',', ':'))
        )
        
        if retries > 0:
            logger.info(f"Retrying LLM call for batch (Attempt {retries + 1}).") # Fixed NameError here
            # If retrying, provide feedback to the LLM about previous validation failures
            feedback_messages = []
            # Iterate through the last LLM output to find invalid items and generate specific feedback
            for i, item in enumerate(llm_batch_output): 
                is_valid, missing_fields = await _validate_llm_output(item)
                if not is_valid:
                    feedback_messages.append(f"For post_id '{item.get('post_id', f'index_{i}')}', the previous output was invalid. Missing or malformed fields: {', '.join(missing_fields)}. Please ensure all required fields are present and correctly formatted as per the rules, especially for 'newCategory' or 'categoryId' and 'serviceRadiusCities'.")
            if feedback_messages:
                full_batch_prompt += "\n\nPrevious attempt failed validation. Please correct the following issues:\n" + "\n".join(feedback_messages)
                logger.info(f"Sending LLM feedback: {feedback_messages}")

        try:
            response = await llm_model.generate_content_async(full_batch_prompt)
            raw_response_text = response.text
            cleaned_text = raw_response_text.strip().replace("```json", "").replace("```", "").strip()
            llm_batch_output = json.loads(cleaned_text)
            logger.info(f"LLM batch output: {json.dumps(llm_batch_output, indent=2)}")

            # Validate each item in the batch output
            all_valid = True
            for item in llm_batch_output:
                is_valid, _ = await _validate_llm_output(item)
                if not is_valid:
                    all_valid = False
                    logger.warning(f"LLM output validation failed for post_id '{item.get('post_id')}'. This item will be retried or marked as 'error'.")
            
            if all_valid:
                break # Exit retry loop if the entire batch is valid
            else:
                retries += 1
                if retries > MAX_LLM_RETRIES:
                    logger.error(f"Max LLM retries reached for batch. LLM failed to assign a valid category after {MAX_LLM_RETRIES} attempts. Marking leads as 'error'.")
                    # No longer raising an exception here, but marking as 'error'
                    break

        except json.JSONDecodeError as e:
            retries += 1
            logger.warning(f"Attempt {retries + 1}: JSON decode error for batch: {e}") # Fixed NameError here
            logger.debug(f"Raw Response Text: {raw_response_text[:500] if 'raw_response_text' in locals() else 'No text found'}...")
            if retries > MAX_LLM_RETRIES:
                logger.error(f"Max LLM retries reached. LLM failed to assign a valid category after {MAX_LLM_RETRIES} attempts. Marking leads as 'error'.")
                break
        except Exception as e:
            retries += 1
            logger.error(f"CRITICAL ERROR during batch LLM call (Attempt {retries + 1}/{MAX_LLM_RETRIES}): {e}") # Fixed NameError here
            if retries > MAX_LLM_RETRIES:
                logger.error(f"Max LLM retries reached. LLM failed to assign a valid category after {MAX_LLM_RETRIES} attempts. Marking leads as 'error'.")
                return [] # Return empty list on critical failure
    
    # Map LLM output back to leads and apply retry/error logic
    final_llm_output_map = {}
    for item in llm_batch_output:
        if item.get('post_id'):
            is_valid, _ = await _validate_llm_output(item)
            if is_valid:
                final_llm_output_map[item['post_id']] = item
            else:
                logger.warning(f"Post_id '{item.get('post_id')}': LLM result invalid after retries. This lead will be marked as 'error'.")
                final_llm_output_map[item['post_id']] = {"category": "error"} # Mark as error

    updated_leads_batch = []
    for lead in leads_batch:
        post_id = lead.get("post_id")
        llm_result = final_llm_output_map.get(post_id)
        
        canonical_category_id = None
        
        if not llm_result or llm_result.get("category") == "error":
            canonical_category_id = "error"
            logger.error(f"Lead {post_id}: LLM failed to assign a valid category after retries. Setting category to 'error'.")
        elif llm_result.get("newCategory"):
            canonical_category_id = llm_result["newCategory"]["id"]
            logger.info(f"Lead {post_id}: New category suggested: '{canonical_category_id}'")
        elif llm_result.get("categoryId"):
            canonical_category_id = llm_result["categoryId"]
            logger.info(f"Lead {post_id}: Matched existing category: '{canonical_category_id}'")
        else:
            # Fallback for unexpected valid but uncategorized LLM result
            canonical_category_id = "error"
            logger.error(f"Lead {post_id}: LLM result was valid but did not contain categoryId or newCategory. Setting category to 'error'.")

        # If category is 'error', do not push to surfer queue
        if canonical_category_id == "error":
            lead['category'] = "error"
            updated_leads_batch.append(lead)
            continue

        # Validate categoryId against existing cache if it's not a new category
        if not llm_result.get("newCategory"): # If it's an existing category match
            # The cache key is now category|location
            cache_key_for_lookup = f"{canonical_category_id}|{lead.get('city', '').lower().replace(' ', '-')}-{lead.get('state', '').lower()}"
            if cache_key_for_lookup not in _arbitrage_data_cache:
                logger.warning(f"Lead {post_id}: LLM returned existing category '{canonical_category_id}' for location '{lead.get('city')}-{lead.get('state')}' not found in provided existingCategories. Marking for retry or setting to 'error'.")
                # This is where the retry logic would go. For now, setting to error as per fallback.
                canonical_category_id = "error"
                lead['category'] = "error"
                updated_leads_batch.append(lead)
                continue

        customer_domain_for_surfer = lead.get('website_url', 'https://example.com')

        avg_job_amount = 0.0
        if llm_result.get("newCategory"):
            avg_job_amount = llm_result["newCategory"].get("avgJobAmount", 0.0)
        elif llm_result.get("categoryId"):
            avg_job_amount = llm_result.get("avgJobAmount", 0.0)

        avg_conversion_rate = 0.05

        # Extract serviceRadiusCities from the initial LLM result
        service_radius_cities = llm_result.get("serviceRadiusCities", [])
        if not service_radius_cities:
            logger.warning(f"Lead {post_id}: Initial LLM result missing 'serviceRadiusCities'. Setting category to 'error'.")
            canonical_category_id = "error"
            lead['category'] = "error"
            updated_leads_batch.append(lead)
            continue

        # Generate city-specific keywords for each city in service_radius_cities
        all_city_specific_keywords = []
        batch_size = 26 # User-defined batch size
        for i in range(0, len(service_radius_cities), batch_size):
            batch_of_cities = service_radius_cities[i:i + batch_size]
            try:
                city_keywords_data = await generate_city_specific_keywords(canonical_category_id, batch_of_cities, llm_model)
                for city_name, keywords_obj in city_keywords_data.items():
                    for kw in keywords_obj.get("keywords", []):
                        normalized_kw = _normalize_keyword(kw) # Normalize LLM generated keywords
                        all_city_specific_keywords.append(normalized_kw)
                        logger.debug(f"Normalized LLM keyword: '{kw}' -> '{normalized_kw}'")
            except Exception as e:
                logger.error(f"Lead {post_id}: Error generating keywords for batch of cities {batch_of_cities}: {e}")
        
        # Remove duplicates and convert to list
        unique_all_city_specific_keywords = list(set(all_city_specific_keywords))

        if not unique_all_city_specific_keywords:
            logger.warning(f"Lead {post_id}: No unique city-specific keywords generated for Surfer call. Setting category to 'error'.")
            canonical_category_id = "error"
            lead['category'] = "error"
            updated_leads_batch.append(lead)
            continue

        surfer_target_pool_size = 510

        # Push to queue
        push_to_surfer_queue(
            db_path=master_db_path,
            seed_keywords=unique_all_city_specific_keywords,
            customer_domain=customer_domain_for_surfer,
            avg_job_amount=avg_job_amount,
            avg_conversion_rate=avg_conversion_rate,
            category=canonical_category_id,
            state=lead.get("state", ""),
            service_radius_cities=service_radius_cities,
            target_pool_size=surfer_target_pool_size,
            min_volume_filter=20,
            country="US"
        )
        logger.info(f"Lead {post_id}: Pushed Surfer Prospecting task for '{canonical_category_id}' to queue.")

        # The category field of the lead is updated
        lead['category'] = canonical_category_id
        updated_leads_batch.append(lead)

    return updated_leads_batch
