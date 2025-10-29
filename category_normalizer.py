import httpx
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Set
import logging
import sqlite3 # Added for queue interaction
import json # Added for serializing/deserializing queue data

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
        logging.info(f"    [Category Normalizer] Pushed task for category '{category}' to surfer_prospector_queue.")
    except sqlite3.Error as e:
        logging.error(f"!!! [Category Normalizer] ERROR pushing to surfer_prospector_queue: {e}")
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
You are a professional SEO keyword researcher. Your task is to generate exactly 10 high-quality, short-tail SEO keywords for each city in the category '{category}'. These keywords MUST be highly relevant and represent what a human would search for with the highest probability for services related to '{category}' in that specific city.

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
            logging.info(f"[Category Normalizer] LLM Input for generate_city_specific_keywords (Attempt {attempt + 1}):\n{keywords_prompt}")

            response = await llm_model.generate_content_async(keywords_prompt)
            raw_response = response.text.strip()
            logging.info(f"[Category Normalizer] LLM Raw Output for generate_city_specific_keywords (Attempt {attempt + 1}):\n{raw_response}")

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
                logging.debug(f"    [Category Normalizer] Successfully generated keywords for batch of {len(batch_cities)} cities.")
                return city_keywords_data
            else:
                logging.warning(f"!!! [Category Normalizer] Attempt {attempt + 1}: LLM returned invalid keyword data for batch. Expected dict with city keys, each containing 'primaryKeywords' (5 strings) and 'additionalKeywords' (10 strings). Response: {city_keywords_data}")
                if attempt == max_retries - 1:
                    logging.error(f"!!! [Category Normalizer] Skipping batch after {max_retries} failed attempts. Returning empty dict.")
                    return {}
                continue

        except json.JSONDecodeError as e:
            logging.warning(f"!!! [Category Normalizer] Attempt {attempt + 1}: JSON decode error for batch: {e}")
            if attempt == max_retries - 1:
                logging.error(f"!!! [Category Normalizer] Skipping batch after {max_retries} failed JSON parsing attempts. Returning empty dict.")
                return {}
        except Exception as e:
            if "429" in str(e): # Quota exceeded error
                logging.warning(f"!!! [Category Normalizer] Attempt {attempt + 1}: Quota exceeded (429) for LLM call. Pausing for 20 seconds before retrying.")
                await asyncio.sleep(20)
            else:
                logging.error(f"!!! [Category Normalizer] Attempt {attempt + 1}: Unexpected error generating keywords for batch: {e}")
            
            if attempt == max_retries - 1:
                logging.error(f"!!! [Category Normalizer] Skipping batch after {max_retries} failed attempts. Returning empty dict.")
                return {}
    return {} # Should not be reached if retries are handled correctly

async def _initialize_cache(master_db_path: str):
    """
    Loads all category-location data from the canonical_categories table into the in-memory cache.
    This function should be called only once per session.
    """
    global _arbitrage_data_cache, _cache_initialized
    if _cache_initialized:
        logging.debug("    [Category Normalizer] Cache already initialized.")
        return

    logging.debug("    [Category Normalizer] Initializing in-memory cache from DB...")
    try:
        with sqlite3.connect(master_db_path) as con:
            cur = con.cursor()
            cur.execute("SELECT id, json_metadata FROM canonical_categories")
            for row_id, json_data in cur.fetchall():
                try:
                    metadata = json.loads(json_data)
                    # Explicitly check for essential fields. If any are missing, log and skip.
                    required_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "avgJobAmount"] # Removed suggestedKeywords
                    if not all(field in metadata for field in required_fields):
                        logging.error(f"!!! [Category Normalizer] ERROR: Essential fields missing for ID: {row_id}. Required: {required_fields}. Found: {list(metadata.keys())}. Skipping entry.")
                        continue
                    if not isinstance(metadata.get("aliases"), list):
                        logging.error(f"!!! [Category Normalizer] ERROR: 'aliases' for ID: {row_id} is not a list. Skipping entry.")
                        continue
                    if not isinstance(metadata.get("examplePhrases"), list):
                        logging.error(f"!!! [Category Normalizer] ERROR: 'examplePhrases' for ID: {row_id} is not a list. Skipping entry.")
                        continue

                    _arbitrage_data_cache[row_id] = metadata
                except json.JSONDecodeError:
                    logging.error(f"!!! [Category Normalizer] ERROR decoding JSON for ID: {row_id}. Skipping entry.")
        logging.debug(f"    [Category Normalizer] Loaded {len(_arbitrage_data_cache)} entries into cache.")
        _cache_initialized = True
    except sqlite3.Error as e:
        logging.error(f"!!! [Category Normalizer] ERROR initializing cache from DB: {e}")
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
        required_new_cat_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "avgJobAmount"]
        for field in required_new_cat_fields:
            if field not in new_cat:
                missing_fields.append(f"newCategory.{field}")
                is_valid = False
        if not isinstance(new_cat.get("aliases"), list):
            missing_fields.append("newCategory.aliases (must be a list)")
            is_valid = False
        if not isinstance(new_cat.get("examplePhrases"), list):
            missing_fields.append("newCategory.examplePhrases (must be a list)")
            is_valid = False
        if not isinstance(new_cat.get("avgJobAmount"), (int, float)):
            missing_fields.append("newCategory.avgJobAmount (must be a number)")
            is_valid = False
    elif llm_result.get("categoryId"):
        required_matched_cat_fields = ["categoryId", "matchedAlias", "confidence", "avgJobAmount"]
        for field in required_matched_cat_fields:
            if field not in llm_result:
                missing_fields.append(field)
                is_valid = False
        if not isinstance(llm_result.get("avgJobAmount"), (int, float)):
            missing_fields.append("avgJobAmount (must be a number)")
            is_valid = False
    else:
        missing_fields.append("categoryId or newCategory (neither found)")
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
            logging.debug(f"    [Category Normalizer] Truncated serviceRadiusCities to 51 cities.")
        elif len(current_cities) < 51:
            llm_result["serviceRadiusCities"].extend([""] * (51 - len(current_cities)))
            logging.debug(f"    [Category Normalizer] Padded serviceRadiusCities to 51 cities.")
            
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
    logging.info(f"    [Category Normalizer] Starting normalization for batch of {len(leads_batch)} leads.")

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

Example expected outputs (for a batch of 2 inputs):
[
  {{
    "post_id": "post123",
    "categoryId": "plumbing",
    "matchedAlias": "plumber",
    "confidence": 0.95,
    "newCategory": null,
    "avgJobAmount": 500,
    "serviceRadiusCities": ["Portland", "Vancouver", "Gresham", "Hillsboro", "Beaverton", "Happy Valley", "Lake Oswego", "Oregon City", "Milwaukie", "Tigard", "Troutdale", "Fairview", "Wood Village", "Clackamas", "Wilsonville", "Sherwood", "Canby", "Forest Grove", "Cornelius", "Banks", "North Plains", "Gaston", "St. Helens", "Scappoose", "Columbia City", "Rainier", "Vernonia", "Estacada", "Sandy", "Welches", "Government Camp", "Brightwood", "Rhododendron", "Zigzag", "Mount Hood Village", "Parkdale", "Hood River", "The Dalles", "White Salmon", "Bingen", "Stevenson", "Carson", "North Bonneville", "Skamania", "Underwood", "Lyle", "Goldendale", "Klickitat", "Wishram", "Dufur", "Maupin"]
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
            logging.info(f"    [Category Normalizer] Retrying LLM call for batch (Attempt {retries}/{MAX_LLM_RETRIES}).")
            # If retrying, provide feedback to the LLM about previous validation failures
            feedback_messages = []
            # Iterate through the last LLM output to find invalid items and generate specific feedback
            for i, item in enumerate(llm_batch_output): 
                is_valid, missing_fields = await _validate_llm_output(item)
                if not is_valid:
                    feedback_messages.append(f"For post_id '{item.get('post_id', f'index_{i}')}', the previous output was invalid. Missing or malformed fields: {', '.join(missing_fields)}. Please ensure all required fields are present and correctly formatted as per the rules, especially for 'newCategory' or 'categoryId' and 'serviceRadiusCities'.")
            if feedback_messages:
                full_batch_prompt += "\n\nPrevious attempt failed validation. Please correct the following issues:\n" + "\n".join(feedback_messages)
                logging.info(f"    [Category Normalizer] Sending LLM feedback: {feedback_messages}")

        try:
            response = await llm_model.generate_content_async(full_batch_prompt)
            raw_response_text = response.text
            cleaned_text = raw_response_text.strip().replace("```json", "").replace("```", "").strip()
            llm_batch_output = json.loads(cleaned_text)
            logging.info(f"    [Category Normalizer] LLM batch output: {json.dumps(llm_batch_output, indent=2)}")

            # Validate each item in the batch output
            all_valid = True
            for item in llm_batch_output:
                is_valid, missing_fields = await _validate_llm_output(item)
                if not is_valid:
                    all_valid = False
                    logging.warning(f"!!! [Category Normalizer] LLM output validation failed for post_id '{item.get('post_id')}'. Missing fields: {', '.join(missing_fields)}. Skipping this item for further processing.")
            
            if all_valid:
                break # Exit retry loop if the entire batch is valid
            else:
                retries += 1
                if retries > MAX_LLM_RETRIES:
                    logging.error(f"!!! [Category Normalizer] Max LLM retries reached for batch. Processing with potentially invalid data.")
                    break

        except json.JSONDecodeError as e:
            retries += 1
            logging.warning(f"!!! [Category Normalizer] Failed to decode JSON from LLM response (Attempt {retries}/{MAX_LLM_RETRIES}). Error: {e}")
            logging.debug(f"    Raw Response Text: {raw_response_text[:500] if 'raw_response_text' in locals() else 'No text found'}...")
            if retries > MAX_LLM_RETRIES:
                logging.error(f"!!! [Category Normalizer] Max LLM retries reached. Processing with potentially invalid data.")
                break
        except Exception as e:
            retries += 1
            logging.error(f"!!! [Category Normalizer] CRITICAL ERROR during batch LLM call (Attempt {retries}/{MAX_LLM_RETRIES}): {e}")
            if retries > MAX_LLM_RETRIES:
                logging.error(f"!!! [Category Normalizer] Max LLM retries reached. Processing with potentially invalid data.")
                return [] # Return empty list on critical failure
    
    # Map LLM output back to leads
    llm_output_map = {}
    for item in llm_batch_output:
        if item.get('post_id'):
            is_valid, _ = await _validate_llm_output(item)
            if is_valid:
                llm_output_map[item['post_id']] = item
            else:
                logging.warning(f"!!! [Category Normalizer] Post_id '{item.get('post_id')}': LLM result invalid after retries. Skipping this item for further processing.")
    
    updated_leads_batch = []
    for lead in leads_batch:
        post_id = lead.get("post_id")
        llm_result = llm_output_map.get(post_id)
        
        if not llm_result:
            logging.warning(f"!!! [Category Normalizer] Lead {post_id}: LLM result is missing or invalid after retries. Skipping processing for this lead.")
            lead['category'] = lead.get('category')
            updated_leads_batch.append(lead)
            continue

        canonical_category_id = None
        location_slug = lead.get("city", "").lower().replace(" ", "-") + "-" + lead.get("state", "").lower().replace(" ", "-")
        
        serp_data = None
        combined_json_metadata = {}

        is_new_category = False
        needs_refresh = False
        
        if llm_result.get("newCategory"):
            is_new_category = True
            canonical_category_id = llm_result["newCategory"]["id"]
            logging.info(f"    [Category Normalizer] Lead {post_id}: New category suggested: '{canonical_category_id}'")
        elif llm_result.get("categoryId"):
            canonical_category_id = llm_result["categoryId"]
            logging.info(f"    [Category Normalizer] Lead {post_id}: Matched existing category: '{canonical_category_id}'")
        else:
            logging.warning(f"!!! [Category Normalizer] Lead {post_id}: LLM did not return a valid categoryId or newCategory. Skipping Surfer call.")
            lead['category'] = lead.get('category')
            updated_leads_batch.append(lead)
            continue

        category_location_id = f"{canonical_category_id}/{location_slug}"

        cached_data = _arbitrage_data_cache.get(category_location_id)
        if cached_data:
            last_updated_str = cached_data.get("lastUpdated")
            if last_updated_str:
                last_updated_dt = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00')).astimezone(timezone.utc)
                if datetime.now(timezone.utc) - last_updated_dt > timedelta(days=category_arbitrage_update_interval_days):
                    needs_refresh = True
                    logging.info(f"    [Category Normalizer] Lead {post_id}: Existing category '{canonical_category_id}' needs refresh.")
                else:
                    logging.info(f"    [Category Normalizer] Lead {post_id}: Existing category '{canonical_category_id}' is up-to-date. Skipping Surfer call.")
                    combined_json_metadata = cached_data
                    serp_data = cached_data.get("arbitrageData")
            else:
                needs_refresh = True
                logging.info(f"    [Category Normalizer] Lead {post_id}: Existing category '{canonical_category_id}' has no 'lastUpdated' timestamp. Assuming needs refresh.")
        else:
            needs_refresh = True
            logging.info(f"    [Category Normalizer] Lead {post_id}: Existing category '{canonical_category_id}' not in cache. Assuming needs refresh.")

        # Always generate city-specific keywords and call Surfer if new category or needs refresh
        # Or if the cached data doesn't contain serviceRadiusCities (new field)
        if is_new_category or needs_refresh or "serviceRadiusCities" not in combined_json_metadata:
            logging.info(f"    [Category Normalizer] Lead {post_id}: Calling Surfer Prospecting for category '{canonical_category_id}' and location '{location_slug}'...")

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
                logging.warning(f"!!! [Category Normalizer] Lead {post_id}: Initial LLM result missing 'serviceRadiusCities'. Skipping Surfer call.")
                lead['category'] = canonical_category_id
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
                            logging.debug(f"    [Category Normalizer] Normalized LLM keyword: '{kw}' -> '{normalized_kw}'")
                except Exception as e:
                    logging.error(f"!!! [Category Normalizer] Lead {post_id}: Error generating keywords for batch of cities {batch_of_cities}: {e}")
            
            # Remove duplicates and convert to list
            unique_all_city_specific_keywords = list(set(all_city_specific_keywords))

            if not unique_all_city_specific_keywords:
                logging.warning(f"!!! [Category Normalizer] Lead {post_id}: No unique city-specific keywords generated for Surfer call. Skipping Surfer call.")
                lead['category'] = canonical_category_id
                updated_leads_batch.append(lead)
                continue

            surfer_target_pool_size = 510

            # Instead of running directly, push to queue
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
            logging.info(f"    [Category Normalizer] Lead {post_id}: Pushed Surfer Prospecting task for '{canonical_category_id}' to queue.")

            # Since we are queuing, we don't have serp_data immediately.
            # We will need to update the cache and Firebase push logic to reflect this.
            # For now, we'll set serp_data to a placeholder or handle it as None.
            serp_data = {"status": "queued", "message": "Surfer prospecting task queued."}

            current_utc_timestamp = datetime.now(timezone.utc).isoformat().replace('Z', '+00:00')
            
            if is_new_category:
                new_cat_data = llm_result["newCategory"]
                required_new_cat_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "avgJobAmount"]
                if not all(field in new_cat_data for field in required_new_cat_fields):
                    logging.warning(f"!!! [Category Normalizer] Lead {post_id}: New category data from LLM is incomplete. Required: {required_new_cat_fields}. Skipping save/push.")
                    lead['category'] = canonical_category_id
                    updated_leads_batch.append(lead)
                    continue
                
                combined_json_metadata = new_cat_data
                combined_json_metadata["suggestedAt"] = current_utc_timestamp
                combined_json_metadata["createdBy"] = "llm"
            else:
                if cached_data:
                    combined_json_metadata = cached_data.copy()
                    combined_json_metadata["confidence"] = llm_result.get("confidence", combined_json_metadata.get("confidence"))
                    combined_json_metadata["avgJobAmount"] = llm_result.get("avgJobAmount", combined_json_metadata.get("avgJobAmount"))
                else:
                    logging.info(f"    [Category Normalizer] Lead {post_id}: LLM matched existing category '{canonical_category_id}' not found in cache. Treating as new entry for saving.")
                    combined_json_metadata = {
                        "id": canonical_category_id,
                        "displayName": "",
                        "aliases": [],
                        "description": "",
                        "examplePhrases": [],
                        "parent": None,
                        "confidence": llm_result.get("confidence", 0.0),
                        "avgJobAmount": llm_result.get("avgJobAmount", 0.0),
                        "suggestedAt": current_utc_timestamp,
                        "createdBy": "llm_fallback"
                    }

            combined_json_metadata["lastUpdated"] = current_utc_timestamp
            combined_json_metadata["location"] = location_slug
            combined_json_metadata["arbitrageData"] = serp_data
            combined_json_metadata["serviceRadiusCities"] = service_radius_cities # Store the generated cities
            
            # Add the new short_term_strategy data
            if serp_data and "short_term_strategy" in serp_data:
                combined_json_metadata["short_term_strategy"] = serp_data["short_term_strategy"]
                logging.info(f"    [Category Normalizer] Lead {post_id}: Added short_term_strategy to combined_json_metadata.")

            # 🆕 NEW: Generate city-specific clusters for landing page creation
            city_clusters = {}
            # The city_keywords_map is no longer directly used here, but the all_city_specific_keywords list is
            if service_radius_cities and unique_all_city_specific_keywords: # Check if we have cities and keywords
                try:
                    from surfer_prospector_module import generate_city_specific_clusters
                    logging.info(f"    [Category Normalizer] Lead {post_id}: Generating city-specific clusters...")

                    master_keyword_pool_normalized = {}
                    if serp_data and isinstance(serp_data, dict) and "scored_keywords" in serp_data:
                        for keyword_data in serp_data["scored_keywords"]:
                            keyword = keyword_data.get("keyword")
                            if keyword:
                                normalized_keyword = _normalize_keyword(keyword) # Normalize Surfer API keywords
                                master_keyword_pool_normalized[normalized_keyword] = keyword_data
                                logging.debug(f"    [Category Normalizer] Normalized Surfer keyword: '{keyword}' -> '{normalized_keyword}'")

                    # Reconstruct city_keywords_map for generate_city_specific_clusters
                    temp_city_keywords_map = {city_name: [] for city_name in service_radius_cities}
                    normalized_service_radius_cities = [_normalize_keyword(city) for city in service_radius_cities]

                    for normalized_kw, kw_data in master_keyword_pool_normalized.items():
                        for city_name, normalized_city in zip(service_radius_cities, normalized_service_radius_cities):
                            if normalized_city in normalized_kw:
                                temp_city_keywords_map[city_name].append(normalized_kw)
                                logging.debug(f"    [Category Normalizer] Associated normalized keyword '{normalized_kw}' with city '{city_name}'.")
                                break # Associate with the first matching city found

                    city_clusters = await generate_city_specific_clusters(
                        master_keyword_pool_normalized, # Pass the normalized master_keyword_pool
                        temp_city_keywords_map, # Pass the filtered map with normalized keywords
                        customer_domain_for_surfer,
                        avg_job_amount,
                        avg_conversion_rate,
                        llm_model
                    )
                    logging.info(f"    [Category Normalizer] Lead {post_id}: Generated city clusters for {len(city_clusters)} cities")

                except Exception as e:
                    logging.error(f"!!! [Category Normalizer] Lead {post_id}: Error generating city clusters: {e}")
                    city_clusters = {}

            if city_clusters:
                combined_json_metadata["cityClusters"] = city_clusters
                logging.info(f"    [Category Normalizer] Lead {post_id}: Added cityClusters array with {len(city_clusters)} cities")
        else: # If not new and not needing refresh, use cached data, but ensure serviceRadiusCities is present
            if cached_data:
                combined_json_metadata = cached_data.copy()
                serp_data = cached_data.get("arbitrageData")
                # Ensure serviceRadiusCities is always present from the initial LLM call
                combined_json_metadata["serviceRadiusCities"] = llm_result.get("serviceRadiusCities", [])
            else:
                # This case should ideally not happen if needs_refresh is true when not in cache
                logging.warning(f"!!! [Category Normalizer] Lead {post_id}: Unexpected state: not new, not needing refresh, but no cached data. Skipping Surfer call.")
                lead['category'] = canonical_category_id
                updated_leads_batch.append(lead)
                continue


        final_required_fields = ["id", "displayName", "aliases", "description", "examplePhrases", "confidence", "lastUpdated", "location", "arbitrageData", "serviceRadiusCities"]
        if not all(field in combined_json_metadata for field in final_required_fields):
            logging.warning(f"!!! [Category Normalizer] Lead {post_id}: Final combined_json_metadata is incomplete. Required: {final_required_fields}. Skipping save/push.")
            lead['category'] = canonical_category_id
            updated_leads_batch.append(lead)
            continue
        if not isinstance(combined_json_metadata.get("aliases"), list) or \
           not isinstance(combined_json_metadata.get("examplePhrases"), list) or \
           not isinstance(combined_json_metadata.get("serviceRadiusCities"), list):
            logging.warning(f"!!! [Category Normalizer] Lead {post_id}: Final combined_json_metadata has malformed list fields. Skipping save/push.")
            lead['category'] = canonical_category_id
            updated_leads_batch.append(lead)
            continue

        try:
            with sqlite3.connect(master_db_path) as con:
                cur = con.cursor()
                cur.execute(
                    "INSERT OR REPLACE INTO canonical_categories (id, json_metadata) VALUES (?, ?)",
                    (category_location_id, json.dumps(combined_json_metadata))
                )
                con.commit()
            _arbitrage_data_cache[category_location_id] = combined_json_metadata
            logging.info(f"    [Category Normalizer] Lead {post_id}: Saved/Updated '{category_location_id}' in local DB and cache.")
        except sqlite3.Error as e:
            logging.error(f"!!! [Category Normalizer] Lead {post_id}: ERROR saving/updating '{category_location_id}' in local DB: {e}")

        lead['category'] = canonical_category_id
        updated_leads_batch.append(lead)

    for lead in updated_leads_batch:
        post_id = lead.get("post_id")
        canonical_category_id = lead.get("category")
        location_slug = lead.get("city", "").lower().replace(" ", "-") + "-" + lead.get("state", "").lower().replace(" ", "-")
        category_location_id = f"{canonical_category_id}/{location_slug}"
        
        combined_json_metadata = _arbitrage_data_cache.get(category_location_id)

        if combined_json_metadata:
            # Create the arbitrageData object for the Firebase payload
            # It should be the entire combined_json_metadata, but without the 'location' field
            # as 'location' is a separate top-level field in the Firebase Callable Function payload.
            arbitrage_data_for_firebase = combined_json_metadata.copy()
            if "location" in arbitrage_data_for_firebase:
                del arbitrage_data_for_firebase["location"]
            
            # The Firebase Callable Function expects the payload to be directly sent,
            # and the SDK wraps it in a 'data' field.
            # So, our Python script should send the structure expected *inside* the 'data' field.
            firebase_payload = {
                "location": combined_json_metadata["location"],
                "arbitrageData": arbitrage_data_for_firebase
            }
            try:
                async with httpx.AsyncClient() as client:
                    # The Firebase Callable Function expects a POST request with the JSON payload
                    response = await client.post(firebase_arbitrage_sync_url, json=firebase_payload, timeout=30.0)
                    if response.is_success:
                        logging.info(f"    [Category Normalizer] Lead {post_id}: Successfully pushed '{category_location_id}' to Firebase.")
                    else:
                        logging.warning(f"!!! [Category Normalizer] Lead {post_id}: FAILED to push '{category_location_id}' to Firebase. Status: {response.status_code}, Body: {response.text}")
            except httpx.RequestError as e:
                logging.error(f"!!! [Category Normalizer] Lead {post_id}: HTTPX error pushing '{category_location_id}' to Firebase: {e}")
            except Exception as e:
                logging.error(f"!!! [Category Normalizer] Lead {post_id}: Unexpected error pushing '{category_location_id}' to Firebase: {e}")
        else:
            logging.warning(f"!!! [Category Normalizer] Lead {post_id}: Could not find '{category_location_id}' in cache for Firebase push. Skipping.")

    return updated_leads_batch
