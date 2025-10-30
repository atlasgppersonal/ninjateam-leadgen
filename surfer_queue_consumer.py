import asyncio
import sqlite3
import json
import time
import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
import httpx # Added for Firebase push
import google.generativeai as genai # Added for LLM initialization

# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set to DEBUG to capture all messages

# Create a file handler for this module's logs
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "surfer_queue_consumer.log")
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

logger.info(f"Surfer Queue Consumer logging to {log_file_path}")

# Load config for LLM model name and Firebase URL
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)

LLM_MODEL_NAME = CONFIG['global_settings']['llm_model']
MASTER_DB_PATH = CONFIG['producer_settings']['master_database_file']
FIREBASE_ARBITRAGE_SYNC_URL = CONFIG['producer_settings']['firebase_category_arbitrage_sync_url']
CATEGORY_ARBITRAGE_UPDATE_INTERVAL_DAYS = CONFIG['producer_settings']['category_arbitrage_update_interval_days']


# Import necessary functions from surfer_prospector_module and scoring_utils
from surfer_prospector_module import run_prospecting_async, generate_city_specific_clusters
from scoring_utils import _normalize_keyword

# Placeholder for LLM model initialization (will be done once in main)
llm_model_instance = None

# Module-level cache for canonical categories and their associated arbitrage data
_canonical_categories_cache: Dict[str, Dict[str, Any]] = {}
_cache_initialized = False

async def _initialize_canonical_categories_cache(master_db_path: str):
    """
    Loads all category-location data from the canonical_categories table into the in-memory cache.
    This function should be called only once per session or refreshed periodically.
    """
    global _canonical_categories_cache, _cache_initialized
    logger.debug("Initializing canonical categories cache from DB...")
    try:
        with sqlite3.connect(master_db_path) as con:
            cur = con.cursor()
            cur.execute("SELECT id, json_metadata FROM canonical_categories")
            for row_id, json_data in cur.fetchall():
                try:
                    metadata = json.loads(json_data)
                    _canonical_categories_cache[row_id] = metadata
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for ID: {row_id}. Skipping entry.")
        logger.debug(f"Loaded {len(_canonical_categories_cache)} entries into canonical categories cache.")
        _cache_initialized = True
    except sqlite3.Error as e:
        logger.error(f"Error initializing canonical categories cache from DB: {e}")
        _canonical_categories_cache = {} # Ensure cache is empty on error
        _cache_initialized = False # Mark as not initialized


async def process_queue_item(task_id: int, task_data: Dict[str, Any], llm_model: Any):
    """
    Processes a single item from the surfer_prospector_queue.
    Executes run_prospecting_async, generates city clusters, updates DB, and pushes to Firebase.
    """
    conn = None
    try:
        conn = sqlite3.connect(MASTER_DB_PATH)
        cursor = conn.cursor()

        # Update status to 'processing'
        cursor.execute("UPDATE surfer_prospector_queue SET status = 'processing', processed_at = ? WHERE id = ?",
                       (datetime.now(timezone.utc).isoformat(), task_id))
        conn.commit()
        logger.info(f"Task {task_id}: Status updated to 'processing'.")
        logger.debug(f"Task {task_id}: Processing task data: {task_data}")

        canonical_category_id = task_data['category']
        location_slug = f"{task_data['state'].lower()}-{task_data['country'].lower()}"
        category_location_id = f"{canonical_category_id}/{location_slug}"

        # --- Cache Check Logic ---
        await _initialize_canonical_categories_cache(MASTER_DB_PATH) # Ensure cache is up-to-date

        cached_data = _canonical_categories_cache.get(category_location_id)
        needs_prospecting = True

        if cached_data:
            last_updated_str = cached_data.get("lastUpdated")
            if last_updated_str:
                last_updated_dt = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00')).astimezone(timezone.utc)
                if datetime.now(timezone.utc) - last_updated_dt < timedelta(days=CATEGORY_ARBITRAGE_UPDATE_INTERVAL_DAYS):
                    needs_prospecting = False
                    logger.info(f"Task {task_id}: Category '{category_location_id}' is up-to-date in cache. Skipping prospecting.")
                else:
                    logger.info(f"Task {task_id}: Category '{category_location_id}' needs refresh (older than {CATEGORY_ARBITRAGE_UPDATE_INTERVAL_DAYS} days).")
            else:
                logger.info(f"Task {task_id}: Category '{category_location_id}' has no 'lastUpdated' timestamp. Proceeding with prospecting.")
        else:
            logger.info(f"Task {task_id}: Category '{category_location_id}' not found in cache. Proceeding with prospecting.")

        if not needs_prospecting:
            cursor.execute("UPDATE surfer_prospector_queue SET status = 'completed' WHERE id = ?", (task_id,))
            conn.commit()
            logger.info(f"Task {task_id}: Status updated to 'completed' (skipped due to cache).")
            return # Exit early if no prospecting is needed
        # --- End Cache Check Logic ---


        # Deserialize parameters
        seed_keywords = json.loads(task_data['seed_keywords'])
        service_radius_cities = json.loads(task_data['service_radius_cities'])
        logger.debug(f"Task {task_id}: Deserialized seed_keywords: {seed_keywords[:5]}...")
        logger.debug(f"Task {task_id}: Deserialized service_radius_cities: {service_radius_cities[:5]}...")
        
        # Prepare parameters for run_prospecting_async
        prospecting_params = {
            "seed_keywords": seed_keywords,
            "customer_domain": task_data['customer_domain'],
            "avg_job_amount": task_data['avg_job_amount'],
            "avg_conversion_rate": task_data['avg_conversion_rate'],
            "llm_model": llm_model, # Use the initialized LLM model
            "category": task_data['category'],
            "state": task_data['state'],
            "service_radius_cities": service_radius_cities,
            "target_pool_size": task_data['target_pool_size'],
            "min_volume_filter": task_data['min_volume_filter'],
            "country": task_data['country']
        }

        logger.info(f"Task {task_id}: Executing run_prospecting_async for category '{task_data['category']}'.")
        serp_data = await run_prospecting_async(**prospecting_params)
        logger.info(f"Task {task_id}: run_prospecting_async completed.")
        logger.debug(f"Task {task_id}: Received SERP data: {json.dumps(serp_data, indent=2)}")

        # Generate city-specific clusters
        city_clusters = {}
        if service_radius_cities and serp_data and isinstance(serp_data, dict) and "scored_keywords" in serp_data:
            try:
                logger.info(f"Task {task_id}: Generating city-specific clusters...")
                master_keyword_pool_normalized = {}
                for keyword_data in serp_data["scored_keywords"]:
                    keyword = keyword_data.get("keyword")
                    if keyword:
                        normalized_keyword = _normalize_keyword(keyword)
                        master_keyword_pool_normalized[normalized_keyword] = keyword_data
                logger.debug(f"Task {task_id}: Master keyword pool normalized: {len(master_keyword_pool_normalized)} entries.")

                temp_city_keywords_map = {city_name: [] for city_name in service_radius_cities}
                normalized_service_radius_cities = [_normalize_keyword(city) for city in service_radius_cities]
                logger.debug(f"Task {task_id}: Normalized service radius cities for clustering: {normalized_service_radius_cities[:5]}...")

                for normalized_kw, kw_data in master_keyword_pool_normalized.items():
                    for city_name, normalized_city in zip(service_radius_cities, normalized_service_radius_cities):
                        if normalized_city in normalized_kw:
                            temp_city_keywords_map[city_name].append(normalized_kw)
                            break
                logger.debug(f"Task {task_id}: Temporary city keywords map created.")

                city_clusters = await generate_city_specific_clusters(
                    master_keyword_pool_normalized,
                    temp_city_keywords_map,
                    task_data['customer_domain'],
                    task_data['avg_job_amount'],
                    task_data['avg_conversion_rate'],
                    llm_model
                )
                logger.info(f"Task {task_id}: Generated city clusters for {len(city_clusters)} cities.")
                logger.debug(f"Task {task_id}: City clusters generated: {json.dumps(city_clusters, indent=2)}")
            except Exception as e:
                logger.error(f"Task {task_id}: Error generating city clusters: {e}")
                city_clusters = {"error": str(e)}
        else:
            logger.warning(f"Task {task_id}: Skipping city cluster generation due to missing service_radius_cities or scored_keywords in SERP data.")

        # Prepare combined_json_metadata for master_contacts.db
        current_utc_timestamp = datetime.now(timezone.utc).isoformat().replace('Z', '+00:00')
        combined_json_metadata = {
            "id": task_data['category'],
            "displayName": task_data['category'], # Placeholder, ideally from initial LLM classification
            "aliases": [], # Placeholder
            "description": "", # Placeholder
            "confidence": 1.0, # Placeholder
            "avgJobAmount": task_data['avg_job_amount'],
            "suggestedAt": current_utc_timestamp,
            "createdBy": "surfer_consumer",
            "lastUpdated": current_utc_timestamp,
            "location": f"{task_data['category']}/{task_data['state'].lower()}", # Simplified location slug
            "arbitrageData": serp_data,
            "serviceRadiusCities": service_radius_cities,
            "cityClusters": city_clusters
        }
        if serp_data and "short_term_strategy" in serp_data:
            combined_json_metadata["short_term_strategy"] = serp_data["short_term_strategy"]
        logger.debug(f"Task {task_id}: Combined JSON metadata prepared: {json.dumps(combined_json_metadata, indent=2)}")

        # Update canonical_categories table
        cursor.execute(
            "INSERT OR REPLACE INTO canonical_categories (id, json_metadata) VALUES (?, ?)",
            (category_location_id, json.dumps(combined_json_metadata))
        )
        conn.commit()
        logger.info(f"Task {task_id}: Updated canonical_categories for '{category_location_id}'.")

        # Push to Firebase
        firebase_payload = {
            "location": combined_json_metadata["location"],
            "arbitrageData": combined_json_metadata
        }
        try:
            logger.info(f"Task {task_id}: Pushing '{category_location_id}' to Firebase...")
            async with httpx.AsyncClient() as client:
                response = await client.post(FIREBASE_ARBITRAGE_SYNC_URL, json=firebase_payload, timeout=30.0)
                if response.is_success:
                    logger.info(f"Task {task_id}: Successfully pushed '{category_location_id}' to Firebase.")
                else:
                    logger.warning(f"Task {task_id}: FAILED to push '{category_location_id}' to Firebase. Status: {response.status_code}, Body: {response.text}")
                    logger.debug(f"Task {task_id}: Firebase response body: {response.text}")
        except httpx.RequestError as e:
            logger.error(f"Task {task_id}: HTTPX error pushing '{category_location_id}' to Firebase: {e}")
        except Exception as e:
            logger.error(f"Task {task_id}: Unexpected error pushing '{category_location_id}' to Firebase: {e}")

        # Update status to 'completed'
        cursor.execute("UPDATE surfer_prospector_queue SET status = 'completed' WHERE id = ?", (task_id,))
        conn.commit()
        logger.info(f"Task {task_id}: Status updated to 'completed'.")

    except Exception as e:
        logger.error(f"Task {task_id}: An error occurred during processing: {e}")
        if conn:
            cursor.execute("UPDATE surfer_prospector_queue SET status = 'failed', error_message = ? WHERE id = ?",
                           (str(e), task_id))
            conn.commit()
    finally:
        if conn:
            conn.close()

async def main():
    global llm_model_instance
    # Initialize LLM model once
    try:
        genai.configure(api_key=CONFIG['global_settings']['google_api_key'])
        llm_model_instance = genai.GenerativeModel(LLM_MODEL_NAME)
        logger.info(f"LLM model '{LLM_MODEL_NAME}' initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize LLM model '{LLM_MODEL_NAME}': {e}")
        logger.error("Exiting consumer due to LLM initialization failure.")
        return

    logger.info("Surfer Queue Consumer started. Waiting for tasks...")
    while True:
        conn = None
        try:
            conn = sqlite3.connect(MASTER_DB_PATH)
            cursor = conn.cursor()

            cursor.execute("SELECT id, seed_keywords, customer_domain, avg_job_amount, avg_conversion_rate, category, state, service_radius_cities, target_pool_size, min_volume_filter, country FROM surfer_prospector_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
            task = cursor.fetchone()

            if task:
                task_id, seed_keywords, customer_domain, avg_job_amount, avg_conversion_rate, category, state, service_radius_cities, target_pool_size, min_volume_filter, country = task
                task_data = {
                    "seed_keywords": seed_keywords,
                    "customer_domain": customer_domain,
                    "avg_job_amount": avg_job_amount,
                    "avg_conversion_rate": avg_conversion_rate,
                    "category": category,
                    "state": state,
                    "service_radius_cities": service_radius_cities,
                    "target_pool_size": target_pool_size,
                    "min_volume_filter": min_volume_filter,
                    "country": country
                }
                logger.info(f"Found pending task {task_id} for category '{category}'.")
                await process_queue_item(task_id, task_data, llm_model_instance) # Pass llm_model_instance
            else:
                logger.debug("No pending tasks found. Waiting...")
            
            await asyncio.sleep(10) # Poll every 10 seconds

        except sqlite3.Error as e:
            logger.error(f"Database error in main loop: {e}")
            await asyncio.sleep(30) # Longer pause on DB error
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            await asyncio.sleep(30)
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    asyncio.run(main())
