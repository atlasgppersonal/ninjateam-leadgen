import asyncio
import json
import os
import sys
import sqlite3
from datetime import datetime, timedelta
import google.generativeai as genai
import httpx

# Add the directory containing category_normalizer.py and surfer_prospector_module.py to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from category_normalizer import normalize_business_category
from surfer_prospector_module import run_prospecting_async # Import the function

# --- Logging Setup (similar to contact-extractor.py for consistency) ---
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_category_arbitrage_log.txt")
class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.__stdout__  # original console
        self.log = open(filename, "w", buffering=1)

    def write(self, message):
        self.terminal.write(message)   # write to console
        self.log.write(message)        # also write to file

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(log_file_path)
sys.stderr = sys.stdout
print(f"--- Test Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
print(f"--- All test output redirected to {log_file_path} ---")

async def main_test():
    print("\n--- Starting test_category_arbitrage.py ---")

    # --- 1. Load Configuration ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_filename = os.path.join(script_dir, 'config.json')
    
    from category_normalizer import _arbitrage_data_cache, _cache_initialized

    print(f"    [Test] Attempting to load config from: {config_filename}")
    try:
        with open(config_filename, 'r') as f:
            config = json.load(f)
        print("    [Test] Config loaded successfully.")
    except FileNotFoundError:
        print(f"!!! [Test] CRITICAL ERROR: '{config_filename}' file not found. Exiting.")
        return
    except json.JSONDecodeError:
        print("!!! [Test] CRITICAL ERROR: Could not parse config file. Ensure it's valid JSON. Exiting.")
        return

    producer_cfg = config['producer_settings']
    global_cfg = config['global_settings']

    # Configure LLM
    genai.configure(api_key=global_cfg['google_api_key'])
    llm_model_instance = genai.GenerativeModel(global_cfg['llm_model'])
    print(f"    [Test] LLM Model configured: {global_cfg['llm_model']}")

    master_db_path = producer_cfg['master_database_file']
    firebase_arbitrage_sync_url = producer_cfg['firebase_category_arbitrage_sync_url']
    category_arbitrage_update_interval_days = producer_cfg['category_arbitrage_update_interval_days']

    print(f"    [Test] Master DB Path: {master_db_path}")
    print(f"    [Test] Firebase Arbitrage Sync URL: {firebase_arbitrage_sync_url}")
    print(f"    [Test] Arbitrage Update Interval: {category_arbitrage_update_interval_days} days")

    # --- Ensure master_contacts.db and canonical_categories table exist ---
    print(f"    [Test] Ensuring master_contacts.db and canonical_categories table exist...")
    try:
        conn = sqlite3.connect(master_db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS canonical_categories (
                id TEXT PRIMARY KEY,
                json_metadata TEXT
            )
        ''')
        conn.commit()
        conn.close()
        print("    [Test] canonical_categories table checked/created successfully.")
    except sqlite3.Error as e:
        print(f"!!! [Test] ERROR ensuring canonical_categories table: {e}")
        return

    # --- Define Test Data for normalize_business_category call ---
    # This is the raw_contact provided by the user, converted to JSON
    raw_contact = {
        "body_text": "QR Code Link to This Post Your Last-Minute Go-To: Movers, Laborers, Long-Distance Moving, Junk Removal, Hauling & Cleaning Services! Looking for reliable, professional movers? Strongman Mover is here for all your moving needs – big or small, local or long-distance. We handle everything with speed, care, and reliability! Why Choose Strongman Mover? Experienced Crews - Over 3 years of experience per crew ensures a smooth move. Any Move, Any Distance - Residential or commercial, local or long-distance – we do it all! Specialty Item Experts - Safes, pianos, hot tubs? No problem! We move large and delicate items with care. Last-Minute & Same-Day Service - Need to move now? We're available anytime, anywhere, even for last-minute jobs. Flexible Scheduling - No extra charges for date or time changes. Our Services: (For any move - Big or Small) Residential Moves: Apartments, homes, condos. Commercial Moves: Offices, businesses. Labor Only: Packing, loading, and unloading. Junk Removal: Clearing out unwanted items Residential & Commercial Cleaning: Thorough cleaning and sanitization for homes and businesses, perfect for pre-move-in or post-move-out. Availability & Contact: • 7 Days a Week: 8 AM - 6 PM • After-Hours/Emergencies: Call us directly at (503) 433-5602. Simple Terms of Service:: • Deposit Policy: No refunds or cancellations once processed. Reschedule requests are based on availability. • Guaranteed Completion: Your move will be completed within 72 hours of the estimated time of arrival. Call/Text Now: (503) 433-5602 Visit Us Online: www.STRONGMANMOVER.com STRONGMAN MOVER - Your Trusted Partner for a Stress-Free Move! Thank you for choosing STRONGMAN MOVER for your Movers and Laborers!",
        "business_name": "Strongman Mover",
        "cant_text": 1,
        "category": "Moving",
        "city": "portland",
        "email": None,
        "image_hash": "04Q03d",
        "name": None,
        "name_confidence": 0,
        "original_category": "labor & moving",
        "phone": "5034335602",
        "post_id": "7881623662",
        "services_rendered": [
            "Residential Moves: Apartments, homes, condos.",
            "Commercial Moves: Offices, businesses.",
            "Labor Only: Packing, loading, and unloading.",
            "Junk Removal: Clearing out unwanted items",
            "Residential & Commercial Cleaning: Thorough cleaning and sanitization for homes and businesses, perfect for pre-move-in or post-move-out."
        ],
        "state": "OR",
        "url": "https://portland.craigslist.org/lbs/d/portland-your-last-minute-local-move/7881623662.html",
        "website_url": "https://www.STRONGMANMOVER.com"
    }
    leads_batch = [raw_contact] # Wrap the single contact in a list for leads_batch

    print(f"    [Test] Preparing to call normalize_business_category with a batch of {len(leads_batch)} leads.")

    # --- Call normalize_business_category ---
    print("\n--- Calling normalize_business_category ---")
    normalized_leads = await normalize_business_category(
        leads_batch=leads_batch,
        llm_model=llm_model_instance,
        master_db_path=master_db_path,
        firebase_arbitrage_sync_url=firebase_arbitrage_sync_url,
        category_arbitrage_update_interval_days=category_arbitrage_update_interval_days
    )
    print("\n--- normalize_business_category completed ---")

    print("\n--- Normalized Leads Results ---")
    print(json.dumps(normalized_leads, indent=2))
    print("--- End of Normalized Leads Results ---")

    # --- Retrieve and print arbitrageData from cache for the test lead ---
    if normalized_leads and normalized_leads[0].get("category") and normalized_leads[0].get("city") and normalized_leads[0].get("state"):
        canonical_category_id = normalized_leads[0]["category"]
        location_slug = normalized_leads[0]["city"].lower().replace(" ", "-") + "-" + normalized_leads[0]["state"].lower().replace(" ", "-")
        category_location_id = f"{canonical_category_id}/{location_slug}"
        
        from category_normalizer import _arbitrage_data_cache
        cached_arbitrage_data = _arbitrage_data_cache.get(category_location_id)

        print("\n--- Cached Arbitrage Data (including SERP) ---")
        if cached_arbitrage_data:
            print(json.dumps(cached_arbitrage_data, indent=2))
        else:
            print(f"No arbitrage data found in cache for {category_location_id}")
        print("--- End of Cached Arbitrage Data ---")


    print("\n--- Test Script Finished ---")

if __name__ == "__main__":
    asyncio.run(main_test())
