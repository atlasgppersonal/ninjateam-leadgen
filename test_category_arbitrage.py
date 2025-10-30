import asyncio
import json
import os
import sys
import sqlite3
import logging
from datetime import datetime, timedelta
import google.generativeai as genai
import httpx
import subprocess
import psutil # For process checking

# Add the directory containing category_normalizer.py and surfer_prospector_module.py to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from category_normalizer import normalize_business_category

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set to DEBUG to capture all messages

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_category_arbitrage_log.txt")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG) # Log all messages to file

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

# Optionally, add a stream handler to also output to console (e.g., for INFO and above)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info(f"--- Test Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
logger.info(f"--- All test output redirected to {log_file_path} ---")

CONSUMER_SCRIPT_NAME = "surfer_queue_consumer.py"
LOCK_FILE_NAME = "surfer_consumer.lock" # Must match the consumer's lock file

def is_consumer_running() -> bool:
    """Checks if the surfer_queue_consumer.py script is currently running."""
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = process.cmdline()
            if CONSUMER_SCRIPT_NAME in " ".join(cmdline):
                # Further check for the lock file to be more robust
                if os.path.exists(LOCK_FILE_NAME):
                    with open(LOCK_FILE_NAME, 'r') as f:
                        lock_pid = int(f.read().strip())
                    if lock_pid == process.pid:
                        logger.info(f"    [Test] Consumer script {CONSUMER_SCRIPT_NAME} is running with PID {process.pid}.")
                        return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    logger.info(f"    [Test] Consumer script {CONSUMER_SCRIPT_NAME} is NOT running.")
    return False

def launch_consumer_script():
    """Launches the surfer_queue_consumer.py script as a detached background process."""
    if is_consumer_running():
        logger.info(f"    [Test] Consumer script {CONSUMER_SCRIPT_NAME} is already running. Skipping launch.")
        return

    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONSUMER_SCRIPT_NAME)
    if not os.path.exists(script_path):
        logger.error(f"!!! [Test] ERROR: Consumer script not found at {script_path}. Cannot launch.")
        return

    logger.info(f"    [Test] Launching consumer script: {CONSUMER_SCRIPT_NAME}...")

    try:
        # Inherit and ensure venv environment variables
        env = os.environ.copy()
        # Determine the virtual environment path dynamically
        # Assumes .venv is in the project root, or script is run from within venv
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
        venv_path = os.path.join(project_root, ".venv")
        
        # Fallback if .venv is not in the project root, try current script's directory
        if not os.path.exists(venv_path):
            venv_path = os.path.join(script_dir, ".venv")

        if os.path.exists(venv_path):
            env["VIRTUAL_ENV"] = venv_path
            # Add venv's bin/Scripts directory to PATH
            if sys.platform == "win32":
                env["PATH"] = os.path.join(venv_path, "Scripts") + os.pathsep + env["PATH"]
            else: # Unix-like systems
                env["PATH"] = os.path.join(venv_path, "bin") + os.pathsep + env["PATH"]
            logger.info(f"    [Test] Virtual environment detected at {venv_path}. Setting VIRTUAL_ENV and PATH.")
        else:
            logger.warning(f"    [Test] Virtual environment not found at {venv_path}. Proceeding without explicit venv activation for subprocess.")

        # Always use current interpreter from this environment
        python_executable = sys.executable

        if sys.platform == "win32":
            subprocess.Popen(
                [python_executable, script_path],
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env  # Pass the virtualenv environment
            )
        else:
            subprocess.Popen(
                [python_executable, script_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid,
                env=env  # Pass the virtualenv environment
            )
        logger.info(f"    [Test] Consumer script {CONSUMER_SCRIPT_NAME} launched successfully in background.")
        time.sleep(2) # Give it a moment to start up and create its lock file
    except Exception as e:
        logger.error(f"!!! [Test] ERROR launching consumer script: {e}")

async def main_test():
    logger.info("\n--- Starting test_category_arbitrage.py ---")

    # --- Ensure consumer is running ---
    launch_consumer_script()
    
    # --- 1. Load Configuration ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_filename = os.path.join(script_dir, 'config.json')
    
    from category_normalizer import _arbitrage_data_cache, _cache_initialized

    logger.info(f"    [Test] Attempting to load config from: {config_filename}")
    try:
        with open(config_filename, 'r') as f:
            config = json.load(f)
        logger.info("    [Test] Config loaded successfully.")
    except FileNotFoundError:
        logger.critical(f"!!! [Test] CRITICAL ERROR: '{config_filename}' file not found. Exiting.")
        return
    except json.JSONDecodeError:
        logger.critical("!!! [Test] CRITICAL ERROR: Could not parse config file. Ensure it's valid JSON. Exiting.")
        return

    producer_cfg = config['producer_settings']
    global_cfg = config['global_settings']

    # Configure LLM
    genai.configure(api_key=global_cfg['google_api_key'])
    llm_model_instance = genai.GenerativeModel(global_cfg['llm_model'])
    logger.info(f"    [Test] LLM Model configured: {global_cfg['llm_model']}")

    master_db_path = producer_cfg['master_database_file']
    firebase_arbitrage_sync_url = producer_cfg['firebase_category_arbitrage_sync_url']
    category_arbitrage_update_interval_days = producer_cfg['category_arbitrage_update_interval_days']

    logger.info(f"    [Test] Master DB Path: {master_db_path}")
    logger.info(f"    [Test] Firebase Arbitrage Sync URL: {firebase_arbitrage_sync_url}")
    logger.info(f"    [Test] Arbitrage Update Interval: {category_arbitrage_update_interval_days} days")

    # --- Ensure master_contacts.db and canonical_categories table exist ---
    logger.info(f"    [Test] Ensuring master_contacts.db and canonical_categories table exist...")
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
        logger.info("    [Test] canonical_categories table checked/created successfully.")
    except sqlite3.Error as e:
        logger.error(f"!!! [Test] ERROR ensuring canonical_categories table: {e}")
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

    logger.info(f"    [Test] Preparing to call normalize_business_category with a batch of {len(leads_batch)} leads.")

    # --- Call normalize_business_category ---
    logger.info("\n--- Calling normalize_business_category ---")
    normalized_leads = await normalize_business_category(
        leads_batch=leads_batch,
        llm_model=llm_model_instance,
        master_db_path=master_db_path,
        firebase_arbitrage_sync_url=firebase_arbitrage_sync_url,
        category_arbitrage_update_interval_days=category_arbitrage_update_interval_days
    )
    logger.info("\n--- normalize_business_category completed ---")

    logger.info("\n--- Normalized Leads Results ---")
    logger.info(json.dumps(normalized_leads, indent=2))
    logger.info("--- End of Normalized Leads Results ---")

    # --- Retrieve and print arbitrageData from cache for the test lead ---
    if normalized_leads and normalized_leads[0].get("category") and normalized_leads[0].get("city") and normalized_leads[0].get("state"):
        canonical_category_id = normalized_leads[0]["category"]
        location_slug = normalized_leads[0]["city"].lower().replace(" ", "-") + "-" + normalized_leads[0]["state"].lower().replace(" ", "-")
        category_location_id = f"{canonical_category_id}/{location_slug}"
        
        from category_normalizer import _arbitrage_data_cache
        cached_arbitrage_data = _arbitrage_data_cache.get(category_location_id)

        logger.info("\n--- Cached Arbitrage Data (including SERP) ---")
        if cached_arbitrage_data:
            logger.info(json.dumps(cached_arbitrage_data, indent=2))
        else:
            logger.info(f"No arbitrage data found in cache for {category_location_id}")
        logger.info("--- End of Cached Arbitrage Data ---")


    logger.info("\n--- Test Script Finished ---")

if __name__ == "__main__":
    asyncio.run(main_test())
