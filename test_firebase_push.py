import httpx
import asyncio
import json
import logging
import sqlite3

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

FIREBASE_FUNCTION_URL = "https://us-central1-fourth-outpost-470409-u3.cloudfunctions.net/saveArbitrageData"
MASTER_DB_PATH = "master_contacts.db"
TARGET_CATEGORY_LOCATION_ID = "movers/portland-or"

async def test_firebase_push():
    logger.info("Starting Firebase push test...")

    # 1. Retrieve data from master_contacts.db
    combined_json_metadata = None
    try:
        with sqlite3.connect(MASTER_DB_PATH) as con:
            cur = con.cursor()
            cur.execute(
                "SELECT json_metadata FROM canonical_categories WHERE id = ?",
                (TARGET_CATEGORY_LOCATION_ID,)
            )
            result = cur.fetchone()
            if result:
                combined_json_metadata = json.loads(result[0])
                logger.info(f"Successfully retrieved data for '{TARGET_CATEGORY_LOCATION_ID}' from DB.")
            else:
                logger.error(f"Data for '{TARGET_CATEGORY_LOCATION_ID}' not found in DB. Please ensure it exists.")
                return
    except sqlite3.Error as e:
        logger.error(f"Error accessing database: {e}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from DB for '{TARGET_CATEGORY_LOCATION_ID}': {e}")
        return

    # 2. Construct the payload by wrapping the combined_json_metadata in a "data" field,
    # as expected by the Firebase Callable Function.
    payload_to_send = {
        "data": combined_json_metadata
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(FIREBASE_FUNCTION_URL, json=payload_to_send)

        logger.info(f"Firebase Function URL: {FIREBASE_FUNCTION_URL}")
        logger.info(f"Payload sent: {json.dumps(payload_to_send, indent=2)}")
        logger.info(f"Response Status Code: {response.status_code}")
        logger.info(f"Response Body: {response.text}")

        if response.is_success:
            logger.info("Firebase push test successful!")
        else:
            logger.error(f"Firebase push test FAILED. Status: {response.status_code}, Body: {response.text}")

    except httpx.RequestError as e:
        logger.error(f"HTTPX Request Error during Firebase push test: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during Firebase push test: {e}")

if __name__ == "__main__":
    asyncio.run(test_firebase_push())
