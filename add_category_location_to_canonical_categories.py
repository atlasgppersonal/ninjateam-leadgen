import sqlite3
import os
import json
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set to DEBUG to capture all messages

# Create a file handler for this module's logs
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_migration.log")
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

logger.info(f"DB Migration logging to {log_file_path}")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)

MASTER_DB_PATH = CONFIG['producer_settings']['master_database_file']

def migrate_canonical_categories_table(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        logger.info(f"Starting migration for canonical_categories table in {db_file}...")

        # 1. Check if the old 'id' column exists (indicating old schema)
        cursor.execute("PRAGMA table_info(canonical_categories)")
        columns = cursor.fetchall()
        old_schema_exists = any(col[1] == 'id' for col in columns)

        if not old_schema_exists:
            logger.info("canonical_categories table already has the new schema or does not exist with old schema. No migration needed.")
            return

        # 2. Rename the old table
        cursor.execute("ALTER TABLE canonical_categories RENAME TO canonical_categories_old")
        logger.info("Renamed canonical_categories to canonical_categories_old.")

        # 3. Create the new table with category and location columns
        cursor.execute("""
            CREATE TABLE canonical_categories (
                category TEXT NOT NULL,
                location TEXT NOT NULL,
                json_metadata TEXT,
                PRIMARY KEY (category, location)
            );
        """)
        logger.info("Created new canonical_categories table with (category, location) as PRIMARY KEY.")

        # 4. Migrate data from old table to new table
        cursor.execute("SELECT id, json_metadata FROM canonical_categories_old")
        old_entries = cursor.fetchall()

        migrated_count = 0
        for old_id, json_data in old_entries:
            try:
                # Assuming old_id was in format "category/location_slug"
                parts = old_id.split('/')
                if len(parts) == 2:
                    category = parts[0]
                    location = parts[1]
                    cursor.execute(
                        "INSERT OR REPLACE INTO canonical_categories (category, location, json_metadata) VALUES (?, ?, ?)",
                        (category, location, json_data)
                    )
                    migrated_count += 1
                else:
                    logger.warning(f"Skipping migration for old_id '{old_id}' due to unexpected format.")
            except Exception as e:
                logger.error(f"Error migrating entry with old_id '{old_id}': {e}")

        logger.info(f"Migrated {migrated_count} entries to the new canonical_categories table.")

        # 5. Drop the old table
        cursor.execute("DROP TABLE canonical_categories_old")
        logger.info("Dropped canonical_categories_old table.")

        conn.commit()
        logger.info("Database migration completed successfully.")

    except sqlite3.Error as e:
        logger.error(f"Database error during migration for {db_file}: {e}")
        if conn:
            conn.rollback() # Rollback changes on error
    except Exception as e:
        logger.error(f"Unexpected error during migration for {db_file}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    master_db_file = os.path.join(script_dir, MASTER_DB_PATH)
    
    logger.info(f"Attempting to migrate schema for {master_db_file}...")
    migrate_canonical_categories_table(master_db_file)
    logger.info("Migration script finished.")
