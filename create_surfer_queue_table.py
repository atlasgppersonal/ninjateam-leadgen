import sqlite3
import json
from datetime import datetime

def create_surfer_queue_table(db_path="master_contacts.db"):
    """
    Creates the surfer_prospector_queue table in the specified SQLite database.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS surfer_prospector_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seed_keywords TEXT NOT NULL,
                customer_domain TEXT NOT NULL,
                avg_job_amount REAL NOT NULL,
                avg_conversion_rate REAL NOT NULL,
                category TEXT NOT NULL,
                state TEXT NOT NULL,
                service_radius_cities TEXT NOT NULL,
                target_pool_size INTEGER NOT NULL,
                min_volume_filter INTEGER NOT NULL,
                country TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                error_message TEXT
            )
        """)
        conn.commit()
        print(f"Table 'surfer_prospector_queue' created or already exists in {db_path}")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_surfer_queue_table()
