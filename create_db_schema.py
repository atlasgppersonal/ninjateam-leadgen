import sqlite3
import os

def create_master_db_schema(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create contacts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                phone TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                last_sent TEXT,
                source_url TEXT,
                image_hash TEXT,
                business_name TEXT,
                category TEXT,
                services_rendered TEXT,
                status TEXT,
                city TEXT,
                lead_data_json TEXT
            );
        """)
        print(f"Table 'contacts' created or already exists in {db_file}.")

        # Create processor_queue table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processor_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                number_of_leads_to_process INTEGER NOT NULL,
                status TEXT,
                request_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print(f"Table 'processor_queue' created or already exists in {db_file}.")

        conn.commit()
        print(f"Database schema for {db_file} updated successfully.")

    except sqlite3.Error as e:
        print(f"Database error for {db_file}: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    master_db_file = os.path.join(script_dir, "master_contacts.db")
    
    print(f"Attempting to create/update schema for {master_db_file}...")
    create_master_db_schema(master_db_file)
    print("Schema creation script finished.")
