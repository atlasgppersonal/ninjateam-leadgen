import sqlite3
import sys

db_file = "master_contacts.db"

def add_column_if_not_exists(cursor, table_name, column_name, column_type):
    try:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")
        print(f"Column '{column_name}' added to table '{table_name}'.")
    except sqlite3.OperationalError as e:
        if f"duplicate column name: {column_name}" in str(e):
            print(f"Column '{column_name}' already exists in table '{table_name}'. Skipping.")
        else:
            print(f"Error adding column '{column_name}' to table '{table_name}': {e}")
            sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

def create_processor_queue_table(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processor_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                number_of_leads_to_process INTEGER NOT NULL,
                request_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("Table 'processor_queue' created or already exists.")
    except Exception as e:
        print(f"Error creating table 'processor_queue': {e}")
        sys.exit(1)

if __name__ == "__main__":
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Add columns to contacts table
        add_column_if_not_exists(cursor, "contacts", "status", "TEXT")
        add_column_if_not_exists(cursor, "contacts", "city", "TEXT")
        add_column_if_not_exists(cursor, "contacts", "lead_data_json", "TEXT")

        # Create processor_queue table
        create_processor_queue_table(cursor)

        conn.commit()
        print("Database schema updated successfully.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
