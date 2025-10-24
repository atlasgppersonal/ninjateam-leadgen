import sqlite3
import os
import json

def drop_table(db_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS processor_queue;")
        conn.commit()
        print(f"Table 'processor_queue' dropped from {db_path}.")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_filename = os.path.join(script_dir, 'config.json')
    
    try:
        with open(config_filename, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"CRITICAL ERROR: '{config_filename}' file not found.")
        exit(1)
    except json.JSONDecodeError:
        print("CRITICAL ERROR: Could not parse config file.")
        exit(1)

    producer_cfg = config['producer_settings']
    db_file = producer_cfg['master_database_file'] # Corrected to master_database_file
    
    drop_table(db_file)
