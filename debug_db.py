import sqlite3
import json

master_db_path = "master_contacts.db"

try:
    conn = sqlite3.connect(master_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT id, json_metadata FROM canonical_categories")
    rows = cursor.fetchall()

    if not rows:
        print("No entries found in canonical_categories table.")
    else:
        for row_id, json_data in rows:
            print(f"ID: {row_id}")
            try:
                metadata = json.loads(json_data)
                print(f"  JSON Metadata: {json.dumps(metadata, indent=2)}")
            except json.JSONDecodeError:
                print(f"  ERROR: Could not decode JSON for ID: {row_id}")
            print("-" * 20)

except sqlite3.Error as e:
    print(f"ERROR accessing database: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
