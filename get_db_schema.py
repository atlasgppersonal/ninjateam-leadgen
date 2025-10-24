import sqlite3
import sys

db_name = sys.argv[1]

try:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables in {db_name}:")
    for table_name in tables:
        table_name = table_name[0]
        print(f"\n--- Schema for table: {table_name} ---")
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        for col in schema:
            print(f"  {col[1]} ({col[2]})")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
