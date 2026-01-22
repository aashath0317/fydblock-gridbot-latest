import sqlite3
import os

db_path = "bot_data.db"
if not os.path.exists(db_path):
    print(f"Error: Database file not found at {os.path.abspath(db_path)}")
    exit(1)

print(f"Opening database at: {os.path.abspath(db_path)}")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1. Check Tables
print("\n--- Tables ---")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for t in tables:
    print(t[0])

# 2. Check Bot Balances
print("\n--- Bot Balances ---")
try:
    cursor.execute("SELECT * FROM bot_balances")
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    print(f"Columns: {columns}")
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error reading bot_balances: {e}")

# 3. Check Active Bots
print("\n--- Active Bots ---")
try:
    cursor.execute("SELECT bot_id, status, config_json FROM bots")
    rows = cursor.fetchall()
    for row in rows:
        print(f"Bot {row[0]}: {row[1]}")
except Exception as e:
    print(f"Error reading bots: {e}")

conn.close()
