import sqlite3
import os

db_path = "bot_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

ids = [258, 261]
print(f"Checking balances for bots: {ids}")

cursor.execute(f"SELECT * FROM bot_balances WHERE bot_id IN ({','.join(map(str, ids))})")
rows = cursor.fetchall()
columns = [description[0] for description in cursor.description]
print(f"Columns: {columns}")
for row in rows:
    print(dict(zip(columns, row)))

conn.close()
