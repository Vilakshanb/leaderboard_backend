
import os
import pymongo
from dotenv import load_dotenv

load_dotenv(dotenv_path="backend/local.settings.json")

# Explicit connection string as fallback/primary to ensure we hit the same DB
conn_str = os.getenv("MONGODB_CONNECTION_STRING")
client = pymongo.MongoClient(conn_str)
db = client["PLI_Leaderboard_v2"]

print(f"Connected to {db.name}")

query = {"$or": [
    {"RM_Name": {"$regex": "Test", "$options": "i"}},
    {"NameOfEmp": {"$regex": "Test", "$options": "i"}},
    {"name": {"$regex": "Test", "$options": "i"}},
    {"rm_name": {"$regex": "Test", "$options": "i"}}
]}

# Verify count before delete
count = db.Public_Leaderboard.count_documents(query)
print(f"Found {count} records to delete.")

if count > 0:
    result = db.Public_Leaderboard.delete_many(query)
    print(f"Deleted {result.deleted_count} records.")
else:
    print("No records found.")
