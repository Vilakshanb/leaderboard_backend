
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "PLI_Leaderboard_v2")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"Applying Lumpsum Config Override (Rate Boost) to {DB_NAME}...")
override_slabs = [
    {"min_pct": 2.0, "rate": 1.0, "label": "Test Boost"},
    {"min_pct": 0.0, "max_pct": 2.0, "rate": 0.0, "label": "<2%"}
]
db.config.update_one(
    {"_id": "Leaderboard_Lumpsum"},
    {"$set": {"rate_slabs": override_slabs, "version": 999}},
    upsert=True
)
print("Done.")
