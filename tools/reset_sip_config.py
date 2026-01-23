
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "PLI_Leaderboard_v2")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"Resetting SIP Config to Defaults (Horizon=24) in {DB_NAME}...")
db.config.update_one(
    {"_id": "Leaderboard_SIP"},
    {"$set": {"options.sip_horizon_months": 24, "version": 1}},
    upsert=True
)
print("Done.")
