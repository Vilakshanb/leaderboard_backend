
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING") or os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "PLI_Leaderboard_v2")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"Applying SIP Config Override (Horizon=48) to {DB_NAME}...")
db.config.update_one(
    {"_id": "Leaderboard_SIP"},
    {"$set": {"options.sip_horizon_months": 48, "version": 999}},
    upsert=True
)
print("Done.")
