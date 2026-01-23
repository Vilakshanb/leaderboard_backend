
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "PLI_Leaderboard_v2")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"DB_NAME: {DB_NAME}")

collections = [
    "Zoho_Users",
    "Insurance_Policy_Scoring",
    "referralLeaderboard",
    "Referral_Incentives",
    "Leaderboard_Lumpsum",
    "MF_SIP_Leaderboard",
    "Public_Leaderboard",
    "__engine__Leaderboard_Lumpsum",
    "__engine__MF_SIP_Leaderboard",
    "__engine__Public_Leaderboard",
    "transactions",
    "AUM_Report"
]

for col in collections:
    try:
        count = db[col].estimated_document_count()
        print(f"{col}: {count}")
    except Exception as e:
        print(f"{col}: ERROR - {e}")

client.close()
