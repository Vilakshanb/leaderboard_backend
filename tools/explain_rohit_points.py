from pymongo import MongoClient
from datetime import datetime
import os

CONN = os.getenv("MONGODB_CONNECTION_STRING")
c = MongoClient(CONN)
db_v2 = c['PLI_Leaderboard_v2']

print("Explaining Points for Rohit Bhardwaj (Dec 2025)...")

pipeline = [
    {
        "$match": {
            "employee_name": "Rohit Bhardwaj",
            "renewal_date": {"$gte": datetime(2025, 12, 1), "$lt": datetime(2026, 1, 1)}
        }
    },
    {
        "$project": {
            "policy_number": 1,
            "renewal_date": 1,
            "total_points": 1,
            "base_points": 1,
            "policy_type": 1,
            "fresh_premium_eligible": 1
        }
    }
]

res = list(db_v2.Insurance_Policy_Scoring.aggregate(pipeline))
total = 0
for r in res:
    print(f"Policy: {r.get('policy_number')} | Date: {r.get('renewal_date')} | Type: {r.get('policy_type')} | Points: {r.get('total_points')} (Base: {r.get('base_points')}) | FreshPrem: {r.get('fresh_premium_eligible')}")
    total += r.get('total_points', 0)

print(f"Total Dec Points: {total}")
