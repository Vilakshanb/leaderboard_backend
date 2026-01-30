from pymongo import MongoClient
import os
from datetime import datetime

CONN = os.getenv("MONGODB_CONNECTION_STRING")
c = MongoClient(CONN)
db_v2 = c['PLI_Leaderboard_v2']

print("Checking Insurance_Policy_Scoring date range in v2...")

pipeline = [
    {
        "$match": {
            "renewal_date": {"$gte": datetime(2025, 12, 1), "$lt": datetime(2026, 1, 1)}
        }
    },
    {
        "$project": {
            "employee_id": 1,
            "employee_name": 1,
            "renewal_date": 1
        }
    },
    {"$limit": 5}
]

print("Sample Dec 2025 Records:")
res = list(db_v2.Insurance_Policy_Scoring.aggregate(pipeline))
for r in res:
    print(f"Emp: {r.get('employee_name')} ({r.get('employee_id')}) - Date: {r.get('renewal_date')}")

