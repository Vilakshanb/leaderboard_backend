from pymongo import MongoClient
from datetime import datetime
import json
import os

CONN = os.getenv("MONGODB_CONNECTION_STRING")
c = MongoClient(CONN)
db_v2 = c['PLI_Leaderboard_v2']

print("Calculating Insurance Bonus Breakdown for Dec 2025 (Q3: Oct-Dec)...")

# Q3 date range
q_start = datetime(2025, 10, 1)
q_end = datetime(2026, 1, 1) # Exclusive

# Fetch all Q3 records
pipeline = [
    {
        "$match": {
            "renewal_date": {"$gte": q_start, "$lt": q_end}
        }
    },
    {
        "$group": {
            "_id": "$employee_id",
            "employee_name": {"$first": "$employee_name"},
            "q_fresh_premium": {"$sum": "$fresh_premium_eligible"},
            "q_total_points": {"$sum": "$total_points"},
            "dec_points": {
                "$sum": {
                    "$cond": [
                        {"$and": [{"$gte": ["$renewal_date", datetime(2025, 12, 1)]}, {"$lt": ["$renewal_date", datetime(2026, 1, 1)]}]},
                        "$total_points",
                        0
                    ]
                }
            }
        }
    }
]

def get_q_bonus(fresh_premium):
    fp = float(fresh_premium or 0)
    if 1_500_000 <= fp < 1_700_000: return 3200
    if 1_700_000 <= fp < 2_000_000: return 9000
    if 2_000_000 <= fp < 2_500_000: return 17_500
    if fp >= 2_500_000: return 31_000
    return 0

results = []
agg = list(db_v2.Insurance_Policy_Scoring.aggregate(pipeline))

for r in agg:
    q_fresh = r.get('q_fresh_premium', 0)
    q_bonus = get_q_bonus(q_fresh)

    results.append({
        "Employee": r.get('employee_name', 'Unknown'),
        "ID": r.get('_id'),
        "Q3_Fresh_Premium": round(q_fresh, 2),
        "Quarterly_Bonus": q_bonus,
        "Dec_Points": round(r.get('dec_points', 0), 2),
        "Total_Payout_Impact": round(r.get('dec_points', 0) + q_bonus, 2)
    })

# Output JSON
print(json.dumps(results, indent=2))
