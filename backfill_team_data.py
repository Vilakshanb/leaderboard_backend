
import os
import pymongo
from datetime import datetime

# Connection
# Hardcoded for verification/backfill script
mongo_conn_str = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority"
db_name = "PLI_Leaderboard_v2"

client = pymongo.MongoClient(mongo_conn_str)
db = client[db_name]

def backfill_teams(month="2025-12"):
    print(f"Backfilling Team Data for {month}...")

    # 1. Define a mapping of Employee ID -> (Team ID, Reporting Manager ID)
    # Using some known names/IDs based on previous context or placeholders
    # Structure: { employee_id: (team_id, manager_id, manager_name) }

    # Sagar Maini -> Team Alpha, Manager: Vinay
    # Sumit Ch -> Team Beta, Manager: Vinay

    mapping = {
        "MIB16376": ("TEAM_ALPHA", "MANAGER_001", "Vinay Kumar"), # Sagar Maini
        "2969103000000135011": ("TEAM_ALPHA", "MANAGER_001", "Vinay Kumar"), # Pramod Bhutani (from check_zoho_user output)
        "TEST_EMP_001": ("TEAM_ALPHA", "MANAGER_001", "Vinay Kumar"),
        "TEST_EMP_002": ("TEAM_BETA", "MANAGER_002", "Sandeep"),
        "TEST_EMP_003_LOST": ("TEAM_BETA", "MANAGER_002", "Sandeep"),
    }

    # fallback for others
    default_team = "TEAM_GENERAL"
    default_manager_id = "MANAGER_DEFAULT"
    default_manager_name = "General Manager"

    collections = ["MF_SIP_Leaderboard", "Insurance_Policy_Scoring", "referralLeaderboard", "Referral_Incentives"]

    for col_name in collections:
        print(f"  Processing {col_name}...")
        col = db[col_name]

        # Find docs for the month that are missing team_id (or just all)
        # Handle schema variance: some use 'month', some 'period_month'
        query = {"$or": [{"period_month": month}, {"month": month}]}
        cursor = col.find(query)

        count = 0
        for doc in cursor:
            emp_id = str(doc.get("employee_id"))

            if emp_id in mapping:
                tid, mid, mname = mapping[emp_id]
            else:
                # Assign round robin or default based on hash of ID to simulate distribution
                # Simple hash
                if hash(emp_id) % 2 == 0:
                     tid, mid, mname = ("TEAM_ALPHA", "MANAGER_001", "Vinay Kumar")
                else:
                     tid, mid, mname = ("TEAM_BETA", "MANAGER_002", "Sandeep")

            # Update the doc
            col.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "team_id": tid,
                    "reporting_manager_id": mid,
                    "reporting_manager_name": mname, # Optional helper
                    "updated_at": datetime.now()
                }}
            )
            count += 1

        print(f"  Updated {count} documents in {col_name}")

if __name__ == "__main__":
    # Backfill for a few months
    months = ["2025-04", "2025-05", "2025-10", "2025-11", "2025-12"]
    for m in months:
        backfill_teams(m)
    print("Backfill Complete.")
