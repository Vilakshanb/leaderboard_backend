import os
import logging

# Force connection string and DB Name (MUST BE BEFORE IMPORTS)
os.environ["MONGODB_CONNECTION_STRING"] = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/PLI_Leaderboard?retryWrites=true&w=majority"
os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["KEY_VAULT_URL"] = ""

from Leaderboard import run

# Set logging to INFO to see run output
logging.basicConfig(level=logging.ERROR)

print("Starting Aggregation & Verification Run...")
try:
    seeded_months = ["2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"]

    # 1. Run Aggregation
    for m in seeded_months:
        print(f"Aggregating {m}...")
        try:
            # Suppress logs from run internal if needed, but error is fine
            run(m, process_full_fy=False)
        except Exception as ex:
            print(f"Error aggregating {m}: {ex}")

    # 2. Verify Findings
    from pymongo import MongoClient

    uri = os.environ["MONGODB_CONNECTION_STRING"]
    client = MongoClient(uri)
    db_name = os.environ["PLI_DB_NAME"]
    db = client[db_name]

    print("\n--- FINAL VERIFICATION ---")

    for month in seeded_months:
        print(f"\nMonth: {month}")
        pub_c = db.Public_Leaderboard.count_documents({"period_month": month})
        print(f"  Rows Generated: {pub_c}")

        if pub_c > 0:
             # Check for lost premium
             stats = list(db.Public_Leaderboard.aggregate([
                {"$match": {"period_month": month}},
                {"$group": {"_id": None,
                            "total_pts": {"$sum": "$total_points_public"},
                            "lost_prem": {"$sum": "$ins_renewal_lost_premium"}
                           }}
             ]))
             if stats:
                 print(f"  Total Points: {stats[0]['total_pts']:,.2f}")
                 print(f"  Total Lost Premium: ₹{stats[0]['lost_prem']:,.2f}")

    print("\nDone.")
except Exception as e:
    print(f"Critical Error: {e}")
    import traceback
    traceback.print_exc()
