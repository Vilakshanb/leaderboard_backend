import os
import sys
import datetime
import logging
from pymongo import MongoClient
from datetime import timezone

# Setup paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set Environment Variables for v2
os.environ["DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["CORE_DB_NAME"] = "iwell" # Explicitly match default
os.environ["PLI_LOG_LEVEL"] = "INFO"
# Ensure we don't accidentally load .env files overriding this if modules use dotenv
os.environ["APP_ENV"] = "Production"

# Connect
CONN = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority"
os.environ["MongoDb-Connection-String"] = CONN
os.environ["MONGO_URI"] = CONN # Fallback for some modules

client = MongoClient(CONN)
db_v2 = client["PLI_Leaderboard_v2"]

# Clear v2 Collections
colls_to_clear = [
    "Public_Leaderboard",
    "Leaderboard_Lumpsum",
    "MF_SIP_Leaderboard",
    "Leaderboard_Incentives"
]
print("--- Clearing v2 Collections ---")
for c in colls_to_clear:
    # Just delete_many({}) even if empty/not exists (it's safe)
    try:
        res = db_v2[c].delete_many({})
        print(f"Cleared {c}: {res.deleted_count} docs deleted")
    except Exception as e:
        print(f"Error clearing {c}: {e}")

# Import Modules (masked)
print("\n--- Importing Modules ---")
try:
    # We modify sys.path so modules can find their siblings if needed
    from Lumpsum_Scorer import run_net_purchase
    from SIP_Scorer import run_pipeline as run_sip_pipeline
    from Leaderboard import run_for_configured_range as run_leaderboard
except ImportError as e:
    print(f"Failed to import modules: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# TARGET MONTH
TARGET_MONTH = "2025-11"
# Calculate start/end dates for SIP
y, m = map(int, TARGET_MONTH.split('-'))
start_date = datetime.datetime(y, m, 1, tzinfo=timezone.utc)
if m == 12:
    end_date = datetime.datetime(y + 1, 1, 1, tzinfo=timezone.utc)
else:
    end_date = datetime.datetime(y, m + 1, 1, tzinfo=timezone.utc)


print(f"\n--- Running Lumpsum Scorer for {TARGET_MONTH} ---")
# run_net_purchase(leaderboard_db, ..., target_month=..., mongo_client=...)
# Note: Lumpsum scorer internally handles iteration if we pass target_month (Simulated mode)
try:
    run_net_purchase(db_v2, target_month=TARGET_MONTH, mongo_client=client)
    print("Lumpsum Scorer Completed.")
except Exception as e:
    print(f"Lumpsum Scorer Failed: {e}")
    import traceback
    traceback.print_exc()

print(f"\n--- Running SIP Scorer for {start_date} to {end_date} ---")
# run_pipeline(start_date, end_date, mongo_uri=...)
try:
    run_sip_pipeline(start_date, end_date, mongo_uri=CONN)
    print("SIP Scorer Completed.")
except Exception as e:
    print(f"SIP Scorer Failed: {e}")
    import traceback
    traceback.print_exc()

print(f"\n--- Running Aggregator for {TARGET_MONTH} ---")
# run_for_configured_range(anchor_month, mongo_uri=..., db_name=...)
try:
    run_leaderboard(TARGET_MONTH, mongo_uri=CONN, db_name="PLI_Leaderboard_v2")
    print("Aggregator Completed.")
except Exception as e:
    print(f"Aggregator Failed: {e}")
    import traceback
    traceback.print_exc()

print("\n--- Rebuild Complete ---")
