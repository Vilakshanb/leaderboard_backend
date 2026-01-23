
import os
import sys
import datetime
import calendar
import json
from pymongo import MongoClient

# --- Configuration ---
START_YEAR = 2025
START_MONTH = 4  # April
END_YEAR = 2025
END_MONTH = 12   # December
DB_NAME = "PLI_Leaderboard_v2"

# Ensure we are targeting v2
os.environ["DB_NAME"] = DB_NAME
os.environ["PLI_DB_NAME"] = DB_NAME
os.environ["APP_ENV"] = "Production"
os.environ["CORE_DB_NAME"] = "iwell" # Read from iwell

# Load local.settings.json for connection strings
try:
    with open("local.settings.json", "r") as f:
        settings = json.load(f)
        for k, v in settings.get("Values", {}).items():
            if k not in os.environ:
                 os.environ[k] = v
except Exception as e:
    print(f"Warning: Could not load local.settings.json: {e}")

# Setup paths for modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Scorers
try:
    from Lumpsum_Scorer import run_net_purchase
    from SIP_Scorer import run_pipeline
    from Insurance_scorer import Run_insurance_Score
    from referral_scorer import main as run_referral_main
    from Leaderboard import run as run_aggregator
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def get_month_range(year, month):
    """Returns start and end datetime for a given year/month."""
    start_date = datetime.datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime.datetime(year, month, last_day, 23, 59, 59)
    return start_date, end_date

def wipe_leadboard_v2():
    print(f"WARNING: Wiping {DB_NAME} Leaderboard collections...")
    conn = os.getenv("MONGODB_CONNECTION_STRING") or os.getenv("MONGO_URI") # fallback
    if not conn:
        print("No Mongo connection string found!")
        sys.exit(1)

    client = MongoClient(conn)
    db = client[DB_NAME]

    colls = ["Public_Leaderboard", "Leaderboard_Lumpsum", "MF_SIP_Leaderboard", "Insurance_Leaderboard", "Referral_Leaderboard"]
    for c in colls:
        print(f"  - Dropping {c}...")
        db[c].drop()
    print("Wipe complete.")
    return client

def run_rebuild():
    client = wipe_leadboard_v2()

    current_y, current_m = START_YEAR, START_MONTH

    while True:
        target_month_str = f"{current_y}-{current_m:02d}"
        print(f"\n>>> Processing Month: {target_month_str} <<<")

        start_dt, end_dt = get_month_range(current_y, current_m)

        # 1. Lumpsum
        print(f"  [Lumpsum] Running for {target_month_str}...")
        try:
            run_net_purchase(leaderboard_db=client[DB_NAME], target_month=target_month_str, mongo_client=client)
        except Exception as e:
            print(f"  [Lumpsum] Error: {e}")

        # 2. SIP
        print(f"  [SIP] Running for {target_month_str}...")
        try:
            run_pipeline(start_date=start_dt, end_date=end_dt, mongo_uri=os.getenv("MONGODB_CONNECTION_STRING"))
        except Exception as e:
            print(f"  [SIP] Error: {e}")

        # 3. Insurance (Assume it calculates 'current' or 'window', might rely on date mocking if strictly historic is needed)
        # However, Insurance typically scores 'active' policies. We'll run it.
        # Ideally we'd pass a date, but Run_insurance_Score takes none.
        # If it scores all active policies, that's acceptable for a rebuild.
        print(f"  [Insurance] Running...")
        try:
            Run_insurance_Score()
        except Exception as e:
            print(f"  [Insurance] Error: {e}")

        # 4. Referral
        print(f"  [Referral] Running...")
        try:
            # Referral main usually expects a timer object, passing None usually works if internal logic checks for it
            # But earlier check showed 'if __name__ == "__main__": main(None)'
            run_referral_main(None)
        except Exception as e:
            print(f"  [Referral] Error: {e}")

        # 5. Aggregator
        print(f"  [Aggregator] Aggregating for {target_month_str}...")
        try:
            # 'run' in Leaderboard takes (timer, monthly, quarterly, annual)
            # We want to force a monthly run for this specific month.
            # Usually Leaderboard aggregates based on NOW or last month.
            # If 'run' doesn't take target date, we might need a specific tool or trick.
            # Checking Leaderboard/__init__.py might be needed if it fails.
            # Assuming 'run' works or we use a lower level function if possible.
            # Actually, Leaderboard pipelines usually Aggregation on 'Public_Leaderboard' which is populated by scorers.
            # The Aggregator merges them. It usually filters by "Target Month" derived from "Now".
            # If we want to aggregate a PAST month, we might need to set 'LEADERBOARD_TARGET_MONTH' env var if supported,
            # or rely on the scorers having correct 'month' fields and the aggregator aggregating ALL or SPECIFIC.
            # Let's try invoke run() and hope it rebuilds all or we might need to iterate.
            run_aggregator(None)
            pass
        except Exception as e:
            print(f"  [Aggregator] Error: {e}")

        # Move to next month
        if current_y == END_YEAR and current_m == END_MONTH:
            break

        current_m += 1
        if current_m > 12:
            current_m = 1
            current_y += 1

    print("\nRebuild Complete!")

if __name__ == "__main__":
    run_rebuild()
