
import os
import sys
import logging
from datetime import datetime

# Add parent directory to path so we can import Leaderboard/Scorers
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import Leaderboard

# Set Env
os.environ["DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["APP_ENV"] = "Production"
# Ensuring Mongo Connection String is available (it should be since I'm running locally with func core tools or manually setup)
# If missing, I'll rely on Leaderboard to raise error, or I can hardcode it here based on checking prev output
# But let's assume I run this where I can pass env var or it picks up if I set it.

def rerun_aggregator():
    logging.basicConfig(level=logging.INFO)

    months = [
        "2025-04", "2025-05", "2025-06", "2025-07",
        "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"
    ]

    print("Starting Aggregator Rerun for FY25-26...")
    print(f"Goal: Populate Public_Leaderboard in {os.environ['DB_NAME']}")

    for m in months:
        print(f"Aggregating {m}...")
        try:
            Leaderboard.run(month=m, db_name="PLI_Leaderboard_v2")
            print(f"✓ {m} Done")
        except Exception as e:
            print(f"✗ {m} Failed: {e}")

if __name__ == "__main__":
    # Ensure Mongo URI. If not in env, this will fail.
    # user instruction imply I am in local env where I might need to export it.
    rerun_aggregator()
