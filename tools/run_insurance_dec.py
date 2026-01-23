import os
import datetime
import logging

# Ensure we target v2
os.environ["DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"
os.environ["WIPE_MONTHLY_LEADERBOARD"] = "0"

# Configure logging
logging.basicConfig(level=logging.INFO)

# Import and run
try:
    from Insurance_scorer import Run_insurance_Score
    print("Starting Insurance Scorer for Dec 2025 (and active window)...")
    # Run_insurance_Score does not take arguments, it calculates for "now" or "active window"
    # Typically this scorer runs for the current FY or period.
    Run_insurance_Score()
    print("Insurance Scorer completed.")
except ImportError:
    print("Failed to import Run_insurance_Score. Checking path...")
except Exception as e:
    print(f"Error running scorer: {e}")
