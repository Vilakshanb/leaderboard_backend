
import os
import sys
import logging
from dotenv import load_dotenv

# Add parent directory to path to allow importing Leaderboard module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(dotenv_path="backend/local.settings.json")

# Ensure connection string is set
if "MONGODB_CONNECTION_STRING" not in os.environ:
    # Fallback to the known connection string if not in env
    os.environ["MONGODB_CONNECTION_STRING"] = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority"
    os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"

from Leaderboard import run

logging.basicConfig(level=logging.INFO)

target_month = "2025-11"
print(f"Triggering leaderboard run for {target_month} on {os.environ.get('PLI_DB_NAME', 'PLI_Leaderboard_v2')}...")

run(target_month)

print("Aggregation complete.")
