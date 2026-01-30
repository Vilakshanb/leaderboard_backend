
import pymongo
import os
import sys
import datetime
from bson.son import SON

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Leaderboard import build_public_leaderboard_pipeline

month = "2025-05"
start = datetime.datetime(2025, 5, 1)
end = datetime.datetime(2025, 6, 1)

client = pymongo.MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
db = client["PLI_Leaderboard_v2"]

print(f"Tracing using ACTUAL pipeline for {month}...")

pipeline = build_public_leaderboard_pipeline(month, start, end)

# Remove the $merge stage for testing, so we get results back
pipeline_no_merge = pipeline[:-1]

print(f"Pipeline stages: {len(pipeline_no_merge)}")

try:
    results = list(db.MF_SIP_Leaderboard.aggregate(pipeline_no_merge))
    print(f"Result Count: {len(results)}")

    if len(results) > 0:
        print("First doc:")
        print(results[0])
    else:
        print("No results found.")

except Exception as e:
    print(f"Aggregation Failed: {e}")
