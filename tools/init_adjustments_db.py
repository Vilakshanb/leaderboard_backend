#!/usr/bin/env python3
import os
import sys
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING

def init_db():
    mongo_uri = os.getenv("MongoDb-Connection-String") or os.getenv("MONGODB_URI")
    db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")

    if not mongo_uri:
        print("Error: Mongo URI missing")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll_name = "Leaderboard_Adjustments"

    if coll_name not in db.list_collection_names():
        print(f"Creating collection {coll_name}...")
        db.create_collection(coll_name)

    coll = db[coll_name]

    # Indexes
    # 1. Lookup by Employee/Month/Status (for API fetches)
    print("Creating index: employee_id_1_month_1_status_1")
    coll.create_index(
        [("employee_id", ASCENDING), ("month", ASCENDING), ("status", ASCENDING)],
        background=True
    )

    # 2. Lookup by creation time (audit/history)
    print("Creating index: created_at_-1")
    coll.create_index([("created_at", DESCENDING)], background=True)

    print("Done.")

if __name__ == "__main__":
    init_db()
