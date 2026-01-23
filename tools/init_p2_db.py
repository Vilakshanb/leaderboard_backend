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

    # 1. Leaderboard_Disputes
    if "Leaderboard_Disputes" not in db.list_collection_names():
        print("Creating Leaderboard_Disputes...")
        db.create_collection("Leaderboard_Disputes")

    db.Leaderboard_Disputes.create_index(
        [("employee_id", ASCENDING), ("month", ASCENDING), ("status", ASCENDING)],
        background=True
    )
    db.Leaderboard_Disputes.create_index([("created_at", DESCENDING)], background=True)

    # 2. Forecast_Events
    if "Forecast_Events" not in db.list_collection_names():
        print("Creating Forecast_Events...")
        db.create_collection("Forecast_Events")

    db.Forecast_Events.create_index(
        [("employee_id", ASCENDING), ("month", ASCENDING), ("product", ASCENDING)],
        background=True
    )
    db.Forecast_Events.create_index([("expected_close_date", ASCENDING)], background=True)

    # 3. Forecast_Leaderboard
    if "Forecast_Leaderboard" not in db.list_collection_names():
        print("Creating Forecast_Leaderboard...")
        db.create_collection("Forecast_Leaderboard")

    # Unique constraint for upsert
    db.Forecast_Leaderboard.create_index(
        [("employee_id", ASCENDING), ("month", ASCENDING), ("channel", ASCENDING)],
        unique=True,
        background=True
    )
    db.Forecast_Leaderboard.create_index(
        [("month", ASCENDING), ("channel", ASCENDING)],
        background=True
    )

    print("Phase 2 DB foundation applied.")

if __name__ == "__main__":
    init_db()
