import os
import sys
import json
import logging
import argparse
import datetime
from datetime import timezone
import pymongo
from bson import json_util

logging.basicConfig(level=logging.INFO, format="%(message)s")

def load_settings():
    try:
        with open("local.settings.json", "r") as f:
            data = json.load(f)
            return data.get("Values", {})
    except FileNotFoundError:
        return {}

def get_db():
    settings = load_settings()
    uri = os.getenv("MONGODB_CONNECTION_STRING") or settings.get("MONGODB_CONNECTION_STRING")
    db_name = os.getenv("DB_NAME") or settings.get("DB_NAME") or "PLI_Leaderboard"
    if not uri:
        logging.error("No connection string found.")
        sys.exit(1)
    client = pymongo.MongoClient(uri)
    return client[db_name]

def import_config(file_path, activate):
    try:
        with open(file_path, 'r') as f:
            doc = json_util.loads(f.read())
    except Exception as e:
        logging.error(f"Failed to read file: {e}")
        return

    doc_id = doc.get("_id")
    if not doc_id:
        logging.error("JSON document must have an _id field")
        return

    # Enforce activate if requested
    if activate:
        doc["status"] = "active"

    # Update timestamp
    doc["updatedAt"] = datetime.datetime.now(timezone.utc).isoformat()

    db = get_db()
    coll = db["config"]

    res = coll.replace_one({"_id": doc_id}, doc, upsert=True)

    logging.info(f"Imported {doc_id}: Matched={res.matched_count}, Modified={res.modified_count}, Upserted={res.upserted_id}")
    logging.info(f"Status: {doc.get('status', 'unknown')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import scoring config from JSON")
    parser.add_argument("--file", required=True, help="Path to JSON config file")
    parser.add_argument("--activate", action="store_true", help="Set status to active")

    args = parser.parse_args()
    import_config(args.file, args.activate)
