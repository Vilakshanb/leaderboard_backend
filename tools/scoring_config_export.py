import os
import sys
import json
import logging
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

def export_config(module):
    db = get_db()
    # Assume default collection 'config'
    coll = db["config"]

    if module == "lumpsum":
        doc_id = "Leaderboard_Lumpsum"
    elif module == "sip":
        doc_id = "Leaderboard_SIP"
    else:
        logging.error("Module must be 'lumpsum' or 'sip'")
        return

    doc = coll.find_one({"_id": doc_id})
    if not doc:
        logging.error(f"Config not found for {doc_id}")
        return

    print(json_util.dumps(doc, indent=2))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 tools/scoring_config_export.py <lumpsum|sip>")
        sys.exit(1)
    export_config(sys.argv[1])
