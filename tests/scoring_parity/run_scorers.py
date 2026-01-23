"""
Scoring Parity Test Runner.

Executes Lumpsum_Scorer and SIP_Scorer against PLI_Leaderboard_v2 source fixtures,
generates Public_Leaderboard output, and allows snapshotting.

Usage:
    python tests/scoring_parity/run_scorers.py [suffix]

    If suffix="all", runs default -> snapshot -> override -> snapshot.

Safety:
    Must run against DB_NAME=PLI_Leaderboard_v2
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime
from unittest.mock import patch, MagicMock
import pymongo
from pymongo import MongoClient

# Add function app root to path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

# Safety Check
MONGO_URI = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
if not MONGO_URI:
    print("ERROR: MongoDb-Connection-String or MONGO_URI must be set")
    sys.exit(1)

TEST_DB_NAME = "PLI_Leaderboard_v2"
os.environ["DB_NAME"] = TEST_DB_NAME
os.environ["CORE_DB_NAME"] = TEST_DB_NAME  # Force Lumpsum scorer to read from v2
os.environ["APP_ENV"] = "test"

# Import scorers (AFTER env vars set to ensure they pick up defaults if any)
try:
    import Lumpsum_Scorer
    import SIP_Scorer
    import Leaderboard
except ImportError as e:
    print(f"ERROR: Could not import scorers. Check sys.path: {sys.path}")
    raise e

# --- V2 Redirect Client ---
class V2RedirectClient(MongoClient):
    """Wraps MongoClient to redirect ALL database access to PLI_Leaderboard_v2."""
    def get_database(self, name=None, codec_options=None, read_preference=None, write_concern=None, read_concern=None):
        # Redirect mostly everything to TEST_DB_NAME
        # We can be aggressive here for the test runner.
        target = TEST_DB_NAME
        # If no name provided (default db), use test db
        if name is None:
             pass
        # If 'admin' or 'local', maybe keep them? But mostly we want v2.
        # Let's redirect specific known DBs
        if name in ["internal", "iwell", "PLI_Leaderboard", "core", "leaderboard"]:
             target = TEST_DB_NAME

        # Call super with the target name
        return super().get_database(target, codec_options, read_preference, write_concern, read_concern)

    def __getitem__(self, name):
        return self.get_database(name)

def run_scoring_iteration(suffix, snapshot_dir=None):
    """Run full scoring pipeline for Nov 2025."""
    print(f"\n--- Running Scoring Iteration: {suffix} ---")

    # 1. Connect (using our V2RedirectClient logic implicitly if patched, or explicit here)
    # Since we patch MongoClient below, calling MongoClient() returns V2RedirectClient-like behavior?
    # No, patch replaces class.
    client = MongoClient(MONGO_URI)
    db = client[TEST_DB_NAME]

    # Verify transaction count in v2
    txn_count = db.transactions.count_documents({})
    print(f"DEBUG: db.transactions count in {TEST_DB_NAME}: {txn_count}")

    # TARGET MONTH
    target_month = "2025-11"

    # 2. Run Lumpsum Scorer
    print(f"Running Lumpsum Scorer for {target_month}...")
    try:
        Lumpsum_Scorer.run_net_purchase(
            leaderboard_db=db,
            target_month=target_month,
            mongo_client=client
        )
    except Exception as e:
        print(f"ERROR: Lumpsum Scorer failed: {e}")
        traceback.print_exc()

    # 3. Run SIP Scorer
    print(f"Running SIP Scorer for {target_month}...")
    start_dt = datetime(2025, 11, 1)
    end_dt = datetime(2025, 12, 1) # Exclusive end

    try:
        # We also patch _tx_coll for extra safety, but MongoClient patch handles the DB resolution
        SIP_Scorer.run_pipeline(
            start_date=start_dt,
            end_date=end_dt,
            mongo_uri=MONGO_URI
        )
    except Exception as e:
        print(f"ERROR: SIP Scorer failed: {e}")
        traceback.print_exc()

    # 4. Run Leaderboard Aggregation
    print(f"Running Leaderboard Aggregation for {target_month}...")
    try:
        Leaderboard.run(
            month=target_month,
            mongo_uri=MONGO_URI,
            db_name=TEST_DB_NAME,
            process_full_fy=False
        )
    except Exception as e:
        print(f"ERROR: Leaderboard aggregation failed: {e}")
        traceback.print_exc()

    # 5. Snapshot Outputs
    if snapshot_dir is None:
        snapshot_dir = os.getenv("SNAPSHOT_DIR") or os.path.join(os.path.dirname(__file__), "snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)

    cols_to_snap = ["Leaderboard_Lumpsum", "MF_SIP_Leaderboard", "Public_Leaderboard", "config"]

    for col_name in cols_to_snap:
        if col_name == "config":
             query = {"_id": "Leaderboard_Lumpsum"}
        else:
             if db[col_name].count_documents({"month": target_month}) > 0:
                 query = {"month": target_month}
             elif db[col_name].count_documents({"period_month": target_month}) > 0:
                 query = {"period_month": target_month}
             else:
                 query = {"month": target_month}

        docs = list(db[col_name].find(query))

        docs.sort(key=lambda x: x.get("employee_id", "") or x.get("_id", ""))

        normalized = []
        for d in docs:
            if col_name != "config":
                d.pop("_id", None)
                d.pop("updatedAt", None)
                d.pop("createdAt", None)
                d.pop("created_at", None)
                d.pop("updated_at", None)
                d.pop("config_hash", None)
                d.pop("AuditMeta", None)
            else:
                d.pop("updatedAt", None)

            for k, v in d.items():
                if isinstance(v, float):
                    d[k] = round(v, 2)

            normalized.append(d)

        out_file = os.path.join(snapshot_dir, f"{col_name}_{suffix}.json")
        with open(out_file, "w") as f:
            json.dump(normalized, f, indent=2, default=str)
        print(f"Saved snapshot: {out_file} ({len(normalized)} records)")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    arg = sys.argv[1] if len(sys.argv) > 1 else "default"

    # GLOBAL PATCH: MongoClient
    # This ensures ANY module creating a new MongoClient(uri) gets our RedirectClient
    # This fixes SIP_Scorer connecting to prod DB for output.
    with patch('pymongo.MongoClient', side_effect=V2RedirectClient):
        # Additional safe patch for SIP transaction collection
        SIP_Scorer._tx_coll = lambda client: client[TEST_DB_NAME]["transactions"]

        if arg == "all":
            run_scoring_iteration("default")

            print("\n>>> Applying Config Override (Rate Slab 2.0% -> 100.0%)")
            # We must use V2RedirectClient explicitly here if inside patch, or just client path
            client = MongoClient(MONGO_URI)
            db = client[TEST_DB_NAME]

            override_slabs = [
                {"min_pct": 2.0, "rate": 1.0, "label": "Test Boost"}, # 100% rate!
                {"min_pct": 0.0, "max_pct": 2.0, "rate": 0.0, "label": "<2%"}
            ]
            db.config.update_one(
                {"_id": "Leaderboard_Lumpsum"},
                {"$set": {"rate_slabs": override_slabs, "version": 999}},
                upsert=True
            )
            print("db.config updated.")

            run_scoring_iteration("override")

        else:
            run_scoring_iteration(arg)
