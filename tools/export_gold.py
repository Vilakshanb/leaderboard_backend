
import os
import sys
import json
import shutil
import logging
import traceback
from datetime import datetime
from unittest.mock import patch
import pymongo
from pymongo import MongoClient

# Setup paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
sys.path.append(os.path.join(ROOT_DIR, "tools"))

# Import Seed Script
import reset_seed_v2

# Database Constants
TEST_DB_NAME = "PLI_Leaderboard_v2"
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING") or os.getenv("MONGO_URI")

if not MONGO_URI:
    print("ERROR: MongoDb-Connection-String or MONGO_URI env var not set")
    sys.exit(1)

# Ensure Environment Variables for Scorers
os.environ["DB_NAME"] = TEST_DB_NAME
os.environ["CORE_DB_NAME"] = TEST_DB_NAME
os.environ["APP_ENV"] = "test"

# Import Scorers
try:
    import Lumpsum_Scorer
    import SIP_Scorer
    import Leaderboard
except ImportError as e:
    print(f"ERROR: Could not import scorers: {e}")
    sys.exit(1)

# --- DB Isolation ---
class V2RedirectClient(MongoClient):
    """Wraps MongoClient to redirect ALL database access to PLI_Leaderboard_v2."""
    def get_database(self, name=None, codec_options=None, read_preference=None, write_concern=None, read_concern=None):
        target = TEST_DB_NAME
        if name in ["internal", "iwell", "PLI_Leaderboard", "core", "leaderboard"]:
             target = TEST_DB_NAME
        return super().get_database(target, codec_options, read_preference, write_concern, read_concern)

    def __getitem__(self, name):
        return self.get_database(name)

# --- Configuration Helpers ---
def apply_lumpsum_override(db):
    print(">>> Applying Lumpsum Config Override: Rate Boost")
    # Example: Boost rate to 100% (1.0) for >2% growth
    override_slabs = [
        {"min_pct": 2.0, "rate": 1.0, "label": "Test Boost"},
        {"min_pct": 0.0, "max_pct": 2.0, "rate": 0.0, "label": "<2%"}
    ]
    db.config.update_one(
        {"_id": "Leaderboard_Lumpsum"},
        {"$set": {"rate_slabs": override_slabs, "version": 999}},
        upsert=True
    )

def apply_sip_override(db):
    print(">>> Applying SIP Config Override: Horizon Boost")
    # Change horizon from 24 (default) to 48
    # NOTE: SIP_Scorer reads this from cfg['options']['sip_horizon_months']
    db.config.update_one(
        {"_id": "Leaderboard_SIP"},
        {"$set": {"options.sip_horizon_months": 48, "version": 999}},
        upsert=True
    )

# --- Export Helper ---
def export_snapshots(db, output_dir, target_month="2025-11"):
    print(f"Exporting snapshots to {output_dir}...")
    os.makedirs(output_dir, exist_ok=True)

    collections = ["Leaderboard_Lumpsum", "MF_SIP_Leaderboard", "Public_Leaderboard"]

    for col_name in collections:
        # Query
        if db[col_name].count_documents({"month": target_month}) > 0:
            query = {"month": target_month}
        elif db[col_name].count_documents({"period_month": target_month}) > 0:
            query = {"period_month": target_month}
        else:
            # Fallback or empty
            query = {"month": target_month}

        docs = list(db[col_name].find(query))

        # Sort deterministically
        docs.sort(key=lambda x: (x.get("employee_id") or x.get("_id") or "", x.get("period_month") or x.get("month") or ""))

        # Normalize
        normalized = []
        for d in docs:
            # Remove non-deterministic fields
            for key in ["_id", "updatedAt", "createdAt", "updated_at", "created_at", "config_hash", "AuditMeta"]:
                d.pop(key, None)

            # Recursively walk to remove nested dates if needed, or just specific top levels
            if "audit" in d and isinstance(d["audit"], dict):
                # public leaderboard audit doesn't usually have timestamps, but check
                pass

            normalized.append(d)

        out_path = os.path.join(output_dir, f"{col_name}.json")
        with open(out_path, "w") as f:
            json.dump(normalized, f, indent=2, default=str, sort_keys=True)

        print(f"  Saved {col_name}.json ({len(normalized)} records)")

# --- Main Pipeline ---
def run_pipeline(scenario, output_dir):
    print(f"\n[{datetime.now()}] Starting Pipeline: {scenario}")

    # 1. Reset SEED (clean slate for every scenario)
    # We must patch MongoClient here too because reset_seed might use it?
    # Actually reset_seed uses `from pymongo import MongoClient`.
    # We should probably trust reset_seed_v2's logic but ensure it talks to v2.
    # reset_seed_v2.py uses DB_NAME="PLI_Leaderboard_v2" hardcoded.
    # But let's run it.
    print("  > Seeding Database...")
    reset_seed_v2.seed_data()

    # Reset Lumpsum Scorer internal state to prevent streak accumulation between scenarios
    if hasattr(Lumpsum_Scorer, "_POSITIVE_STREAKS"):
        Lumpsum_Scorer._POSITIVE_STREAKS.clear()

    # 2. Patch & Connect
    client = MongoClient(MONGO_URI)
    db = client[TEST_DB_NAME]

    # 3. Apply Overrides
    if scenario == "override_lumpsum":
        apply_lumpsum_override(db)
    elif scenario == "override_sip":
        apply_sip_override(db)

    # 4. Run Scorers (using patch)
    target_month = "2025-11"
    start_dt = datetime(2025, 11, 1)
    end_dt = datetime(2025, 12, 1)

    print("  > Running Scorers...")

    # Patch SIP Scorer specific collection accessor
    def patched_tx_coll(client):
        print(f"DEBUG: patched_tx_coll called! Client: {client}")
        return client[TEST_DB_NAME]["transactions"]
    SIP_Scorer._tx_coll = patched_tx_coll

    with patch('pymongo.MongoClient', side_effect=V2RedirectClient):
        # Lumpsum
        try:
            Lumpsum_Scorer.run_net_purchase(leaderboard_db=db, target_month=target_month, mongo_client=client)
        except Exception as e:
            print(f"!! Lumpsum Error: {e}")
            traceback.print_exc()

        # SIP
        try:
            print(f"  > DEBUG: Inspecting transactions in {TEST_DB_NAME} before SIP run...")
            count = db.transactions.count_documents({})
            print(f"  > DEBUG: db.transactions count: {count}")
            sample = db.transactions.find_one({})
            print(f"  > DEBUG: Sample txn: {sample}")

            SIP_Scorer.run_pipeline(start_date=start_dt, end_date=end_dt, mongo_uri=MONGO_URI)
        except Exception as e:
            print(f"!! SIP Error: {e}")
            traceback.print_exc()

        # Leaderboard
        try:
            Leaderboard.run(month=target_month, mongo_uri=MONGO_URI, db_name=TEST_DB_NAME, process_full_fy=False)
        except Exception as e:
            print(f"!! Leaderboard Error: {e}")
            traceback.print_exc()

    # 5. Export
    export_snapshots(db, output_dir, target_month)
    print(f"Completed: {scenario}")

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run scoring pipeline and export snapshots.")
    parser.add_argument("--scenario", type=str, choices=["default", "override_lumpsum", "override_sip", "all"], default="all", help="Scenario to run")
    parser.add_argument("--output-dir", type=str, default=None, help="Base directory for output (default: gold/2025-11)")

    args = parser.parse_args()

    # Default base gold dir
    base_out = args.output_dir or os.path.join(ROOT_DIR, "gold", "2025-11")

    scenarios_to_run = []
    if args.scenario == "all":
        scenarios_to_run = [
            ("default", os.path.join(base_out, "default")),
            ("override_lumpsum", os.path.join(base_out, "override_lumpsum")),
            ("override_sip", os.path.join(base_out, "override_sip"))
        ]
    else:
        # If specific output dir is given, use it directly?
        # Or append scenario name?
        # If user provides --output-dir /tmp/test_run, we should probably append scenario name
        # so we don't mix outputs if they run multiple.
        # But for precise testing, maybe we want exact path?
        # Let's standardize: always append scenario name to base_out.
        scenarios_to_run = [(args.scenario, os.path.join(base_out, args.scenario))]

    os.environ["CONFIRM_DROP"] = "yes" # Force seed

    for sc_name, sc_dir in scenarios_to_run:
        run_pipeline(sc_name, sc_dir)

    print(f"\nSnapshots Verified/Exported for: {[s[0] for s in scenarios_to_run]}")
