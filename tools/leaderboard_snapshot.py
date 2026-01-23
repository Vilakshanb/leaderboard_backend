#!/usr/bin/env python3
import json
import os
import sys
import datetime
from datetime import timezone
import argparse
import pymongo
from pymongo import MongoClient

# Load Config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "regression_config.json")
if not os.path.exists(CONFIG_PATH):
    # Fallback to relative from root if run from root
    CONFIG_PATH = "tools/regression_config.json"

with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

def get_target_months(cli_months=None):
    if cli_months:
        return cli_months.split(",")

    # Resolve relative months
    now = datetime.datetime.now(timezone.utc)
    current_month = now.strftime("%Y-%m")

    last_month_dt = now.replace(day=1) - datetime.timedelta(days=1)
    last_month = last_month_dt.strftime("%Y-%m")

    two_months_ago_dt = last_month_dt.replace(day=1) - datetime.timedelta(days=1)
    two_months_ago = two_months_ago_dt.strftime("%Y-%m")

    resolved = []
    for m in CONFIG["months"]:
        if m == "current_month": resolved.append(current_month)
        elif m == "current_month_minus_1": resolved.append(last_month)
        elif m == "current_month_minus_2": resolved.append(two_months_ago)
        else: resolved.append(m) # Assume literal if not code
    return sorted(list(set(resolved)))

def default_converter(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, bytes):
        return str(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def export_collection(db, coll_name, months, out_dir):
    coll = db[coll_name]

    # Filter logic
    query = {}
    if coll_name in [c["name"] for c in CONFIG["collections"]["outputs"]]:
        # Most outputs use period_month or month
        # We try both
        if coll.find_one({"period_month": {"$in": months}}):
            query = {"period_month": {"$in": months}}
        elif coll.find_one({"month": {"$in": months}}):
            query = {"month": {"$in": months}}
        # If Insurance has 'conversion_date' logic? Snapshot all for safety if small, or limit recent?
        # For simplicity in V2, if output collection has no month field found, we export ALL (assuming integration test DB size is managed)
        # OR we rely on 'updated_at' if available?
        # Let's stick to query={} if no month key match found to be safe.

    # Inputs: transaction tables usually huge.
    # In V2, we might want to filter Purchase_txn by date range implied by months.
    # But inactive logic needs history.
    # SAFETY: If running against PROD, exporting ALL `purchase_txn` is hazardous (millions of rows?).
    # We should add a limit or date filter for prod.
    # Logic: Start of earliest month - 6 months buffer?
    if "txn" in coll_name and months:
        earliest_month = min(months)
        dt_start = datetime.datetime.strptime(earliest_month, "%Y-%m") - datetime.timedelta(days=180) # 6 month buffer

        # Try finding date field
        date_field = None
        sample = coll.find_one()
        if sample:
            if "Date" in sample: date_field = "Date"
            elif "transactionDate" in sample: date_field = "transactionDate"

        if date_field:
            query = {date_field: {"$gte": dt_start}}

    try:
        cursor = coll.find(query)
        count = 0
        out_path = os.path.join(out_dir, f"{coll_name}.jsonl")

        with open(out_path, "w") as f:
            for doc in cursor:
                f.write(json.dumps(doc, default=default_converter) + "\n")
                count += 1
        print(f"  Captured {coll_name}: {count} records")
        return count
    except Exception as e:
        print(f"  Error capturing {coll_name}: {e}")
        return 0

def main():
    parser = argparse.ArgumentParser(description="Snapshot DB for Regression")
    parser.add_argument("--mode", choices=["prod", "nonprod"], default="nonprod", help="Safety gate")
    parser.add_argument("--months", help="Comma separated YYYY-MM")
    parser.add_argument("--out-dir", default=CONFIG["snapshot"]["baseline_dir"])
    args = parser.parse_args()

    # Safety Check
    if args.mode == "prod":
        allow_var = CONFIG["snapshot"]["allow_prod_snapshot_env"]
        if os.environ.get(allow_var) != "true":
            print(f"FATAL: Snapshotting PROD requires env {allow_var}=true")
            sys.exit(1)

    mongo_uri = os.getenv("MongoDb-Connection-String") or os.getenv("MONGODB_URI")
    if not mongo_uri:
        print("Error: MongoDb-Connection-String not found")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    target_months = get_target_months(args.months)

    # Detect DBs
    # Default is PLI_Leaderboard
    db_name = "PLI_Leaderboard"
    core_db_name = "iwell"

    # Verify core db exists
    dbs = client.list_database_names()
    if core_db_name not in dbs:
        # Maybe config override?
        core_db_name = os.getenv("CORE_DB_NAME", "iwell")

    print(f"Snapshotting from DBs: {db_name}, {core_db_name}")
    print(f"Target Months: {target_months}")

    os.makedirs(args.out_dir, exist_ok=True)

    stats = {}

    # Inputs
    for c in CONFIG["collections"]["inputs"]:
        # Routing: Core vs PLI
        # Heuristic: txn/leads in Core?
        # Based on Scorer code inputs:
        # Lumpsum: purchase_txn, redemption_txn, etc -> CORE
        # SIP: internal.transactions -> CORE (assumed, or PLI?)
        # Insurance: Insurance_Leads -> PLI (code used `get_pli_records` which called API, but we want to confirm if it writes to Mongo inputs or assume API only?)
        # Actually Insurance Code: reads API.
        # Referrals: reads API.
        # IF inputs are API-only, we can't snapshot them from Mongo unless we previously cached them.
        # BUT User request says: "Insurance_Leads", "Investment_leads" in "inputs_to_capture".
        # This implies they exist in Mongo.
        # Let's assume they are in PLI_Leaderboard or iwell.

        target_db = client[db_name]
        try_cols = [c]

        # Check existence
        final_coll_name = c
        if c in client[core_db_name].list_collection_names():
            target_db = client[core_db_name]
        elif c in client[db_name].list_collection_names():
            target_db = client[db_name]
        else:
            print(f"  Warning: Input collection {c} not found in {db_name} or {core_db_name}")
            continue

        cnt = export_collection(target_db, final_coll_name, target_months, args.out_dir)
        stats[c] = cnt

    # Outputs
    for c_obj in CONFIG["collections"]["outputs"]:
        c = c_obj["name"]
        target_db = client[db_name] # Outputs usually in PLI
        cnt = export_collection(target_db, c, target_months, args.out_dir)
        stats[c] = cnt

    # Metadata
    meta = {
        "captured_at": datetime.datetime.now(timezone.utc).isoformat(),
        "months": target_months,
        "db_source": { "leaderboard": db_name, "core": core_db_name },
        "counts": stats,
        "git_sha": os.popen("git rev-parse HEAD").read().strip()
    }
    with open(os.path.join(args.out_dir, "metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print("Snapshot complete.")

if __name__ == "__main__":
    main()
