#!/usr/bin/env python3
import json
import os
import sys
import fnmatch
from typing import Any
import pymongo
from pymongo import MongoClient

# Load Config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "regression_config.json")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = "tools/regression_config.json"
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

DIFF_CFG = CONFIG["diff"]
OUTPUTS = CONFIG["collections"]["outputs"]

def get_replay_docs(client, db_name, coll_name, months):
    db = client[db_name]
    # Simple query for now
    # If "period_month" exists filter, else get all (assuming clean DB)
    return list(db[coll_name].find({}))

def load_baseline_docs(baseline_dir, coll_name):
    path = os.path.join(baseline_dir, f"{coll_name}.jsonl")
    if not os.path.exists(path):
        return []
    docs = []
    with open(path) as f:
        for line in f:
            docs.append(json.loads(line))
    return docs

def normalize_val(val, key_path=""):
    # Float rounding
    if isinstance(val, float):
        return round(val, DIFF_CFG["float_round_for_compare_decimals"])
    return val

def compare_docs(b, r, strict_fields):
    diffs = {}

    # Check strict fields
    for f in strict_fields:
        # Resolve ignored
        ignored = False
        for glob in DIFF_CFG["ignore_fields_glob"]:
            if fnmatch.fnmatch(f, glob): ignored = True; break
        if ignored: continue

        bv = b.get(f)
        rv = r.get(f)

        # Normalize
        nb = normalize_val(bv)
        nr = normalize_val(rv)

        # Tolerance check for floats
        is_float = isinstance(nb, (float, int)) and isinstance(nr, (float, int))
        if is_float:
            if abs(nb - nr) > DIFF_CFG["float_abs_tolerance"]:
                diffs[f] = {"base": bv, "replay": rv}
        else:
            if nb != nr:
                diffs[f] = {"base": bv, "replay": rv}
    return diffs

def get_pk(doc, pk_fields):
    vals = []
    for f in pk_fields:
        v = doc.get(f)
        if v is None: # Aliases?
            if f == "period_month": v = doc.get("month")
            elif f == "month": v = doc.get("period_month")
            elif f == "employee_id": v = doc.get("rm_name")
        vals.append(str(v))
    return "|".join(vals)

def main():
    mongo_uri = os.getenv("MongoDb-Connection-String")
    replay_db = os.environ.get(CONFIG["replay"]["replay_db_name_env"], "PLI_Leaderboard_TEST")
    baseline_dir = CONFIG["snapshot"]["baseline_dir"]

    client = MongoClient(mongo_uri)

    report = {"collections": {}, "summary": {"status": "PASS"}}
    fail = False

    for coll_def in OUTPUTS:
        name = coll_def["name"]
        pk = coll_def["primary_key"]
        strict = coll_def["strict_fields"]

        base_docs = load_baseline_docs(baseline_dir, name)
        rep_docs = get_replay_docs(client, replay_db, name, []) # Get all from replay DB

        pmap_base = {get_pk(d, pk): d for d in base_docs}
        pmap_rep = {get_pk(d, pk): d for d in rep_docs}

        missing = []
        extra = []
        mismatch = []

        keys = set(pmap_base.keys()) | set(pmap_rep.keys())
        for k in keys:
            if k not in pmap_base:
                extra.append(k)
                continue
            if k not in pmap_rep:
                missing.append(k)
                continue

            diffs = compare_docs(pmap_base[k], pmap_rep[k], strict)
            if diffs:
                mismatch.append({"key": k, "diffs": diffs})

        res = {
            "missing_count": len(missing),
            "extra_count": len(extra),
            "mismatch_count": len(mismatch),
            "mismatches": mismatch
        }
        report["collections"][name] = res

        if len(missing) > 0 and DIFF_CFG["treat_missing_as_fail"]: fail = True
        if len(mismatch) > 0: fail = True

    if fail: report["summary"]["status"] = "FAIL"

    print(json.dumps(report, indent=2))
    if fail: sys.exit(1)

if __name__ == "__main__":
    main()
