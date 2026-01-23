import os
import sys
import json
import logging
import datetime
from datetime import timezone
import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Default Config Data (Taken from Scorer modules)
TIER_THRESHOLDS = [
    ("T6", 60000), ("T5", 40000), ("T4", 25000), ("T3", 15000),
    ("T2", 8000), ("T1", 2000), ("T0", -float("inf")),
]
TIER_MONTHLY_FACTORS = {
    "T0": 0.0, "T1": 0.000016667, "T2": 0.000020833, "T3": 0.000025000,
    "T4": 0.000029167, "T5": 0.000033333, "T6": 0.000037500,
}
SIP_POINTS_COEFF = 0.0288
LUMPSUM_POINTS_COEFF = 0.001

DEFAULT_RATE_SLABS = [
    {"min_pct": 0.0, "max_pct": 0.25, "rate": 0.0006, "label": "0–<0.25%"},
    {"min_pct": 0.25, "max_pct": 0.5, "rate": 0.0009, "label": "0.25–<0.5%"},
    {"min_pct": 0.5, "max_pct": 0.75, "rate": 0.00115, "label": "0.5–<0.75%"},
    {"min_pct": 0.75, "max_pct": 1.25, "rate": 0.00135, "label": "0.75–<1.25%"},
    {"min_pct": 1.25, "max_pct": 1.5, "rate": 0.00145, "label": "1.25–<1.5%"},
    {"min_pct": 1.5, "max_pct": 2.0, "rate": 0.00148, "label": "1.5–<2%"},
    {"min_pct": 2.0, "max_pct": None, "rate": 0.0015, "label": "≥2%"},
]
DEFAULT_MEETING_SLABS = [
    {"max_count": 5, "mult": 1.0, "label": "0–5"},
    {"max_count": 11, "mult": 1.05, "label": "6–11"},
    {"max_count": 17, "mult": 1.075, "label": "12–17"},
    {"max_count": None, "mult": 1.10, "label": "18+"},
]
DEFAULT_WEIGHTS = {
    "cob_in_pct": 50, "cob_out_pct": 120, "switch_in_pct": 100, "switch_out_pct": 100,
    "debt_bonus": {"enable": True, "bonus_pct": 20, "max_debt_ratio_pct": 75, "debt_categories": ["debt"]},
}
DEFAULT_LS_PENALTY_CFG = {
    "enable": True, "band1_trail_pct": 0.5, "band1_cap_rupees": 5000.0, "band2_rupees": 2500.0,
}

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
        logging.error("MongoDb-Connection-String env var not set and not found in local.settings.json")
        sys.exit(1)

    client = pymongo.MongoClient(uri)
    logging.info(f"Connected to DB: {db_name}")
    return client[db_name]

def seed_lumpsum(db):
    coll = db["config"]
    doc_id = "Leaderboard_Lumpsum"
    now_iso = datetime.datetime.now(timezone.utc).isoformat()

    doc = {
        "_id": doc_id,
        "schema": "Leaderboard_Lumpsum",
        "schema_version": "2025-11-15.r1",
        "status": "active",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "qtr_bonus_template": {"slabs": [{"min_np": 0, "bonus_rupees": 0}, {"min_np": 1000000, "bonus_rupees": 0}, {"min_np": 2500000, "bonus_rupees": 0}, {"min_np": 5000000, "bonus_rupees": 0}]},
        "annual_bonus_template": {"slabs": [{"min_np": 0, "bonus_rupees": 0}, {"min_np": 3000000, "bonus_rupees": 0}, {"min_np": 7500000, "bonus_rupees": 0}, {"min_np": 12000000, "bonus_rupees": 0}]},
        "rate_slabs": DEFAULT_RATE_SLABS,
        "meeting_slabs": DEFAULT_MEETING_SLABS,
        "ls_penalty": DEFAULT_LS_PENALTY_CFG,
        "weights": DEFAULT_WEIGHTS,
        "options": {
            "range_mode": "last5", "fy_mode": "FY_APR", "periodic_bonus_enable": False, "periodic_bonus_apply": True, "audit_mode": "compact"
        }
    }

    res = coll.replace_one({"_id": doc_id}, doc, upsert=True)
    logging.info(f"Seeded {doc_id}: Matched={res.matched_count}, Modified={res.modified_count}, Upserted={res.upserted_id}")

def seed_sip(db):
    coll = db["config"]
    doc_id = "Leaderboard_SIP"
    now_iso = datetime.datetime.now(timezone.utc).isoformat()

    doc = {
        "_id": doc_id,
        "schema": "Leaderboard_SIP",
        "schema_version": "2025-11-13.r1",
        "status": "active",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "options": {
            "range_mode": "month", "fy_mode": "FY_APR", "audit_mode": "compact",
            "ls_gate_pct": -3.0, "ls_gate_min_rupees": 50000.0,
            "sip_net_mode": "sip_only", "sip_include_swp_in_net": False,
            "swp_weights": {"registration": -1.0, "cancellation": 1.0},
            "sip_horizon_months": 24
        },
        "tier_thresholds": TIER_THRESHOLDS,
        "tier_monthly_factors": TIER_MONTHLY_FACTORS,
        "sip_points_coeff": SIP_POINTS_COEFF,
        "lumpsum_points_coeff": LUMPSUM_POINTS_COEFF
    }

    res = coll.replace_one({"_id": doc_id}, doc, upsert=True)
    logging.info(f"Seeded {doc_id}: Matched={res.matched_count}, Modified={res.modified_count}, Upserted={res.upserted_id}")

if __name__ == "__main__":
    db = get_db()
    seed_lumpsum(db)
    seed_sip(db)
