import logging
import azure.functions as func
import json
import os
import sys
from datetime import datetime
import pymongo
from bson import json_util

# Add parent dir to path to import scorer logic directly as SOT
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Scorers to access their constants directly
try:
    from Lumpsum_Scorer import (
        DEFAULT_RATE_SLABS, DEFAULT_MEETING_SLABS, DEFAULT_QTR_BONUS_JSON,
        DEFAULT_ANNUAL_BONUS_JSON, DEFAULT_LS_PENALTY_CFG, DEFAULT_WEIGHTS,
        RUNTIME_OPTIONS, FY_MODE, PERIODIC_BONUS_ENABLE, PERIODIC_BONUS_APPLY,
        CONFIG_COLL_ENV, CONFIG_DEFAULT_COLL, CONFIG_ID_ENV, CONFIG_DEFAULT_ID,
        SCHEMA_VERSION
    )
    from SIP_Scorer import (
        TIER_THRESHOLDS, TIER_MONTHLY_FACTORS, SIP_POINTS_COEFF,
        SIP_LS_GATE_PCT_DEFAULT, SIP_LS_GATE_MIN_RUPEES_DEFAULT,
        SIP_NET_MODE_DEFAULT, SIP_INCLUDE_SWP_IN_NET_DEFAULT,
        SWP_WEIGHTS_DEFAULT, SIP_HORIZON_MONTHS_DEFAULT,
        SIP_CONFIG_COLL_ENV, SIP_CONFIG_DEFAULT_COLL,
        SIP_CONFIG_ID_ENV, SIP_CONFIG_DEFAULT_ID,
        SCHEMA_VERSION_SIP
    )
    IMPORTS_OK = True
except Exception as e:
    logging.error(f"Failed to import scorer constants: {e}")
    IMPORTS_OK = False
    # Fallback minimal defaults
    DEFAULT_RATE_SLABS = []
    DEFAULT_MEETING_SLABS = []
    SCHEMA_VERSION = "unknown"
    SCHEMA_VERSION_SIP = "unknown"

# --- Constants & Config ---
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = os.getenv("DB_NAME", "PLI_Leaderboard")
APP_ENV = os.getenv("APP_ENV", "dev")

# SAFETY GUARD: Prevent test/dev code from accidentally writing to production DB
if APP_ENV != "prod" and DB_NAME == "PLI_Leaderboard":
    logging.error(f"SAFETY GUARD TRIGGERED: APP_ENV={APP_ENV} but DB_NAME={DB_NAME} (production). Set DB_NAME=PLI_Leaderboard_v2 for testing.")
    raise RuntimeError("Safety guard: Cannot use production DB in non-prod environment. Set DB_NAME=PLI_Leaderboard_v2")

logging.info(f"Admin_Scorer_API initialized: APP_ENV={APP_ENV}, DB_NAME={DB_NAME}")

def _get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Admin_Scorer_API processed a request.')

    # 1. basic auth check (can be swapped for RBAC middleware later)
    # For now assuming Gateway/EasyAuth handles protection or basic check

    module = req.route_params.get('module')
    method = req.method.lower()

    if module not in ['lumpsum', 'sip']:
        return func.HttpResponse("Invalid module. Use 'lumpsum' or 'sip'", status_code=400)

    try:
        if method == 'get':
            return get_config(module)
        elif method == 'put':
            return update_config(req, module)
        else:
             return func.HttpResponse("Method not supported", status_code=405)

    except Exception as e:
        logging.error(f"Error in Admin_Scorer_API: {e}", exc_info=True)
        return func.HttpResponse(f"Internal Server Error: {str(e)}", status_code=500)

def get_config(module: str):
    db = _get_db()

    if module == 'lumpsum':
        coll_name = os.getenv(CONFIG_COLL_ENV, CONFIG_DEFAULT_COLL)
        doc_id = os.getenv(CONFIG_ID_ENV, CONFIG_DEFAULT_ID)

        # Load Raw from Mongo
        raw_doc = db[coll_name].find_one({"_id": doc_id}) or {}

        # Merge with Defaults (Effective Config)
        effective = {
            "rate_slabs": raw_doc.get("rate_slabs", DEFAULT_RATE_SLABS),
            "meeting_slabs": raw_doc.get("meeting_slabs", DEFAULT_MEETING_SLABS),
            "qtr_bonus_template": raw_doc.get("qtr_bonus_template", DEFAULT_QTR_BONUS_JSON),
            "annual_bonus_template": raw_doc.get("annual_bonus_template", DEFAULT_ANNUAL_BONUS_JSON),
            "ls_penalty": raw_doc.get("ls_penalty", DEFAULT_LS_PENALTY_CFG),
            "weights": raw_doc.get("weights", DEFAULT_WEIGHTS),
            "options": {
                 "range_mode": raw_doc.get("options", {}).get("range_mode", RUNTIME_OPTIONS["range_mode"]),
                 "fy_mode": raw_doc.get("options", {}).get("fy_mode", FY_MODE),
                 "periodic_bonus_enable": raw_doc.get("options", {}).get("periodic_bonus_enable", PERIODIC_BONUS_ENABLE),
                 "periodic_bonus_apply": raw_doc.get("options", {}).get("periodic_bonus_apply", PERIODIC_BONUS_APPLY),
                 "apply_streak_bonus": raw_doc.get("options", {}).get("apply_streak_bonus", True),
                 "cob_in_correction_factor": raw_doc.get("options", {}).get("cob_in_correction_factor", 1.0)
            }
        }

    elif module == 'sip':
        coll_name = os.getenv(SIP_CONFIG_COLL_ENV, SIP_CONFIG_DEFAULT_COLL)
        doc_id = os.getenv(SIP_CONFIG_ID_ENV, SIP_CONFIG_DEFAULT_ID)

        raw_doc = db[coll_name].find_one({"_id": doc_id}) or {}

        effective = {
            "tier_thresholds": raw_doc.get("tier_thresholds", TIER_THRESHOLDS),
            "tier_monthly_factors": raw_doc.get("tier_monthly_factors", TIER_MONTHLY_FACTORS),
            "sip_points_coeff": raw_doc.get("sip_points_coeff", SIP_POINTS_COEFF),
            "options": {
                "ls_gate_pct": raw_doc.get("options", {}).get("ls_gate_pct", SIP_LS_GATE_PCT_DEFAULT),
                "ls_gate_min_rupees": raw_doc.get("options", {}).get("ls_gate_min_rupees", SIP_LS_GATE_MIN_RUPEES_DEFAULT),
                "sip_net_mode": raw_doc.get("options", {}).get("sip_net_mode", SIP_NET_MODE_DEFAULT),
                "sip_include_swp_in_net": raw_doc.get("options", {}).get("sip_include_swp_in_net", SIP_INCLUDE_SWP_IN_NET_DEFAULT),
                "swp_weights": raw_doc.get("options", {}).get("swp_weights", SWP_WEIGHTS_DEFAULT),
                "sip_horizon_months": raw_doc.get("options", {}).get("sip_horizon_months", SIP_HORIZON_MONTHS_DEFAULT)
            }
        }

    return func.HttpResponse(
        json.dumps({
            "module": module,
            "effective_config": effective,
            "raw_config": raw_doc,
            "schema_version": SCHEMA_VERSION if module == 'lumpsum' else SCHEMA_VERSION_SIP
        }, default=str),
        mimetype="application/json",
        headers={
            "X-DB-Name": DB_NAME,
            "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
            "Access-Control-Allow-Credentials": "true",
        }
    )

def update_config(req, module: str):
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    db = _get_db()

    # VALIDATION LOGIC 1:1 with Python constraints
    errors = validate_config(module, body)
    if errors:
        return func.HttpResponse(
            json.dumps({"errors": errors}), 
            status_code=400, 
            mimetype="application/json",
            headers={
              "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
              "Access-Control-Allow-Credentials": "true",
            } 
            )

    # PERSISTENCE
    if module == 'lumpsum':
        coll_name = os.getenv(CONFIG_COLL_ENV, CONFIG_DEFAULT_COLL)
        doc_id = os.getenv(CONFIG_ID_ENV, CONFIG_DEFAULT_ID)
    else:
        coll_name = os.getenv(SIP_CONFIG_COLL_ENV, SIP_CONFIG_DEFAULT_COLL)
        doc_id = os.getenv(SIP_CONFIG_ID_ENV, SIP_CONFIG_DEFAULT_ID)

    # Get current doc to increment version
    current = db[coll_name].find_one({"_id": doc_id}) or {}
    current_version = current.get("version", 0)

    # Add Audit Meta
    body['updatedAt'] = datetime.utcnow()
    body['version'] = current_version + 1
    # body['updatedBy'] = ... (get from auth headers if available - TODO)
    body['_id'] = doc_id  # Ensure _id is set for upsert

    # Preserve schema metadata
    body['schema'] = f"Leaderboard_{module.capitalize()}"
    body['schema_version'] = SCHEMA_VERSION if module == 'lumpsum' else SCHEMA_VERSION_SIP

    db[coll_name].replace_one(
        {"_id": doc_id},
        body,
        upsert=True
    )

    logging.info(f"Config updated for {module}: version {body['version']}, doc_id={doc_id}")

    # Return new state
    return get_config(module)

def validate_config(module: str, cfg: dict) -> list:
    errors = []

    if module == 'lumpsum':
        # Rate Slabs
        if 'rate_slabs' in cfg:
            for i, slab in enumerate(cfg['rate_slabs']):
                min_p = float(slab.get('min_pct', 0))
                max_p = slab.get('max_pct')
                rate = float(slab.get('rate', 0))
                if max_p is not None and min_p >= float(max_p):
                    errors.append(f"rate_slabs[{i}]: min_pct ({min_p}) must be < max_pct ({max_p})")
                if rate < 0:
                    errors.append(f"rate_slabs[{i}]: rate must be >= 0")

        # Meeting Slabs
        if 'meeting_slabs' in cfg:
            last_max = -1
            for i, slab in enumerate(cfg['meeting_slabs']):
                curr_max = slab.get('max_count')
                mult = float(slab.get('mult', 1.0))
                if mult < 1.0:
                    errors.append(f"meeting_slabs[{i}]: multiplier must be >= 1.0")
                if curr_max is not None:
                    if curr_max <= last_max:
                         errors.append(f"meeting_slabs[{i}]: max_count ({curr_max}) must be > prev ({last_max})")
                    last_max = curr_max

        # Options Enum checks
        if 'options' in cfg:
            opts = cfg['options']
            if 'range_mode' in opts and opts['range_mode'] not in ['last5', 'fy', 'since']:
                errors.append(f"options.range_mode: invalid value {opts['range_mode']}")
            if 'fy_mode' in opts and opts['fy_mode'] not in ['FY_APR', 'CAL']:
                errors.append(f"options.fy_mode: invalid value {opts['fy_mode']}")

    elif module == 'sip':
         # Tier Thresholds
        if 'tier_thresholds' in cfg:
             # Ensure pairs of [name, val]
             for i, item in enumerate(cfg['tier_thresholds']):
                 if not isinstance(item, (list, tuple)) or len(item) != 2:
                     errors.append(f"tier_thresholds[{i}]: must be [Name, Amount]")

        if 'options' in cfg:
            opts = cfg['options']
            if 'sip_net_mode' in opts and opts['sip_net_mode'] not in ['sip_only', 'sip_plus_swp']:
                 errors.append("options.sip_net_mode: invalid enum")

    return errors
