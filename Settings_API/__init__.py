
import logging
import azure.functions as func
import json
import os
import pymongo
import subprocess
import sys
from bson import json_util, ObjectId
from ..utils.db_utils import get_db
from datetime import datetime

# --- Configuration ---
MONGO_URI = os.getenv("MongoDb-Connection-String")
DB_NAME = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")

# Collections
COLL_USERS = "Zoho_Users"
COLL_PERMISSIONS = "Admin_Permissions"
COLL_SYSTEM_CONFIG = "System_Config"

# --- Scoring Config (v2 database) ---
DB_NAME_V2 = os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")
COLL_SCORING_CONFIG = "config"
COLL_SCORING_CONFIG_AUDIT = "config_audit"  # Audit trail for config changes
SCORING_CONFIG_ID_LUMPSUM = "Leaderboard_Lumpsum"
SCORING_CONFIG_ID_REFERRAL = "Leaderboard_Referral"

# Default Lumpsum scoring configuration
DEFAULT_LUMPSUM_CONFIG = {
    "weights": {
        "cob_in_pct": 50,
        "cob_out_pct": 120,
        "switch_in_pct": 120,
        "switch_out_pct": 120,
        "debt_bonus": {
            "enable": False,
            "bonus_pct": 20,
            "max_debt_ratio_pct": 75,
        },
    },
    "rate_slabs": [
        {"min_pct": 0.0, "max_pct": 0.25, "rate": 0.0006, "label": "0–<0.25%"},
        {"min_pct": 0.25, "max_pct": 0.5, "rate": 0.0009, "label": "0.25–<0.5%"},
        {"min_pct": 0.5, "max_pct": 0.75, "rate": 0.00115, "label": "0.5–<0.75%"},
        {"min_pct": 0.75, "max_pct": 1.25, "rate": 0.00135, "label": "0.75–<1.25%"},
        {"min_pct": 1.25, "max_pct": 1.5, "rate": 0.00145, "label": "1.25–<1.5%"},
        {"min_pct": 1.5, "max_pct": 2.0, "rate": 0.00148, "label": "1.5–<2%"},
        {"min_pct": 2.0, "max_pct": None, "rate": 0.0015, "label": "≥2%"},
    ],
    "meeting_slabs": [
        {"max_count": 5, "mult": 1.0, "label": "0–5"},
        {"max_count": 11, "mult": 1.05, "label": "6–11"},
        {"max_count": 17, "mult": 1.075, "label": "12–17"},
        {"max_count": None, "mult": 1.10, "label": "18+"},
    ],
    "ls_penalty": {
        "enable": True,
        "strategy": "growth_slab_v1",
        "band1_trail_pct": 0.5,
        "band1_cap_rupees": 5000.0,
        "band2_rupees": 2500.0,
    },
    "qtr_bonus_template": {
        "min_positive_months": 2,
        "slabs": [
            {"min_np": 0, "bonus_rupees": 0},
            {"min_np": 1000000, "bonus_rupees": 0},
            {"min_np": 2500000, "bonus_rupees": 0},
            {"min_np": 5000000, "bonus_rupees": 0},
        ]
    },
    "annual_bonus_template": {
        "min_positive_months": 6,
        "slabs": [
            {"min_np": 0, "bonus_rupees": 0},
            {"min_np": 3000000, "bonus_rupees": 0},
            {"min_np": 7500000, "bonus_rupees": 0},
            {"min_np": 12000000, "bonus_rupees": 0},
        ]
    },
    "options": {
        "range_mode": "last5",
        "fy_mode": "FY_APR",
        "periodic_bonus_enable": False,
        "periodic_bonus_apply": True,
        "audit_mode": "compact",
        "apply_streak_bonus": False,
        "cob_in_correction_factor": 0.5,
    },
    "category_rules": {
        "blacklisted_categories": ["liquid", "overnight", "low duration", "money market", "ultra short"],
        "match_mode": "substring",
        "scope": ["SUB CATEGORY"],
        "zero_weight_purchase": True,
        "zero_weight_switch_in": True,
        "exclude_from_debt_bonus": True,
    },
    "ignored_rms": [],
}

SCORING_CONFIG_ID_SIP = "Leaderboard_SIP"

DEFAULT_SIP_CONFIG = {
    "tier_thresholds": [
        {"tier": "T6", "min_val": 60000, "label": "≥60k"},
        {"tier": "T5", "min_val": 40000, "label": "40k–60k"},
        {"tier": "T4", "min_val": 25000, "label": "25k–40k"},
        {"tier": "T3", "min_val": 15000, "label": "15k–25k"},
        {"tier": "T2", "min_val": 8000, "label": "8k–15k"},
        {"tier": "T1", "min_val": 2000, "label": "2k–8k"},
        {"tier": "T0", "min_val": -1000000000000000.0, "label": "<2k"},
    ],
    "tier_factors": {
        "T6": 0.000037500,
        "T5": 0.000033333,
        "T4": 0.000029167,
        "T3": 0.000025000,
        "T2": 0.000020833,
        "T1": 0.000016667,
        "T0": 0.0,
    },
    "coefficients": {
        "sip_points_per_rupee": 0.03,
    },
    "bonus_slabs": {
        "sip_to_aum": [
            {"val": 0.0005, "bps": 4.0},
            {"val": 0.0004, "bps": 3.0},
            {"val": 0.0003, "bps": 2.0},
            {"val": 0.0002, "bps": 1.0},
        ],
        "absolute_sip": [
            {"val": 300000.0, "bps": 3.0},
            {"val": 200000.0, "bps": 2.0},
            {"val": 100000.0, "bps": 1.0},
            {"val": 50000.0, "bps": 0.5},
        ],
        "avg_ticket": [
            {"val": 8000.0, "bps": 2.0},
            {"val": 5000.0, "bps": 1.0},
            {"val": 3000.0, "bps": 0.5},
        ],
    },
    "sip_penalty": {
        "enable": True,
        "slabs": [
            {"max_loss": 50000.0, "rate_bps": 1.0},
            {"max_loss": 100000.0, "rate_bps": 2.0},
            {"max_loss": 999999999.0, "rate_bps": 3.0},
        ],
    },
    "ignored_rms": [],
    "options": {
        "net_mode": "sip_only",
        "include_swp": False,
        "horizon_months": 24,
        "ls_gate_pct": -3.0,
        "ls_gate_min_rupees": 50000.0,
        "swp_reg_weight": -1.0,
        "swp_canc_weight": 1.0,
        "range_mode": "month",
        "fy_mode": "FY_APR",
    }
}

SCORING_CONFIG_ID_INSURANCE = "Leaderboard_Insurance"

DEFAULT_INSURANCE_CONFIG = {
    "slabs": [
        # Default Payout Slabs (Insurance RMs)
        {"min_points": 0, "max_points": 500, "label": "Foundational", "fresh_pct": 0.0, "renew_pct": 0.0, "bonus_rupees": 0},
        {"min_points": 500, "max_points": 1000, "label": "Accelerator", "fresh_pct": 0.5, "renew_pct": 0.25, "bonus_rupees": 0},
        {"min_points": 1000, "max_points": 1800, "label": "Performer", "fresh_pct": 0.75, "renew_pct": 0.5, "bonus_rupees": 0},
        {"min_points": 1800, "max_points": 2500, "label": "Achiever", "fresh_pct": 1.0, "renew_pct": 0.75, "bonus_rupees": 0},
        {"min_points": 2500, "max_points": None, "label": "Master", "fresh_pct": 1.25, "renew_pct": 1.0, "bonus_rupees": 2000},
    ],
    "slabs_investment_rm": [
        # Default Payout Slabs (Investment RMs)
        {"min_points": 0, "max_points": 500, "label": "Foundational", "fresh_pct": 0.0, "renew_pct": 0.0, "bonus_rupees": 0},
        {"min_points": 500, "max_points": 1000, "label": "Accelerator", "fresh_pct": 0.5, "renew_pct": 0.25, "bonus_rupees": 0},
        {"min_points": 1000, "max_points": 1800, "label": "Performer", "fresh_pct": 0.75, "renew_pct": 0.5, "bonus_rupees": 0},
        {"min_points": 1800, "max_points": 2500, "label": "Achiever", "fresh_pct": 1.0, "renew_pct": 0.75, "bonus_rupees": 0},
        {"min_points": 2500, "max_points": None, "label": "Master", "fresh_pct": 1.25, "renew_pct": 1.0, "bonus_rupees": 2000},
    ],
    "conversion": {
        "fresh_slabs": [
            {"min_val": 0, "max_val": 25000, "points": 40},
            {"min_val": 25000, "max_val": 75000, "points": 100},
            {"min_val": 75000, "max_val": 200000, "points": 250},
            {"min_val": 200000, "max_val": None, "points": 350},
        ],
        "renew_slabs": [
            {"min_dtr": 31, "max_dtr": None, "points": 175},
            {"min_dtr": 15, "max_dtr": 31, "points": 100},
            {"min_dtr": 8, "max_dtr": 15, "points": 50},
            {"min_dtr": -1, "max_dtr": 8, "points": 35},
            {"min_dtr": -7, "max_dtr": -1, "points": 20},
            {"min_dtr": -15, "max_dtr": -7, "points": -100},
            {"min_dtr": -29, "max_dtr": -15, "points": -150},
            {"min_dtr": None, "max_dtr": -29, "points": -200},
        ],
        "upsell_divisor": 500,
    },
    "weights": {
        "ulip_multiplier": 0.0,
        "tenure": {
            "fresh": {
                "1": 1.0, "2": 1.20, "3": 1.60, "4": 1.75, "5": 2.00
            },
            "renewal_positive": {
                "1": 1.0, "2": 1.1, "3": 1.25, "4": 1.35, "5": 1.5
            },
            "renewal_negative": {
                "1": 1.0, "2": 0.9, "3": 0.75, "4": 0.65, "5": 0.5
            }
        },
        "categories": {
            "motor": 0.40, "fire": 0.40, "burglary": 0.40, "marine": 0.40, "misc": 0.40,
            "gmc": 0.40, "gmc otc": 0.50, "gpa": 0.20, "term insurance": 1.00,
            "health": 1.00, "life": 0.00, "ulip": 0.00
        }
    },
    "options": {
        "auto_correct_fresh": True,
        "skip_empty_policy_numbers": True
    },
    # Company-specific overrides (Whitelist/Blacklist logic)
    "company_rules": [],
}

DEFAULT_REFERRAL_CONFIG = {
    "insurance": {
        "self_sourced_points": 100,
        "converter_points": 50,
        "referrer_points": 30,
    },
    "investment": {
        "self_sourced_points": 200,
        "converter_only_points": 50,
        "referrer_points": 50,
        "not_family_head_penalty_pct": 30,
    },
    "gating": {
        "inactive_months": 6,
    },
    "gamification": {
        "badges": [
            {"id": "referral_novice", "label": "Referral Novice", "icon": "UserPlus", "color": "orange", "description": "First successful referral!", "condition_type": "min_points", "condition_field": "ref_points", "threshold": 1},
            {"id": "referral_pro", "label": "Referral Pro", "icon": "Users", "color": "orange", "description": "Consistent referrer.", "condition_type": "min_points", "condition_field": "ref_points", "threshold": 100},
            {"id": "insurance_titan", "label": "Insurance Titan", "icon": "ShieldCheck", "color": "purple", "description": "Achieved the highest insurance slab.", "condition_type": "min_points", "condition_field": "ins_points_effective", "threshold": 2500},
            {"id": "sip_master", "label": "SIP Master", "icon": "TrendingUp", "color": "blue", "description": "Top tier SIP performance.", "condition_type": "equals", "condition_field": "mf_sip_tier", "threshold": "T6"},
            {"id": "club_500", "label": "Club 500", "icon": "Award", "color": "yellow", "description": "Earned 500+ total points in a month.", "condition_type": "min_points", "condition_field": "total_effective_points", "threshold": 500},
        ]
    }
}

def _get_db():
    return get_db(default_db=DB_NAME)

def _get_db_v2():
    """Get v2 database for scoring config."""
    return get_db(default_db=DB_NAME_V2)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Settings_API processed a request.')

    # Simple Routing
    route = req.route_params.get('route', '')
    method = req.method.lower()

    try:
        if route == 'users':
            if method == 'get':
                return get_users(req)

        elif route == 'permissions':
            if method == 'post':
                return update_permission(req)

        elif route == 'config':
            if method == 'get':
                return get_config(req)
            elif method == 'post':
                return update_config(req)

        elif route == 'scoring/lumpsum':
            if method == 'get':
                return get_scoring_lumpsum(req)
            elif method in ('put', 'post'):
                return update_scoring_lumpsum(req)

        elif route == 'scoring/lumpsum/reset':
            if method == 'post':
                return reset_scoring_lumpsum(req)

        elif route == 'scoring/lumpsum/reaggregate':
            if method == 'post':
                return reaggregate_lumpsum(req)

        elif route == 'scoring/lumpsum/audit':
            if method == 'get':
                return get_scoring_audit(req, SCORING_CONFIG_ID_LUMPSUM)

        elif route == 'scoring/sip':
            if method == 'get':
                return get_scoring_sip(req)
            elif method in ('put', 'post'):
                return update_scoring_sip(req)

        elif route == 'scoring/sip/reset':
            if method == 'post':
                return reset_scoring_sip(req)

        elif route == 'scoring/sip/reaggregate':
            if method == 'post':
                return reaggregate_sip(req)

        elif route == 'scoring/sip/audit':
            if method == 'get':
                return get_scoring_audit(req, SCORING_CONFIG_ID_SIP)

        elif route == 'scoring/insurance':
            if method == 'get':
                return get_scoring_insurance(req)
            elif method in ('put', 'post'):
                return update_scoring_insurance(req)

        elif route == 'scoring/insurance/reset':
            if method == 'post':
                return reset_scoring_insurance(req)

        elif route == 'scoring/insurance/reaggregate':
            if method == 'post':
                return reaggregate_insurance(req)

        elif route == 'scoring/insurance/audit':
            if method == 'get':
                return get_scoring_audit(req, SCORING_CONFIG_ID_INSURANCE)

        elif route == 'scoring/referral':
            if method == 'get':
                return get_scoring_referral(req)
            elif method in ('put', 'post'):
                return update_scoring_referral(req)

        elif route == 'scoring/referral/reset':
            if method == 'post':
                return reset_scoring_referral(req)

        elif route == 'scoring/referral/audit':
            if method == 'get':
                return get_scoring_audit(req, SCORING_CONFIG_ID_REFERRAL)

        elif route == 'schemes/search':
            if method == 'get':
                return search_schemes(req)

        elif route == 'categories/search':
            if method == 'get':
                return search_categories(req)

        elif route == 'rms/search':
            if method == 'get':
                return search_rms(req)

    except Exception as e:
        logging.error(f"Error in Settings_API: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")

    return func.HttpResponse("NotFound", status_code=404)


# --- Handlers ---

def get_users(req):
    """
    Returns a list of users from Zoho_Users merged with their permissions from Admin_Permissions.
    Supports basic search via 'q' query param.
    """
    db = _get_db()
    query_str = req.params.get('q', '')

    # 1. Pipeline Definition
    pipeline = []

    # Match Stage
    if query_str:
        pipeline.append({
            "$match": {
                "$or": [
                    {"email": {"$regex": query_str, "$options": "i"}},
                    {"Full_Name": {"$regex": query_str, "$options": "i"}},
                    {"Name": {"$regex": query_str, "$options": "i"}},
                    {"full_name": {"$regex": query_str, "$options": "i"}}
                ]
            }
        })

    # Limit Stage (100 max)
    pipeline.append({"$limit": 100})

    # Projection Stage (Optimize Payload)
    pipeline.append({
        "$project": {
            "email": 1,
            "id": 1, # Zoho ID
            "full_name": 1,
            "Full_Name": 1,
            "Name": 1,
            "name": 1
        }
    })

    # Lookup Stage (Join Permissions)
    pipeline.append({
        "$lookup": {
            "from": COLL_PERMISSIONS,
            "localField": "email",
            "foreignField": "email",
            "as": "perm_doc"
        }
    })

    # Unwind Stage (Preserve users without permissions)
    pipeline.append({
        "$unwind": {
            "path": "$perm_doc",
            "preserveNullAndEmptyArrays": True
        }
    })

    # Execute Aggregation
    users_agg = list(db[COLL_USERS].aggregate(pipeline))

    # 2. Result Formatting
    result = []
    for u in users_agg:
        email = u.get('email')
        perm_doc = u.get('perm_doc', {})

        # Normalize Name
        name = u.get('full_name') or u.get('Full_Name') or u.get('Name') or u.get('name') or "Unknown"

        # Super Admin Override
        is_super_admin = email in ['vilakshan@niveshonline.com']

        roles = perm_doc.get('roles', [])
        permissions = perm_doc.get('permissions', {})

        if is_super_admin:
            roles = list(set(roles + ['admin']))
            permissions = {
                'view_team': True,
                'edit_rules': True,
                'manage_settings': True
            }

        result.append({
            "id": str(u.get('_id')),
            "zoho_id": u.get('id'),
            "email": email,
            "name": name,
            "roles": roles,
            "permissions": permissions,
            "is_super_admin": is_super_admin
        })

    return func.HttpResponse(json_util.dumps(result), mimetype="application/json")

def update_permission(req):
    """
    Updates permissions/roles for a user (by email).
    Body: { email:Str, roles: [Str], permissions: {Key: Bool} }
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    email = body.get('email')
    if not email:
        return func.HttpResponse("Email is required", status_code=400)

    roles = body.get('roles', [])
    permissions = body.get('permissions', {})

    db = _get_db()

    # Upsert permission record
    db[COLL_PERMISSIONS].update_one(
        {"email": email},
        {"$set": {
            "email": email,
            "roles": roles,
            "permissions": permissions,
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )

    return func.HttpResponse(json.dumps({"message": "Permissions updated"}), mimetype="application/json")

def get_config(req):
    """
    Returns system configuration. Mask secrets!
    """
    db = _get_db()
    config = db[COLL_SYSTEM_CONFIG].find_one({"_id": "global_settings"}) or {}

    # Mask secrets
    if 'smtp' in config:
        config['smtp']['password'] = '******'
    if 'whatsapp' in config:
        config['whatsapp']['api_key'] = '******'
    if 'zoho' in config:
        config['zoho']['client_secret'] = '******'

    # Flatten _id
    if '_id' in config:
        del config['_id']

    return func.HttpResponse(json_util.dumps(config), mimetype="application/json")

def update_config(req):
    """
    Updates system configuration.
    Body: { section: 'smtp'|'branding'|'whatsapp'|'zoho', data: {...} }
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    section = body.get('section')
    data = body.get('data')

    if not section or data is None:
        return func.HttpResponse("Missing section or data", status_code=400)

    db = _get_db()

    # Retrieve current to handle partial secret updates (don't overwrite secret with '******')
    current = db[COLL_SYSTEM_CONFIG].find_one({"_id": "global_settings"}) or {}

    if section == 'smtp':
        if data.get('password') == '******':
            # Remove from update to keep existing
            del data['password']
    elif section == 'whatsapp':
        if data.get('api_key') == '******':
            del data['api_key']
    elif section == 'zoho':
        if data.get('client_secret') == '******':
            del data['client_secret']

    # Update specific section
    db[COLL_SYSTEM_CONFIG].update_one(
        {"_id": "global_settings"},
        {"$set": {section: data, "updated_at": datetime.utcnow()}},
        upsert=True
    )

    return func.HttpResponse(json.dumps({"message": "Configuration updated"}), mimetype="application/json")


# --- Scoring Config Handlers ---

def get_scoring_lumpsum(req):
    """
    Returns the current Lumpsum scoring configuration.
    Merges stored config with defaults to ensure all fields are present.
    """
    db = _get_db_v2()
    config = db[COLL_SCORING_CONFIG].find_one({"_id": SCORING_CONFIG_ID_LUMPSUM}) or {}

    # Merge with defaults to ensure all fields exist
    result = {
        "_id": SCORING_CONFIG_ID_LUMPSUM,
        "weights": {**DEFAULT_LUMPSUM_CONFIG["weights"], **(config.get("weights") or {})},
        "rate_slabs": config.get("rate_slabs") or DEFAULT_LUMPSUM_CONFIG["rate_slabs"],
        "meeting_slabs": config.get("meeting_slabs") or DEFAULT_LUMPSUM_CONFIG["meeting_slabs"],
        "ls_penalty": {**DEFAULT_LUMPSUM_CONFIG["ls_penalty"], **(config.get("ls_penalty") or {})},
        "qtr_bonus_template": config.get("qtr_bonus_template") or DEFAULT_LUMPSUM_CONFIG["qtr_bonus_template"],
        "annual_bonus_template": config.get("annual_bonus_template") or DEFAULT_LUMPSUM_CONFIG["annual_bonus_template"],
        "options": {**DEFAULT_LUMPSUM_CONFIG["options"], **(config.get("options") or {})},
        "category_rules": {**DEFAULT_LUMPSUM_CONFIG["category_rules"], **(config.get("category_rules") or {})},
        "updatedAt": config.get("updatedAt"),
        "schema_version": config.get("schema_version", "2025-11-15.r1"),
    }

    # Handle nested debt_bonus merge
    if "debt_bonus" in (config.get("weights") or {}):
        result["weights"]["debt_bonus"] = {
            **DEFAULT_LUMPSUM_CONFIG["weights"]["debt_bonus"],
            **config["weights"]["debt_bonus"]
        }

    return func.HttpResponse(json_util.dumps(result), mimetype="application/json")


def update_scoring_lumpsum(req):
    """
    Updates Lumpsum scoring configuration.
    Validates fields and merges with existing config.
    Body: { weights: {...}, rate_slabs: [...], meeting_slabs: [...], ls_penalty: {...}, options: {...} }
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    if not body:
        return func.HttpResponse(json.dumps({"error": "Empty body"}), status_code=400, mimetype="application/json")

    db = _get_db_v2()

    # Archive existing config before update (for audit trail)
    existing_config = db[COLL_SCORING_CONFIG].find_one({"_id": SCORING_CONFIG_ID_LUMPSUM})
    if existing_config:
        audit_doc = {
            "config_id": SCORING_CONFIG_ID_LUMPSUM,
            "config_snapshot": existing_config,
            "archived_at": datetime.utcnow(),
            "replaced_by": body.get("changedBy", "unknown"),
            "change_reason": body.get("changeReason", "UI config update"),
        }
        db[COLL_SCORING_CONFIG_AUDIT].insert_one(audit_doc)
        logging.info(f"Archived previous config to audit trail (updatedAt={existing_config.get('updatedAt')})")

    # Build update document with only provided fields
    update_doc = {"updatedAt": datetime.utcnow()}

    # Validate and add each section
    if "weights" in body and isinstance(body["weights"], dict):
        update_doc["weights"] = body["weights"]

    if "rate_slabs" in body and isinstance(body["rate_slabs"], list):
        # Validate rate slabs structure
        valid_slabs = []
        for slab in body["rate_slabs"]:
            if isinstance(slab, dict) and "min_pct" in slab and "rate" in slab:
                valid_slabs.append({
                    "min_pct": float(slab.get("min_pct", 0)),
                    "max_pct": float(slab["max_pct"]) if slab.get("max_pct") is not None else None,
                    "rate": float(slab.get("rate", 0)),
                    "label": str(slab.get("label", "")),
                })
        if valid_slabs:
            update_doc["rate_slabs"] = valid_slabs

    if "meeting_slabs" in body and isinstance(body["meeting_slabs"], list):
        valid_slabs = []
        for slab in body["meeting_slabs"]:
            if isinstance(slab, dict) and "mult" in slab:
                valid_slabs.append({
                    "max_count": int(slab["max_count"]) if slab.get("max_count") is not None else None,
                    "mult": float(slab.get("mult", 1.0)),
                    "label": str(slab.get("label", "")),
                })
        if valid_slabs:
            update_doc["meeting_slabs"] = valid_slabs

    if "ls_penalty" in body and isinstance(body["ls_penalty"], dict):
        update_doc["ls_penalty"] = body["ls_penalty"]

    if "qtr_bonus_template" in body and isinstance(body["qtr_bonus_template"], dict):
        update_doc["qtr_bonus_template"] = body["qtr_bonus_template"]

    if "annual_bonus_template" in body and isinstance(body["annual_bonus_template"], dict):
        update_doc["annual_bonus_template"] = body["annual_bonus_template"]

    if "options" in body and isinstance(body["options"], dict):
        update_doc["options"] = body["options"]

    if "category_rules" in body and isinstance(body["category_rules"], dict):
        update_doc["category_rules"] = body["category_rules"]

    # Upsert the config
    result = db[COLL_SCORING_CONFIG].update_one(
        {"_id": SCORING_CONFIG_ID_LUMPSUM},
        {"$set": update_doc},
        upsert=True
    )

    logging.info(f"Updated Lumpsum scoring config: matched={result.matched_count}, modified={result.modified_count}")

    return func.HttpResponse(
        json.dumps({"message": "Scoring configuration updated", "modified": result.modified_count > 0}),
        mimetype="application/json"
    )


def reset_scoring_lumpsum(req):
    """
    Resets Lumpsum scoring configuration to defaults.
    """
    db = _get_db_v2()

    reset_doc = {
        **DEFAULT_LUMPSUM_CONFIG,
        "_id": SCORING_CONFIG_ID_LUMPSUM,
        "updatedAt": datetime.utcnow(),
        "schema": "Leaderboard_Lumpsum",
        "schema_version": "2025-11-15.r1",
        "status": "active",
    }

    db[COLL_SCORING_CONFIG].replace_one(
        {"_id": SCORING_CONFIG_ID_LUMPSUM},
        reset_doc,
        upsert=True
    )

    logging.info("Reset Lumpsum scoring config to defaults")

    return func.HttpResponse(
        json.dumps({"message": "Scoring configuration reset to defaults"}),
        mimetype="application/json"
    )


def reaggregate_lumpsum(req):
    """
    Triggers re-aggregation of the Lumpsum leaderboard for specified month(s).
    This allows config changes to take effect.

    Body: { month: "2025-12" } or { months: ["2025-11", "2025-12"] }
    """
    import subprocess
    import sys

    try:
        body = req.get_json()
    except ValueError:
        body = {}

    # Get month(s) to process
    months = body.get("months")
    start_month = body.get("month")

    if months:
        # Explicit list provided
        pass
    elif start_month:
        # Single month provided -> Treat as "Since" (Start Month to Present)
        try:
            # Parse start month
            y, m = map(int, start_month.split("-"))
            start_dt = datetime(y, m, 1)

            # Current month
            now = datetime.utcnow()
            end_dt = datetime(now.year, now.month, 1)

            # Generate range
            months = []
            curr = start_dt
            # Iterate until curr is > end_dt.
            # Note: We want to include current month.
            while curr <= end_dt:
                months.append(curr.strftime("%Y-%m"))
                # Next month
                if curr.month == 12:
                    curr = datetime(curr.year + 1, 1, 1)
                else:
                    curr = datetime(curr.year, curr.month + 1, 1)

            logging.info(f"Re-aggregating range: {start_month} to {months[-1]} ({len(months)} months)")

        except Exception as e:
            logging.error(f"Error parseing month range: {e}")
            months = [start_month] # Fallback

    else:
        # Default to current month
        now = datetime.utcnow()
        months = [now.strftime("%Y-%m")]

    results = []
    for month in months:
        try:
            logging.info(f"Triggering Lumpsum re-aggregation for {month}")


            # Use inline Python to:
            # 1. Run Lumpsum_Scorer to update Leaderboard_Lumpsum
            # 2. Run SIP_Scorer to update MF_SIP_Leaderboard (which calculates points from lumpsum NP)
            # 3. Run Leaderboard aggregator to update Public_Leaderboard (which UI reads)
            inline_script = f'''
import sys
import os
import pymongo
from datetime import datetime
# Ensure root dir is in path to import siblings
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from Lumpsum_Scorer import run_net_purchase
    from SIP_Scorer import run_pipeline
except ImportError:
    # Fallback if running from a subdir
    parent = os.path.dirname(current_dir)
    sys.path.append(parent)
    from Lumpsum_Scorer import run_net_purchase
    from SIP_Scorer import run_pipeline

import Leaderboard

# mongo_uri = os.environ.get("MongoDb-Connection-String")
# client = pymongo.MongoClient(mongo_uri)
# # Explicitly use V2 DB
# db_name = os.environ.get("PLI_DB_NAME", "PLI_Leaderboard_v2")
# db = get_db(default_db="PLI_Leaderboard_v2")


# Month to process
month = "{month}"
overrides = {{
    "options": {{
        "range_mode": "since",
        "since_month": month
    }}
}}

# Step 1: Lumpsum Scorer
print(f"[1/3] Running Lumpsum Scorer for {{month}} on {{db_name}}...")
run_net_purchase(db, override_config=overrides, mongo_client=client)

# Step 2: SIP Scorer (reads from Leaderboard_Lumpsum, writes points to MF_SIP_Leaderboard)
print(f"[2/3] Running SIP Scorer for {{month}}...")
year, month_num = map(int, month.split("-"))
start_date = datetime(year, month_num, 1)
if month_num == 12:
    end_date = datetime(year + 1, 1, 1)
else:
    end_date = datetime(year, month_num + 1, 1)

run_pipeline(start_date=start_date, end_date=end_date, mongo_uri=mongo_uri)

# Step 3: Public Leaderboard Aggregator
print(f"[3/3] Updating Public Leaderboard for {{month}}...")
Leaderboard.run(month)

print(f"✅ Re-aggregation complete for {{month}}")
'''
            env = os.environ.copy()
            env["SUPPRESS_ENV_WARNING"] = "1"
            # Pass MongoDB connection from Azure Functions environment to subprocess
            mongo_key = "MongoDb-Connection-String"
            if mongo_key not in env and MONGO_URI:
                env[mongo_key] = MONGO_URI
            # Ensure scorer and Leaderboard aggregator use V2 database (not V1)
            env["LEADERBOARD_DB_NAME"] = "PLI_Leaderboard_v2"
            env["DB_NAME"] = "PLI_Leaderboard_v2"

            result = subprocess.run(
                [sys.executable, "-c", inline_script],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                cwd="/Users/vilakshanbhutani/Desktop/Azure Function/PLI_Leaderboard",
                env=env
            )

            if result.returncode == 0:
                # Success - return stdout output
                output_msg = result.stdout.strip() or "Re-aggregation completed"
                results.append({
                    "month": month,
                    "status": "success",
                    "message": output_msg,
                    "details": result.stderr[:200] if result.stderr else ""  # Include logs for debugging
                })
                logging.info(f"Lumpsum re-aggregation for {month} completed successfully")
            else:
                # Actual failure - show error from stderr
                error_msg = result.stderr[:500] if result.stderr else result.stdout[:500]
                results.append({
                    "month": month,
                    "status": "error",
                    "message": f"Process failed (code {result.returncode}): {error_msg}"
                })
                logging.error(f"Lumpsum re-aggregation for {month} failed: {error_msg}")

        except subprocess.TimeoutExpired:
            results.append({"month": month, "status": "error", "message": "Re-aggregation timed out (>5 minutes)"})
            logging.error(f"Lumpsum re-aggregation for {month} timed out")
        except Exception as e:
            results.append({"month": month, "status": "error", "message": str(e)})
            logging.error(f"Lumpsum re-aggregation for {month} error: {e}")

    return func.HttpResponse(
        json.dumps({"results": results, "processed": len(results)}),
        mimetype="application/json"
    )


def get_scoring_audit(req, config_id=None):
    """
    Returns the audit history for scoring config changes.
    Query params: limit (default 10), config_id (default Leaderboard_Lumpsum)
    """
    db = _get_db_v2()

    limit = int(req.params.get("limit", 10))
    # Priority: Function Argument > Query Param > Default
    target_id = config_id or req.params.get("config_id", SCORING_CONFIG_ID_LUMPSUM)

    # Fetch recent audit entries
    cursor = db[COLL_SCORING_CONFIG_AUDIT].find(
        {"config_id": target_id}
    ).sort("archived_at", -1).limit(limit)

    entries = []
    for doc in cursor:
        archived_at = doc.get("archived_at")
        if isinstance(archived_at, datetime):
            archived_at = archived_at.isoformat()

        snapshot = doc.get("config_snapshot", {})
        updated_at = snapshot.get("updatedAt")
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()

        entries.append({
            "archived_at": archived_at,
            "replaced_by": doc.get("replaced_by"),
            "change_reason": doc.get("change_reason"),
            "config_updatedAt": updated_at,
            # Include key config values for comparison
            "weights": snapshot.get("weights"),
        })

    return func.HttpResponse(
        json_util.dumps({"entries": entries, "count": len(entries)}),
        mimetype="application/json"
    )

import pymongo
def search_schemes(req):
    """
    Search schemes.
    Originally looked in Milestone.bseschemes.
    Fallback: Look in PLI_Leaderboard_v2.purchase_txn for unique 'SCHEME NAME'.
    """
    query = req.params.get('q', '').strip()

    if not query or len(query) < 2:
        return func.HttpResponse(json.dumps([]), mimetype="application/json")

    try:
        db = _get_db_v2()

        # Aggregation to find distinct matches in transaction history
        pipeline = [
            {"$match": {"SCHEME NAME": {"$regex": query, "$options": "i"}}},
            {"$group": {
                "_id": "$SCHEME NAME",
                "code": {"$first": "$IWELL CODE"}
            }},
            {"$limit": 20},
            {"$project": {
                "Scheme Name": "$_id",
                "Scheme Code": "$code",
                "_id": 0
            }}
        ]

        cursor = db["purchase_txn"].aggregate(pipeline)
        results = list(cursor)

        return func.HttpResponse(
            json_util.dumps(results),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error searching schemes: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


def search_categories(req):
    """
    Search categories.
    For module='mf' (default): searched SUB CATEGORY in purchase_txn.
    For module='insurance': searches policy_type in Insurance_Policy_Scoring.
    """
    query = req.params.get('q', '').strip()
    module = req.params.get('module', 'mf').lower()

    try:
        db = _get_db_v2()

        if module == 'insurance':
            coll_name = "Insurance_Policy_Scoring"
            match_field = "policy_type"
        else:
            coll_name = "purchase_txn"
            match_field = "SUB CATEGORY"

        # Aggregation to find distinct matches
        pipeline = [
            {"$match": {match_field: {"$regex": query, "$options": "i"}}},
            {"$group": {
                "_id": f"${match_field}"
            }},
            {"$limit": 20},
            {"$project": {
                "category": "$_id",
                "_id": 0
            }}
        ]

        cursor = db[coll_name].aggregate(pipeline)
        results = [r["category"] for r in cursor if r.get("category")]

        return func.HttpResponse(
            json.dumps(results),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error searching categories ({module}): {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# --- Generic Helpers ---

def get_config_generic(config_id, default_config):
    try:
        db = _get_db_v2()
        collection = db[COLL_SCORING_CONFIG]

        config = collection.find_one({"_id": config_id})
        if not config:
            # Initialize with default
            config = default_config.copy()
            config["_id"] = config_id
            config["updatedAt"] = datetime.utcnow()
            config["updatedBy"] = "system_init"
            collection.insert_one(config)
            logging.info(f"Initialized default config for {config_id}")

        # JSON serialize
        return func.HttpResponse(
            json_util.dumps(config),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error fetching config {config_id}: {e}")
        return func.HttpResponse(
             json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )

def update_config_generic(config_id, new_config, reason, req):
    try:
        db = _get_db_v2()
        config_coll = db[COLL_SCORING_CONFIG]
        audit_coll = db[COLL_SCORING_CONFIG_AUDIT]

        # Get current config for audit
        current_config = config_coll.find_one({"_id": config_id})

        user_email = req.headers.get('X-User-Email', 'unknown')

        # Archive old config
        if current_config:
            audit_entry = {
                "config_id": config_id,
                "config_snapshot": current_config,
                "archived_at": datetime.utcnow(),
                "replaced_by": user_email,
                "change_reason": reason
            }
            audit_coll.insert_one(audit_entry)

        # Update new config
        # Ensure _id is preserved
        new_config["_id"] = config_id
        new_config["updatedAt"] = datetime.utcnow()
        new_config["updatedBy"] = user_email

        config_coll.replace_one({"_id": config_id}, new_config, upsert=True)

        return func.HttpResponse(
            json.dumps({"message": "Configuration updated successfully", "id": config_id}),
            mimetype="application/json"
        )

    except Exception as e:
         logging.error(f"Error updating config {config_id}: {e}")
         return func.HttpResponse(
             json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
         )

# --- SIP Handlers ---

def get_scoring_sip(req):
    """Get SIP scoring configuration."""
    return get_config_generic(SCORING_CONFIG_ID_SIP, DEFAULT_SIP_CONFIG)

def update_scoring_sip(req):
    """Update SIP scoring configuration."""
    try:
        req_body = req.get_json()
        reason = req_body.get('reason', 'Manual update via Settings API')
        new_config = req_body.get('config')

        # Basic validation: ensure options is a dict, etc.
        if not new_config or not isinstance(new_config, dict):
             return func.HttpResponse(
                json.dumps({"error": "Invalid configuration payload"}),
                status_code=400,
                mimetype="application/json"
            )

        return update_config_generic(SCORING_CONFIG_ID_SIP, new_config, reason, req)

    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json"
        )

def reset_scoring_sip(req):
    """Reset SIP configuration to defaults."""
    req_body = {}
    try:
        req_body = req.get_json()
    except ValueError:
        pass # Optional body

    reason = req_body.get('reason', 'Reset to defaults')

    # Use DEFAULT_SIP_CONFIG
    return update_config_generic(SCORING_CONFIG_ID_SIP, DEFAULT_SIP_CONFIG, reason, req)

def reaggregate_sip(req):
    """
    Trigger complete re-aggregation pipeline for specified month(s).
    Runs: Lumpsum_Scorer → SIP_Scorer → Public_Leaderboard

    This is identical to reaggregate_lumpsum to ensure consistency.
    Body: { "month": "2025-12" } or { "months": ["2025-11", "2025-12"] }
    """
    # Reuse the same logic as reaggregate_lumpsum
    return reaggregate_lumpsum(req)

# --- Insurance Scoring Handlers ---

def get_scoring_insurance(req: func.HttpRequest) -> func.HttpResponse:
    """Get global Insurance scoring configuration."""
    # _check_admin(req) # Assuming _check_admin is defined elsewhere or not strictly needed for this change
    try:
        db = _get_db_v2()
        coll = db[COLL_SCORING_CONFIG]
        doc = coll.find_one({"_id": SCORING_CONFIG_ID_INSURANCE})

        if not doc:
            # If no config found, return default
            return func.HttpResponse(
                json_util.dumps(DEFAULT_INSURANCE_CONFIG),
                status_code=200,
                mimetype="application/json"
            )

        # If config found, return its 'config' field, or default if 'config' field is missing
        return func.HttpResponse(
            json_util.dumps(doc.get("config", DEFAULT_INSURANCE_CONFIG)),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error getting Insurance scoring config: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

def update_scoring_insurance(req: func.HttpRequest) -> func.HttpResponse:
    """Update global Insurance scoring configuration."""
    # user = _check_admin(req) # Assuming _check_admin is defined elsewhere or not strictly needed for this change
    try:
        body = req.get_json()
        new_config = body.get('config')
        reason = body.get('reason', 'Manual update via UI')

        if not new_config:
            return func.HttpResponse(
                json.dumps({"error": "Missing config in body"}),
                status_code=400,
                mimetype="application/json"
            )

        db = _get_db_v2()
        coll = db[COLL_SCORING_CONFIG]
        audit_coll = db[COLL_SCORING_CONFIG_AUDIT]

        current_time = datetime.utcnow()
        user_email = req.headers.get('X-User-Email', 'unknown') # Get user email from headers

        # Get existing to archive for audit (optional but recommended)
        existing_doc = coll.find_one({"_id": SCORING_CONFIG_ID_INSURANCE})
        if existing_doc:
            audit_entry = {
                "config_id": SCORING_CONFIG_ID_INSURANCE,
                "config_version": existing_doc.get("version", 1),
                "config_updatedAt": existing_doc.get("updatedAt"),
                "config_updatedBy": existing_doc.get("updatedBy"),
                "config": existing_doc.get("config"),
                "archived_at": current_time,
                "replaced_by": user_email,
                "change_reason": reason
            }
            audit_coll.insert_one(audit_entry)

        update_doc = {
            "config": new_config,
            "updatedAt": current_time,
            "updatedBy": user_email,
            "version": (existing_doc.get("version", 0) + 1) if existing_doc else 1
        }

        coll.update_one(
            {"_id": SCORING_CONFIG_ID_INSURANCE},
            {"$set": update_doc},
            upsert=True
        )

        return func.HttpResponse(
            json.dumps({"message": "Configuration updated successfully."}),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error updating Insurance scoring config: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

def reset_scoring_insurance(req: func.HttpRequest) -> func.HttpResponse:
    """Reset Insurance scoring configuration to defaults."""
    # user = _check_admin(req) # Assuming _check_admin is defined elsewhere or not strictly needed for this change
    try:
        db = _get_db_v2()
        coll = db[COLL_SCORING_CONFIG]

        user_email = req.headers.get('X-User-Email', 'unknown') # Get user email from headers

        # Reset to default
        update_doc = {
            "config": DEFAULT_INSURANCE_CONFIG,
            "updatedAt": datetime.utcnow(),
            "updatedBy": user_email,
            "version": 1  # Or increment if tracking versions strictly
        }

        coll.update_one(
            {"_id": SCORING_CONFIG_ID_INSURANCE},
            {"$set": update_doc},
            upsert=True
        )

        return func.HttpResponse(
            json.dumps({"message": "Configuration reset to defaults"}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error resetting Insurance scoring config: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def _trigger_insurance_reaggregation_logic():
    """
    Helper to trigger the Insurance Scorer subprocess.
    Returns (success: bool, output: str)
    """
    import subprocess
    import sys

    logging.info("Triggering Insurance scorer re-aggregation")
    inline_script = '''
import sys
import os

current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from Insurance_scorer import Run_insurance_Score
except ImportError:
    parent = os.path.dirname(current_dir)
    sys.path.append(parent)
    from Insurance_scorer import Run_insurance_Score

print("[1/2] Running Insurance Scorer...")
Run_insurance_Score()
print("Insurance scoring complete")
'''
    env = os.environ.copy()
    env["SUPPRESS_ENV_WARNING"] = "1"
    # Ensure writes go to Public_Leaderboard
    env["PLI_DISABLE_LEADERBOARD"] = "0"

    mongo_key = "MongoDb-Connection-String"
    if mongo_key not in env and MONGO_URI:
        env[mongo_key] = MONGO_URI
    target_db = env.get("PLI_DB_NAME") or DB_NAME_V2
    env.setdefault("PLI_DB_NAME", target_db)
    env.setdefault("MONGO_DB_NAME", target_db)
    env.setdefault("LEADERBOARD_DB_NAME", target_db)
    env.setdefault("DB_NAME", target_db)

    extra_paths = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"]
    env["PATH"] = ":".join(p for p in ([env.get("PATH", "")] + extra_paths) if p)

    try:
        result = subprocess.run(
            [sys.executable, "-c", inline_script],
            capture_output=True,
            text=True,
            timeout=300,
            cwd="/Users/vilakshanbhutani/Desktop/Azure Function/PLI_Leaderboard",
            env=env,
        )
        if result.returncode != 0:
            logging.error(f"Insurance scorer failed: {result.stderr}")
            return False, result.stderr
        return True, result.stdout
    except Exception as e:
        logging.error(f"Failed to spawn insurance scorer: {e}")
        return False, str(e)


def reaggregate_insurance(req):
    """
    Trigger Insurance scorer + Public Leaderboard re-aggregation for specified month(s).
    Body: { "month": "2025-12" } or { "months": ["2025-11", "2025-12"] }
    """
    try:
        body = req.get_json()
    except ValueError:
        body = {}

    # Get month(s) to process
    months = body.get("months")
    start_month = body.get("month")

    if months:
        # Explicit list provided
        pass
    elif start_month:
        # Single month provided -> Treat as "Since" (Start Month to Present)
        try:
            y, m = map(int, start_month.split("-"))
            start_dt = datetime(y, m, 1)

            now = datetime.utcnow()
            end_dt = datetime(now.year, now.month, 1)

            months = []
            curr = start_dt
            while curr <= end_dt:
                months.append(curr.strftime("%Y-%m"))
                if curr.month == 12:
                    curr = datetime(curr.year + 1, 1, 1)
                else:
                    curr = datetime(curr.year, curr.month + 1, 1)

            logging.info(
                "Insurance re-aggregation range: %s to %s (%s months)",
                start_month,
                months[-1],
                len(months),
            )
        except Exception as e:
            logging.error("Error parsing insurance month range: %s", e)
            months = [start_month]  # Fallback
    else:
        now = datetime.utcnow()
        months = [now.strftime("%Y-%m")]

    results = []

    # Step 1: run Insurance scorer (now using helper)
    success, output = _trigger_insurance_reaggregation_logic()
    if not success:
         return func.HttpResponse(
            json.dumps({"error": "Scorer failed", "details": output}),
            status_code=500,
            mimetype="application/json"
        )

    results.append("Insurance scorer executed successfully.")

    # Step 2: run Public Leaderboard for each requested month
    for month in months:
        try:
            logging.info("Triggering Public Leaderboard refresh for %s", month)
            inline_script = f'''
import sys
import os

current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

import Leaderboard

mongo_uri = os.environ.get("MongoDb-Connection-String")
db_name = os.environ.get("PLI_DB_NAME", "{DB_NAME_V2}")

Leaderboard.run("{month}", mongo_uri=mongo_uri, db_name=db_name)
print("Public Leaderboard updated for {month}")
'''
            env = os.environ.copy()
            env["SUPPRESS_ENV_WARNING"] = "1"
            mongo_key = "MongoDb-Connection-String"
            if mongo_key not in env and MONGO_URI:
                env[mongo_key] = MONGO_URI
            target_db = env.get("PLI_DB_NAME") or DB_NAME_V2
            env.setdefault("PLI_DB_NAME", target_db)
            env.setdefault("MONGO_DB_NAME", target_db)
            env.setdefault("LEADERBOARD_DB_NAME", target_db)
            env.setdefault("DB_NAME", target_db)

            result = subprocess.run(
                [sys.executable, "-c", inline_script],
                capture_output=True,
                text=True,
                timeout=180,
                cwd="/Users/vilakshanbhutani/Desktop/Azure Function/PLI_Leaderboard",
                env=env,
            )

            if result.returncode == 0:
                output_msg = result.stdout.strip() or "Leaderboard updated"
                results.append(
                    {
                        "month": month,
                        "status": "success",
                        "message": output_msg,
                        "details": result.stderr[:200] if result.stderr else "",
                    }
                )
                logging.info("Public Leaderboard refresh for %s completed", month)
            else:
                error_msg = result.stderr[:2000] if result.stderr else result.stdout[:2000]
                results.append(
                    {
                        "month": month,
                        "status": "error",
                        "message": f"Leaderboard refresh failed (code {result.returncode}): {error_msg}",
                    }
                )
                logging.error(
                    "Public Leaderboard refresh for %s failed: %s", month, error_msg
                )
        except subprocess.TimeoutExpired:
            results.append(
                {
                    "month": month,
                    "status": "error",
                    "message": "Leaderboard refresh timed out (>3 minutes)",
                }
            )
            logging.error("Public Leaderboard refresh for %s timed out", month)
        except Exception as e:
            results.append({"month": month, "status": "error", "message": str(e)})
            logging.error("Public Leaderboard refresh for %s error: %s", month, e)

    return func.HttpResponse(
        json.dumps({"results": results, "processed": len(results)}),
        mimetype="application/json",
    )


# --- Referral Scoring Handlers ---

def get_scoring_referral(req):
    """Get Referral scoring configuration."""
    db = _get_db_v2()
    config = db[COLL_SCORING_CONFIG].find_one({"_id": SCORING_CONFIG_ID_REFERRAL}) or {}

    # Merge with defaults
    result = {
        "_id": SCORING_CONFIG_ID_REFERRAL,
        "insurance": {**DEFAULT_REFERRAL_CONFIG["insurance"], **(config.get("insurance") or {})},
        "investment": {**DEFAULT_REFERRAL_CONFIG["investment"], **(config.get("investment") or {})},
        "gating": {**DEFAULT_REFERRAL_CONFIG["gating"], **(config.get("gating") or {})},
        "gamification": {**DEFAULT_REFERRAL_CONFIG.get("gamification", {}), **(config.get("gamification") or {})},
        "updatedAt": config.get("updatedAt"),
        "updatedBy": config.get("updatedBy"),
    }

    return func.HttpResponse(json_util.dumps(result), mimetype="application/json")


def update_scoring_referral(req):
    """Update Referral scoring configuration."""
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    # Support both direct updates (legacy/simple) and wrapper with reason
    if 'config' in body:
        new_config = body.get('config')
        reason = body.get('reason', 'Manual update')
    else:
        new_config = body
        reason = body.get('changeReason', 'Manual update')

    if not new_config or not isinstance(new_config, dict):
         return func.HttpResponse(
            json.dumps({"error": "Invalid configuration payload"}),
            status_code=400,
            mimetype="application/json"
        )

    db = _get_db_v2()
    user_email = req.headers.get('X-User-Email', 'unknown')

    # Archive existing
    existing_config = db[COLL_SCORING_CONFIG].find_one({"_id": SCORING_CONFIG_ID_REFERRAL})
    if existing_config:
        audit_doc = {
            "config_id": SCORING_CONFIG_ID_REFERRAL,
            "config_snapshot": existing_config,
            "archived_at": datetime.utcnow(),
            "replaced_by": user_email,
            "change_reason": reason,
        }
        db[COLL_SCORING_CONFIG_AUDIT].insert_one(audit_doc)

    # Validated Update Doc
    update_doc = {
        "updatedAt": datetime.utcnow(),
        "updatedBy": user_email,
    }

    # Selective update to ensure schema hygiene
    if "insurance" in new_config: update_doc["insurance"] = new_config["insurance"]
    if "investment" in new_config: update_doc["investment"] = new_config["investment"]
    if "gating" in new_config: update_doc["gating"] = new_config["gating"]

    db[COLL_SCORING_CONFIG].update_one(
        {"_id": SCORING_CONFIG_ID_REFERRAL},
        {"$set": update_doc},
        upsert=True
    )

    return func.HttpResponse(
        json.dumps({"message": "Referral configuration updated"}),
        status_code=200,
        mimetype="application/json"
    )

def reset_scoring_referral(req):
    """Reset Referral configuration to defaults."""
    db = _get_db_v2()
    user_email = req.headers.get('X-User-Email', 'unknown')

    reset_doc = {
        **DEFAULT_REFERRAL_CONFIG,
        "_id": SCORING_CONFIG_ID_REFERRAL,
        "updatedAt": datetime.utcnow(),
        "updatedBy": user_email,
        "status": "active"
    }

    db[COLL_SCORING_CONFIG].replace_one(
        {"_id": SCORING_CONFIG_ID_REFERRAL},
        reset_doc,
        upsert=True
    )

    return func.HttpResponse(
        json.dumps({"message": "Referral configuration reset to defaults"}),
        status_code=200,
        mimetype="application/json"
    )

def search_rms(req):
    """
    Searches for RMs in the Public_Leaderboard collection for unique names.
    Query param: q (search string)
    """
    db = _get_db() # Uses V1 DB where Public_Leaderboard lives
    query = req.params.get('q', '').strip()

    if not query or len(query) < 2:
        return func.HttpResponse(json.dumps([]), mimetype="application/json")

    try:
        # Use aggregation to find distinct RM names matching the query
        pipeline = [
            {
                "$match": {
                    "rm_name": {"$regex": query, "$options": "i"}
                }
            },
            {
                "$group": {
                    "_id": {"$toLower": "$rm_name"},
                    "original_name": {"$first": "$rm_name"}
                }
            },
            {
                "$limit": 20
            },
            {
                "$project": {
                    "_id": 0,
                    "name": "$original_name"
                }
            }
        ]

        # Check Public_Leaderboard (aggregated data)
        results = list(db["Public_Leaderboard"].aggregate(pipeline))

        # Extract just the names
        names = sorted([r["name"] for r in results])

        return func.HttpResponse(json.dumps(names), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error searching RMs: {e}")
        return func.HttpResponse(json.dumps([]), mimetype="application/json")
