from __future__ import annotations
import pandas as pd
import math
from collections import defaultdict
import json
import logging
import sys
from typing import Dict, Any, cast
from datetime import datetime, timedelta
import pymongo
import re
import os
import requests
from pymongo.errors import DuplicateKeyError, PyMongoError
import azure.functions as func
from pymongo import ReturnDocument


import atexit
from types import SimpleNamespace
import hashlib
import os

# --- Database Name ---
DB_NAME = os.getenv("PLI_DB_NAME") or os.getenv("MONGO_DB_NAME") or "PLI_Leaderboard_v2"


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(name)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

logger = logging.getLogger("Lumpsum_Scorer")
logger.setLevel(logging.INFO)

for noisy_logger in (
    "pymongo",
    "pymongo.pool",
    "pymongo.topology",
    "pymongo.server",
    "pymongo.monitoring",
    "azure",
    "azure.identity",
    "azure.core",
    "urllib3",
):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


SCHEMA_VERSION = "2025-11-15.r1"

# --- Streak bonus settings (env overridable) ---
HATTRICK_BONUS = float(os.getenv("PLI_BONUS_HATTRICK", "500"))
FIVE_STREAK_BONUS = float(os.getenv("PLI_BONUS_FIVE", "500"))

# --- Lumpsum negative NP penalty (Mongo-configurable) ---
# These defaults mirror the new growth-slab rules (band1+band2).
DEFAULT_LS_PENALTY_CFG: dict[str, Any] = {
    "enable": True,  # master switch
    # Slabs: List of dicts {max_growth_pct, trail_pct, cap_rupees, flat_rupees}
    # Sorted automatically by max_growth_pct ascending during application.
    "slabs": [
        # Band 1 (High Negative): Growth <= -1.0%
        {
            "max_growth_pct": -1.0,
            "trail_pct": 0.5,
            "cap_rupees": 5000.0,
            "flat_rupees": 0.0,
        },
        # Band 2 (Moderate Negative): -1.0 < Growth <= -0.5%
        # Note: If logic checks "growth <= max" sequentially, this slab handles range (-1.0, -0.5] if placed after -1.0 check.
        # But to be safe, we sort by max_growth_pct ascending.
        # -1.0 is smaller than -0.5, so it hits first.
        {
            "max_growth_pct": -0.5,
            "trail_pct": 0.0,
            "cap_rupees": 0.0,
            "flat_rupees": 2500.0,
        },
    ],
}

# Penalty capping strategy for negative months:
# 'min'  -> softer (use smaller of flat rupees or % of monthly trail)
# 'max'  -> harsher (use larger of the two)
LS_PENALTY_STRATEGY = os.getenv("PLI_LS_PENALTY_STRATEGY", "min").strip().lower()


# Live snapshot (overridden from Mongo config)
LS_PENALTY_CFG: dict[str, Any] = dict(DEFAULT_LS_PENALTY_CFG)

# --- Periodic bonus options (quarterly / annual) ---
# Top-level enable switch
PERIODIC_BONUS_ENABLE = os.getenv("PLI_PERIODIC_BONUS_ENABLE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)

# Fiscal-year mode: "FY_APR" (default, Indian FY Apr→Mar) or "CAL" (Jan→Dec)
FY_MODE = os.getenv("PLI_FY_MODE", "FY_APR").strip().upper()

# Quarterly bonus config
QTR_BONUS_RUPEES = float(os.getenv("PLI_QTR_BONUS_RUPEES", "2000"))
QTR_MIN_POS_MONTHS = int(
    os.getenv("PLI_QTR_MIN_POS_MONTHS", "2")
)  # min positive months within quarter

# Annual bonus config
ANNUAL_BONUS_RUPEES = float(os.getenv("PLI_ANNUAL_BONUS_RUPEES", "10000"))
ANNUAL_MIN_POS_MONTHS = int(
    os.getenv("PLI_ANNUAL_MIN_POS_MONTHS", "6")
)  # min positive months within FY

# Apply or just report: set true to add the rupee bonus into final incentive
PERIODIC_BONUS_APPLY = os.getenv("PLI_PERIODIC_BONUS_APPLY", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)

# --- Central runtime options (env defaults, overridable via Mongo Config) ---
RUNTIME_OPTIONS: dict[str, Any] = {
    "range_mode": os.getenv("PLI_RANGE_MODE", "last5").strip().lower(),
    "fy_mode": FY_MODE,
    "periodic_bonus_enable": bool(PERIODIC_BONUS_ENABLE),
    "periodic_bonus_apply": bool(PERIODIC_BONUS_APPLY),
}

# In-process overrides (e.g. HTTP-triggered custom runs)
RUNTIME_OVERRIDES: dict[str, Any] = {}

# --- Periodic bonus JSON templates (advanced) ---
# You can define slabbed bonus structures via JSON in env:
# PLI_QTR_BONUS_JSON, PLI_ANNUAL_BONUS_JSON
# Structure example (4 slabs) — NP based:
# {
#   "slabs": [
#     {"min_np": 0,        "bonus_rupees": 0},
#     {"min_np": 1000000,  "bonus_rupees": 0},
#     {"min_np": 2500000,  "bonus_rupees": 0},
#     {"min_np": 5000000,  "bonus_rupees": 0}
#   ]
# }

DEFAULT_QTR_BONUS_JSON = {
    "slabs": [
        {"min_np": 0, "bonus_rupees": 0},
        {"min_np": 1_000_000, "bonus_rupees": 0},
        {"min_np": 2_500_000, "bonus_rupees": 0},
        {"min_np": 5_000_000, "bonus_rupees": 0},
    ]
}
DEFAULT_ANNUAL_BONUS_JSON = {
    "slabs": [
        {"min_np": 0, "bonus_rupees": 0},
        {"min_np": 3_000_000, "bonus_rupees": 0},
        {"min_np": 7_500_000, "bonus_rupees": 0},
        {"min_np": 12_000_000, "bonus_rupees": 0},
    ]
}

# --- Generic slab templates (env/Mongo overridable) ---
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

# Runtime-configurable slabs (initialized to defaults; can be overridden by Mongo/ENV)
RATE_SLABS = list(DEFAULT_RATE_SLABS)
MEETING_SLABS = list(DEFAULT_MEETING_SLABS)

DEFAULT_WEIGHTS = {
    "cob_in_pct": 50,
    "cob_out_pct": 120,
    "switch_in_pct": 120,  # Changed from 100 to match Legacy
    "switch_out_pct": 120,  # Changed from 100 to match Legacy
    "hattrick_bonus": 5000,
    "hattrick_threshold_pct": 0.1,
    "debt_bonus": {
        "enable": False,  # Changed from True - Legacy doesn't have debt bonus
        "bonus_pct": 20,
        "max_debt_ratio_pct": 75,
        "debt_categories": ["debt"],  # Substring match for debt categories
    },
    # Scheme-specific overrides (Whitelist/Blacklist logic)
    # List of { keyword: str, match_type: 'contains'|'exact', weight_pct: float }
    "scheme_rules": [],
}
# Defaults for Ignored RMs (initially empty)
SKIP_RM_ALIASES: set[str] = set()

WEIGHTS = dict(DEFAULT_WEIGHTS)

# --- Runtime config bootstrap (shared Config collection, multi-schema, versioned) ---
CONFIG_COLL_ENV = "PLI_CONFIG_COLL"
CONFIG_ID_ENV = "PLI_CONFIG_ID"
CONFIG_DEFAULT_COLL = "config"  # per-new layout (lowercase)
CONFIG_DEFAULT_ID = "Leaderboard_Lumpsum"  # distinct id per leaderboard/module
CONFIG_SCHEMA_NAME = "Leaderboard_Lumpsum"  # used to allow multiple schemas in same collection
CONFIG_STATUS_ACTIVE = "active"

# Separate Schema registry (shared across leaderboards)
SCHEMA_COLL_ENV = "PLI_SCHEMA_COLL"
SCHEMA_ID_ENV = "PLI_SCHEMA_ID"
SCHEMA_DEFAULT_COLL = "Schemas"  # capitalized as requested
SCHEMA_DEFAULT_ID = "Lumpsum_Schema"  # this module's schema doc id


# Will be set each run by run_net_purchase() after loading config
_LAST_CFG_HASH: str | None = None
db_leaderboard = None  # module-level placeholder to satisfy static checks; assigned in main()


# ---- Effective config snapshot + hash (for auditability/repro) ----
def _hash_dict(d: dict) -> str:
    try:
        s = json.dumps(d, sort_keys=True, separators=(",", ":"))
    except Exception:
        s = repr(d)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _effective_config_snapshot() -> dict:
    """
    Build a compact, serializable snapshot of slabs/templates/options actually in use
    so each computed row can be traced back to config.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "options": {
            "range_mode": RUNTIME_OPTIONS.get("range_mode"),
            "fy_mode": RUNTIME_OPTIONS.get("fy_mode"),
            "since_month": RUNTIME_OPTIONS.get("since_month"),
            "periodic_bonus_enable": bool(RUNTIME_OPTIONS.get("periodic_bonus_enable")),
            "periodic_bonus_apply": bool(RUNTIME_OPTIONS.get("periodic_bonus_apply")),
            "audit_mode": RUNTIME_OPTIONS.get("audit_mode", "compact"),
            "apply_streak_bonus": bool(RUNTIME_OPTIONS.get("apply_streak_bonus", True)),
            "cob_in_correction_factor": float(RUNTIME_OPTIONS.get("cob_in_correction_factor", 1.0)),
        },
        "rate_slabs": RATE_SLABS,
        "meeting_slabs": MEETING_SLABS,
        "qtr_bonus_template": QTR_BONUS_TEMPLATE,
        "annual_bonus_template": ANNUAL_BONUS_TEMPLATE,
        "ls_penalty_cfg": LS_PENALTY_CFG,
        "weights": WEIGHTS,
    }


# --- Schema registry helpers ---
def _default_schema_doc(schema_id: str) -> dict:
    """
    Return a schema-registry document for this module.
    Lives in PLI_Leaderboard / Schemas collection.
    Carries shape, keys, and default templates for remote inspection/change control.
    """
    now_iso = datetime.utcnow().isoformat()
    return {
        "_id": schema_id,
        "module": "Lumpsum_Scorer",
        "schema": CONFIG_SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "status": CONFIG_STATUS_ACTIVE,
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "description": "Schema registry doc for Lumpsum leaderboard; keeps canonical field layout and default templates.",
        "defaults": {
            "qtr_bonus_template": DEFAULT_QTR_BONUS_JSON,
            "annual_bonus_template": DEFAULT_ANNUAL_BONUS_JSON,
            "rate_slabs": DEFAULT_RATE_SLABS,
            "meeting_slabs": DEFAULT_MEETING_SLABS,
            "ls_penalty": DEFAULT_LS_PENALTY_CFG,
            "weights": DEFAULT_WEIGHTS,
            "category_rules": CATEGORY_RULES,
        },
        "keys": {
            "leaderboard_collection": "Leaderboard_Lumpsum",
            "metrics": ["NetPurchase", "Lumpsum"],
            "identity_fields": ["employee_id", "employee_name", "month"],
        },
        "meta": {
            "notes": "Auto-created by runtime bootstrap. Safe to edit values; preserve top-level keys."
        },
    }


def _default_config_doc(config_id: str) -> dict:
    """Return a default runtime-config document with proper schema + versioning."""
    now_iso = datetime.utcnow().isoformat()
    return {
        "_id": config_id,
        "schema": CONFIG_SCHEMA_NAME,  # enables multiple schemas in same collection
        "schema_version": SCHEMA_VERSION,  # code schema version
        "status": CONFIG_STATUS_ACTIVE,  # active/inactive toggle for future use
        "createdAt": now_iso,
        "updatedAt": now_iso,
        # Templates/slabs seeded from current defaults (can be overridden later)
        "qtr_bonus_template": DEFAULT_QTR_BONUS_JSON,
        "annual_bonus_template": DEFAULT_ANNUAL_BONUS_JSON,
        "rate_slabs": DEFAULT_RATE_SLABS,
        "meeting_slabs": DEFAULT_MEETING_SLABS,
        "ls_penalty": DEFAULT_LS_PENALTY_CFG,
        "weights": DEFAULT_WEIGHTS,
        # Remote-editable runtime options (env still has precedence)
        "options": {
            "range_mode": RUNTIME_OPTIONS.get("range_mode", "last5"),
            "fy_mode": RUNTIME_OPTIONS.get("fy_mode", FY_MODE),
            "periodic_bonus_enable": bool(
                RUNTIME_OPTIONS.get("periodic_bonus_enable", PERIODIC_BONUS_ENABLE)
            ),
            "periodic_bonus_apply": bool(
                RUNTIME_OPTIONS.get("periodic_bonus_apply", PERIODIC_BONUS_APPLY)
            ),
            "audit_mode": "compact",
        },
        # For forward compatibility / audit
        "meta": {
            "module": "Lumpsum_Scorer",
            "notes": "Auto-created by runtime bootstrap. Safe to edit fields, preserve schema keys.",
        },
    }


# --- Schema registry bootstrap ---
def _ensure_schema_bootstrap(db_leaderboard):
    """
    Ensure a schema-registry doc exists under PLI_Leaderboard / Schemas.
    Idempotent; updates schema + version and bumps updatedAt each run.
    """
    try:
        coll_name = os.getenv(SCHEMA_COLL_ENV, SCHEMA_DEFAULT_COLL).strip()
        doc_id = os.getenv(SCHEMA_ID_ENV, SCHEMA_DEFAULT_ID).strip()
        col = db_leaderboard[coll_name]
        try:
            col.create_index([("schema", 1)])
            col.create_index([("status", 1)])
        except Exception:
            pass

        now_iso = datetime.utcnow().isoformat()
        default_doc = _default_schema_doc(doc_id)

        on_insert = dict(default_doc)
        for k in ("updatedAt", "schema_version", "schema"):
            on_insert.pop(k, None)

        update_ops = {
            "$setOnInsert": on_insert,
            "$set": {
                "schema": CONFIG_SCHEMA_NAME,
                "schema_version": SCHEMA_VERSION,
            },
            "$currentDate": {"updatedAt": True},
        }

        res = col.find_one_and_update(
            {"_id": doc_id},
            update_ops,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            logging.info("[Schema] Bootstrapped/ensured schema registry: %s/%s", coll_name, doc_id)
        return res
    except Exception as _e:
        logging.warning("[Schema] Bootstrap ensure failed: %s", _e)
        return None


def _ensure_config_bootstrap(db_leaderboard):
    """
    Ensure a config doc exists for this schema in the shared collection.
    If missing, insert a default document with proper schema+version.
    Always update 'updatedAt' on touch. Idempotent.
    """
    try:
        coll_name = os.getenv(CONFIG_COLL_ENV, CONFIG_DEFAULT_COLL).strip()
        doc_id = os.getenv(CONFIG_ID_ENV, CONFIG_DEFAULT_ID).strip()
        col = db_leaderboard[coll_name]
        # Helpful indexes for future multi-schema / querying:
        try:
            col.create_index([("schema", 1)])
            col.create_index([("status", 1)])
        except Exception:
            pass

        # Back-compat: if collection is empty and default is 'config' but old 'Config' exists, read from it once
        try:
            if col.estimated_document_count() == 0 and CONFIG_DEFAULT_COLL == "config":
                legacy_col = db_leaderboard["Config"]
                legacy = legacy_col.find_one(
                    {"_id": os.getenv(CONFIG_ID_ENV, CONFIG_DEFAULT_ID).strip()}
                )
                if legacy and not db_leaderboard[coll_name].find_one({"_id": legacy.get("_id")}):
                    db_leaderboard[coll_name].insert_one(legacy)
                    logging.info("[Config] Migrated legacy doc from 'Config' to 'config'.")
        except Exception:
            pass

        # Upsert default-on-missing, and bump updatedAt on every call
        now_iso = datetime.utcnow().isoformat()
        default_doc = _default_config_doc(doc_id)

        # Avoid ConflictingUpdateOperators: do NOT set the same field in $setOnInsert and $set/$currentDate
        on_insert = dict(default_doc)
        # Ensure these are **not** set via $setOnInsert
        for k in ("updatedAt", "schema_version", "schema"):
            on_insert.pop(k, None)

        update_ops: dict[str, dict] = {
            "$setOnInsert": on_insert,
            "$set": {
                # schema tag + version can safely be refreshed each run
                "schema": CONFIG_SCHEMA_NAME,
                "schema_version": SCHEMA_VERSION,
            },
            # updatedAt should only be set here to avoid conflicts
            "$currentDate": {"updatedAt": True},
        }

        # Optional: tiny debug footprint to inspect operators if we hit this path again
        try:
            logging.debug(
                "[Config] Upsert ops keys=%s set_keys=%s setOnInsert_keys=%s",
                list(update_ops.keys()),
                list(update_ops.get("$set", {}).keys()),
                list(update_ops.get("$setOnInsert", {}).keys())[:6],
            )
        except Exception:
            pass

        res = col.find_one_and_update(
            {"_id": doc_id},
            update_ops,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res and "createdAt" in res and res.get("createdAt"):
            # If document was just inserted, createdAt will exist; this message is harmless on re-runs.
            logging.info(
                "[Config] Bootstrapped default runtime config (exists or created): %s/%s",
                coll_name,
                doc_id,
            )
        return res
    except Exception as _e:
        logging.warning("[Config] Bootstrap ensure failed: %s", _e)
        return None


def _rate_from_slabs(growth_pct: float) -> tuple[float, str]:
    """Return (rate, label) from RATE_SLABS for given growth_pct."""
    try:
        v = float(growth_pct)
    except Exception:
        v = 0.0
    for slab in RATE_SLABS:
        lo = float(slab.get("min_pct", 0.0) or 0.0)
        hi = slab.get("max_pct", None)
        if hi is None:
            if v >= lo:
                return float(slab.get("rate", 0.0) or 0.0), str(slab.get("label", ""))
        else:
            try:
                hi_f = float(hi)
            except Exception:
                hi_f = None
            if hi_f is not None and (v >= lo) and (v < hi_f):
                return float(slab.get("rate", 0.0) or 0.0), str(slab.get("label", f"{lo}–<{hi}"))
    # Fallback: no match
    return 0.0, "<2%" if v < 2.0 else "≥2%"


def _meeting_from_slabs(count: int) -> tuple[float, str]:
    """Return (multiplier, label) from MEETING_SLABS for given meeting count."""
    try:
        c = int(count)
    except Exception:
        c = 0
    for slab in MEETING_SLABS:
        cap = slab.get("max_count", None)
        if cap is None:
            return float(slab.get("mult", 1.0) or 1.0), str(slab.get("label", ""))
        try:
            cap_i = int(cap)
        except Exception:
            cap_i = None
        if cap_i is not None and c <= cap_i:
            return float(slab.get("mult", 1.0) or 1.0), str(slab.get("label", ""))
    return 1.0, "0–5"


def _load_bonus_template(env_key: str, default_obj: dict) -> dict:
    """Parse JSON from env into a dict; return a safe default on failure."""
    raw = os.getenv(env_key, "").strip()
    if not raw:
        return default_obj
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and "slabs" in obj and isinstance(obj["slabs"], list):
            # Normalize items
            cleaned = []
            for it in obj["slabs"]:
                try:
                    min_np = it.get("min_np", None)
                    if min_np is None:
                        # Back-compat: allow min_pos_months but convert to NP=0 (effectively disabled unless overridden)
                        _legacy = it.get("min_pos_months", 0)
                        try:
                            _ = int(_legacy)  # validate int
                        except Exception:
                            _ = 0
                        min_np = 0
                    cleaned.append(
                        {
                            "min_np": float(min_np) if min_np is not None else 0.0,
                            "bonus_rupees": float(it.get("bonus_rupees", 0) or 0),
                        }
                    )
                except Exception:
                    continue
            if cleaned:
                return {"slabs": cleaned}
        return default_obj
    except Exception:
        logging.warning(f"[Bonus] Failed to parse JSON for {env_key}; using defaults.")
        return default_obj


def _select_np_slab_bonus(np_value: float, template: dict) -> tuple[float, dict]:
    """
    Pick the highest qualifying NP slab for the given cumulative NP value.
    Returns (bonus_rupees, picked_meta).
    """
    try:
        v = float(np_value or 0.0)
    except Exception:
        v = 0.0
    slabs = sorted(template.get("slabs", []), key=lambda x: float(x.get("min_np", 0.0)))
    picked = {"min_np": None, "bonus_rupees": 0.0, "index": None}
    bonus = 0.0
    for idx, slab in enumerate(slabs):
        try:
            threshold = float(slab.get("min_np", 0.0) or 0.0)
            b = float(slab.get("bonus_rupees", 0) or 0)
        except Exception:
            continue
        if v >= threshold:
            picked = {"min_np": threshold, "bonus_rupees": b, "index": idx}
            bonus = b
    return float(bonus), picked


# Load templates (defaults to 4 slabs with zeros)

QTR_BONUS_TEMPLATE = _load_bonus_template("PLI_QTR_BONUS_JSON", DEFAULT_QTR_BONUS_JSON)
ANNUAL_BONUS_TEMPLATE = _load_bonus_template("PLI_ANNUAL_BONUS_JSON", DEFAULT_ANNUAL_BONUS_JSON)

# --- Config from Mongo (optional, same DB for remote tweaks) ---
# Env knobs: PLI_CONFIG_COLL (default: 'Config'), PLI_CONFIG_ID (default: 'Leaderboard_Lumpsum')


def _merge_list_of_dicts(dst: list[dict], src: list[dict], keys: tuple[str, ...]) -> list[dict]:
    """Merge src into dst by matching any of the provided keys; on conflict, src wins."""
    if not isinstance(dst, list):
        dst = []
    if not isinstance(src, list):
        return dst

    def _key(d: dict):
        for k in keys:
            if k in d:
                return (k, str(d.get(k)))
        return ("_", json.dumps(d, sort_keys=True))

    out = {_key(d): dict(d) for d in dst}
    for d in src:
        out[_key(d)] = dict(d)
    return list(out.values())


def _init_runtime_config(db_leaderboard, override_cfg: dict | None = None):
    """Fetch optional config doc from Mongo (or use override) and override templates/slabs in-memory.
    Expects a collection `<PLI_CONFIG_COLL>` with doc `_id=<PLI_CONFIG_ID>`.
    Recognized fields:
      - qtr_bonus_template: {slabs:[{min_np, bonus_rupees},...]}
      - annual_bonus_template: {slabs:[{min_np, bonus_rupees},...]}
      - rate_slabs: [{min_pct, max_pct|null, rate, label?}, ...]
      - meeting_slabs: [{max_count|null, mult, label?}, ...]
    """
    global QTR_BONUS_TEMPLATE, ANNUAL_BONUS_TEMPLATE, RATE_SLABS, MEETING_SLABS
    global FY_MODE, PERIODIC_BONUS_ENABLE, PERIODIC_BONUS_APPLY, RUNTIME_OPTIONS
    global LS_PENALTY_CFG, WEIGHTS
    try:
        # Ensure schema registry doc exists (Schemas)
        _ensure_schema_bootstrap(db_leaderboard)
        # Ensure a versioned, schema-tagged config document exists (shared Config collection)
        # We ignore the return value to perform a fresh, consistent find_one below
        _ensure_config_bootstrap(db_leaderboard)

        # 1. Fetch from DB
        coll_name = os.getenv("PLI_CONFIG_COLL", CONFIG_DEFAULT_COLL).strip()  # default 'config'
        doc_id = os.getenv("PLI_CONFIG_ID", CONFIG_DEFAULT_ID).strip()
        col = db_leaderboard[coll_name]

        # Explicit fetch to ensure we get the full document as verified by inspection tools
        doc = col.find_one({"_id": doc_id}) or {}

        # CRITICAL FIX: The Settings API saves the actual config inside a "config" key
        # Handle both flat and nested structures robustly
        cfg = doc.get("config") if (doc.get("config") and isinstance(doc.get("config"), dict)) else doc

        # 2. Merge overrides if present
        if override_cfg:
            logging.info("[Config] Applying OVERRIDE config on top of DB config.")
            # Shallow merge for top-level keys
            # For deeper merging (like 'options'), we might need more logic,
            # but usually overrides are distinct keys or complete objects.
            # Assuming 'options' in override replaces 'options' in DB is safer for now
            # unless we specifically want partial option updates.
            # Let's do a smart update for specific dicts if needed.

            # Simple recursive update for critical dictionaries
            for key, val in override_cfg.items():
                if isinstance(val, dict) and isinstance(cfg.get(key), dict):
                    # Update sub-dict
                    cfg[key].update(val)
                else:
                    # Overwrite
                    cfg[key] = val

        if cfg:
            # 1) Slabs/templates
            qb = cfg.get("qtr_bonus_template")
            if isinstance(qb, dict) and qb.get("slabs"):
                QTR_BONUS_TEMPLATE = qb

            ab = cfg.get("annual_bonus_template")
            if isinstance(ab, dict) and ab.get("slabs"):
                ANNUAL_BONUS_TEMPLATE = ab

            rs = cfg.get("rate_slabs")
            if isinstance(rs, list) and rs:
                RATE_SLABS = _merge_list_of_dicts(DEFAULT_RATE_SLABS, rs, ("min_pct", "max_pct"))

            ms = cfg.get("meeting_slabs")
            if isinstance(ms, list) and ms:
                MEETING_SLABS = _merge_list_of_dicts(DEFAULT_MEETING_SLABS, ms, ("max_count",))

            lp = cfg.get("ls_penalty")
            if isinstance(lp, dict):
                merged_lp = dict(DEFAULT_LS_PENALTY_CFG)
                for k, v in lp.items():
                    if v is not None:
                        merged_lp[k] = v
                LS_PENALTY_CFG = merged_lp

            w = cfg.get("weights")
            if isinstance(w, dict):
                merged_w = dict(DEFAULT_WEIGHTS)
                for k, v in w.items():
                    if v is not None:
                        merged_w[k] = v
                # Update in-place to ensure all references to WEIGHTS see the change
                WEIGHTS.clear()
                WEIGHTS.update(merged_w)

            # 2) Runtime options from Mongo (override env defaults)
            opts = cfg.get("options") or {}
            if isinstance(opts, dict):
                # range_mode
                try:
                    rm = (
                        str(opts.get("range_mode", RUNTIME_OPTIONS.get("range_mode", "last5")))
                        .strip()
                        .lower()
                    )
                    if rm in ("last5", "fy", "since"):
                        RUNTIME_OPTIONS["range_mode"] = rm
                except Exception:
                    pass

                # fy_mode
                try:
                    fy = str(opts.get("fy_mode", FY_MODE)).strip().upper()
                    if fy in ("FY_APR", "CAL"):
                        FY_MODE = fy
                        RUNTIME_OPTIONS["fy_mode"] = fy
                except Exception:
                    pass

                # periodic_bonus_enable
                try:
                    pbe = opts.get("periodic_bonus_enable", PERIODIC_BONUS_ENABLE)
                    PERIODIC_BONUS_ENABLE = bool(pbe)
                    RUNTIME_OPTIONS["periodic_bonus_enable"] = bool(pbe)
                except Exception:
                    pass

                # periodic_bonus_apply
                try:
                    pba = opts.get("periodic_bonus_apply", PERIODIC_BONUS_APPLY)
                    PERIODIC_BONUS_APPLY = bool(pba)
                    RUNTIME_OPTIONS["periodic_bonus_apply"] = bool(pba)
                except Exception:
                    pass

                # audit_mode
                try:
                    am = (
                        str(opts.get("audit_mode", RUNTIME_OPTIONS.get("audit_mode", "compact")))
                        .strip()
                        .lower()
                    )
                    if am in ("compact", "full"):
                        RUNTIME_OPTIONS["audit_mode"] = am
                except Exception:
                    pass

                # streak bonus
                try:
                    asb = opts.get("apply_streak_bonus", True)
                    RUNTIME_OPTIONS["apply_streak_bonus"] = bool(asb)
                except Exception:
                    pass

                # cob correction
                try:
                    ccf = opts.get("cob_in_correction_factor", 1.0)
                    RUNTIME_OPTIONS["cob_in_correction_factor"] = float(ccf)
                except Exception:
                    pass

            # [NEW] Load Ignored RMs
            ign = cfg.get("ignored_rms")
            if isinstance(ign, list):
                SKIP_RM_ALIASES.clear()
                for v in ign:
                    if v:
                        SKIP_RM_ALIASES.add(str(v).strip().lower())
                logging.info("[Config] Updated SKIP_RM_ALIASES: %d entries", len(SKIP_RM_ALIASES))

            # 3) Category rules (don't let this kill config if it's buggy)
            try:
                _load_category_rules_from_cfg(cfg)
            except Exception as e:
                logging.warning("[Config] Category rules load failed: %s", e)

            # 4) Now log the *effective* config
            logging.info("[Config] Loaded runtime config from Mongo: %s/%s", coll_name, doc_id)
            try:
                logging.info(
                    "[Config] Options: range_mode=%s fy_mode=%s periodic_bonus_enable=%s periodic_bonus_apply=%s audit_mode=%s",
                    RUNTIME_OPTIONS.get("range_mode"),
                    FY_MODE,
                    PERIODIC_BONUS_ENABLE,
                    PERIODIC_BONUS_APPLY,
                    RUNTIME_OPTIONS.get("audit_mode", "compact"),
                )
                logging.info(
                    "[Config] Slabs: rate=%d meeting=%d qtr_slabs=%d annual_slabs=%d",
                    len(RATE_SLABS or []),
                    len(MEETING_SLABS or []),
                    len((QTR_BONUS_TEMPLATE or {}).get("slabs", [])),
                    len((ANNUAL_BONUS_TEMPLATE or {}).get("slabs", [])),
                )
            except Exception:
                pass
            # Category blacklist / matching rules (optional section)
            _load_category_rules_from_cfg(cfg)
            # Runtime options (range/fy/periodic bonus) from Mongo
            opts = cfg.get("options") or {}
            if isinstance(opts, dict):
                # range_mode
                try:
                    rm = (
                        str(opts.get("range_mode", RUNTIME_OPTIONS.get("range_mode", "last5")))
                        .strip()
                        .lower()
                    )
                    if rm in ("last5", "fy", "since"):
                        RUNTIME_OPTIONS["range_mode"] = rm
                except Exception:
                    pass
                # fy_mode
                try:
                    fy = str(opts.get("fy_mode", FY_MODE)).strip().upper()
                    if fy in ("FY_APR", "CAL"):
                        FY_MODE = fy
                        RUNTIME_OPTIONS["fy_mode"] = fy
                except Exception:
                    pass
                # periodic_bonus_enable
                try:
                    pbe = opts.get("periodic_bonus_enable", PERIODIC_BONUS_ENABLE)
                    PERIODIC_BONUS_ENABLE = bool(pbe)
                    RUNTIME_OPTIONS["periodic_bonus_enable"] = bool(pbe)
                except Exception:
                    pass
                # periodic_bonus_apply
                try:
                    pba = opts.get("periodic_bonus_apply", PERIODIC_BONUS_APPLY)
                    PERIODIC_BONUS_APPLY = bool(pba)
                    RUNTIME_OPTIONS["periodic_bonus_apply"] = bool(pba)
                except Exception:
                    pass
                # audit_mode: "compact" (default) or "full"
                try:
                    am = (
                        str(opts.get("audit_mode", RUNTIME_OPTIONS.get("audit_mode", "compact")))
                        .strip()
                        .lower()
                    )
                    if am in ("compact", "full"):
                        RUNTIME_OPTIONS["audit_mode"] = am
                except Exception:
                    pass
                # 5) Apply explicit in-process overrides (e.g. HTTP-triggered 'since' runs)
                try:
                    overrides = RUNTIME_OVERRIDES or {}
                    if isinstance(overrides, dict):
                        rm_override = overrides.get("range_mode")
                        if rm_override:
                            rm = str(rm_override).strip().lower()
                            if rm in ("last5", "fy", "since"):
                                RUNTIME_OPTIONS["range_mode"] = rm
                        since_override = overrides.get("since_month")
                        if since_override:
                            RUNTIME_OPTIONS["since_month"] = str(since_override).strip()
                except Exception:
                    # Never let overrides break config loading
                    pass
        else:
            logging.debug(
                "[Config] No Mongo config doc found (%s/%s); using defaults/env.", coll_name, doc_id
            )
    except Exception as _e:
        logging.warning("[Config] Failed to load Mongo config: %s", _e)


def _choose_penalty(flat_pen: float, pct_pen: float) -> float:
    return max(flat_pen, pct_pen) if LS_PENALTY_STRATEGY == "max" else min(flat_pen, pct_pen)





# -----------------------------------------------------------------------------
# Lumpsum positive streak bonus logic (HATTRICK / FIVE-STREAK)
# -----------------------------------------------------------------------------
def _update_positive_np_streak(emp_key: str, growth_pct: float) -> tuple[int, float]:
    """
    Update the in-memory positive NP streak for this employee for the current month.

    Returns (streak_len, streak_bonus_rupees).

    Rules:
      - growth_pct > 0       → streak +1
      - growth_pct <= 0      → streak reset to 0
      - When streak hits 3   → add HATTRICK_BONUS once
      - When streak hits 5   → add FIVE_STREAK_BONUS once
    """
    # Normalise key (avoid accidental duplicates from spaces / case)
    k = " ".join(str(emp_key or "").strip().lower().split())
    if not k:
        return 0, 0.0

    hattrick_bonus = float(WEIGHTS.get("hattrick_bonus", HATTRICK_BONUS))
    five_streak_bonus = float(WEIGHTS.get("five_streak_bonus", FIVE_STREAK_BONUS))
    threshold = float(WEIGHTS.get("hattrick_threshold_pct", 0.1))

    if growth_pct > threshold:
        _POSITIVE_STREAKS[k] += 1
    else:
        _POSITIVE_STREAKS[k] = 0

    streak = _POSITIVE_STREAKS[k]
    bonus = 0.0

    # Fire bonuses when we *hit* the streak length, not on every month beyond
    if streak == 3:
        bonus += hattrick_bonus
    if streak == 5:
        bonus += five_streak_bonus

    return streak, float(bonus)


def _apply_ls_positive_streak_bonus(rec: dict) -> dict:
    """Apply positive NP/Lumpsum streak bonus (HATTRICK / FIVE-STREAK) on Lumpsum rows.

    Uses the in-memory _POSITIVE_STREAKS tracker and the configured
    HATTRICK_BONUS / FIVE_STREAK_BONUS values.

    Rules:
      - Only applies to Metric == 'Lumpsum'.
      - growth_pct > 0 increments the streak; growth_pct <= 0 resets it.
      - When streak hits 3, add HATTRICK_BONUS once for that month.
      - When streak hits 5, add FIVE_STREAK_BONUS once for that month.
      - Bonus is added on top of whatever `final_incentive` exists
        *after* penalties, and we preserve a before/after snapshot.
    """
    try:
        if not isinstance(rec, dict):
            return rec

        # Guard: only Lumpsum metric rows participate
        if str(rec.get("Metric", "")).strip() != "Lumpsum":
            return rec

        # growth_pct is expected to be present on the record
        try:
            growth = float(rec.get("growth_pct", 0.0) or 0.0)
        except Exception:
            growth = 0.0

        # Use a stable employee key for streaks: prefer employee_id, then name
        emp_key = (
            rec.get("employee_id") or rec.get("employee_name") or rec.get("employee_alias") or ""
        )

        streak_len, bonus = _update_positive_np_streak(emp_key, growth)

        # Always expose the current streak on the record
        rec["positive_np_streak"] = int(streak_len)

        # No bonus this month; just return with streak info populated
        if bonus <= 0.0:
            rec.setdefault("streak_bonus_rupees", 0.0)
            return rec

        # Add streak bonus on top of whatever final_incentive we currently have
        try:
            base_final = float(rec.get("final_incentive", 0.0) or 0.0)
        except Exception:
            base_final = 0.0

        rec["final_incentive_before_streak_bonus"] = base_final
        rec["streak_bonus_rupees"] = float(bonus)
        rec["final_incentive"] = base_final + float(bonus)

        return rec
    except Exception as e:
        logging.warning("[Lumpsum] _apply_ls_positive_streak_bonus failed: %s", e)
        return rec


# In-memory positive NP streak tracker for this run (keyed by employee_id)
_POSITIVE_STREAKS: dict[str, int] = defaultdict(int)


# Inactive eligibility cache: key = (normalized_rm_name, month_key) → bool
_INACTIVE_ELIGIBILITY_CACHE: dict[tuple[str, str], bool] = {}
# Employee identity cache: key = normalized_rm_name → (employee_id|None, is_active)
_EMP_ID_CACHE: dict[str, tuple[str | None, bool]] = {}

# --- Debug knobs (env-driven) ----------------------------------------------
# Set these via env when you want verbose diagnostics:
#   PLI_LS_DEBUG_IDENTITY=1  → log Zoho identity resolution details
#   PLI_LS_DEBUG_ATTACH=1    → log detailed NP→Lumpsum attach info
LS_DEBUG_IDENTITY = os.getenv("PLI_LS_DEBUG_IDENTITY", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
LS_DEBUG_ATTACH = os.getenv("PLI_LS_DEBUG_ATTACH", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)


def _rm_eligible_by_inactive(lb_db, rm_name: str, month_key: str) -> bool:
    """
    Apply the 6-month inactive rule for an RM:
      - If Zoho_Users has no record for this RM → eligible.
      - If status is not 'inactive' or inactive_since is missing → eligible.
      - If status == 'inactive' and inactive_since is present:
          * Compute month_index for period_month (YYYY-MM).
          * Compute inactive_index from inactive_since.year/month.
          * Eligible ONLY when 0 <= (month_index - inactive_index) < 6.
      - Months before inactive_since are treated as not-eligible when re-running
        old periods for an already-inactive RM (consistent with aggregation pipelines).
    The lookup is cached per (rm_name, month_key) to avoid repeated DB hits.
    """
    try:
        if lb_db is None:
            return True
        rm_clean = " ".join(str(rm_name or "").strip().split())
        if not rm_clean:
            return True
        if not month_key or "-" not in str(month_key):
            return True

        norm = rm_clean.lower()
        cache_key = (norm, str(month_key))
        if cache_key in _INACTIVE_ELIGIBILITY_CACHE:
            return _INACTIVE_ELIGIBILITY_CACHE[cache_key]

        zu_col = lb_db["Zoho_Users"]
        # Case-insensitive match against Full Name / Name
        try:
            import re as _re  # local alias to avoid top-level pollution if not wanted

            pat = f"^{_re.escape(rm_clean)}$"
            doc = zu_col.find_one(
                {
                    "$or": [
                        {"Full Name": {"$regex": pat, "$options": "i"}},
                        {"Name": {"$regex": pat, "$options": "i"}},
                        {"full_name": {"$regex": pat, "$options": "i"}},
                    ]
                },
                {"status": 1, "Status": 1, "inactive_since": 1},
            )
        except Exception:
            doc = None

        # No Zoho mapping → treat as eligible for now (we still rely on name-based identity)
        if not doc:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
            logging.info(
                "[InactiveGate-LS] RM='%s' month='%s' has no Zoho_Users record; treating as eligible.",
                rm_clean,
                month_key,
            )
            return True

        status = str(doc.get("status") or doc.get("Status") or "").strip().lower()
        inactive_since = doc.get("inactive_since")

        # Active or no inactive_since → eligible
        if status != "inactive" or not inactive_since:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
            return True

        # Compute month indices for 6-month window
        try:
            parts = str(month_key).split("-")
            py = int(parts[0])
            pm = int(parts[1])
            period_index = py * 12 + pm
        except Exception:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
            return True

        try:
            iy = int(getattr(inactive_since, "year", 0))
            im = int(getattr(inactive_since, "month", 0))
            if iy <= 0 or im <= 0:
                _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
                return True
            inactive_index = iy * 12 + im
        except Exception:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
            return True

        diff = period_index - inactive_index
        # Consistent with aggregation pipelines:
        #   Eligible for months in [inactive_month, inactive_month+5]
        eligible = (diff >= 0) and (diff < 6)

        _INACTIVE_ELIGIBILITY_CACHE[cache_key] = bool(eligible)
        if not eligible:
            logging.debug(
                "[InactiveGate-LS] Skipping RM='%s' month='%s' (status=inactive, inactive_since=%s, diff=%s)",
                rm_clean,
                month_key,
                getattr(inactive_since, "isoformat", lambda: inactive_since)(),
                diff,
            )
        return bool(eligible)
    except Exception as e:
        logging.warning(
            "[InactiveGate-LS] Fallback to eligible for RM='%s' month='%s' due to error: %s",
            rm_name,
            month_key,
            e,
        )
        return True


# --- Helper to resolve Zoho employee id and active status ---


def _lookup_employee_active_and_id(
    lb_db,
    rm_clean: str,
) -> tuple[str | None, bool]:
    """
    Internal helper to resolve (employee_id, is_active) for a cleaned RM name.

    - employee_id:
        Prefer Zoho 'User ID' / 'employee_id' / 'Employee ID'.
        Returns None if no Zoho mapping is found.
    - is_active:
        True when Zoho status is not 'inactive' or lookup fails.
    """
    try:
        if lb_db is None:
            return None, True

        key = " ".join(str(rm_clean or "").strip().lower().split())
        if not key:
            return None, False

        # Cache hit
        if key in _EMP_ID_CACHE:
            if LS_DEBUG_IDENTITY:
                emp_id_cached, is_active_cached = _EMP_ID_CACHE[key]
                logging.debug(
                    "[Identity-LS] Cache hit for rm='%s' → emp_id=%r is_active=%s",
                    rm_clean,
                    emp_id_cached,
                    is_active_cached,
                )
            return _EMP_ID_CACHE[key]

        zu_col = lb_db["Zoho_Users"]
        try:
            import re as _re

            pat = f"^{_re.escape(rm_clean)}$"
            doc = zu_col.find_one(
                {
                    "$or": [
                        {"Full Name": {"$regex": pat, "$options": "i"}},
                        {"Name": {"$regex": pat, "$options": "i"}},
                        {"full_name": {"$regex": pat, "$options": "i"}},
                    ]
                },
                {
                    # Prefer the canonical Zoho v6 user id if present
                    "id": 1,
                    # Backward-compatible aliases from older dumps
                    "User ID": 1,
                    "employee_id": 1,
                    "Employee ID": 1,
                    # Status can appear in either case
                    "status": 1,
                    "Status": 1,
                },
            )
        except Exception:
            doc = None

        # No Zoho mapping → mark as inactive with no canonical id so we can skip on write
        if not doc:
            _EMP_ID_CACHE[key] = (None, False)
            if LS_DEBUG_IDENTITY:
                logging.warning(
                    "[Identity-LS] No Zoho_Users match for rm='%s' (normalized='%s'); "
                    "marking as inactive and skipping for leaderboard writes.",
                    rm_clean,
                    key,
                )
            return _EMP_ID_CACHE[key]

        emp_id = (
            doc.get("id") or doc.get("User ID") or doc.get("employee_id") or doc.get("Employee ID")
        )
        status = str(doc.get("status") or doc.get("Status") or "").strip().lower()
        is_active = status != "inactive"

        if emp_id is not None:
            emp_id = str(emp_id).strip() or None

        _EMP_ID_CACHE[key] = (emp_id, is_active)
        if LS_DEBUG_IDENTITY:
            logging.info(
                "[Identity-LS] Zoho match rm='%s' (normalized='%s') → emp_id=%r status=%r is_active=%s",
                rm_clean,
                key,
                emp_id,
                status,
                is_active,
            )
        return emp_id, is_active
    except Exception as e:
        logging.warning(
            "[Identity-LS] Zoho lookup failed for rm='%s': %s",
            rm_clean,
            e,
        )
        # On any unexpected error, do not block incentives
        return None, True


# def _resolve_employee_identity_for_lumpsum(lb_db, rm_name: str) -> tuple[str, str, bool]:
#     """Return (employee_id, employee_alias, is_active) for the given RM name.

#     - employee_id:
#         Prefer Zoho 'User ID' / 'employee_id' / 'Employee ID'.
#         If missing, fall back to the cleaned RM name so older rows still key
#         consistently, but at least all new data uses a proper id when present.
#     - employee_alias:
#         Always the original display name (cleaned). This preserves how RMs see
#         themselves in exports/dashboards.
#     - is_active:
#         Derived from Zoho status != 'inactive'. If there is no Zoho mapping
#         or lookup fails, we treat as active (True) so incentives do not break.
#     """
#     rm_clean = " ".join(str(rm_name or "").strip().split())
#     if not rm_clean:
#         # Hard fallback: nothing to key on; mark inactive so this can be
#         # filtered out explicitly if needed.
#         return "", rm_clean, False

#     emp_id, is_active = _lookup_employee_active_and_id(lb_db, rm_clean)
#     if emp_id is None or str(emp_id).strip() == "":
#         resolved_emp_id = rm_clean
#     else:
#         resolved_emp_id = str(emp_id).strip()

#     # Use a display-friendly alias: first letter of each word capitalized
#     display_alias = rm_clean.title()

#     return resolved_emp_id, display_alias, bool(is_active)


def _resolve_employee_identity_for_lumpsum(lb_db, rm_name: str) -> tuple[str, str, bool]:
    """Return (employee_id, employee_name, is_active) for the given RM name.

    - employee_id:
        Canonical Zoho id when available (from Zoho_Users).
        If missing, we return an empty string "". We do *not* fall back to the
        RM name here; employee_id is reserved for canonical ids only.
    - employee_name:
        Cleaned display name (title-cased). This is what we use for all
        name-based joins and for display in leaderboards.
    - is_active:
        Derived from Zoho status != 'inactive'. If there is no Zoho mapping
        or lookup fails, we treat the RM as active (True) so incentives do not
        break.
    """
    # Normalise the raw RM name into a stable, trimmed form
    rm_clean = " ".join(str(rm_name or "").strip().split())
    if not rm_clean:
        if LS_DEBUG_IDENTITY:
            logging.warning(
                "[Identity-LS] Empty/whitespace RM name encountered in _resolve_employee_identity_for_lumpsum; "
                "record will be skipped."
            )
        return "", "", False

    emp_id, is_active = _lookup_employee_active_and_id(lb_db, rm_clean)
    if emp_id is not None:
        emp_id = str(emp_id).strip() or None

    # Fallback: if Zoho has no id, use RM name as a stable key (legacy behaviour)
    if emp_id is None:
        resolved_emp_id = rm_clean
    else:
        resolved_emp_id = emp_id

    # If Zoho explicitly says inactive, we still honour that flag
    if not is_active:
        if LS_DEBUG_IDENTITY:
            logging.warning(
                "[Identity-LS] RM='%s' is inactive in Zoho; records may be skipped by writer.",
                rm_clean,
            )

    employee_name = rm_clean.title()
    return resolved_emp_id, employee_name, bool(is_active)


# Helper function to round to significant figures
def round_sig(x, sig=4):
    if x == 0:
        return 0.0
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)


# RMs to exclude from scoring/records (match against lowercased aliases)
# RMs to exclude from scoring/records (match against lowercased aliases)


SKIP_RM_ALIASES = {
    "vilakshan bhutani",
    "vilakshan p bhutani",
    "pramod bhutani",
    "dilip kumar singh",
    "dillip kumar",
    "dilip kumar",
    "ruby",
    "manisha p tendulkar",
    "ankur khurana",
    "amaya -virtual assistant",
    "amaya - virtual assistant",
    "anchal chandra",
    "kanchan bhalla",
    "himanshu",
    "poonam gulati",
}


# Effective RM-name skip set: aliases ∪ hardcoded ∪ env
SKIP_RM_NAMES: set[str] = set(SKIP_RM_ALIASES)

# --- Hard sanitation for employee/RM names ---
INVALID_NAME_TOKENS: set[str] = {"", "nan", "none", "null", "-", "—", "na", "n/a"}


def _sanitize_employee_name(name: str) -> tuple[str, bool]:
    """Return (cleaned_name, ok). ok=False when name is invalid and must be dropped."""
    s = " ".join(str(name or "").strip().split())
    return (s, s.lower() not in INVALID_NAME_TOKENS)


def _log_rm_variant_warnings(rm_names) -> None:
    """
    Debug helper: log cases where multiple RM name variants normalise to the same key.

    This is a pure diagnostic sanity-check to catch situations where
    'ISHU MAVAR' and 'Ishu Mavar' (or similar spacing/case variants) are still
    leaking through as distinct keys before canonicalisation.

    Safe to call with any iterable of names. On error, it fails silently
    and never blocks scoring.
    """
    try:
        if not rm_names:
            return

        variant_map: dict[str, set[str]] = {}
        for raw in rm_names:
            norm_key = " ".join(str(raw or "").strip().lower().split())
            if not norm_key:
                continue
            bucket = variant_map.setdefault(norm_key, set())
            bucket.add(str(raw))

        for norm_key, variants in variant_map.items():
            if len(variants) > 1:
                try:
                    logging.warning(
                        "[RM Normalize] Multiple RM variants normalised to '%s': %s",
                        norm_key,
                        sorted(variants),
                    )
                except Exception:
                    # Best-effort logging; never break the main flow.
                    logging.warning(
                        "[RM Normalize] Multiple RM variants normalised to '%s' (count=%d).",
                        norm_key,
                        len(variants),
                    )
    except Exception as e:
        # Fully swallow any errors here; this is purely diagnostic.
        try:
            logging.debug("[RM Normalize] variant warning helper failed: %s", e)
        except Exception:
            pass


# --- Zero helpers for schema defaults ---
def _zero_audit_by_type(purchase: float = 0.0, redemption: float = 0.0) -> list[dict]:
    """Return a ByType audit array initialized with provided purchase/redemption and zeros elsewhere."""
    return [
        {"type": "Purchase", "sum": float(purchase)},
        {"type": "Redemption", "sum": float(redemption)},
        {"type": "Switch In", "sum": 0.0},
        {"type": "Switch Out", "sum": 0.0},
        {"type": "COB In", "sum": 0.0},
        {"type": "COB Out", "sum": 0.0},
    ]


def _zero_audit_by_category(include_excluded: bool = False) -> list[dict]:
    """Return a ByCategory audit array initialized to zeros. If include_excluded=True, also include the excluded bucket."""
    rows = [
        {"category": "Equity", "sum": 0.0},
        {"category": "Debt - Non-Liquid", "sum": 0.0},
        {"category": "Hybrid", "sum": 0.0},
        {"category": "Arbitrage", "sum": 0.0},
        {"category": "Gold", "sum": 0.0},
    ]
    if include_excluded:
        rows.append({"category": "Blacklisted/Liquid/Overnight (Excluded)", "sum": 0.0})
    return rows


def _zero_breakdown() -> dict:
    """Return a zeroed Breakdown dict with the exact keys used elsewhere in the code."""
    # Helper to format label same as _recompute logic
    def _lbl(base: str, w_key: str, default: int):
        pct = float(WEIGHTS.get(w_key, default))
        return f"{base} ({pct:.0f}%)"

    return {
        "Additions": {
            "Total Purchase (100%)": 0.0,
            _lbl("Switch In", "switch_in_pct", 100): 0.0,
            "Debt Purchase Bonus (+20% if <75%)": 0.0,
            "Blacklisted & Liquid Purchase (0%)": 0.0,
            _lbl("Change Of Broker In - TICOB", "cob_in_pct", 50): 0.0,
        },
        "Subtractions": {
            "Redemption (100%)": 0.0,
            _lbl("Switch Out", "switch_out_pct", 100): 0.0,
            _lbl("Change Of Broker Out - TOCOB", "cob_out_pct", 120): 0.0,
        },
        "Totals": {
            "Total Additions": 0.0,
            "Total Subtractions": 0.0,
            "Net Purchase (Formula)": 0.0,
        },
    }


def _compact_audit_payload(audit: dict | None) -> dict | None:
    """
    Return a compacted audit dict:
      - Keep ByType sums as-is.
      - For ByCategory, keep only non-zero entries and at most top 3 by absolute sum,
        but always include the blacklisted/excluded bucket if its sum is non-zero.
    If input is None or malformed, returns it unchanged.
    """
    if not isinstance(audit, dict):
        return audit
    out = dict(audit)
    try:
        bt = out.get("ByType")
        if isinstance(bt, list):
            out["ByType"] = [
                {"type": str(r.get("type", "")), "sum": float(r.get("sum", 0) or 0)} for r in bt
            ]
        bc = out.get("ByCategory")
        if isinstance(bc, list):
            tmp: list[dict] = []
            blacklisted_row: dict | None = None
            for r in bc:
                try:
                    s = float(r.get("sum", 0) or 0)
                except Exception:
                    s = 0.0
                cat = str(r.get("category", ""))
                # Remember the excluded bucket separately so we can always surface it
                if cat == "Blacklisted/Liquid/Overnight (Excluded)":
                    blacklisted_row = {"category": cat, "sum": s}
                    # Do not short-circuit here; we still want it to participate in
                    # the non-zero/top-3 logic if relevant.
                if s != 0.0:
                    tmp.append({"category": cat, "sum": s})
            tmp.sort(key=lambda x: abs(x["sum"]), reverse=True)
            top = tmp[:3]
            # Ensure blacklisted bucket is present whenever it has a non-zero sum,
            # even if it was not in the top-3 by absolute value.
            if blacklisted_row is not None and blacklisted_row.get("sum", 0.0) != 0.0:
                if not any(row.get("category") == blacklisted_row["category"] for row in top):
                    top.append(blacklisted_row)
            out["ByCategory"] = top
    except Exception:
        return audit
    return out


def _is_zero_breakdown(br: dict | None) -> bool:
    """Return True if a Breakdown dict is missing or all numeric values are zero.

    This treats a fully-zero schema placeholder (from `_zero_breakdown`) as empty so
    that we can safely overwrite it with the real NetPurchase breakdown when present.
    """
    if not isinstance(br, dict):
        return True
    try:
        adds = br.get("Additions") or {}
        subs = br.get("Subtractions") or {}
        tots = br.get("Totals") or {}
        for section in (adds, subs, tots):
            for _k, v in section.items():
                try:
                    if float(v or 0.0) != 0.0:
                        return False
                except Exception:
                    # Non-numeric / bad values are treated as zero for this purpose
                    continue
        return True
    except Exception:
        # On any unexpected structure, err on the side of treating it as non-zero
        return False


def _ensure_np_audit_from_breakdown(rec: dict) -> dict:
    """
    Best-effort helper to backfill NetPurchase Audit.ByType from the Breakdown
    totals when the Audit section is missing or all-zero.

    This ensures the NetPurchase document always carries aggregate figures for:
      - Purchase
      - Redemption
      - Switch In
      - Switch Out
      - COB In
      - COB Out

    We only override when:
      - Audit is missing or not a dict, OR
      - All existing ByType sums are zero / missing.
    """
    try:
        if not isinstance(rec, dict):
            return rec

        def _extract_from_breakdown(
            br: dict | None,
        ) -> tuple[float, float, float, float, float, float]:
            if not isinstance(br, dict):
                return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            add = br.get("Additions") or {}
            sub = br.get("Subtractions") or {}

            # Static lookups for fixed 100% components
            def _val(d: dict, k: str) -> float:
                try:
                    return float(d.get(k, 0) or 0.0)
                except Exception:
                    return 0.0

            purchase = _val(add, "Total Purchase (100%)")
            redemption = _val(sub, "Redemption (100%)")

            # Dynamic lookup helper: find key by prefix, then reverse weight
            # Raw = WeightedVal / (WeightPct / 100.0)
            def _scan_raw(d: dict, prefix: str, w_key: str, w_default: float) -> float:
                pct = float(WEIGHTS.get(w_key, w_default))
                if pct == 0.0:
                    return 0.0

                # Scan all keys starting with prefix, pick largest magnitude (handles debris/zero keys)
                max_val = 0.0
                for k, v in d.items():
                    if k.startswith(prefix):
                        try:
                            val = float(v or 0.0)
                            if abs(val) > abs(max_val):
                                max_val = val
                        except Exception:
                            pass

                return max_val / (pct / 100.0)

            switch_in = _scan_raw(add, "Switch In", "switch_in_pct", 100.0)
            switch_out = _scan_raw(sub, "Switch Out", "switch_out_pct", 100.0)
            cob_in = _scan_raw(add, "Change Of Broker In - TICOB", "cob_in_pct", 50.0)
            cob_out = _scan_raw(sub, "Change Of Broker Out - TOCOB", "cob_out_pct", 120.0)

            return purchase, redemption, switch_in, switch_out, cob_in, cob_out

        # 1) Try primary Breakdown
        purchase, redemption, switch_in, switch_out, cob_in, cob_out = _extract_from_breakdown(
            rec.get("Breakdown")
        )

        # 2) If everything is zero, optionally fall back to BreakdownMTD
        if (
            purchase == 0.0
            and redemption == 0.0
            and switch_in == 0.0
            and switch_out == 0.0
            and cob_in == 0.0
            and cob_out == 0.0
        ):
            purchase, redemption, switch_in, switch_out, cob_in, cob_out = _extract_from_breakdown(
                rec.get("BreakdownMTD")
            )

        # Still nothing? Then don’t override.
        if (
            purchase == 0.0
            and redemption == 0.0
            and switch_in == 0.0
            and switch_out == 0.0
            and cob_in == 0.0
            and cob_out == 0.0
        ):
            return rec

        audit = rec.get("Audit")
        override = False
        if not isinstance(audit, dict):
            override = True
        else:
            bytype = audit.get("ByType") or []
            try:
                if all(float((row or {}).get("sum", 0) or 0.0) == 0.0 for row in bytype):
                    override = True
            except Exception:
                override = True

        if not override:
            return rec

        # Preserve any existing ByCategory if present; else use a zeroed default.
        existing_bc = None
        if isinstance(audit, dict):
            existing_bc = audit.get("ByCategory")
        if not isinstance(existing_bc, list):
            existing_bc = _zero_audit_by_category(True)

        rec["Audit"] = {
            "ByType": [
                {"type": "Purchase", "sum": purchase},
                {"type": "Redemption", "sum": redemption},
                {"type": "Switch In", "sum": switch_in},
                {"type": "Switch Out", "sum": switch_out},
                {"type": "COB In", "sum": cob_in},
                {"type": "COB Out", "sum": cob_out},
            ],
            "ByCategory": existing_bc,
        }
        return rec
    except Exception as _e:
        logging.warning("[NetPurchase] _ensure_np_audit_from_breakdown failed: %s", _e)
        return rec


def _recompute_lumpsum_breakdown_and_np(rec: dict) -> dict:
    """
    Rebuild Lumpsum Breakdown + NetPurchase fields from Audit.ByType when present.
    """
    try:
        if not isinstance(rec, dict):
            return rec

        audit = rec.get("Audit") or {}
        bytype = audit.get("ByType") or []

        type_sums: dict[str, float] = {}
        for row in bytype:
            if not isinstance(row, dict):
                continue
            t = str(row.get("type", "")).strip().lower()
            if not t:
                continue
            try:
                v = float(row.get("sum", 0) or 0.0)
            except Exception:
                v = 0.0
            type_sums[t] = type_sums.get(t, 0.0) + v

        purchase = float(type_sums.get("purchase", 0.0))
        redemption = float(type_sums.get("redemption", 0.0))
        switch_in = float(type_sums.get("switch in", 0.0))
        switch_out = float(type_sums.get("switch out", 0.0))
        cob_in = float(type_sums.get("cob in", 0.0))
        cob_out = float(type_sums.get("cob out", 0.0))

        si_pct = float(WEIGHTS.get("switch_in_pct", 120.0))
        so_pct = float(WEIGHTS.get("switch_out_pct", 120.0))
        ci_pct = float(WEIGHTS.get("cob_in_pct", 50.0))
        co_pct = float(WEIGHTS.get("cob_out_pct", 120.0))

        switch_in_w = switch_in * (si_pct / 100.0)
        switch_out_w = switch_out * (so_pct / 100.0)
        cob_in_w = cob_in * (ci_pct / 100.0)
        cob_out_w = cob_out * (co_pct / 100.0)

        bd = rec.get("Breakdown")
        if not isinstance(bd, dict):
            bd = _zero_breakdown()
            rec["Breakdown"] = bd

        adds = bd.get("Additions") or {}
        subs = bd.get("Subtractions") or {}
        tots = bd.get("Totals") or {}

        # Robustly extract debt bonus (key changes with config, e.g. 20% vs 40%)
        debt_bonus = 0.0
        debt_key_used = "Debt Purchase Bonus (+20% if <75%)"  # default fallback
        # Robustly extract debt/equity/hybrid bonuses
        # We assume any key containing "Purchase Bonus" is a bonus component
        extracted_bonuses = {}
        try:
            for k, v in adds.items():
                if "Purchase Bonus" in str(k):
                    extracted_bonuses[k] = float(v or 0.0)
        except Exception:
            pass

        adds["Total Purchase (100%)"] = float(purchase)
        adds[f"Switch In ({si_pct:.0f}%)"] = float(switch_in_w)
        adds[f"Change Of Broker In - TICOB ({ci_pct:.0f}%)"] = float(cob_in_w)

        # Re-inject all extracted bonuses
        for k, v in extracted_bonuses.items():
            adds[k] = v

        blacklisted_sum = 0.0
        bc = audit.get("ByCategory") or []
        try:
            blacklist_lc = set()
            if "BLACKLISTED_CATEGORIES" in globals():
                _bc = globals().get("BLACKLISTED_CATEGORIES")
                if isinstance(_bc, (set, list, tuple)):
                    blacklist_lc = {str(x).lower() for x in _bc}

            for row in bc:
                if not isinstance(row, dict):
                    continue
                cat = str(row.get("category", "")).strip()
                if not cat:
                    continue
                try:
                    val = float(row.get("sum", 0) or 0.0)
                except Exception:
                    continue
                if cat == "Blacklisted/Liquid/Overnight (Excluded)" or cat.lower() in blacklist_lc:
                    blacklisted_sum += val
        except Exception:
            pass
        adds["Blacklisted & Liquid Purchase (0%)"] = float(blacklisted_sum)

        subs["Redemption (100%)"] = float(redemption)
        subs[f"Switch Out ({so_pct:.0f}%)"] = float(switch_out_w)
        subs[f"Change Of Broker Out - TOCOB ({co_pct:.0f}%)"] = float(cob_out_w)

        total_additions = (
            float(adds.get("Total Purchase (100%)", 0.0) or 0.0)
            + switch_in_w
            + sum(extracted_bonuses.values())
            + cob_in_w
        )
        total_subtractions = (
            float(subs.get("Redemption (100%)", 0.0) or 0.0)
            + switch_out_w
            + cob_out_w
        )

        np_val = total_additions - total_subtractions
        np_final = float(np_val)

        # Store totals for additions/subtractions, and the net separately
        tots["Total Additions"] = float(total_additions)
        tots["Total Subtractions"] = float(total_subtractions)
        tots["Net Purchase (Formula)"] = np_final

        bd["Additions"] = adds
        bd["Subtractions"] = subs
        bd["Totals"] = tots
        rec["Breakdown"] = bd
        rec["NetPurchase"] = float(np_final)
        rec["net_purchase"] = float(np_final)

        audit_meta = rec.get("AuditMeta") or {}
        try:
            has_activity = any(abs(float((row or {}).get("sum", 0) or 0.0)) > 0.0 for row in bytype)
        except Exception:
            has_activity = False
        if has_activity:
            audit_meta["HasActivity"] = True
            audit_meta["ZeroTransactionWindow"] = False
        rec["AuditMeta"] = audit_meta

        return rec
    except Exception as _e:
        logging.warning("[Lumpsum] _recompute_lumpsum_breakdown_and_np failed: %s", _e)
        return rec


# ---------------------------------------------------------------------------
# Helper: Recompute Breakdown and NetPurchase from Audit.ByType
# ---------------------------------------------------------------------------
def _recompute_breakdown_and_np(rec: dict) -> dict:
    """
    Recompute weighted Breakdown and NetPurchase from Audit.ByType.
    """
    # Guard: If weights appear to be defaults (e.g. 120 vs expected 12000), abort to prevent corruption.
    if float(WEIGHTS.get("cob_out_pct", 120)) == 120.0:
        # logging.warning("[Lumpsum] Skipping _recompute: Detected default weights (120%).")
        return rec

    try:
        if not isinstance(rec, dict):
            return rec

        audit = rec.get("Audit") or {}
        bytype = audit.get("ByType") or []

        # Index ByType by type name
        bytype_map: dict[str, float] = {}
        for row in bytype:
            if not isinstance(row, dict):
                continue
            t = str(row.get("type", "") or "").strip()
            if not t:
                continue
            try:
                s = float(row.get("sum", 0.0) or 0.0)
            except Exception:
                s = 0.0
            bytype_map[t] = s

        # Raw transaction sums
        purchase = float(bytype_map.get("Purchase", 0.0) or 0.0)
        redemption = float(bytype_map.get("Redemption", 0.0) or 0.0)
        switch_in_raw = float(bytype_map.get("Switch In", 0.0) or 0.0)
        switch_out_raw = float(bytype_map.get("Switch Out", 0.0) or 0.0)
        cob_in_raw = float(bytype_map.get("COB In", 0.0) or 0.0)
        cob_out_raw = float(bytype_map.get("COB Out", 0.0) or 0.0)

        # If there is literally no activity, don't touch anything.
        if (
            purchase == 0.0
            and redemption == 0.0
            and switch_in_raw == 0.0
            and switch_out_raw == 0.0
            and cob_in_raw == 0.0
            and cob_out_raw == 0.0
        ):
            return rec

        # Apply weights from WEIGHTS config (not hardcoded!)
        switch_in_w_pct = float(WEIGHTS.get("switch_in_pct", 100)) / 100.0
        switch_out_w_pct = float(WEIGHTS.get("switch_out_pct", 100)) / 100.0
        cob_in_w_pct = float(WEIGHTS.get("cob_in_pct", 50)) / 100.0
        cob_out_w_pct = float(WEIGHTS.get("cob_out_pct", 120)) / 100.0

        switch_in_w = switch_in_raw * switch_in_w_pct
        switch_out_w = switch_out_raw * switch_out_w_pct
        cob_in_w = cob_in_raw * cob_in_w_pct
        cob_out_w = cob_out_raw * cob_out_w_pct

        # Existing Breakdown (use as a base to preserve any extra keys)
        br = rec.get("Breakdown")
        if not isinstance(br, dict):
            br = _zero_breakdown()
        add = br.get("Additions") or {}
        sub = br.get("Subtractions") or {}
        tots = br.get("Totals") or {}

        # Robustly extract debt/equity/hybrid bonuses
        extracted_bonuses = {}
        try:
            for k, v in add.items():
                if "Purchase Bonus" in str(k):
                    extracted_bonuses[k] = float(v or 0.0)
        except Exception:
            pass

        # Aggregate blacklisted bucket from Audit.ByCategory (0% weight)
        blacklisted_sum = 0.0
        try:
            bc = audit.get("ByCategory") or []
            for row in bc:
                if not isinstance(row, dict):
                    continue
                cat = str(row.get("category", "") or "").strip()
                if cat == "Blacklisted/Liquid/Overnight (Excluded)":
                    try:
                        blacklisted_sum = float(row.get("sum", 0.0) or 0.0)
                    except Exception:
                        blacklisted_sum = 0.0
                    break
        except Exception:
            blacklisted_sum = 0.0

        # Rebuild additions with correct weights (dynamic labels)
        add["Total Purchase (100%)"] = float(purchase)
        add[f"Switch In ({float(WEIGHTS.get('switch_in_pct', 100)):.0f}%)"] = float(switch_in_w)

        # Re-inject all extracted bonuses
        for k, v in extracted_bonuses.items():
            add[k] = v

        add["Blacklisted & Liquid Purchase (0%)"] = float(blacklisted_sum)
        add[f"Change Of Broker In - TICOB ({float(WEIGHTS.get('cob_in_pct', 50)):.0f}%)"] = float(cob_in_w)

        # Rebuild subtractions with correct weights (dynamic labels)
        sub["Redemption (100%)"] = float(redemption)
        sub[f"Switch Out ({float(WEIGHTS.get('switch_out_pct', 100)):.0f}%)"] = float(switch_out_w)
        sub[f"Change Of Broker Out - TOCOB ({float(WEIGHTS.get('cob_out_pct', 120)):.0f}%)"] = float(cob_out_w)

        # Compute totals. Blacklisted bucket is 0% weight → not added to NP.
        total_add = float(purchase + switch_in_w + sum(extracted_bonuses.values()) + cob_in_w)
        total_sub = float(redemption + switch_out_w + cob_out_w)
        net_val = float(total_add - total_sub)

        tots["Total Additions"] = total_add
        tots["Total Subtractions"] = total_sub
        tots["Net Purchase (Formula)"] = net_val

        br["Additions"] = add
        br["Subtractions"] = sub
        br["Totals"] = tots
        rec["Breakdown"] = br

        # If BreakdownMTD is just a zeroed placeholder, mirror the monthly breakdown.
        br_mtd = rec.get("BreakdownMTD")
        if isinstance(br_mtd, dict) and _is_zero_breakdown(br_mtd):
            rec["BreakdownMTD"] = br

        # Stamp top-level NetPurchase fields from the formula
        rec["NetPurchase"] = net_val
        rec["net_purchase"] = net_val

        return rec
    except Exception as _e:
        logging.warning("[NetPurchase] _recompute_breakdown_and_np failed: %s", _e)
        return rec


def _normalize_np_record(rec: dict, start: datetime, end: datetime) -> dict:
    """Normalise a NetPurchase record into a fully-populated, self-consistent shape.

    This function guarantees that, for Metric == 'NetPurchase':
      - month / AUM fields are present and numeric
      - Audit / AuditMTD always exist with ByType + ByCategory arrays
      - Breakdown / BreakdownMTD exist and are aligned with Audit.ByType
      - Totals.Total Additions / Total Subtractions / Net Purchase (Formula)
        are derived from Audit.ByType using the standard weights
      - top-level NetPurchase / net_purchase mirror the formula value
      - AuditMeta / AuditMetaMTD carry stable window metadata
    """
    if not isinstance(rec, dict):
        return rec

    # Metric and month key ---------------------------------------------------
    rec.setdefault("Metric", "NetPurchase")

    if not rec.get("month"):
        try:
            rec["month"] = _month_key(start)
        except Exception:
            # Fallback: best-effort YYYY-MM from the start date
            rec["month"] = start.strftime("%Y-%m")

    # AUM fields -------------------------------------------------------------
    try:
        aum_start = float(rec.get("AUM (Start of Month)", rec.get("AUM", 0.0)) or 0.0)
    except Exception:
        aum_start = 0.0
    try:
        aum = float(rec.get("AUM", aum_start) or 0.0)
    except Exception:
        aum = aum_start

    rec["AUM (Start of Month)"] = aum_start
    rec["AUM"] = aum

    # Core schema scaffolding ------------------------------------------------
    audit = rec.get("Audit")
    if not isinstance(audit, dict):
        rec["Audit"] = {
            "ByType": _zero_audit_by_type(0.0, 0.0),
            "ByCategory": _zero_audit_by_category(True),
        }

    audit_mtd = rec.get("AuditMTD")
    if not isinstance(audit_mtd, dict):
        rec["AuditMTD"] = {
            "ByType": _zero_audit_by_type(0.0, 0.0),
            "ByCategory": _zero_audit_by_category(False),
        }

    if not isinstance(rec.get("Breakdown"), dict):
        rec["Breakdown"] = _zero_breakdown()
    if not isinstance(rec.get("BreakdownMTD"), dict):
        rec["BreakdownMTD"] = _zero_breakdown()

    audit_meta = rec.get("AuditMeta")
    if not isinstance(audit_meta, dict):
        rec["AuditMeta"] = {
            "WindowStart": start.strftime("%Y-%m-%d"),
            "WindowEnd": end.strftime("%Y-%m-%d"),
            "HasActivity": False,
            "ZeroTransactionWindow": True,
        }

    audit_meta_mtd = rec.get("AuditMetaMTD")
    if not isinstance(audit_meta_mtd, dict):
        rec["AuditMetaMTD"] = {
            "WindowStart": start.replace(day=1).strftime("%Y-%m-%d"),
            "WindowEnd": end.strftime("%Y-%m-%d"),
            "HasActivity": False,
        }

    # Backfill Audit.ByType from Breakdown (when missing/all-zero) ----------
    rec = _ensure_np_audit_from_breakdown(rec)

    # Canonical recompute of Breakdown + NetPurchase from Audit.ByType -------
    rec = _recompute_breakdown_and_np(rec)

    # Stamp schema + config metadata and apply compaction --------------------
    rec["SchemaVersion"] = SCHEMA_VERSION

    if RUNTIME_OPTIONS.get("audit_mode", "compact") == "compact":
        if "Audit" in rec:
            rec["Audit"] = _compact_audit_payload(rec.get("Audit"))
        if "AuditMTD" in rec:
            rec["AuditMTD"] = _compact_audit_payload(rec.get("AuditMTD"))

    try:
        if _LAST_CFG_HASH:
            rec["config_hash"] = _LAST_CFG_HASH
            rec["config_schema_version"] = SCHEMA_VERSION
            rec.setdefault(
                "config_meta",
                {
                    "range_mode": RUNTIME_OPTIONS.get("range_mode"),
                    "fy_mode": RUNTIME_OPTIONS.get("fy_mode"),
                    "periodic_bonus_enable": bool(RUNTIME_OPTIONS.get("periodic_bonus_enable")),
                    "periodic_bonus_apply": bool(RUNTIME_OPTIONS.get("periodic_bonus_apply")),
                },
            )
    except Exception:
        pass

    return rec


def _normalize_ls_record(rec: dict, start: datetime, end: datetime) -> dict:
    rec.setdefault("Metric", "Lumpsum")

    # Ensure core structures are present so downstream recomputes never see
    # a half-baked record.
    audit = rec.get("Audit")
    if not isinstance(audit, dict):
        rec["Audit"] = {
            "ByType": _zero_audit_by_type(0.0, 0.0),
            "ByCategory": _zero_audit_by_category(True),
        }

    if not isinstance(rec.get("Breakdown"), dict):
        rec["Breakdown"] = _zero_breakdown()
    if not isinstance(rec.get("BreakdownMTD"), dict):
        rec["BreakdownMTD"] = _zero_breakdown()

    audit_meta = rec.get("AuditMeta")
    if not isinstance(audit_meta, dict):
        rec["AuditMeta"] = {
            "WindowStart": start.strftime("%Y-%m-%d"),
            "WindowEnd": end.strftime("%Y-%m-%d"),
            "HasActivity": False,
            "ZeroTransactionWindow": True,
        }

    # If Audit is still missing/all-zero, derive it from Breakdown/BreakdownMTD
    # before running the canonical recompute.
    rec = _ensure_np_audit_from_breakdown(rec)

    # Canonical recompute from Audit.ByType
    rec = _recompute_breakdown_and_np(rec)

    # Apply Lumpsum-specific recompute (rounding, blacklisted bucket aggregation,
    # AuditMeta.HasActivity, etc.). This ensures the final Breakdown/NetPurchase
    # on Lumpsum rows is fully aligned with the NetPurchase rules but preserves
    # any Lumpsum-specific behaviour.
    rec = _recompute_lumpsum_breakdown_and_np(rec)

    # Apply negative-growth penalty AFTER NP/Breakdown are fully aligned so that:
    #   - incentive_penalty_meta.np_val exactly matches Breakdown['Totals']['Net Purchase (Formula)']
    #   - penalty_rupees_applied and final_incentive stay in sync for every row.
    # LEGACY PARITY: Disabled slabs_v2 post-processing - using inline growth_slab_v1 instead
    # rec = _apply_ls_negative_growth_penalty(rec)

    # Apply positive-streak bonuses (HATTRICK / FIVE-STREAK) on top of penalties
    if RUNTIME_OPTIONS.get("apply_streak_bonus", True):
        rec = _apply_ls_positive_streak_bonus(rec)
    # else: Streak bonus disabled via config (Legacy Parity)

    # ----------------------------------------------------
    # NEW: Continuous Quarterly / Annual Bonus Projection
    # ----------------------------------------------------
    try:
        # Determine current employee Identity (ID or Name)
        # _apply_ls_positive_streak_bonus already handles lookup, but let's be robust using ID if present
        emp_search = {}
        if rec.get("employee_id"):
             emp_search["employee_id"] = rec["employee_id"]
        elif rec.get("employee_name"):
             emp_search["employee_name"] = rec["employee_name"]

        # Determine Current Quarter & FY Bounds
        rec_month_key = rec.get("month", _month_key(start or datetime.utcnow()))
        # Parse month key 'YYYY-MM' to datetime
        try:
             y_str, m_str = rec_month_key.split("-")
             rec_dt = datetime(int(y_str), int(m_str), 15) # mid-month
        except:
             rec_dt = datetime.utcnow()

        fy_mode = str(RUNTIME_OPTIONS.get("fy_mode", FY_MODE)).upper()

        # Quarter Bounds
        qs, qe, q_label = _get_quarter_bounds(rec_dt, fy_mode)
        q_month_keys = []
        cur = qs
        while cur <= qe:
            q_month_keys.append(_month_key(cur))
            # next month logic
            if cur.month == 12: cur = datetime(cur.year + 1, 1, 1)
            else: cur = datetime(cur.year, cur.month + 1, 1)

        # FY Bounds
        fys, fye, fy_label = _get_fy_bounds(rec_dt, fy_mode)
        fy_month_keys = []
        cur = fys
        while cur <= fye:
            fy_month_keys.append(_month_key(cur))
            if cur.month == 12: cur = datetime(cur.year + 1, 1, 1)
            else: cur = datetime(cur.year, cur.month + 1, 1)

        # Helper to get current month stats from THIS record
        try:
             curr_np = float(rec["Breakdown"]["Totals"]["Net Purchase (Formula)"])
        except:
             curr_np = 0.0
        curr_pos = 1 if curr_np > 0 else 0

        # Calculate Quarterly
        past_q_keys = [k for k in q_month_keys if k < rec_month_key]
        q_agg = {"net_purchase": 0.0, "positive_months": 0}
        if past_q_keys and emp_search:
              q_filter = emp_search.copy()
              q_filter["month"] = {"$in": past_q_keys}
              if db_leaderboard is not None:
                  q_agg = _fetch_period_sum(db_leaderboard["Leaderboard_Lumpsum"], q_filter)

        total_q_np = q_agg["net_purchase"] + curr_np
        total_q_pos = q_agg["positive_months"] + curr_pos

        # Calc Bonus for Quarter
        # Using QTR_BONUS_TEMPLATE: { "slabs": [{ "min_np": X, "bonus_rupees": Y }] }
        # And config: "min_positive_months"
        q_bonus_amt, _ = _select_np_slab_bonus(total_q_np, QTR_BONUS_TEMPLATE)
        # Check eligibility (positive months)
        q_min_pos = int((QTR_BONUS_TEMPLATE or {}).get("min_positive_months", 2))
        q_qualified = (total_q_pos >= q_min_pos)

        # Calculate Annual
        past_fy_keys = [k for k in fy_month_keys if k < rec_month_key]
        fy_agg = {"net_purchase": 0.0, "positive_months": 0}
        if past_fy_keys and emp_search:
              fy_filter = emp_search.copy()
              fy_filter["month"] = {"$in": past_fy_keys}
              if db_leaderboard is not None:
                  fy_agg = _fetch_period_sum(db_leaderboard["Leaderboard_Lumpsum"], fy_filter)

        total_fy_np = fy_agg["net_purchase"] + curr_np
        total_fy_pos = fy_agg["positive_months"] + curr_pos

        # Calc Bonus for Annual
        a_bonus_amt, _ = _select_np_slab_bonus(total_fy_np, ANNUAL_BONUS_TEMPLATE)
        a_min_pos = int((ANNUAL_BONUS_TEMPLATE or {}).get("min_positive_months", 6))
        a_qualified = (total_fy_pos >= a_min_pos)

        # Bonus Month Restriction: Only show detailed projected bounty in end-of-quarter months
        is_bonus_month = (rec_dt.month == qe.month)
        if not is_bonus_month:
             rec["bonus_projected"] = None
        else:
            rec["bonus_projected"] = {
                "quarterly": {
                    "period": q_label,
                    "net_purchase_qtd": round_sig(total_q_np, 2),
                    "positive_months": total_q_pos,
                    "projected_amount": q_bonus_amt if q_qualified else 0.0,
                    "potential_amount": q_bonus_amt, # Show what they COULD get
                    "is_qualified": bool(q_qualified),
                    "min_positive_months_req": q_min_pos
                },
                "annual": {
                    "period": fy_label,
                    "net_purchase_ytd": round_sig(total_fy_np, 2),
                    "positive_months": total_fy_pos,
                    "projected_amount": a_bonus_amt if a_qualified else 0.0,
                    "potential_amount": a_bonus_amt,
                    "is_qualified": bool(a_qualified),
                    "min_positive_months_req": a_min_pos
                }
            }

    except Exception as e:
        logging.warning(f"[_normalize_ls_record] Projected bonus calc failed: {e}")
        rec["bonus_projected"] = None


    rec.setdefault("SchemaVersion", SCHEMA_VERSION)


    # Apply compaction + config stamps if enabled / available
    if RUNTIME_OPTIONS.get("audit_mode", "compact") == "compact":
        if "Audit" in rec:
            rec["Audit"] = _compact_audit_payload(rec.get("Audit"))

    try:
        if _LAST_CFG_HASH:
            rec["config_hash"] = _LAST_CFG_HASH
            rec["config_schema_version"] = SCHEMA_VERSION
            rec.setdefault(
                "config_meta",
                {
                    "range_mode": RUNTIME_OPTIONS.get("range_mode"),
                    "fy_mode": RUNTIME_OPTIONS.get("fy_mode"),
                    "periodic_bonus_enable": bool(RUNTIME_OPTIONS.get("periodic_bonus_enable")),
                    "periodic_bonus_apply": bool(RUNTIME_OPTIONS.get("periodic_bonus_apply")),
                },
            )
    except Exception:
        pass

    return rec


def _attach_np_audit_to_lumpsum(lb_db, rec: dict, month_key: str) -> dict:
    """\
    Best-effort helper to graft NetPurchase audit / breakdown information onto a Lumpsum record.

    Primary match key is employee_id (Zoho id). For older NetPurchase rows which were
    keyed by RM name, we fall back to employee_alias / employee_name (case-insensitive).
    """
    try:
        if lb_db is None or not isinstance(rec, dict):
            return rec

        if not month_key or not isinstance(month_key, str):
            return rec

        emp_id = rec.get("employee_id")
        emp_alias = rec.get("employee_alias") or rec.get("employee_name")

        col = lb_db["Leaderboard_Lumpsum"]
        np_doc = None

        # 1) Primary: match by canonical employee_id (Zoho id)
        if emp_id:
            try:
                np_doc = col.find_one(
                    {
                        "Metric": "Lumpsum",
                        "employee_id": emp_id,
                        "month": month_key,
                    },
                    {"Audit": 1, "AuditMeta": 1, "Breakdown": 1, "BreakdownMTD": 1},
                )
                if LS_DEBUG_ATTACH:
                    logging.info(
                        "[Lumpsum] NP lookup by employee_id=%r month=%s → found=%s",
                        emp_id,
                        month_key,
                        bool(np_doc),
                    )
            except Exception as _e:
                logging.warning(
                    "[Lumpsum] Failed primary NP lookup for emp_id=%s month=%s: %s",
                    emp_id,
                    month_key,
                    _e,
                )
                np_doc = None

        # 2) Fallback: match by RM alias / name (legacy NetPurchase rows)
        if np_doc is None and emp_alias:
            alias_clean = " ".join(str(emp_alias).strip().split())
            if alias_clean:
                # a) exact match on employee_id (older rows that used alias as id)
                try:
                    if np_doc is None:
                        np_doc = col.find_one(
                            {
                                "Metric": "Lumpsum",
                                "employee_id": alias_clean,
                                "month": month_key,
                            },
                            {"Audit": 1, "AuditMeta": 1, "Breakdown": 1, "BreakdownMTD": 1},
                        )
                        if LS_DEBUG_ATTACH:
                            logging.info(
                                "[Lumpsum] NP alias lookup by employee_id='%s' month=%s → found=%s",
                                alias_clean,
                                month_key,
                                bool(np_doc),
                            )
                except Exception as _e:
                    logging.warning(
                        "[Lumpsum] Alias-based NP lookup by employee_id='%s' month=%s failed: %s",
                        alias_clean,
                        month_key,
                        _e,
                    )
                    np_doc = None

                # b) exact match on employee_name
                if np_doc is None:
                    try:
                        np_doc = col.find_one(
                            {
                                "Metric": "Lumpsum",
                                "employee_name": alias_clean,
                                "month": month_key,
                            },
                            {"Audit": 1, "AuditMeta": 1, "Breakdown": 1, "BreakdownMTD": 1},
                        )
                        if LS_DEBUG_ATTACH:
                            logging.info(
                                "[Lumpsum] NP alias lookup by employee_name='%s' month=%s → found=%s",
                                alias_clean,
                                month_key,
                                bool(np_doc),
                            )
                    except Exception as _e:
                        logging.warning(
                            "[Lumpsum] Alias-based NP lookup by employee_name='%s' month=%s failed: %s",
                            alias_clean,
                            month_key,
                            _e,
                        )
                        np_doc = None

        if not np_doc:
            # Enhanced: Try fallback query without "Metric" filter if nothing found
            diag_q: dict[str, Any] = {"Metric": "Lumpsum"}
            or_clauses: list[dict] = []
            alias_clean = None
            if emp_alias is not None:
                alias_clean = " ".join(str(emp_alias).strip().split()) or None
            if emp_id:
                or_clauses.append({"employee_id": emp_id})
            if alias_clean:
                # Legacy rows may have used the alias as employee_id or employee_name
                or_clauses.append({"employee_id": alias_clean})
                or_clauses.append({"employee_name": alias_clean})
            if or_clauses:
                diag_q["$or"] = or_clauses

            candidates = []
            candidate_count = 0
            sample_months = []
            sample_labels = []
            diag_query_used = diag_q
            if diag_q.get("$or"):
                try:
                    candidates = list(
                        col.find(
                            diag_q, {"employee_id": 1, "employee_name": 1, "month": 1, "Metric": 1}
                        )
                    )
                    candidate_count = len(candidates)
                except Exception:
                    candidates = []
                    candidate_count = -1
            if candidate_count == 0:
                # Fallback: remove "Metric" filter and try again
                fallback_q = dict(diag_q)
                fallback_q.pop("Metric", None)
                try:
                    fallback_candidates = list(
                        col.find(
                            fallback_q,
                            {"employee_id": 1, "employee_name": 1, "month": 1, "Metric": 1},
                        )
                    )
                    fallback_count = len(fallback_candidates)
                except Exception:
                    fallback_candidates = []
                    fallback_count = -1
                if fallback_count > 0:
                    # Use fallback candidates for diagnostics
                    candidates = fallback_candidates
                    candidate_count = fallback_count
                    diag_query_used = fallback_q
            sample_months = sorted(
                {str(d.get("month")) for d in candidates if d.get("month") is not None}
            )
            sample_labels = [
                f"id={d.get('employee_id')!r}, name={d.get('employee_name')!r}, month={d.get('month')!r}"
                for d in candidates
            ]

            if candidate_count == 0:
                # No NetPurchase document exists for this RM+month. This is now treated as a
                # legitimate "no NP data" case rather than a data error. We leave the
                # Lumpsum Breakdown/Audit as-is and simply annotate the record so downstream
                # consumers can distinguish between "attached" vs "missing" NP audit.
                try:
                    meta = rec.get("np_audit_meta") or {}
                    meta.update(
                        {
                            "status": "missing_netpurchase_doc",
                            "employee_id": emp_id,
                            "employee_name": emp_alias,
                            "month": month_key,
                        }
                    )
                    rec["np_audit_meta"] = meta
                except Exception:
                    # Annotation is best-effort; never block scoring.
                    pass

                if LS_DEBUG_ATTACH:
                    try:
                        logging.warning(
                            "[Lumpsum][Debug] NP attach miss for emp_id=%r alias='%s' month=%s | "
                            "candidate_count=%s sample_months=%s sample_rows=%s diag_q=%s",
                            emp_id,
                            emp_alias,
                            month_key,
                            candidate_count,
                            sample_months,
                            sample_labels,
                            diag_query_used,
                        )
                    except Exception as _dbg_e:
                        logging.warning(
                            "[Lumpsum][Debug] NP attach diagnostics failed for emp_id=%r alias='%s' month=%s: %s",
                            emp_id,
                            emp_alias,
                            month_key,
                            _dbg_e,
                        )

                # No NP doc to graft – downstream normalisation will ensure a valid
                # zeroed Breakdown/Audit structure for this Lumpsum row.
                return rec

        # At this point, if we still don't have a usable NetPurchase document,
        # bail out early so we never dereference a None.
        if not isinstance(np_doc, dict):
            try:
                meta = rec.get("np_audit_meta") or {}
                # Preserve any existing status from the earlier branch, only
                # filling in fields that are missing.
                meta.setdefault("status", "missing_netpurchase_doc")
                meta.setdefault("employee_id", emp_id)
                meta.setdefault("employee_name", emp_alias)
                meta.setdefault("month", month_key)
                rec["np_audit_meta"] = meta
            except Exception:
                pass

            if LS_DEBUG_ATTACH:
                logging.warning(
                    "[Lumpsum] NP attach: no usable NetPurchase doc for emp_id=%r alias='%s' month=%s; skipping attach.",
                    emp_id,
                    emp_alias,
                    month_key,
                )
            return rec

        # Debug: log a concise identity diff between Lumpsum row and attached NP doc
        if LS_DEBUG_ATTACH:
            try:
                logging.warning(
                    "[Lumpsum][Debug] Attach NP vs LS | LS(emp_id=%r, alias=%r, month=%s) "
                    "NP(emp_id=%r, name=%r, month=%s)",
                    emp_id,
                    emp_alias,
                    month_key,
                    np_doc.get("employee_id"),
                    np_doc.get("employee_name"),
                    np_doc.get("month"),
                )
            except Exception:
                # Never break attach flow due to logging issues
                pass

        # Always compute a compact zero-sum diagnostic payload from the attached NetPurchase doc.
        # This is persisted on the Lumpsum row so downstream consumers can inspect NP consistency
        # without re-querying Mongo.
        try:
            audit = np_doc.get("Audit") or {}
            bt = audit.get("ByType") or []
            total_bt = 0.0
            for row in bt:
                if not isinstance(row, dict):
                    continue
                try:
                    total_bt += float((row or {}).get("sum", 0) or 0.0)
                except Exception:
                    continue

            br = np_doc.get("Breakdown") or {}
            adds = br.get("Additions") or {}
            subs = br.get("Subtractions") or {}
            tots = br.get("Totals") or {}

            def _f(d: dict, k: str) -> float:
                try:
                    return float(d.get(k, 0) or 0.0)
                except Exception:
                    return 0.0

            total_add = _f(tots, "Total Additions")
            total_sub = _f(tots, "Total Subtractions")
            np_formula = _f(tots, "Net Purchase (Formula)")

            # If Totals are missing or zeroed, try to reconstruct from additions/subtractions.
            if total_add == 0.0 and total_sub == 0.0 and tots:
                # print("recomputing totals from adds/subs")
                total_add = (
                    _f(adds, "Total Purchase (100%)")
                    + _f(adds, "Switch In (120%)")
                    + _f(adds, "Debt Purchase Bonus (+20% if <75%)")
                    + _f(adds, "Change Of Broker In - TICOB (50%)")
                )
                # print(f"total_add: {total_add}")
                total_sub = (
                    _f(subs, "Redemption (100%)")
                    + _f(subs, "Switch Out (120%)")
                    + _f(subs, "Change Of Broker Out - TOCOB (120%)")
                )
                # print(f"total_sub: {total_sub}")
                np_formula = total_add - total_sub
                # print(f"np_formula: {np_formula}")

            has_activity = any(abs(v) > 0.0 for v in (total_bt, total_add, total_sub, np_formula))
            is_zero_sum_window = has_activity and abs(np_formula) < 1e-2

            rec["NP_ZeroSumDiag"] = {
                "month": month_key,
                "total_bytype": float(total_bt),
                "total_additions": float(total_add),
                "total_subtractions": float(total_sub),
                "net_purchase_formula": float(np_formula),
                "has_activity": bool(has_activity),
                "is_zero_sum_window": bool(is_zero_sum_window),
            }

            if LS_DEBUG_ATTACH and has_activity and is_zero_sum_window:
                logging.warning(
                    "[Lumpsum] NP doc for emp_id=%r alias='%s' month=%s appears zero-sum "
                    "(NP≈0 with non-zero activity). diag=%r",
                    emp_id,
                    emp_alias,
                    month_key,
                    rec.get("NP_ZeroSumDiag"),
                )
        except Exception as _e:
            logging.warning(
                "[Lumpsum] Failed zero-sum diagnostic for emp_id=%r alias='%s' month=%s: %s",
                emp_id,
                emp_alias,
                month_key,
                _e,
            )

        # Attach Audit
        audit = np_doc.get("Audit")
        # print(f"audit: {audit}")
        if isinstance(audit, dict) and audit:
            rec["Audit"] = audit

        # Attach Breakdown/BreakdownMTD if Lumpsum record doesn't already carry them
        for key in ("Breakdown", "BreakdownMTD"):
            if key in np_doc and isinstance(np_doc.get(key), dict):
                existing = rec.get(key)
                # Only replace if missing or schema-placeholder/zeroed-out
                if (not isinstance(existing, dict)) or _is_zero_breakdown(existing):
                    rec[key] = np_doc[key]

        # Merge AuditMeta
        audit_meta = np_doc.get("AuditMeta")
        # print(f"audit_meta: {audit_meta}")
        if isinstance(audit_meta, dict) and audit_meta:
            existing_meta = rec.get("AuditMeta")
            if not isinstance(existing_meta, dict):
                rec["AuditMeta"] = dict(audit_meta)
            else:
                merged = dict(audit_meta)
                merged.update(existing_meta)
                rec["AuditMeta"] = merged

        if LS_DEBUG_ATTACH:
            logging.info(
                "[Lumpsum] Attached NP audit/breakdown to Lumpsum for emp_id=%r alias='%s' month=%s",
                emp_id,
                emp_alias,
                month_key,
            )
        return rec
    except Exception as e:
        logging.warning(
            "[Lumpsum] _attach_np_audit_to_lumpsum failed for month=%s: %s",
            month_key,
            e,
        )
        return rec


# Helper matcher for more robust skipping by tokens
def _skip_match(name: str) -> bool:
    """
    Returns True if `name` should be skipped using the RM-name skip set (SKIP_RM_NAMES),
    which is aliases ∪ hardcoded ∪ env; also applies token-based heuristics
    for common variants (e.g., 'vilakshan p bhutani').
    """
    s = " ".join(str(name or "").lower().split())
    s = " ".join(str(name or "").lower().split())

    # 1. Check dynamic set from config
    # if s in SKIP_RM_ALIASES:
    #     return True

    # 2. Check legacy static set (if any remaining) or env
    # if s in SKIP_RM_NAMES:
    #     return True

    # NOTE: We now score EVERYONE. Exclusion is handled only at the Leaderboard API level (via "Ignored RMs" config).
    return False


# (imports consolidated at top)


from dotenv import load_dotenv, find_dotenv

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE: dict[str, str] = {}
# Cache for AUM lookups to avoid repeated DB hits
_AUM_CACHE: dict[str, float] = {}

# Key Vault URL and Mongo secret names can be configured via env
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL", "https://milestonetsl1.vault.azure.net/")
MONGODB_SECRET_NAME = os.getenv("MONGODB_SECRET_NAME", "MongoDb-Connection-String")
LEADERBOARD_DB_NAME = os.getenv("LEADERBOARD_DB_NAME", os.getenv("PLI_DB_NAME", "PLI_Leaderboard"))


def get_secret(name: str, default: str | None = None) -> str | None:
    """
    Return secret value from environment if present; otherwise fetch from Azure Key Vault.
    Falls back to `default` if neither source is available. Values are cached per-process.
    Supports KV names that disallow underscores by trying hyphenated variants.
    """
    # 1) Env precedence (easy local override for dev/testing)
    if name in os.environ and os.environ[name]:
        return os.environ[name]

    # Back-compat alias: if code asks for the KV key but env only provides legacy name(s)
    if name == "MongoDb-Connection-String":
        legacy = os.getenv("MONGO_CONN") or os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
        if legacy:
            return legacy

    # 2) Cache
    if name in _SECRET_CACHE:
        return _SECRET_CACHE[name]

    # 3) Azure Key Vault (if configured and SDK available)
    if KEY_VAULT_URL and SecretClient and DefaultAzureCredential:
        lookup_names = [name]
        # Azure KV secret names cannot contain underscores; try a hyphenated variant
        if "_" in name:
            lookup_names.append(name.replace("_", "-"))
        try:
            cred = DefaultAzureCredential()
            client = SecretClient(vault_url=KEY_VAULT_URL, credential=cred)
            for _nm in lookup_names:
                try:
                    secret = client.get_secret(_nm)
                    val = getattr(secret, "value", None)
                    if isinstance(val, str):
                        _SECRET_CACHE[name] = val
                        return val
                    else:
                        # If the secret exists but has no value, try the next lookup name
                        continue
                except Exception:
                    continue
        except Exception as e:
            logging.warning("Secrets: failed to fetch '%s' from Key Vault: %s", name, e)

    # 4) Fallback
    return default


def _get_leaderboard_db(client: pymongo.MongoClient | None = None):
    """
    Return a live handle to the leaderboard database.
    If a client is provided we reuse it; otherwise create a fresh one using the
    configured Mongo connection secret.
    """
    if client is None:
        mongo_uri = get_secret(MONGODB_SECRET_NAME)
        if not mongo_uri:
            raise RuntimeError("Mongo connection string not available for leaderboard DB.")
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    return client[LEADERBOARD_DB_NAME]


# Robust .env loading: try env overrides, known paths, then auto-discover (with opt-out warning)
from pathlib import Path


def _load_dotenvs() -> None:
    """Load .env in a predictable order with overrides:
    1) Paths provided via PLI_ENV_PATH or PLI_ENV_PATHS (colon/semicolon separated)
    2) Project-local candidates near this file and its parent
    3) CWD-based auto discovery via find_dotenv(usecwd=True)

    Set SUPPRESS_ENV_WARNING=1 to silence the missing .env warning.
    """
    suppress_warn = os.getenv("SUPPRESS_ENV_WARNING", "0").strip().lower() in ("1", "true", "yes")

    # 1) Explicit override(s) via env
    explicit_paths: list[str] = []
    if os.getenv("PLI_ENV_PATH"):
        explicit_paths.append(os.getenv("PLI_ENV_PATH", ""))
    if os.getenv("PLI_ENV_PATHS"):
        # Support both ':' (POSIX) and ';' (Windows) as separators
        raw = os.getenv("PLI_ENV_PATHS", "")
        for sep in (os.pathsep, ";", ":"):
            if sep in raw:
                explicit_paths.extend([p.strip() for p in raw.split(sep) if p.strip()])
                break
        else:
            if raw.strip():
                explicit_paths.append(raw.strip())

    # 2) Project-local candidates (near this file and repository root-ish)
    here = Path(__file__).resolve()
    candidates = [
        here.parent / ".env",  # same folder as this file
        here.parent.parent / ".env",  # project root parent
        Path.cwd() / ".env",  # current working directory
    ]

    # 3) Append any explicit overrides to the front (highest priority)
    explicit = [Path(p).expanduser().resolve() for p in explicit_paths]
    search_list = [*explicit, *candidates]

    loaded_from: list[str] = []
    for p in search_list:
        try:
            if p.is_file():
                load_dotenv(dotenv_path=str(p), override=False)
                logging.info(f"Loaded .env from: {p}")
                loaded_from.append(str(p))
        except Exception as _e:
            logging.warning(f"Failed loading .env at {p}: {_e}")

    # 4) Auto-discover via python-dotenv if nothing loaded yet
    if not loaded_from:
        auto = find_dotenv(usecwd=True)
        if auto:
            try:
                load_dotenv(dotenv_path=auto, override=False)
                logging.info(f"Loaded .env via find_dotenv: {auto}")
                loaded_from.append(auto)
            except Exception as _e:
                logging.warning(f"find_dotenv located {auto} but load failed: {_e}")

    # Final hint if still nothing loaded
    if not loaded_from and not suppress_warn:
        logging.warning(
            "No .env file found via overrides, candidates, or find_dotenv. "
            "Set PLI_ENV_PATH(S) to point to your .env or set SUPPRESS_ENV_WARNING=1 to silence this."
        )


_load_dotenvs()

# Let Azure Functions' worker manage handlers. Respect PLI_LOG_LEVEL even if handlers already exist.
_root_logger = logging.getLogger()

# Parse desired level from env (name or int)
_level_env = os.getenv("PLI_LOG_LEVEL", "INFO").strip().upper()
_level_map = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}
_level = _level_map.get(_level_env, None)
if _level is None:
    try:
        _level = int(_level_env)
    except Exception:
        _level = logging.INFO

# If no handlers (local CLI / direct run), create a basic stream handler
if not _root_logger.handlers:
    logging.basicConfig(level=_level, format="%(asctime)s - %(levelname)s - %(message)s")
else:
    # Ensure root and existing handlers honor requested level
    _root_logger.setLevel(_level)
    for _h in _root_logger.handlers:
        try:
            _h.setLevel(_level)
        except Exception:
            pass

logging.info(f"[Log] Level set to {logging.getLevelName(_level)} (PLI_LOG_LEVEL={_level_env})")

# Optional log profile to filter INFO noise without changing call sites.
# Usage:
#   PLI_LOG_PROFILE=noisy   (default; show all INFO)
#   PLI_LOG_PROFILE=summary (keep only key high-level INFO lines)
#   PLI_LOG_PROFILE=minimal (only month-completion INFO; Warnings/Errors always pass)
LOG_PROFILE = os.getenv("PLI_LOG_PROFILE", "noisy").strip().lower()


class _ProfileFilter(logging.Filter):
    def __init__(self, profile: str):
        super().__init__()
        self.profile = profile
        self._summary_allow = (
            "[Log]",  # startup level line
            "[NetPurchase]",  # run header
            "🔄",  # month window fetch line
            "[Lumpsum]",  # per-RM incentive line
            "[Month Done]",  # month completion summary
            "[CLI]",  # manual CLI runs / errors
        )
        self._minimal_allow = ("[Month Done]",)

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        # Never hide warnings/errors
        if record.levelno >= logging.WARNING:
            return True
        # Only filter INFO/DEBUG messages based on profile
        msg = record.getMessage()
        if self.profile == "summary":
            return msg.startswith(self._summary_allow)
        if self.profile in ("minimal", "quiet"):
            return msg.startswith(self._minimal_allow)
        return True  # noisy/default


logging.getLogger().addFilter(_ProfileFilter(LOG_PROFILE))
if LOG_PROFILE in ("summary", "minimal", "quiet"):
    logging.info(f"[Log] Using log profile: {LOG_PROFILE}")


# Compact helper to log (possibly large) Mongo queries safely

def _log_query(label: str, query: dict, level=logging.DEBUG):
    """Safely log a MongoDB query, truncating large lists if needed."""
    try:
        q_str = str(query)
        if len(q_str) > 500:
            q_str = q_str[:500] + "... [truncated]"
        logging.log(level, f"{label}: {q_str}")
    except:
        pass

# ----------------------------
# Period / Bounds Helpers
# ----------------------------
def _get_quarter_bounds(dt_val: datetime, fy_mode: str) -> tuple[datetime, datetime, str]:
    """
    Return (start_date, end_date, label) for the quarter containing dt_val.
    fy_mode: "FY_APR" (India) or "FY_JAN" (Calendar).
    """
    m = dt_val.month
    y = dt_val.year

    if fy_mode == "FY_APR":
        # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
        if 4 <= m <= 6:
            q_start, q_end = datetime(y, 4, 1), datetime(y, 7, 1) - timedelta(days=1)
            label = f"Q1 FY{y}-{str(y+1)[-2:]}"
        elif 7 <= m <= 9:
            q_start, q_end = datetime(y, 7, 1), datetime(y, 10, 1) - timedelta(days=1)
            label = f"Q2 FY{y}-{str(y+1)[-2:]}"
        elif 10 <= m <= 12:
            q_start, q_end = datetime(y, 10, 1), datetime(y + 1, 1, 1) - timedelta(days=1)
            label = f"Q3 FY{y}-{str(y+1)[-2:]}"
        else:  # 1 <= m <= 3
            q_start, q_end = datetime(y, 1, 1), datetime(y, 4, 1) - timedelta(days=1)
            # Belongs to FY ending in current year
            label = f"Q4 FY{y-1}-{str(y)[-2:]}"
    else:
        # Calendar Year
        # Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
        if 1 <= m <= 3:
            q_start, q_end = datetime(y, 1, 1), datetime(y, 4, 1) - timedelta(days=1)
            label = f"Q1 {y}"
        elif 4 <= m <= 6:
            q_start, q_end = datetime(y, 4, 1), datetime(y, 7, 1) - timedelta(days=1)
            label = f"Q2 {y}"
        elif 7 <= m <= 9:
            q_start, q_end = datetime(y, 7, 1), datetime(y, 10, 1) - timedelta(days=1)
            label = f"Q3 {y}"
        else:
            q_start, q_end = datetime(y, 10, 1), datetime(y + 1, 1, 1) - timedelta(days=1)
            label = f"Q4 {y}"

    # Clamp end to end-of-day
    q_end_clamped = datetime(q_end.year, q_end.month, q_end.day, 23, 59, 59)
    return q_start, q_end_clamped, label


def _get_fy_bounds(dt_val: datetime, fy_mode: str) -> tuple[datetime, datetime, str]:
    """
    Return (start_date, end_date, label) for the Fiscal Year containing dt_val.
    """
    m = dt_val.month
    y = dt_val.year

    if fy_mode == "FY_APR":
        # FY starts in April. If Jan-Mar, we are in FY(y-1)-y
        if m < 4:
            start_y = y - 1
        else:
            start_y = y

        fy_start = datetime(start_y, 4, 1)
        fy_end = datetime(start_y + 1, 4, 1) - timedelta(days=1)
        label = f"FY {start_y}-{str(start_y+1)[-2:]}"
    else:
        # Calendar Year
        fy_start = datetime(y, 1, 1)
        fy_end = datetime(y + 1, 1, 1) - timedelta(days=1)
        label = f"CY {y}"

    fy_end_clamped = datetime(fy_end.year, fy_end.month, fy_end.day, 23, 59, 59)
    return fy_start, fy_end_clamped, label


def _aggregate_period_metrics(
    lb_db,
    emp_key_normalized: str,
    start_dt: datetime,
    end_dt: datetime,
    current_month_key: str
) -> dict:
    """
    Aggregates metrics for an employee over a date range from Leaderboard_Lumpsum.
    Includes the *current* window's values implicitly if they are already in DB,
    but usually this is called *during* processing so current month might not be fully persisted/indexed yet
    OR we rely on 'month' string based queries.

    Safe approach: Query by 'month' string range.
    """
    # 1. Generate list of month keys in range
    cur = start_dt
    end_limit = end_dt

    month_keys = []
    while cur <= end_limit:
        month_keys.append(_month_key(cur))
        # Advance to next month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)

    # Filter out current month if needed, or include?
    # Logic: The current record being processed has up-to-date NP in 'Breakdown'.
    # We should fetch OTHER months from DB and add CURRENT from memory context?
    # NO, simpler: The caller (normalize) has the full current 'rec'.
    # We query DB for strictly *previous* months in the period, then add current 'rec' values.

    past_keys = [k for k in month_keys if k < current_month_key]

    agg = {
        "net_purchase": 0.0,
        "positive_months": 0
    }

    if not past_keys:
        return agg

    try:
        col = lb_db["Leaderboard_Lumpsum"]
        # Match by normalized name/alias handling is tricky.
        # Ideally we match by employee_id if available, else name.
        # But _normalize_ls_record should help us.
        # Let's assume the caller passes a robust query filter or we standardise on ID/Name.

        # We'll rely on the fact that 'rec' has the canonical identification logic.
        # BUT fetching by ID is safest.
        pass
    except Exception as e:
        logging.warning("Failed to aggregate period metrics: %s", e)

    return agg

# Re-implementing clearer aggregation helper that takes the collection and filter
def _fetch_period_sum(lb_col, query_filter: dict) -> dict:
    try:
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": None,
                "total_np": {"$sum": "$Breakdown.Totals.Net Purchase (Formula)"},
                "pos_months": {
                    "$sum": {
                        "$cond": [{"$gt": ["$Breakdown.Totals.Net Purchase (Formula)", 0]}, 1, 0]
                    }
                }
            }}
        ]
        res = list(lb_col.aggregate(pipeline))
        if res:
            return {
                "net_purchase": res[0].get("total_np", 0.0),
                "positive_months": res[0].get("pos_months", 0)
            }
    except Exception as e:
        logging.warning(f"[_fetch_period_sum] Aggregation failed: {e}")

    return {"net_purchase": 0.0, "positive_months": 0}


    try:
        s = json.dumps(q, default=str)
    except Exception:
        s = str(q)
    if len(s) > max_chars:
        s = s[:max_chars] + "…"
    logging.log(level, f"{label} {s}")


# --- Helper(s) for refresh/purge modes ---
def _month_key(dt: datetime) -> str:
    return f"{dt.year}-{dt.month:02d}"


def _compute_last5_windows(today: datetime) -> list[tuple[datetime, datetime]]:
    """Windows for range_mode='last5' with a 5-day lookback (prev month + current)."""
    last5_date = today - timedelta(days=5)
    windows: list[tuple[datetime, datetime]] = []

    if (last5_date.year, last5_date.month) != (today.year, today.month):
        first_of_this = datetime(today.year, today.month, 1)
        prev_last = first_of_this - timedelta(days=1)
        prev_first = datetime(prev_last.year, prev_last.month, 1)
        windows.append((prev_first, prev_last))
        windows.append((first_of_this, today + timedelta(days=1)))
    else:
        windows.append((datetime(today.year, today.month, 1), today + timedelta(days=1)))

    return windows


# ========== Distributed singleton (Mongo-based) ==========
# Prevents concurrent/duplicate executions across timer/scale-out.
# Tuned via env:
#   PLI_LOCK_ENABLED  -> "1"/"true" to enable (default), "0"/"false" to disable
#   PLI_LOCK_KEY      -> logical job name; default "lumpsum-scorer"
#   PLI_LOCK_TTL_SEC  -> lease duration in seconds; default 5400 (90 min)
#   PLI_LOCK_DB       -> DB name for locks; default "PLI_Leaderboard"
#   PLI_LOCKS_COLL    -> collection name; default "Job_Locks"
def _instance_id() -> str:
    return os.getenv("WEBSITE_INSTANCE_ID") or os.getenv("HOSTNAME") or f"pid-{os.getpid()}"


def _locks_collection(mongo_client: pymongo.MongoClient):
    dbname = LEADERBOARD_DB_NAME
    colname = os.getenv("PLI_LOCKS_COLL", "Job_Locks")
    col = mongo_client[dbname][colname]
    try:
        # idempotent ensures
        col.create_index("_id", unique=True)
    except Exception:
        pass
    try:
        # TTL index on absolute 'expiresAt' timestamps
        col.create_index("expiresAt", expireAfterSeconds=0)
    except Exception:
        pass
    return col


def acquire_distributed_lock(
    mongo_client: pymongo.MongoClient, key: str, ttl_sec: int = 3600
) -> bool:
    """Try to acquire a distributed lock. Returns True if acquired by *this* instance."""
    col = _locks_collection(mongo_client)
    now = datetime.utcnow()
    owner = _instance_id()
    try:
        doc = col.find_one_and_update(
            {
                "_id": key,
                "$or": [
                    {"locked": False},
                    {"locked": {"$exists": False}},
                    {"expiresAt": {"$lte": now}},
                ],
            },
            {
                "$set": {
                    "locked": True,
                    "owner": owner,
                    "acquiredAt": now,
                    "expiresAt": now + timedelta(seconds=ttl_sec),
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if doc and doc.get("locked") is True and doc.get("owner") == owner:
            logging.info(f"[Lock] Acquired '{key}' (owner={owner}, ttl={ttl_sec}s)")
            return True
        holder = (doc or {}).get("owner")
        logging.warning(f"[Lock] Could not acquire '{key}'; currently held by '{holder}'.")
        return False
    except PyMongoError as e:
        logging.warning(f"[Lock] Acquire failed: {e}")
        return False


def release_distributed_lock(mongo_client: pymongo.MongoClient, key: str) -> None:
    """Release the lock if owned by this instance. Best-effort; TTL will clean up if we crash."""
    col = _locks_collection(mongo_client)
    owner = _instance_id()
    try:
        res = col.delete_one({"_id": key, "owner": owner})
        if res.deleted_count == 1:
            logging.info(f"[Lock] Released '{key}'")
        else:
            # If document changed ownership or expired, try soft unlock
            col.update_one(
                {"_id": key, "owner": owner},
                {
                    "$set": {"locked": False},
                    "$unset": {"owner": "", "acquiredAt": "", "expiresAt": ""},
                },
            )
    except PyMongoError as e:
        logging.warning(f"[Lock] Release failed: {e}")


def fetch_active_employee_ids(access_token):
    """
    Return a *DataFrame* of Zoho CRM users that are currently marked **Active**.
    The DataFrame includes columns: Full Name, Email, User ID, Role, Status.
    If a set of active user IDs is required, derive it from the 'User ID' column
    at the call site (for example: `set(df_users["User ID"].astype(str))`).
    """
    active_ids = set()
    all_user_data = []
    url = "https://www.zohoapis.com/crm/v6/users"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    page = 1
    per_page = 200

    while True:
        params = {"type": "ActiveUsers", "page": page, "per_page": per_page}
        resp = requests.get(url, headers=headers, params=params)

        if resp.status_code != 200:
            logging.warning(f"Could not fetch Active users from Zoho: {resp.text}")
            break

        data = resp.json()
        users_page = data.get("users", [])
        logging.info(f"Fetched page {page} from Zoho: {len(users_page)} users")
        logging.debug(f"Sample users: {[u.get('full_name') for u in users_page[:5]]}")
        logging.debug(f"Raw JSON response (truncated): {json.dumps(data)[:1000]}")

        for user in users_page:
            active_ids.add(str(user.get("id")))
            all_user_data.append(
                {
                    "Full Name": user.get("full_name"),
                    "Email": user.get("email"),
                    "User ID": user.get("id"),
                    "Role": user.get("role", {}).get("name"),
                    "Status": user.get("status"),
                }
            )

        if len(users_page) < per_page:
            break
        page += 1

    df_users = pd.DataFrame(all_user_data)
    return df_users


def run_net_purchase(
    leaderboard_db,
    override_config: dict | None = None,
    dry_run: bool = False,
    target_rm: str | None = None,
    target_month: str | None = None,
    mongo_client=None,
):
    """
    Main entrypoint:
      1) Loads runtime config (or uses override).
      2) Determines windows (Current Month + Range Mode).
      3) Runs _run_lumpsum_for_window for each.
    If dry_run=True, returns { "results": [...] }.
    """
    # Load config (slabs/templates/weights) from Mongo or Override
    _init_runtime_config(leaderboard_db, override_config)

    # Wire up globals so helper functions see the correct DB/config
    global db_leaderboard, _LAST_CFG_HASH
    db_leaderboard = leaderboard_db

    # Initialize runtime config from Mongo and compute a config hash for audit

    cfg_snapshot = _effective_config_snapshot()
    _LAST_CFG_HASH = _hash_dict(cfg_snapshot)

    # Core (transactions) DB name; default matches earlier logs
    core_db_name = os.getenv("CORE_DB_NAME", "iwell").strip() or "iwell"

    client_to_use = mongo_client
    if not client_to_use:
        # If not passed, create one (e.g. CLI or Timer run without passing client)
        # Note: If running in Azure Function, we might want to reuse if possible.
        # But for CLI entry points, we create here.
        uri = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
        client_to_use = pymongo.MongoClient(uri)

    db = client_to_use[core_db_name]

    # Collections mapping (matches previous behaviour/logs)
    purchase_col_name = db["purchase_txn"]
    redemption_col_name = db["redemption_txn"]
    switchin_col_name = db["switchin_txn"]
    switchout_col_name = db["switchout_txn"]
    cob_col_name = db["ChangeofBroker"]
    aum_report_col_name = db["AUM_Report"]
    meetings_coll_name = db["Investor_Meetings_Data"]

    logging.info(
        "[Collections] purchase=purchase_txn redemption=redemption_txn "
        "switchin=switchin_txn switchout=switchout_txn cob=ChangeofBroker "
        "aum_report=AUM_Report meetings=Investor_Meetings_Data"
    )

    now_utc = datetime.utcnow()
    all_sim_results = []

    if target_month:
        # Simulation usually targets a specific history month
        # Ignore range mode and just run for that month
        label = "Simulated"
        # Calculate start/end from target_month YYYY-MM
        try:
            y, m = map(int, target_month.split('-'))
            w_start = datetime(y, m, 1)
            # end of month
            if m == 12:
                w_end = datetime(y + 1, 1, 1) - timedelta(seconds=1)
            else:
                w_end = datetime(y, m + 1, 1) - timedelta(seconds=1)
            windows = [(label, w_start, w_end)]
        except Exception as e:
            logging.error(f"[Sim] Invalid target_month {target_month}: {e}")
            return [] if dry_run else 0
    else:
        # Normal range logic
        windows = _compute_month_windows(now_utc)
        windows = [("Normal", s, e) for s, e in windows] # Add a label for consistency

    for label, start, end in windows:
        logging.info(f"Processing window: {label} ({start} -> {end})")
        res = _run_lumpsum_for_window(
            db=db,
            db_leaderboard_conn=leaderboard_db,
            purchase_col=purchase_col_name,
            redemption_col=redemption_col_name,
            switchin_col=switchin_col_name,
            switchout_col=switchout_col_name,
            cob_col=cob_col_name,
            aum_report_col=aum_report_col_name,
            meetings_col=meetings_coll_name,
            start=start,
            end=end,
            target_rm=target_rm,
            dry_run=dry_run,
        )
        if dry_run and isinstance(res, list):
            all_sim_results.extend(res)

    if dry_run:
        return all_sim_results
    return None # This function doesn't return total_docs in non-dry_run mode, _run_lumpsum_for_window does.


def _debug_sanity_write(db_leaderboard) -> None:
    """
    Write a tiny debug document into Leaderboard_Lumpsum to confirm:
      - Mongo connection string is valid
      - DB and collection names are correct
      - We have write privileges
    """
    try:
        if db_leaderboard is None:
            logging.debug(
                "[Debug] Sanity write skipped: db_leaderboard is None (no DB handle in this context)"
            )
            return

        col = db_leaderboard["Leaderboard_Lumpsum"]
        res = col.update_one(
            {"_id": "__debug_sanity_lumpsum__"},
            {
                "$set": {
                    "ts": datetime.utcnow(),
                    "note": "sanity write from Lumpsum_Scorer",
                }
            },
            upsert=True,
        )
        logging.info(
            "[Debug] Sanity write OK: matched=%s modified=%s upserted_id=%s",
            getattr(res, "matched_count", None),
            getattr(res, "modified_count", None),
            getattr(res, "upserted_id", None),
        )
    except Exception as e:
        logging.error("[Debug] Sanity write FAILED: %s", e, exc_info=True)


def get_aum_data_from_collection(run_id, collection):
    """
    Legacy AUM lookup used by older NetPurchase logic.
    It expects AUM_Report documents keyed by `_id == run_id` where
    run_id is typically of the form '<YYYY-MM>_<RM_NAME>'.
    """
    # Cache check first
    if run_id in _AUM_CACHE:
        return _AUM_CACHE[run_id]

    logging.debug("Fetching AUM for run_id: %s", run_id)
    logging.debug("run_id repr: %r", run_id)

    # Only the DB call is wrapped in try/except so we don't confuse Python's parser
    try:
        document = collection.find_one({"_id": run_id})
    except Exception as e:
        logging.warning("Error fetching AUM for run_id=%s: %s", run_id, e)
        _AUM_CACHE[run_id] = 0.0
        return 0.0

    if document:
        logging.debug("AUM Document Found: %s", document)
        try:
            aum_amount = float(document.get("Amount", 0) or 0)
        except Exception:
            aum_amount = 0.0
        logging.debug("AUM Value Extracted: %s", aum_amount)
        _AUM_CACHE[run_id] = aum_amount
        return aum_amount

    logging.debug("No AUM document found for run_id: %s", run_id)
    _AUM_CACHE[run_id] = 0.0
    return 0.0


# --- Canonical AUM RM name resolution helper ---
def _canonical_aum_rm_name(rm_name: str, collection, month_key: str) -> str:
    """
    Canonicalisation hook for AUM RM names used by get_aum_for_rm_month.

    Behaviour:
      - If a valid AUM_Report collection and month_key are provided, it tries to
        map the given rm_name to the exact "MAIN RM" spelling stored in AUM_Report
        for that month.
      - It tries exact, trimmed, and truncated variants (first two tokens, dropped
        last token, first token only).
      - As a last resort it returns an uppercased, whitespace-normalised version
        of rm_name so that call sites always have a deterministic alias.

    This keeps call-site signature stable while allowing future extension without
    changing get_aum_for_rm_month.
    """

    def norm(s: str) -> str:
        return " ".join(str(s or "").strip().lower().split())

    try:
        raw = str(rm_name or "").strip()
        if not raw:
            return ""

        # If we don't have a usable collection or month key, fall back to simple normalisation.
        if collection is None or month_key is None:
            return " ".join(raw.upper().split())

        month_str = str(month_key).strip()
        if not month_str:
            return " ".join(raw.upper().split())

        # Pull all MAIN RM names available for this month once
        try:
            cur = collection.find({"Month": month_str}, {"MAIN RM": 1})
            candidates = [doc.get("MAIN RM", "") for doc in cur]
        except Exception:
            candidates = []

        mapping = {norm(c): c for c in candidates if c}

        # Target normalised form of the requested RM
        target = norm(raw)
        if target in mapping:
            return mapping[target]

        # Try truncated and alias variants
        toks = target.split()
        variants: list[str] = []
        if len(toks) >= 2:
            variants.append(" ".join(toks[:2]))  # first two tokens
            variants.append(" ".join(toks[:-1]))  # drop last token
        if toks:
            variants.append(toks[0])  # first token only

        for v in variants:
            vnorm = norm(v)
            if vnorm in mapping:
                return mapping[vnorm]

        # Prefix match as a last heuristic
        for k, v in mapping.items():
            if toks and (k.startswith(" ".join(toks[:2])) or k.startswith(target)):
                return v

        # Fallback: simple uppercase + whitespace normalisation
        return " ".join(raw.upper().split())
    except Exception:
        # On any unexpected failure, do not block AUM lookups; use a simple alias.
        try:
            raw = str(rm_name or "").strip()
            return " ".join(raw.upper().split()) if raw else ""
        except Exception:
            return ""


def get_aum_for_rm_month(rm_name: str, month_key: str, collection) -> float:
    """
    Fetch AUM for an RM + month from AUM_Report.
    Uses {Month, MAIN RM, Amount} and caches results via _AUM_CACHE.

    Improvements vs older version:
      - Use a stable cache key based on a normalized RM alias.
      - Try canonical name resolution first (if helper available).
      - Fall back to case-insensitive exact match on MAIN RM.
      - As a last resort, try a case-insensitive regex/substring match.
      - Emit DEBUG logs on misses to make name / month mismatches visible.
    """
    # Normalize RM for cache + lookups
    raw_rm = str(rm_name or "").strip()
    if not raw_rm:
        return 0.0

    norm_rm = " ".join(raw_rm.upper().split())
    cache_key = f"{month_key}|{norm_rm}"
    if cache_key in _AUM_CACHE:
        return _AUM_CACHE[cache_key]

    month_candidates: set[str] = {str(month_key)}
    try:
        if re.match(r"^\d{4}-\d{2}$", month_key):
            month_candidates.add(f"{month_key}-01")
    except Exception:
        pass

    try:
        # 1) Canonical name resolution (if helper is present)
        canonical_rm = norm_rm
        try:
            if "_canonical_aum_rm_name" in globals():
                canonical_rm = _canonical_aum_rm_name(raw_rm, collection, month_key)
                if not canonical_rm:
                    canonical_rm = norm_rm
            else:
                canonical_rm = norm_rm
        except Exception as e:
            logging.warning(
                "AUM canonical name resolution failed for rm='%s' month='%s': %s",
                raw_rm,
                month_key,
                e,
            )
            canonical_rm = norm_rm

        # 2) Exact match on Month + MAIN RM using canonical name (case-sensitive first)
        q_primary = {
            "Month": {"$in": list(month_candidates)},
            "MAIN RM": canonical_rm,
        }
        _log_query("[AUM] primary lookup", q_primary, level=logging.DEBUG)
        doc = collection.find_one(q_primary, {"Amount": 1, "_id": 0})

        # 3) Case-insensitive exact match if primary failed
        if not doc:
            q_ci = {
                "Month": {"$in": list(month_candidates)},
                "MAIN RM": {"$regex": f"^{re.escape(canonical_rm)}$", "$options": "i"},
            }
            _log_query("[AUM] ci-exact fallback", q_ci, level=logging.DEBUG)
            doc = collection.find_one(q_ci, {"Amount": 1, "_id": 0})

        # 4) As a very last resort, try a loose contains-based regex on the raw RM
        if not doc:
            loose_pat = re.escape(raw_rm.strip())
            q_loose = {
                "Month": {"$in": list(month_candidates)},
                "MAIN RM": {"$regex": loose_pat, "$options": "i"},
            }
            _log_query("[AUM] loose fallback", q_loose, level=logging.DEBUG)
            doc = collection.find_one(q_loose, {"Amount": 1, "_id": 0})

        if not doc:
            # No AUM found – cache 0.0 but emit a warning for visibility.
            logging.warning(
                "AUM lookup miss | month=%s rm_raw='%s' canonical='%s'",
                month_key,
                raw_rm,
                canonical_rm,
            )
            _AUM_CACHE[cache_key] = 0.0
            return 0.0

        try:
            aum_amount = float(doc.get("Amount", 0) or 0)
        except Exception:
            aum_amount = 0.0

        logging.debug(
            "AUM hit | month=%s rm_raw='%s' canonical='%s' amount=%s",
            month_key,
            raw_rm,
            canonical_rm,
            aum_amount,
        )
        _AUM_CACHE[cache_key] = aum_amount
        return aum_amount
    except Exception as e:
        logging.warning(
            "AUM lookup failed RM='%s' month='%s': %s",
            rm_name,
            month_key,
            e,
        )
        _AUM_CACHE[cache_key] = 0.0
        return 0.0


from collections import defaultdict  # already imported above; re-import is harmless


def _load_cob_for_month(cob_col, month_key: str) -> tuple[dict[str, float], dict[str, float]]:
    """
    Nuclear COB loader: fetch ALL ChangeofBroker docs once, then filter in Python.

    Returns two dicts keyed by RM name:
      - cob_in_by_rm  (TICOB / COB IN)
      - cob_out_by_rm (TOCOB / COB OUT)

    Logs are intentionally noisy so we can inspect schema and matching behaviour.
    """
    cob_in_by_rm: dict[str, float] = defaultdict(float)
    cob_out_by_rm: dict[str, float] = defaultdict(float)

    if cob_col is None:
        logging.info("[COB] Month=%s: cob_col is None; skipping COB.", month_key)
        return cob_in_by_rm, cob_out_by_rm

    # 1) Fetch everything
    try:
        raw_docs = list(cob_col.find({}))
    except Exception as e:
        logging.warning("[COB] Month=%s: failed to fetch ChangeofBroker docs: %s", month_key, e)
        return cob_in_by_rm, cob_out_by_rm

    total_raw = len(raw_docs)
    logging.info("[COB] Month=%s: raw ChangeofBroker docs fetched=%d", month_key, total_raw)

    if not raw_docs:
        logging.info("[COB] Month=%s: no ChangeofBroker rows present in collection.", month_key)
        return cob_in_by_rm, cob_out_by_rm

    try:
        df_cob = pd.DataFrame(raw_docs)
    except Exception as e:
        logging.warning(
            "[COB] Month=%s: failed to build DataFrame from %d docs: %s",
            month_key,
            total_raw,
            e,
        )
        return cob_in_by_rm, cob_out_by_rm

    # Log columns once to understand schema
    try:
        logging.debug("[COB] Month=%s: ChangeofBroker columns=%s", month_key, list(df_cob.columns))
    except Exception:
        pass

    # 2) Identify date column and normalise
    date_col = None
    for cand in ("DATE", "Date", "date", "TRANSFER DATE", "Transfer Date", "transfer_date"):
        if cand in df_cob.columns:
            date_col = cand
            break

    if not date_col:
        logging.warning(
            "[COB] Month=%s: no DATE/TRANSFER DATE column found; columns=%s",
            month_key,
            list(df_cob.columns),
        )
        return cob_in_by_rm, cob_out_by_rm

    def _parse_cob_date(val):
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        s = str(val).strip()
        if not s:
            return None
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    df_cob["__dt"] = df_cob[date_col].apply(_parse_cob_date)
    df_cob = df_cob[df_cob["__dt"].notna()]

    if df_cob.empty:
        logging.info(
            "[COB] Month=%s: all ChangeofBroker rows had unparsable dates (date_col=%s)",
            month_key,
            date_col,
        )
        return cob_in_by_rm, cob_out_by_rm

    # 3) Filter to the target month YYYY-MM
    try:
        y_str, m_str = str(month_key).split("-")
        year = int(y_str)
        month = int(m_str)
    except Exception:
        logging.warning("[COB] Month=%s: invalid month_key format; expected YYYY-MM", month_key)
        return cob_in_by_rm, cob_out_by_rm

    df_month = df_cob[(df_cob["__dt"].dt.year == year) & (df_cob["__dt"].dt.month == month)]
    logging.info(
        "[COB] Month=%s: matched rows after date filter=%d (from raw=%d)",
        month_key,
        len(df_month),
        total_raw,
    )

    if df_month.empty:
        logging.info(
            "[COB] Month=%s: no ChangeofBroker rows matched target month after date parse.",
            month_key,
        )
        return cob_in_by_rm, cob_out_by_rm

    # 4) Identify RM / Amount / Direction columns
    rm_col = None
    # Always prefer MAIN RM first to align with leaderboard identity.
    if "MAIN RM" in df_month.columns:
        rm_col = "MAIN RM"
    else:
        for cand in (
            "RELATIONSHIP  MANAGER",
            "RELATIONSHIP MANAGER",
            "Relationship Manager",
            "RM",
        ):
            if cand in df_month.columns:
                rm_col = cand
                break

    if rm_col is None:
        logging.warning(
            "[COB] Month=%s: no RM column found; columns=%s",
            month_key,
            list(df_month.columns),
        )
        return cob_in_by_rm, cob_out_by_rm

    amt_col = None
    for cand in ("Amount", "AMOUNT", "amount"):
        if cand in df_month.columns:
            amt_col = cand
            break

    if amt_col is None:
        logging.warning(
            "[COB] Month=%s: no Amount column found; columns=%s",
            month_key,
            list(df_month.columns),
        )
        return cob_in_by_rm, cob_out_by_rm

    type_col = None
    for cand in ("COB TYPE", "COB_TYPE", "Direction", "TYPE"):
        if cand in df_month.columns:
            type_col = cand
            break

    if type_col is None:
        logging.warning(
            "[COB] Month=%s: no COB TYPE/Direction column found; columns=%s",
            month_key,
            list(df_month.columns),
        )
        return cob_in_by_rm, cob_out_by_rm

    df_month = df_month.copy()
    # Normalise for grouping
    df_month[amt_col] = pd.to_numeric(df_month[amt_col], errors="coerce").fillna(0.0)
    df_month[rm_col] = df_month[rm_col].astype(str).str.strip()
    df_month[type_col] = df_month[type_col].astype(str).str.upper().str.strip()

    rows_in = 0
    rows_out = 0

    # 5) Walk rows and classify into COB_IN / COB_OUT buckets
    for _, row in df_month.iterrows():
        # Amount
        try:
            amt_raw = row.get(amt_col, 0.0)
            amt = float(amt_raw or 0.0)
        except Exception:
            amt = 0.0
        if amt == 0.0:
            continue

        # RM name
        rm_raw = row.get(rm_col, "")
        rm = " ".join(str(rm_raw or "").strip().split())
        if not rm:
            continue

        # COB type / direction
        t_raw = row.get(type_col, "")
        t = str(t_raw or "").upper().strip()

        is_in = False
        is_out = False

        # Primary classification based on common COB encodings
        if "TICOB" in t or t in {"IN", "COB IN", "COB-IN", "INWARD"}:
            is_in = True
        elif "TOCOB" in t or t in {"OUT", "COB OUT", "COB-OUT", "OUTWARD"}:
            is_out = True
        else:
            # Fallback heuristics for Direction-like or noisy fields
            if t.startswith("TI") or t.endswith("IN"):
                is_in = True
            elif t.startswith("TO") or t.endswith("OUT"):
                is_out = True

        if is_in:
            cob_in_by_rm[rm] += amt
            rows_in += 1
        elif is_out:
            cob_out_by_rm[rm] += amt
            rows_out += 1

    total_in = float(sum(cob_in_by_rm.values()))
    total_out = float(sum(cob_out_by_rm.values()))

    logging.info(
        "[COB] Month=%s: COB_IN rows=%d total_in=%.2f | COB_OUT rows=%d total_out=%.2f",
        month_key,
        rows_in,
        total_in,
        rows_out,
        total_out,
    )

    # Apply configured correction factor (default 1.0)
    # For Legacy Parity, set "cob_in_correction_factor": 0.5 in config
    corr = float(RUNTIME_OPTIONS.get("cob_in_correction_factor", 1.0))
    if corr != 1.0:
        cob_in_by_rm = {rm: amt * corr for rm, amt in cob_in_by_rm.items()}
         # Note: COB Out correction removed as per investigation


    return cob_in_by_rm, cob_out_by_rm


# =========================
# Helper functions for Lumpsum Incentive logic
# =========================
def is_liquid_subcategory(subcat: str) -> bool:
    """
    Returns True if the subcategory is considered 'liquid' for weightage/bonus logic.
    We treat any subcategory containing 'LIQUID' or 'OVERNIGHT' as liquid.
    """
    if not subcat:
        return False
    s = str(subcat).strip().lower()
    return ("liquid" in s) or ("overnight" in s)


def compute_aum_multiple_for_growth(growth_pct: float) -> float:
    """
    Map monthly growth percentage to AUM multiple (slab-based).
    NOTE:
    - These slabs were used last year on a *quarterly* basis.
    - We are applying the same cutoffs on a *monthly* basis for now.
    - Adjust here later if we decide to scale slabs for monthly use.

    Slabs provided:
      upto 2%        -> 0
      2% - 3%        -> 1
      3% - 4.5%      -> 2.5
      4.5% - 5.3%    -> 7
      5.3% - 6%      -> 10
    For growth >= 6%, we continue to award 10 (same as the top slab).
    """
    if growth_pct < 2.0:
        return 0.0
    if 2.0 <= growth_pct < 3.0:
        return 1.0
    if 3.0 <= growth_pct < 4.5:
        return 2.5
    if 4.5 <= growth_pct < 5.3:
        return 7.0
    # 5.3% - 6% and anything above 6% earns 10 as per top slab
    if growth_pct >= 5.3:
        return 10.0
    return 0.0


# Lowercased keywords for blacklist checks (used with .str.lower())
# This is referenced in multiple places via `.isin(BLACKLISTED_CATEGORIES)`
BLACKLISTED_CATEGORIES: set[str] = {
    "liquid",
    "overnight",
    "low duration",
    "money market",
    "ultra short",
}

# Category rules (Mongo-configurable; defaults here)
# - blacklisted_categories: list[str] (tokens, case-insensitive)
# - match_mode: "substring" | "exact"
# - scope: ["SUB CATEGORY"] or ["CATEGORY", "SUB CATEGORY"]
# - zero_weight_purchase / zero_weight_switch_in: future hooks for weight adjustments
# - exclude_from_debt_bonus: flag to omit from debt bonus calc
CATEGORY_RULES: dict[str, Any] = {
    "blacklisted_categories": sorted([c.title() for c in BLACKLISTED_CATEGORIES]),
    "match_mode": "substring",
    "scope": ["SUB CATEGORY"],
    "zero_weight_purchase": True,
    "zero_weight_switch_in": True,  # Changed from False to match Legacy (0% for blacklisted)
    "exclude_from_debt_bonus": True,
}


def _normalize_bl_set(values) -> set[str]:
    out: set[str] = set()
    for v in values or []:
        try:
            s = str(v).strip().lower()
            if s:
                out.add(s)
        except Exception:
            continue
    return out


def _load_category_rules_from_cfg(cfg: dict | None) -> None:
    """Load optional category_rules from config doc into globals."""
    global CATEGORY_RULES, BLACKLISTED_CATEGORIES
    try:
        if not isinstance(cfg, dict):
            return
        rules = cfg.get("category_rules")
        if not isinstance(rules, dict):
            return
        merged = dict(CATEGORY_RULES)
        merged.update({k: v for k, v in rules.items() if v is not None})
        CATEGORY_RULES = merged
        BLACKLISTED_CATEGORIES = _normalize_bl_set(merged.get("blacklisted_categories"))
        logging.info(
            "[Config] Loaded category_rules (match=%s scope=%s count=%d)",
            merged.get("match_mode"),
            merged.get("scope"),
            len(BLACKLISTED_CATEGORIES),
        )
    except Exception as _e:
        logging.warning("[Config] category_rules load failed; using defaults: %s", _e)


def is_blacklisted_category(category: str | None, subcategory: str | None) -> bool:
    """Return True when category/subcategory hits blacklist as per CATEGORY_RULES."""
    try:
        tokens = BLACKLISTED_CATEGORIES
        if not tokens:
            return False
        match_mode = (CATEGORY_RULES.get("match_mode") or "substring").strip().lower()
        scope = CATEGORY_RULES.get("scope") or ["SUB CATEGORY"]
        scope = [str(s).strip().upper() for s in scope]
        vals: list[str] = []
        if "CATEGORY" in scope and category is not None:
            vals.append(str(category).lower())
        if "SUB CATEGORY" in scope and subcategory is not None:
            vals.append(str(subcategory).lower())
        if not vals:
            return False
        if match_mode == "exact":
            return any(v in tokens for v in vals)
        return any(any(tok in v for tok in tokens) for v in vals)
    except Exception:
        return False


def _split_category_blacklist(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame into (valid, blacklisted) based on category rules.
    """
    if df is None or df.empty:
        return df, pd.DataFrame()

    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    cat_col = cols_upper.get("CATEGORY")
    subcat_col = (
        cols_upper.get("SUB CATEGORY")
        or cols_upper.get("SUB-CATEGORY")
        or cols_upper.get("SUBCATEGORY")
    )

    if cat_col is None and subcat_col is None:
        return df, pd.DataFrame()

    def _is_blacklisted(row):
        cat_val = row.get(cat_col) if cat_col else None
        subcat_val = row.get(subcat_col) if subcat_col else None
        return is_blacklisted_category(cat_val, subcat_val)

    mask = df.apply(_is_blacklisted, axis=1)
    if int(mask.sum()) > 0:
        logging.debug(
            "[CategoryFilter] Found %d blacklisted rows.",
            int(mask.sum()),
        )
    return df.loc[~mask].copy(), df.loc[mask].copy()


def _upsert_lumpsum_record(collection, record: dict) -> None:
    """Helper to upsert a single Lumpsum record."""
    emp_id_for_write = record.get("employee_id")
    emp_name_for_write = record.get("employee_name") or record.get("employee_alias")
    is_active_for_write = bool(record.get("is_active", True))
    month_key = record.get("month")

    if emp_id_for_write:
        if emp_name_for_write:  # Check only name; active status handled by eligibility logic
            try:
                collection.update_one(
                    {
                        "Metric": "Lumpsum",
                        "employee_id": emp_id_for_write,
                        "month": month_key,
                    },
                    {"$set": record, "$currentDate": {"updatedAt": True}},
                    upsert=True,
                )
            except Exception as e:
                logging.warning(
                    "[Lumpsum] Failed to upsert row for RM='%s' employee_id='%s' month='%s': %s",
                    emp_name_for_write,
                    emp_id_for_write,
                    month_key,
                    e,
                )
        else:
            logging.info(
                "[Lumpsum] SKIP write for RM='%s' month='%s' (employee_id=%r is_active=%s emp_name_present=%s)",
                emp_name_for_write or record.get("employee_name"),
                month_key,
                emp_id_for_write,
                is_active_for_write,
                bool(emp_name_for_write),
            )
    else:
        logging.info(
            "[Lumpsum] SKIP write for RM='%s' month='%s' (missing employee_id)",
            record.get("employee_name"),
            month_key,
        )


def _run_lumpsum_for_window(
    *,
    db,
    db_leaderboard_conn=None,
    purchase_col,
    redemption_col,
    switchin_col,
    switchout_col,
    cob_col,
    aum_report_col,
    meetings_col,
    start: datetime,
    end: datetime,
    target_rm: str | None = None,
    dry_run: bool = False,
) -> int | list[dict]:
    """
    Window-level processing for Lumpsum leaderboard.
    NOTE: This implementation is intentionally conservative and relies on
    generic heuristics for column names. You *must* align the inferred field
    names (RM, date, amount) with your actual Mongo schema if they differ.

    High-level:
      - Load purchase/redemption/switch-in/switch-out/COB docs for the window.
      - Aggregate Net Purchase per RM using a simplified formula:
            NP = Purchases + SwitchIn + 0.5 * COB_In
                 - Redemptions - SwitchOut - 1.2 * COB_Out
        (COB weights chosen to roughly mirror NetPurchase engine; adjust if needed.)
      - Fetch AUM_start from AUM_Report collection via get_aum_data_from_collection.
      - Count meetings from Investor_Meetings_Data via _compute_meetings_metrics.
      - Compute Lumpsum incentive via _compute_lumpsum_incentive.
      - Upsert one document per (employee_id/employee_name, month) into
        PLI_Leaderboard.Leaderboard_Lumpsum using _normalize_ls_record.
    """

    global db_leaderboard
    if db_leaderboard_conn is not None:
        # Keep the authoritative handle in the global so helpers (like _normalize_ls_record)
        # can see it without threading the DB through every call.
        db_leaderboard = db_leaderboard_conn

    lb_db = db_leaderboard
    if lb_db is None:
        raise RuntimeError("[Window] leaderboard_db is None.")
    if db is None:
        raise RuntimeError("[Window] core db handle is None.")
    try:
        leaderboard_col = lb_db["Leaderboard_Lumpsum"]
    except Exception as e:
        raise RuntimeError(f"[Window] Unable to open Leaderboard_Lumpsum collection: {e}")
    month_key = _month_key(start)
    window_label = f"{month_key} | {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
    # Use specialised COB loader: fetch all ChangeofBroker, month-filter in Python,
    # and return per-RM COB IN / OUT dicts.
    cob_in_by_rm, cob_out_by_rm = _load_cob_for_month(cob_col, month_key)

    # --- Pre-warm streak tracker from previous month if empty ---
    # This supports the "Current Month Only" run mode by restoring state
    global _POSITIVE_STREAKS
    if not _POSITIVE_STREAKS:
        try:
            prev_dt = start - timedelta(days=5)
            prev_key = _month_key(prev_dt)
            logging.info("[Window] Pre-warming streaks from %s...", prev_key)
            prev_docs = lb_db["Leaderboard_Lumpsum"].find(
                {"month": prev_key},
                {"employee_id": 1, "employee_name": 1, "employee_alias": 1, "positive_np_streak": 1}
            )
            count = 0
            for rec in prev_docs:
                emp_key = (
                    rec.get("employee_id") or rec.get("employee_name") or rec.get("employee_alias") or ""
                )
                k = " ".join(str(emp_key).strip().lower().split())
                if k and int(rec.get("positive_np_streak", 0)) > 0:
                    _POSITIVE_STREAKS[k] = int(rec.get("positive_np_streak", 0))
                    count += 1
            logging.info("[Window] Warm-up complete. Loaded %d active streaks.", count)
        except Exception as e:
            logging.warning("[Window] Streak pre-warm failed: %s", e)

    logging.info(
        "[NetPurchase] Window %s (lb_db=%s core_db=%s)",
        window_label,
        getattr(lb_db, "name", "?"),
        getattr(db, "name", "?"),
    )

    # ----------------------------
    # Generic helpers (schema-agnostic)
    # ----------------------------
    def _load_df_for_window(col, label: str) -> pd.DataFrame:
        """
        Load collection into a DataFrame and filter by [start, end] using best-effort
        detection of a date-like field.
        """
        try:
            docs = list(col.find({}))
        except Exception as e:
            logging.warning(f"[Window] {label}: failed to load docs: {e}")
            return pd.DataFrame()

        if not docs:
            logging.debug(f"[Window] {label}: 0 docs in collection")
            return pd.DataFrame()

        df = pd.DataFrame(docs)
        if df.empty:
            return df

        # --- Defensive Filter: Exclude SIP/Systematic leakage ---
        # If upstream logic accidentally dumps SIP records (transactionType="SIP" etc.) into
        # purchase_txn, we must not count them as Lumpsum.
        try:
            # Case-insensitive column search
            cols_map = {c.lower(): c for c in df.columns}

            # Columns to check for forbidden keywords
            type_col = cols_map.get("transactiontype") or cols_map.get("transaction_type") or cols_map.get("type")
            cat_col = cols_map.get("category") or cols_map.get("cat")
            for_col = cols_map.get("transactionfor") or cols_map.get("transaction_for")

            exclude_mask = pd.Series(False, index=df.index)

            # Keywords that definitely indicate non-Lumpsum
            # (We use a simple string contains check)
            if type_col:
                exclude_mask |= df[type_col].astype(str).str.contains(r"SIP|SWP|Systematic", case=False, na=False)

            if cat_col:
                exclude_mask |= df[cat_col].astype(str).str.contains(r"Systematic", case=False, na=False)

            if for_col:
                exclude_mask |= df[for_col].astype(str).str.contains(r"SIP|Systematic", case=False, na=False)

            excluded_count = exclude_mask.sum()
            if excluded_count > 0:
                logging.warning(
                    "[Window] %s: DEFENSIVE FILTER dropped %d rows detecting SIP/Systematic keywords. "
                    "Check upstream ingestion!",
                    label,
                    excluded_count
                )
                df = df[~exclude_mask].copy()

        except Exception as e:
            logging.warning("[Window] %s: Filter SIP check failed (non-fatal): %s", label, e)


        # Special-case: for COB we prefer filtering by TRANSFER DATE using MM-YYYY pattern
        transfer_col = None
        if label == "cob":
            # Support common variants of the column name
            for c in ("TRANSFER DATE", "Transfer Date", "TRANSFER_DATE", "Transfer_Date"):
                if c in df.columns:
                    transfer_col = c
                    break

            if label == "cob" and transfer_col is not None:
                try:
                    # 1) Normalize to string, strip junk
                    transfer_raw = df[transfer_col].astype(str).str.strip()

                    # 2) Best-effort parse as DD-MM-YYYY first, then generic fallback
                    parsed = pd.to_datetime(
                        transfer_raw,
                        format="%d-%m-%Y",
                        errors="coerce",
                        dayfirst=True,
                    )
                    # If strict format failed for some rows, try generic parsing for those
                    need_generic = parsed.isna()
                    if need_generic.any():
                        parsed_generic = pd.to_datetime(
                            transfer_raw[need_generic],
                            errors="coerce",
                            dayfirst=True,
                        )
                        parsed[need_generic] = parsed_generic

                    # 3) Filter by month/year from `start`
                    mask = (parsed.dt.month == start.month) & (parsed.dt.year == start.year)
                    df_cob = df.loc[mask].copy()

                    logging.info(
                        "[Window] cob: %s parsed filter month=%02d year=%d matched %d rows out of %d",
                        transfer_col,
                        start.month,
                        start.year,
                        df_cob.shape[0],
                        df.shape[0],
                    )
                    return df_cob
                except Exception as e:
                    logging.warning(
                        "[Window] cob: failed %s parsed-date filter; falling back to generic DATE logic: %s",
                        transfer_col,
                        e,
                    )
                    # fall through to generic DATE logic below

        # Try explicit candidates first (common variants across exports)
        date_candidates = [
            "TXN DATE",
            "Txn Date",
            "Transaction Date",
            "TXN_DATE",
            "Txn_Date",
            "TRANSACTION_DATE",
            "DATE",
            "Date",
            "date",
            "VALUE DATE",
            "Value Date",
            "value_date",
            "VALUE_DATE",
        ]

        date_col = None
        for c in date_candidates:
            if c in df.columns:
                date_col = c
                break

        # Fallback: any column whose name contains "date" (case-insensitive)
        if date_col is None:
            for c in df.columns:
                try:
                    if "date" in str(c).lower():
                        date_col = c
                        logging.debug(
                            f"[Window] {label}: inferred date column '{date_col}' from columns={list(df.columns)}"
                        )
                        break
                except Exception:
                    continue

        if date_col is None:
            try:
                cols_preview = list(df.columns)
            except Exception:
                cols_preview = "unavailable"
            logging.warning(
                f"[Window] {label}: no recognised date column; skipping date filter (using all rows). Columns={cols_preview}"
            )
            return df

        # Parse to datetime best-effort and filter
        try:
            parsed = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
            mask = (parsed >= pd.Timestamp(start)) & (parsed <= pd.Timestamp(end))
            df = df.loc[mask].copy()
            logging.debug(
                f"[Window] {label}: filtered to {df.shape[0]} rows between {start.date()} and {end.date()} using date column '{date_col}'"
            )
        except Exception as e:
            logging.warning(
                f"[Window] {label}: failed to parse/filter dates from column '{date_col}': {e}"
            )
        return df

    def _load_meetings_df(col, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        """Load meetings (with the same date window) for in-memory filtering."""
        return _load_df_for_window(col, "meetings")

    def _infer_rm_column(df: pd.DataFrame) -> str | None:
        """Best-effort RM name column detection."""
        if df is None or df.empty:
            return None

        rm_candidates = [
            # Core RM naming
            "MAIN RM",
            "Main RM",
            "RM",
            "Rm",
            "rm",
            "EMPLOYEE NAME",
            "Employee Name",
            "employee_name",
            "Employee",
            "employee",
            # Common in your MF exports
            "RELATIONSHIP  MANAGER",
            "Relationship  Manager",
            "Relationship Manager",
            "RELATIONSHIP MANAGER",
            # Meetings / CRM style
            "Owner",
            "OWNER",
            "Owner Name",
            "owner_name",
            "OwnerName",
        ]

        # First, try explicit candidates
        for c in rm_candidates:
            if c in df.columns:
                return c

        # Fallback: any column that looks name-ish
        for c in df.columns:
            lc = str(c).lower()
            if (
                "rm" in lc
                or "employee" in lc
                or "advisor" in lc
                or "owner" in lc
                or "relationship" in lc
            ):
                return c

        return None

    def _infer_amount_column(df: pd.DataFrame) -> str | None:
        """Best-effort transaction amount column detection."""
        if df is None or df.empty:
            return None
        amt_candidates = [
            # Your exports
            "TOTAL AMOUNT",
            "Total Amount",
            "TOTAL_AMOUNT",
            "Total_Amount",
            "TOTAL_AMT",
            "Total Amt",
            # Generic names
            "AMOUNT",
            "Amount",
            "amount",
            "NET AMOUNT",
            "Net Amount",
            "net_amount",
            "TXN AMT",
            "Txn Amt",
            "TXN_AMOUNT",
            "txn_amount",
        ]
        for c in amt_candidates:
            if c in df.columns:
                return c
        # Fallback: first numeric-like column
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]):
                return c
        return None

    def _clean_rm(val) -> str | None:
        """Normalize RM name and apply hard skip/sanitize rules.

        This function now enforces a canonical RM key so that variations like
        'ISHU MAVAR' and 'Ishu   Mavar' are treated as the same RM. The canonical
        form is:
            - stripped
            - internal whitespace collapsed to single spaces
            - upper-cased
        """
        name_raw = str(val or "").strip()
        if not name_raw:
            return None

        rm_norm, ok = _sanitize_employee_name(name_raw)
        if not ok:
            return None

        # --- Canonicalize RM key (IMPORTANT FIX) ---
        # Collapse multiple spaces and force a stable case to avoid duplicate
        # keys such as 'ISHU MAVAR' vs 'Ishu Mavar' or trailing-space variants.
        rm_norm = " ".join(str(rm_norm).strip().split()).upper()

        if _skip_match(rm_norm):
            return None

        return rm_norm

    def _infer_scheme_column(df: pd.DataFrame) -> str | None:
        """Best-effort Scheme Name column detection."""
        if df is None or df.empty:
            return None

        candidates = [
            "SCHEME", "Scheme", "scheme",
            "SCHEME NAME", "Scheme Name", "scheme_name",
            "SCHEME_NAME", "Product", "PRODUCT", "Fund", "FUND"
        ]

        for c in candidates:
            if c in df.columns:
                return c

        # Fallback: look for 'scheme' in any column
        for c in df.columns:
            if "scheme" in str(c).lower():
                return c
        return None

    def _infer_date_column(df: pd.DataFrame) -> str | None:
        """Best-effort Date column detection."""
        if df is None or df.empty:
            return None

        candidates = [
            "TRXN_DATE", "Trxn_Date", "Transaction Date", "Date", "DATE",
            "NAV_DATE", "Nav_Date", "Value Date", "VALUE_DATE"
        ]

        for c in candidates:
            if c in df.columns:
                return c

        # Fallback
        for c in df.columns:
            if "date" in str(c).lower():
                return c
        return None

    def _apply_scheme_weights(df: pd.DataFrame, label: str) -> pd.DataFrame:
        """
        Apply scheme-based weightage multipliers to the 'AMOUNT' column.
        Modifies the dataframe in-place (or returns modified copy).

        Rules are fetched from WEIGHTS['scheme_rules'].
        Each rule: { "keyword": "...", "match_type": "contains"|"exact", "weight_pct": 120 }
        """
        if df is None or df.empty:
            return df

        scheme_rules = WEIGHTS.get("scheme_rules", [])
        if not scheme_rules:
            return df

        scheme_col = _infer_scheme_column(df)
        if not scheme_col:
            logging.debug(f"[Window] {label}: No scheme column found; skipping scheme weights.")
            return df

        logging.info(f"[Window] {label}: Applying {len(scheme_rules)} scheme rules using col='{scheme_col}'")

        # Ensure Scheme Name is string
        df[scheme_col] = df[scheme_col].astype(str).fillna("")
        scheme_series = df[scheme_col].str.strip().str.lower()

        # Date handling
        date_col = _infer_date_column(df)
        date_series = None
        if date_col:
            # Convert to datetime for comparison
            # Using dayfirst=True is common for Indian formats, but safer to assume standardized or try best effort
            date_series = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
        else:
            logging.debug(f"[Window] {label}: No date column found; rules with date ranges will be ignored/fail open.")

        # Track which rows have been modified to enforce "First matching rule wins"
        # validation: boolean mask of processed rows
        processed_mask = pd.Series(False, index=df.index)

        for rule in scheme_rules:
            # 1. Parse Rule Metadata
            kw = str(rule.get("keyword", "")).strip().lower()
            if not kw: continue

            wt_pct = float(rule.get("weight_pct", 100))
            match_type = str(rule.get("match_type", "exact")).strip().lower()

            # Date Range (optional)
            start_date_str = rule.get("start_date") # YYYY-MM-DD
            end_date_str = rule.get("end_date")     # YYYY-MM-DD

            # 2. Build Scheme Match Mask
            if match_type == "exact":
                rule_match = (scheme_series == kw)
            else:
                rule_match = scheme_series.str.contains(kw, regex=False)

            # 3. Apply Date Filter (if configured)
            if date_series is not None and (start_date_str or end_date_str):
                if start_date_str:
                    try:
                        sd = pd.to_datetime(start_date_str)
                        rule_match = rule_match & (date_series >= sd)
                    except: pass
                if end_date_str:
                    try:
                        ed = pd.to_datetime(end_date_str)
                        # Set to end of day? usually comparison is inclusive of day if time is 00:00
                        # If date_series has time, strictly > might fail?
                        # Let's assume date only.
                        # To be safe, end_date + 1 day - 1 microsecond? Or just <=
                        ed = pd.to_datetime(end_date_str) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        rule_match = rule_match & (date_series <= ed)
                    except: pass

            # 4. Exclude already processed rows
            final_mask = rule_match & (~processed_mask)

            if final_mask.any():
                mult = wt_pct / 100.0
                # logging.debug(f"  Rule '{kw}' matched {final_mask.sum()} rows. Weight: {wt_pct}%")
                df.loc[final_mask, "AMOUNT"] *= mult
                processed_mask = processed_mask | final_mask

        return df

    # ----------------------------
    # Load / normalize raw data
    # ----------------------------
    df_pur = _load_df_for_window(purchase_col, "purchase")
    df_red = _load_df_for_window(redemption_col, "redemption")
    df_sin = _load_df_for_window(switchin_col, "switchin")
    df_sout = _load_df_for_window(switchout_col, "switchout")
    df_cob = _load_df_for_window(cob_col, "cob")
    df_meetings = _load_meetings_df(meetings_col, start, end)

    # ... (rest of loading logic) ...


    if df_cob is not None and not df_cob.empty:
        logging.info("[Window] COB raw rows: %d", df_cob.shape[0])

    # Load Zoho Users for alias resolution
    users_df = pd.DataFrame()
    try:
        u_list = list(lb_db.Zoho_Users.find({}))
        if u_list:
            users_df = pd.DataFrame(u_list)
            logging.debug("[Window] Loaded %d Zoho Users for alias resolution", len(users_df))
    except Exception as e:
        logging.warning("[Window] Failed to load Zoho_Users: %s", e)

    # Pre-aggregate meetings by RM once per window (avoid per-RM DB scans)
    meetings_rm_col = None
    meetings_by_rm: dict[str, int] = {}
    if df_meetings is not None and not df_meetings.empty:
        meetings_rm_col = _infer_rm_column(df_meetings)
        if meetings_rm_col is None:
            logging.warning(
                "[Meetings] Could not infer RM column for meetings; "
                "disabling meeting-based multiplier for this window."
            )
            df_meetings = pd.DataFrame()
        else:
            tmp_meet = df_meetings.copy()

            # Helper to resolve owner -> canonical RM name
            def _resolve_meeting_owner(val):
                raw = str(val or "").strip()
                if not raw: return None
                # 1. Try resolving alias
                resolved_name, _, _ = _resolve_rm_identity(raw.lower(), users_df)
                # 2. Normalize to canonical upper key
                return _clean_rm(resolved_name or raw)

            tmp_meet["RM"] = tmp_meet[meetings_rm_col].apply(_resolve_meeting_owner)
            tmp_meet = tmp_meet[tmp_meet["RM"].notna()]
            if not tmp_meet.empty:
                grp = tmp_meet.groupby("RM").size()
                meetings_by_rm = {str(idx): int(val) for idx, val in grp.items()}
                logging.info("[Meetings] Aggregated counts for %d RMs", len(meetings_by_rm))

    # Normalize column names for consistent downstream logic
    dfs = {
        "purchase": df_pur,
        "redemption": df_red,
        "switchin": df_sin,
        "switchout": df_sout,
        "cob": df_cob,
    }

    dfs_valid = {}
    dfs_bl = {}

    for label, df in dfs.items():
        if df is None or df.empty:
            dfs_valid[label] = pd.DataFrame()
            dfs_bl[label] = pd.DataFrame()
            continue
        rm_col = _infer_rm_column(df)
        amt_col = _infer_amount_column(df)
        if rm_col is None or amt_col is None:
            logging.warning(
                "[Window] %s: could not infer RM or Amount column (rm_col=%s, amt_col=%s); skipping.",
                label,
                rm_col,
                amt_col,
            )
            dfs_valid[label] = pd.DataFrame()
            dfs_bl[label] = pd.DataFrame()
            continue

        df = df.copy()
        df["RM"] = df[rm_col].apply(lambda x: _clean_rm(x))
        df["AMOUNT"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0.0)
        df = df[df["RM"].notna()]

        # Apply Scheme Weights (for Purchase and Switch In only)
        if label in ("purchase", "switchin"):
             df = _apply_scheme_weights(df, label)

        # Split valid vs blacklisted
        df_valid, df_bl = _split_category_blacklist(df)
        dfs_valid[label] = df_valid
        dfs_bl[label] = df_bl

    df_pur = dfs_valid["purchase"]
    df_red = dfs_valid["redemption"]
    df_sin = dfs_valid["switchin"]
    df_sout = dfs_valid["switchout"]
    df_cob = dfs_valid["cob"]

    # Handle COB in/out split if possible
    df_cob_in = pd.DataFrame()
    df_cob_out = pd.DataFrame()
    if df_cob is not None and not df_cob.empty:
        direction_col = None
        for c in ("Direction", "DIRECTION"):
            if c in df_cob.columns:
                direction_col = c
                break

        if direction_col:
            src = (
                df_cob[direction_col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.lower()
            )
            in_mask = src.str.contains("in")
            out_mask = src.str.contains("out")
            df_cob_in = df_cob.loc[in_mask].copy()
            df_cob_out = df_cob.loc[out_mask].copy()
        else:
            cob_type_col = None
            for c in ("COB TYPE", "Cob Type", "COB_TYPE"):
                if c in df_cob.columns:
                    cob_type_col = c
                    break

            if cob_type_col:
                src = df_cob[cob_type_col].astype(str).str.strip().str.upper()
                in_mask = src.str.startswith("TI")  # TICOB
                out_mask = src.str.startswith("TO")  # TOCOB
                df_cob_in = df_cob.loc[in_mask].copy()
                df_cob_out = df_cob.loc[out_mask].copy()
            else:
                # If we can't split, treat all as in
                df_cob_in = df_cob.copy()
                df_cob_out = pd.DataFrame()

    # ----------------------------
    # Aggregate per-RM metrics
    # ----------------------------
    def _sum_by_rm(df: pd.DataFrame) -> dict[str, float]:
        if df is None or df.empty:
            return {}
        grp = df.groupby("RM")["AMOUNT"].sum()
        return {str(idx): float(val) for idx, val in grp.items()}

    pur_by_rm = _sum_by_rm(df_pur)
    red_by_rm = _sum_by_rm(df_red)
    sin_by_rm = _sum_by_rm(df_sin)
    sout_by_rm = _sum_by_rm(df_sout)

    # Blacklisted aggregates
    pur_bl_by_rm = _sum_by_rm(dfs_bl["purchase"])
    sin_bl_by_rm = _sum_by_rm(dfs_bl["switchin"])
    sout_bl_by_rm = _sum_by_rm(dfs_bl["switchout"])
    # cob_out_by_rm = _sum_by_rm(df_cob_out)

    # --- Pre-compute Category-wise Purchases for Generic Bonuses ---
    pur_by_rm_cat = {}
    if df_pur is not None and not df_pur.empty:
        # Group by RM and Category, sum Amount
        try:
            temp_df = df_pur.copy()
            # Ensure CATEGORY col exists (it should from _infer)
            c_col = None
            for col in temp_df.columns:
                if str(col).strip().upper() == "CATEGORY":
                    c_col = col
                    break

            if c_col:
                temp_df["CAT_NORM"] = temp_df[c_col].astype(str).str.strip().str.upper()
                grp_cat = temp_df.groupby(["RM", "CAT_NORM"])["AMOUNT"].sum()
                for (r, c), v in grp_cat.items():
                    r_str = str(r)
                    if r_str not in pur_by_rm_cat:
                        pur_by_rm_cat[r_str] = {}
                    pur_by_rm_cat[r_str][c] = float(v)
        except Exception as e:
            logging.warning("[Lumpsum] Failed to pre-agg category purchases: %s", e)

    # --- Pre-compute Debt Purchase for Bonus Logic ---
    # We need to know how much of 'df_pur' constitutes "Debt" to check ratio.
    # We use WEIGHTS["debt_bonus"]["debt_categories"] for filtering.
    debt_cfg = WEIGHTS.get("debt_bonus", {})
    debt_tokens = debt_cfg.get("debt_categories") or ["debt"]

    def _is_debt_row(row):
        # Helper to check if row is Debt based on Category/Sub Category
        # Similar logic to blacklist but inclusive
        cols_upper = {str(c).strip().upper(): c for c in row.index}
        cat_col = cols_upper.get("CATEGORY")
        subcat_col = (cols_upper.get("SUB CATEGORY") or cols_upper.get("SUB-CATEGORY") or cols_upper.get("SUBCATEGORY"))

        vals = []
        if cat_col: vals.append(str(row[cat_col]).lower())
        if subcat_col: vals.append(str(row[subcat_col]).lower())

        for val in vals:
            for token in debt_tokens:
                if token.lower() in val:
                    return True
        return False

    if df_pur is not None and not df_pur.empty and debt_cfg.get("enable"):
        df_debt = df_pur[df_pur.apply(_is_debt_row, axis=1)]
        debt_pur_by_rm = _sum_by_rm(df_debt)
    else:
        debt_pur_by_rm = {}

    all_rms: set[str] = set()
    for d in (pur_by_rm, red_by_rm, sin_by_rm, sout_by_rm, cob_in_by_rm, cob_out_by_rm, pur_bl_by_rm, sin_bl_by_rm, sout_bl_by_rm):
        all_rms.update(d.keys())
    all_rms.update(meetings_by_rm.keys())

    # If target_rm provided, filter cleanly (handling case/whitespace via _clean_rm)
    if target_rm:
        normalized_target = _clean_rm(target_rm)
        if normalized_target and normalized_target in all_rms:
            all_rms = {normalized_target}
        elif normalized_target:
             # Force include target_rm even if no activity to ensure Projected Bounty (zero doc) is generated
             all_rms = {normalized_target}
        else:
            # Invalid target name
            all_rms = set()

    if not all_rms:
        logging.info("[Lumpsum] Window %s: no RMs with activity (or target RM not found); nothing to write.", window_label)
        return [] if dry_run else 0

    # Global config for trail and penalty
    annual_trail_rate = float(os.getenv("PLI_LS_ANNUAL_TRAIL_RATE", "0.8"))

    upserted = 0
    sim_results = []
    # Sanity check: log any RM name variants that still differ at the raw level
    _log_rm_variant_warnings(all_rms)
    for rm_name in sorted(all_rms):
        if not rm_name:
            continue

        # Apply inactive eligibility gate (6-month rule)
        if not _rm_eligible_by_inactive(lb_db, rm_name, month_key):
            continue

        purchase = pur_by_rm.get(rm_name, 0.0)
        redemption = red_by_rm.get(rm_name, 0.0)
        switch_in = sin_by_rm.get(rm_name, 0.0)
        switch_out = sout_by_rm.get(rm_name, 0.0)
        cob_in_val = cob_in_by_rm.get(rm_name, 0.0)
        cob_out_val = cob_out_by_rm.get(rm_name, 0.0)

        # Blacklisted values
        purchase_bl = pur_bl_by_rm.get(rm_name, 0.0)
        switch_in_bl = sin_bl_by_rm.get(rm_name, 0.0)
        switch_out_bl = sout_bl_by_rm.get(rm_name, 0.0)

        # FIXED: Wire up Category Rules (Toggles)
        # If 'zero_weight_purchase' is FALSE, we INCLUDE blacklisted purchases.
        # Default is TRUE (exclude), so we only add if it's False.
        cat_rules = CATEGORY_RULES or {}
        if not cat_rules.get("zero_weight_purchase", True):
             purchase += purchase_bl

        if not cat_rules.get("zero_weight_switch_in", True):
             switch_in += switch_in_bl

        # LEGACY PARITY FIX: Add blacklisted switch-out back to regular switch-out
        # Legacy doesn't separate blacklisted switch-out - it includes them in the total
        # We keep this behavior unless explicitly toggled otherwise (not exposed in UI yet, but robust)
        switch_out += switch_out_bl

        # --- Build Breakdown (weighted components used for NetPurchase formula) ---
        # We keep the Lumpsum NP formula aligned with the docstring:
        #   NP = Purchase + SwitchIn + 0.5 * COB_In
        #        - Redemption - SwitchOut - 1.2 * COB_Out
        # Special Rules for Blacklisted:
        #  - Purchase BL: 0% weight.
        #  - Switch In BL: Treated as Redemption (100% Subtracted).
        #  - Switch Out BL: Treated as Purchase (100% Added).

        cob_in_w_pct = float(WEIGHTS.get("cob_in_pct", 50))
        cob_out_w_pct = float(WEIGHTS.get("cob_out_pct", 120))
        switch_in_w_pct = float(WEIGHTS.get("switch_in_pct", 100))
        switch_out_w_pct = float(WEIGHTS.get("switch_out_pct", 100))

        # --- Calculate Debt Bonus ---
        debt_bonus_val = 0.0
        debt_cfg = WEIGHTS.get("debt_bonus", {})
        if debt_cfg.get("enable"):
            total_pur = float(purchase)
            if total_pur > 0:
                debt_pur = debt_pur_by_rm.get(rm_name, 0.0)
                debt_ratio = (debt_pur / total_pur) * 100.0
                threshold = float(debt_cfg.get("max_debt_ratio_pct", 75))
                if debt_ratio < threshold and debt_pur > 0:
                    bonus_pct = float(debt_cfg.get("bonus_pct", 20))
                    debt_bonus_val = debt_pur * (bonus_pct / 100.0)

        # --- Generic Category Bonuses (Equity, Hybrid, etc.) ---
        cat_bonuses = {}
        for bonus_key in ["equity_bonus", "hybrid_bonus"]:
            cfg = WEIGHTS.get(bonus_key, {})
            if cfg.get("enable"):
                # Determine target category keyword (e.g. 'EQUITY', 'HYBRID')
                # If not explicit, derive from key (equity_bonus -> EQUITY)
                target_cat = str(cfg.get("category_keyword") or bonus_key.split('_')[0]).upper()

                # Sum purchases for this category
                cat_pur = 0.0
                rm_cats = pur_by_rm_cat.get(rm_name, {})
                for cat_name, sum_val in rm_cats.items():
                    # Simple substring match or exact match depending on strictness?
                    # Let's use substring to match "EQUITY" in "EQUITY - LARGE CAP"
                    if target_cat in cat_name:
                        cat_pur += float(sum_val or 0.0)

                # Check percentage gate (max_ratio_pct)
                total_pur = float(purchase)
                ratio_ok = True
                gate_str = ""

                if total_pur > 0:
                    ratio = (cat_pur / total_pur) * 100.0
                    # Use 'gate_pct' or check 'max_ratio_pct' for logic value
                    gate_val = cfg.get("gate_pct") or cfg.get("max_ratio_pct")
                    if gate_val is not None and str(gate_val).strip():
                        gate_thresh = float(gate_val)
                        if ratio < gate_thresh:
                            ratio_ok = False
                        else:
                            gate_str = f" if >{gate_thresh:g}%"

                if cat_pur > 0 and ratio_ok:
                    bpct = float(cfg.get("bonus_pct", 0.0))
                    if bpct != 0:
                        val = cat_pur * (bpct / 100.0)
                        sign_str = "+" if bpct > 0 else ""
                        label = f"{target_cat.title()} Purchase Bonus ({sign_str}{bpct:g}%{gate_str})"
                        cat_bonuses[label] = val

        additions = {
            "Total Purchase (100%)": float(purchase),
            # Labels include dynamic percentage if non-standard
            f"Switch In ({switch_in_w_pct:.0f}%)": float(switch_in * (switch_in_w_pct / 100.0)),
            f"Debt Purchase Bonus (+{debt_cfg.get('bonus_pct', 20)}% if <{debt_cfg.get('max_debt_ratio_pct', 75)}%)": float(debt_bonus_val),
            **cat_bonuses,
            "Blacklisted & Liquid Purchase (0%)": float(purchase_bl),
            "Switch Out (Blacklisted) -> Purchase (100%)": 0.0,  # Disabled for Legacy parity
            f"Change Of Broker In - TICOB ({cob_in_w_pct:.0f}%)": float(cob_in_val * (cob_in_w_pct / 100.0)),
        }
        subtractions = {
            "Redemption (100%)": float(redemption),
            f"Switch Out ({switch_out_w_pct:.0f}%)": float(switch_out * (switch_out_w_pct / 100.0)),
            "Switch In (Blacklisted) -> Redemption (100%)": 0.0,  # Disabled for Legacy parity
            f"Change Of Broker Out - TOCOB ({cob_out_w_pct:.0f}%)": float(cob_out_val * (cob_out_w_pct / 100.0)),
        }

        total_additions = sum(additions.values())
        total_subtractions = sum(subtractions.values())
        net_formula = total_additions - total_subtractions

        breakdown = {
            "Additions": additions,
            "Subtractions": subtractions,
            "Totals": {
                "Total Additions": total_additions,
                "Total Subtractions": total_subtractions,
                "Net Purchase (Formula)": net_formula,
            },
        }

        # NetPurchase value used everywhere else in this window; keep it aligned with Breakdown.
        np_val = net_formula

        # AUM_start from AUM_Report (already cached by helper)
        aum_start = float(get_aum_for_rm_month(rm_name, month_key, aum_report_col) or 0.0)

        if aum_start > 0:
            growth_pct = 100.0 * (np_val / aum_start)
        else:
            growth_pct = 0.0

        # Rate slab + growth band
        rate_used, growth_band = _rate_from_slabs(growth_pct)

        # Meetings multiplier
        meetings_count = int(meetings_by_rm.get(rm_name, 0) or 0)
        meetings_mult, meetings_slab = _meeting_from_slabs(meetings_count)

        # Trail computation (annual % → monthly rupees)
        if aum_start > 0 and annual_trail_rate > 0:
            monthly_trail_used = round(aum_start * annual_trail_rate / 1200.0, 2)
        else:
            monthly_trail_used = 0.0

        base_incentive = monthly_trail_used * rate_used

        # Apply meetings multiplier on top
        final_incentive_raw = base_incentive
        final_incentive = final_incentive_raw * meetings_mult

        # Zoho-based identity resolution
        employee_id, employee_alias, is_active = _resolve_employee_identity_for_lumpsum(
            lb_db, rm_name
        )

        # Activity flags for AuditMeta
        has_activity = any(
            abs(x) > 0.0
            for x in (
                purchase,
                redemption,
                switch_in,
                switch_out,
                cob_in_val,
                cob_out_val,
            )
        )

        # --- Lumpsum negative NP penalty (Mongo-configurable) ---
        ls_pen_cfg = LS_PENALTY_CFG or {}
        penalty_rupees_raw = 0.0
        penalty_rupees_applied = 0.0
        try:
            np_val_float = float(np_val or 0.0)
        except Exception:
            np_val_float = 0.0

        if ls_pen_cfg.get("enable", True) and np_val_float < 0.0:
            try:
                g = float(growth_pct or 0.0)
            except Exception:
                g = 0.0

            try:
                band1_trail_pct = float(ls_pen_cfg.get("band1_trail_pct", 0.0) or 0.0)
            except Exception:
                band1_trail_pct = 0.0
            try:
                band1_cap_rupees = float(ls_pen_cfg.get("band1_cap_rupees", 0.0) or 0.0)
            except Exception:
                band1_cap_rupees = 0.0
            try:
                band2_rupees = float(ls_pen_cfg.get("band2_rupees", 0.0) or 0.0)
            except Exception:
                band2_rupees = 0.0

            # LEGACY PENALTY RULES:
            # Band 1: Growth <= -1.0% → min(0.5% × trail, 5000)
            # Band 2: -1.0% < Growth <= -0.5% → min(0.5% × trail, 2500)
            # Band 3: -0.5% < Growth <= 0% → Zero out all points

            if g <= -1.0:
                # Band 1: Deep negative growth
                trail_component = 0.0
                if band1_trail_pct > 0.0 and monthly_trail_used is not None:
                    try:
                        trail_component = float(monthly_trail_used) * (band1_trail_pct / 100.0)
                    except Exception:
                        trail_component = 0.0
                penalty_rupees_applied = (
                    min(band1_cap_rupees, trail_component)
                    if band1_cap_rupees > 0.0
                    else trail_component
                )
            # Parse slabs to find Band 2 Cap (Replacement for 'band2_rupees' flat key)
            # Band 2 definition: Growth is between -1.0% and -0.5%
            # We look for a slab where max_growth_pct is approx -0.5
            band2_cap_from_slabs = 0.0
            slabs = ls_pen_cfg.get("slabs")
            if isinstance(slabs, list):
                for s in slabs:
                    try:
                        # Loose float matching for -0.5
                        mx = float(s.get("max_growth_pct", 0.0))
                        if abs(mx - (-0.5)) < 0.001:
                             # Found Band 2 slab
                             band2_cap_from_slabs = float(s.get("cap_rupees", 0.0))
                             break
                    except:
                        pass

            if -1.0 < g <= -0.5:
                # Band 2: Moderate negative growth
                # FIXED: Logic now checks Slabs first, then flat key 'band2_rupees', then default 2500
                trail_component = 0.0
                if band1_trail_pct > 0.0 and monthly_trail_used is not None:
                    try:
                        trail_component = float(monthly_trail_used) * (band1_trail_pct / 100.0)
                    except Exception:
                        trail_component = 0.0

                # Priority: Slab Cap > Flat Key > Legacy Hardcode
                cap = 2500.0
                if band2_cap_from_slabs > 0.0:
                    cap = band2_cap_from_slabs
                elif band2_rupees > 0.0:
                    cap = band2_rupees

                penalty_rupees_applied = min(cap, trail_component)
            elif -0.5 < g <= 0.0:
                # Band 3: Slight negative growth - NO PENALTY (0 penalty points)
                penalty_rupees_applied = 0.0
            else:
                # Positive growth - no penalty
                penalty_rupees_applied = 0.0
            penalty_rupees_raw = penalty_rupees_applied

        # LEGACY PARITY: Allow negative final incentive (e.g. -2500 points)
        final_incentive = final_incentive - penalty_rupees_applied

        record: dict[str, Any] = {
            "Metric": "Lumpsum",
            "month": month_key,
            # Identity fields
            "employee_id": employee_id,
            "employee_alias": employee_alias,
            "employee_name": rm_name,
            "is_active": bool(is_active),
            # Core metrics
            "AUM (Start of Month)": aum_start,
            "NetPurchase": round(np_val_float, 2),
            "net_purchase": round(np_val_float, 2),
            "growth_pct": round_sig(growth_pct, sig=4),
            "growth_band": growth_band,
            "rate_used": rate_used if rate_used > 0 else None,
            # Trail + incentive
            "annual_trail_rate": annual_trail_rate,
            "monthly_trail_used": round(monthly_trail_used, 2),
            "base_incentive": round(base_incentive, 2),
            "final_incentive": round(final_incentive, 2),
            # Meetings
            "meetings_count": meetings_count,
            "meetings_multiplier": meetings_mult,
            "meetings_slab": meetings_slab,
            # Penalty diagnostics
            "incentive_penalty_meta": {
                "penalty_rupees_raw": round(penalty_rupees_raw, 2),
                "penalty_rupees_applied": round(penalty_rupees_applied, 2),
                "band1_trail_pct": float(ls_pen_cfg.get("band1_trail_pct", 0.0) or 0.0),
                "band1_cap_rupees": float(ls_pen_cfg.get("band1_cap_rupees", 0.0) or 0.0),
                "band2_rupees": float(ls_pen_cfg.get("band2_rupees", 0.0) or 0.0),
                "ls_penalty_strategy": "growth_slab_v1",
                "np_val": round(np_val_float, 2),
            },
            # New: per-RM breakdown for audit backfill
            "Breakdown": breakdown,
            # For now, treat MTD as full-window; can be refined later if partial months are needed.
            "BreakdownMTD": breakdown,
        }

        # Minimal AuditMeta for window
        record.setdefault("AuditMeta", {})
        record["AuditMeta"].update(
            {
                "WindowStart": start.strftime("%Y-%m-%d"),
                "WindowEnd": end.strftime("%Y-%m-%d"),
                "HasActivity": bool(has_activity),
                "ZeroTransactionWindow": not bool(has_activity),
            }
        )

        # Normalize to ensure schema/version/config stamps
        record = _normalize_ls_record(record, start, end)
        # print(record)

        if dry_run:
            sim_results.append(record)
        else:
            _upsert_lumpsum_record(leaderboard_col, record)
            upserted += 1

    if dry_run:
        return sim_results
    return int(upserted)


def _compute_lumpsum_incentive_points_only(
    *,
    net_purchase: float,
    aum_start: float,
    meetings_count: int,
    meetings_multiplier: float,
    annual_trail_rate: float = 0.8,
) -> dict:
    """
    OPTION A (2025-11-15):
    ----------------------
    Lumpsum scorer no longer applies **cash penalties** for negative months.
    Penalties are handled in the *points engine* only (e.g., -2500 points
    for certain negative growth bands).

    This function:
      - Computes growth_pct = NetPurchase / AUM_start * 100
      - Uses RATE_SLABS via _rate_from_slabs(growth_pct)
      - Computes monthly_trail_used from AUM and annual_trail_rate
      - Computes base_incentive and final_incentive (meeting-multiplied)
      - Returns a zeroed-out incentive_penalty_meta (for backward compat)
    """
    try:
        np_val = float(net_purchase or 0.0)
    except Exception:
        np_val = 0.0

    try:
        aum_val = float(aum_start or 0.0)
    except Exception:
        aum_val = 0.0

    try:
        meet_ct = int(meetings_count or 0)
    except Exception:
        meet_ct = 0

    try:
        meet_mult = float(meetings_multiplier or 1.0)
    except Exception:
        meet_mult = 1.0

    try:
        atr = float(annual_trail_rate or 0.0)
    except Exception:
        atr = 0.0

    growth_pct = 0.0
    if aum_val > 0:
        growth_pct = (np_val / aum_val) * 100.0

    rate_used, band = _rate_from_slabs(growth_pct)

    monthly_trail_used = 0.0
    if aum_val > 0 and atr > 0:
        monthly_trail_used = aum_val * (atr / 100.0) / 12.0

    base_incentive = monthly_trail_used * rate_used
    final_incentive = max(base_incentive, 0.0) * max(meet_mult, 0.0)

    incentive_penalty_meta = {
        "penalty_rupees_raw": 0.0,
        "penalty_rupees_applied": 0.0,
        "penalty_pct_of_trail": 0.0,
        "ls_penalty_strategy": LS_PENALTY_STRATEGY,
        "np_val": float(np_val),
    }

    return {
        "growth_pct": round_sig(growth_pct, 4),
        "band": band,
        "rate_used": float(rate_used),
        "base_incentive": round_sig(base_incentive, 2),
        "final_incentive": round_sig(final_incentive, 2),
        "meetings_count": meet_ct,
        "meetings_multiplier": meet_mult,
        "monthly_trail_used": round_sig(monthly_trail_used, 2),
        "annual_trail_rate": atr,
        "incentive_penalty_meta": incentive_penalty_meta,
    }


def _compute_meetings_metrics(
    meetings_source, rm_name: str, rm_lower: str, mtd_start: datetime, mtd_end: datetime
) -> tuple[int, float, str]:
    """Simplified meetings metric using the preloaded window DataFrame."""
    try:
        logging.debug(
            "[Meetings] Start | RM='%s' (alias='%s') | window=%s→%s",
            rm_name,
            rm_lower,
            mtd_start.strftime("%Y-%m-%d"),
            mtd_end.strftime("%Y-%m-%d"),
        )
        candidates = {str(part or "").strip().lower() for part in (rm_name, rm_lower) if part}
        meetings_count = 0
        if isinstance(meetings_source, pd.DataFrame) and not meetings_source.empty:
            df = meetings_source
            owner_cols = [
                "Owner",
                "OWNER",
                "Owner Name",
                "owner_name",
                "OwnerName",
                "RM",
                "Rm",
                "rm",
                "RM Name",
                "rm_name",
            ]
            date_cols = [
                "date",
                "Date",
                "created_at",
                "Created_At",
                "createdAt",
                "CreatedAt",
                "created",
                "Created",
                "timestamp",
                "Timestamp",
                "created at",
                "Created At",
                "createdOn",
                "CreatedOn",
            ]
            date_mask = None
            for col in date_cols:
                if col in df.columns:
                    parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    mask = (parsed >= mtd_start) & (parsed <= mtd_end)
                    date_mask = mask if date_mask is None else (date_mask & mask)
                    break
            match_mask = pd.Series(False, index=df.index)
            for col in owner_cols:
                if col in df.columns:
                    vals = df[col].astype(str).str.strip().str.lower()
                    match_mask |= vals.isin(candidates)
            if date_mask is not None:
                match_mask &= date_mask
            meetings_count = int(match_mask.sum())
        mult, slab = _meeting_from_slabs(meetings_count)
        logging.info(
            "[Meetings] Result | RM='%s' | count=%d | slab=%s | multiplier=%.3f",
            rm_name,
            meetings_count,
            slab,
            mult,
        )
        return meetings_count, mult, slab
    except Exception as e:
        logging.warning("[Meetings] Failed to count meetings for %s: %s", rm_name, e)
        return 0, 1.0, "0–5"


def _resolve_rm_identity(rm_lower: str, users_df: pd.DataFrame | None):
    """Return (rm_name, zoho_user_id, zoho_email) using Zoho users with alias support.
    Matching priority: AliasKeys → AliasNoSpaceKeys → NameKey → Alias(NameKey back-compat)."""
    rm_name = rm_lower
    zoho_user_id = None
    zoho_email = None
    try:
        if isinstance(users_df, pd.DataFrame) and not users_df.empty:
            df = users_df

            def _norm(s):
                return " ".join(str(s or "").strip().lower().split())

            key = _norm(rm_lower)
            nospace = key.replace(" ", "")

            # 1) Exact alias match (normalized)
            if "AliasKeys" in df.columns:
                m = df[df["AliasKeys"].apply(lambda lst: key in (lst or []))]
                if not m.empty:
                    d = m.iloc[0].to_dict()
                    return d.get("Full Name", rm_lower), d.get("User ID"), d.get("Email")

            # 2) No-space alias match
            if "AliasNoSpaceKeys" in df.columns:
                m = df[df["AliasNoSpaceKeys"].apply(lambda lst: nospace in (lst or []))]
                if not m.empty:
                    d = m.iloc[0].to_dict()
                    return d.get("Full Name", rm_lower), d.get("User ID"), d.get("Email")

            # 3) Full name (normalized)
            if "NameKey" in df.columns:
                m = df[df["NameKey"] == key]
                if not m.empty:
                    d = m.iloc[0].to_dict()
                    return d.get("Full Name", rm_lower), d.get("User ID"), d.get("Email")

            # 4) Back-compat columns used elsewhere ("Alias"==NameKey, "AliasNoSpace"==NameKeyNoSpace)
            if "Alias" in df.columns:
                m = df[df["Alias"] == key]
                if not m.empty:
                    d = m.iloc[0].to_dict()
                    return d.get("Full Name", rm_lower), d.get("User ID"), d.get("Email")

            if "AliasNoSpace" in df.columns:
                m = df[df["AliasNoSpace"] == nospace]
                if not m.empty:
                    d = m.iloc[0].to_dict()
                    return d.get("Full Name", rm_lower), d.get("User ID"), d.get("Email")
    except Exception as _e:
        logging.debug(f"[Identity] _resolve_rm_identity fallback for '{rm_lower}': {_e}")
    return rm_name, zoho_user_id, zoho_email


def growth_slab_label(p: float) -> str:
    if p < 2.0:
        return "<2%"
    if p < 3.0:
        return "2–<3%"
    if p < 4.5:
        return "3–<4.5%"
    if p < 5.3:
        return "4.5–<5.3%"
    return "≥5.3%"


# --- Helper: Compute monthly trail for a given AUM and annual rate ---
def compute_monthly_trail(aum_start: float, annual_rate: float | None = None) -> float:
    """
    Return monthly trail in rupees for the provided AUM_start and annual trail rate.
    Defaults to env PLI_TRAIL_ANNUAL (0.008 == 0.8% annual).
    """
    import os

    try:
        r = (
            float(annual_rate)
            if annual_rate is not None
            else float(os.getenv("PLI_TRAIL_ANNUAL", "0.008"))
        )
    except Exception:
        r = 0.008
    try:
        a = float(aum_start or 0.0)
    except Exception:
        a = 0.0
    return a * r / 12.0 if a > 0 else 0.0


def _compute_lumpsum_incentive(
    net_purchase: float,
    aum_start: float,
    meetings_count: int,
    meetings_multiplier: float,
) -> dict[str, Any]:
    """Compute Lumpsum incentive for a single RM-month window.

    Behaviour:
    - growth_pct = NP / AUM_start * 100 (0 if AUM_start <= 0).
    - growth_band uses `growth_slab_label` for reporting.
    - We map growth_pct → AUM_multiple using `compute_aum_multiple_for_growth`.
    - Base incentive is: monthly_trail * AUM_multiple.
    - A soft penalty is applied when NP is negative, capped via `_choose_penalty`.
    - A meeting-based multiplier is applied at the end.

    Returns a dict with keys:
      - growth_pct (float)
      - band (str)
      - aum_multiple (float)
      - monthly_trail_used (float)
      - annual_trail_rate (float)
      - base_incentive (float)
      - final_incentive (float)
      - meetings_count (int)
      - meetings_multiplier (float)
      - incentive_penalty_meta (dict)
    """

    try:
        np_val = float(net_purchase or 0.0)
    except Exception:
        np_val = 0.0

    try:
        aum_val = float(aum_start or 0.0)
    except Exception:
        aum_val = 0.0

    # Growth percentage
    if aum_val > 0:
        growth_pct = (np_val / aum_val) * 100.0
    else:
        growth_pct = 0.0

    band = growth_slab_label(growth_pct)

    # AUM multiple from growth slabs
    aum_multiple = compute_aum_multiple_for_growth(growth_pct)

    # Monthly trail (rupees) for this RM-month
    annual_trail_rate_env = os.getenv("PLI_TRAIL_ANNUAL", "0.008")
    try:
        annual_trail_rate = float(annual_trail_rate_env)
    except Exception:
        annual_trail_rate = 0.008

    monthly_trail = compute_monthly_trail(aum_val, annual_rate=annual_trail_rate)

    # Base incentive before penalties / multipliers
    base_incentive = monthly_trail * aum_multiple

    # Penalty for negative NP: cap by both a flat and a percentage of monthly trail
    penalty_rupees = 0.0
    penalty_pct_of_trail = 0.0

    if np_val < 0:
        try:
            # Flat penalty: 25% of |NP|
            flat_pen = 0.25 * abs(np_val)
            # Percentage penalty: 50% of monthly trail
            pct_pen = 0.50 * monthly_trail
            penalty_rupees = _choose_penalty(flat_pen, pct_pen)
            if monthly_trail > 0:
                penalty_pct_of_trail = (penalty_rupees / monthly_trail) * 100.0
        except Exception:
            penalty_rupees = 0.0
            penalty_pct_of_trail = 0.0

    # Apply penalty but do not let it exceed the base incentive (no negative base)
    effective_penalty = min(penalty_rupees, base_incentive) if base_incentive > 0 else 0.0
    post_penalty = max(base_incentive - effective_penalty, 0.0)

    # Apply meeting multiplier (already computed via slabs)
    try:
        m_mult = float(meetings_multiplier or 1.0)
    except Exception:
        m_mult = 1.0

    final_incentive = post_penalty * m_mult

    return {
        "growth_pct": round_sig(growth_pct, 4),
        "band": band,
        "aum_multiple": round_sig(aum_multiple, 4),
        "monthly_trail_used": round_sig(monthly_trail, 2),
        "annual_trail_rate": round_sig(annual_trail_rate * 100.0, 4),  # as % per annum
        "base_incentive": round_sig(base_incentive, 2),
        "final_incentive": round_sig(final_incentive, 2),
        "meetings_count": int(meetings_count or 0),
        "meetings_multiplier": round_sig(m_mult, 4),
        "incentive_penalty_meta": {
            "penalty_rupees_raw": round_sig(penalty_rupees, 2),
            "penalty_rupees_applied": round_sig(effective_penalty, 2),
            "penalty_pct_of_trail": round_sig(penalty_pct_of_trail, 4),
            "ls_penalty_strategy": LS_PENALTY_STRATEGY,
            "np_val": round_sig(np_val, 2),
        },
    }


# -----------------------------
# CLI entrypoint for manual runs
# -----------------------------


def _compute_month_windows(now: datetime | None = None) -> list[tuple[datetime, datetime]]:
    """
    Return a list of (start, end) windows to process based on RUNTIME_OPTIONS.

    - range_mode == 'last5' (default):
        Delegates to _compute_last5_windows, which uses a 5-day lookback and
        always ends the current window at (today + 1 day).

    - range_mode == 'fy':
        All months in the current FY up to the month before `now` as full
        calendar months (1st → last day), plus a final window for the current
        month from day 1 up to (today + 1 day) i.e. MTD+1.

    - range_mode == 'since':
        From the configured since_month (YYYY-MM) up to the current month,
        where:
          * intermediate months are full calendar months
          * the current month window ends at (today + 1 day) i.e. MTD+1.
    """
    now = now or datetime.utcnow()
    mode = str(RUNTIME_OPTIONS.get("range_mode", "last5")).strip().lower()

    def _month_start(y: int, m: int) -> datetime:
        return datetime(y, m, 1)

    def _month_end(y: int, m: int) -> datetime:
        if m == 12:
            return datetime(y, 12, 31)
        return datetime(y, m + 1, 1) - timedelta(days=1)

    windows: list[tuple[datetime, datetime]] = []

    if mode == "fy":
        # Work out FY start for the current date
        if FY_MODE == "FY_APR":
            # Indian FY: April–March
            fy_year = now.year
            if now.month < 4:
                fy_year -= 1
            start_y, start_m = fy_year, 4
        else:
            # Calendar FY: January–December
            start_y, start_m = now.year, 1

        # 1) Full-month windows up to the month before current
        y, m = start_y, start_m
        while (y, m) < (now.year, now.month):
            s = _month_start(y, m)
            e = _month_end(y, m)
            windows.append((s, e))
            m += 1
            if m == 13:
                m = 1
                y += 1

        # 2) Current month as MTD+1
        cur_start = datetime(now.year, now.month, 1)
        cur_end = datetime(now.year, now.month, now.day) + timedelta(days=1)
        windows.append((cur_start, cur_end))

    elif mode == "since":
        since_raw = str(RUNTIME_OPTIONS.get("since_month", "") or "").strip()
        if not since_raw:
            logging.warning(
                "[Range] range_mode='since' but no since_month configured; falling back to last5."
            )
            return _compute_last5_windows(now)

        try:
            parts = since_raw.split("-")
            if len(parts) < 2:
                raise ValueError("Expected YYYY-MM.")
            start_y = int(parts[0])
            start_m = int(parts[1])
            if start_m < 1 or start_m > 12:
                raise ValueError("Month out of range.")
        except Exception as e:
            logging.warning(
                "[Range] Invalid since_month=%r (%s); falling back to last5.",
                since_raw,
                e,
            )
            return _compute_last5_windows(now)

        # Clamp start to not go beyond the current month
        if (start_y, start_m) > (now.year, now.month):
            logging.warning(
                "[Range] since_month=%s is in the future relative to now=%s; "
                "clamping start to current month.",
                since_raw,
                now.date().isoformat(),
            )
            start_y, start_m = now.year, now.month

        y, m = start_y, start_m
        # Full months strictly before the current month
        while (y, m) < (now.year, now.month):
            s = _month_start(y, m)
            e = _month_end(y, m)
            windows.append((s, e))
            m += 1
            if m == 13:
                m = 1
                y += 1

        # Current month as MTD+1
        cur_start = datetime(now.year, now.month, 1)
        cur_end = datetime(now.year, now.month, now.day) + timedelta(days=1)
        windows.append((cur_start, cur_end))

    else:
        # Range Mode: Auto / Default
        # Rule:
        #  - If Day <= 10: Calculate Last Month + Current Month
        #  - If Day > 10: Calculate Current Month Only

        current_date_val = now.day

        # 1) Determine Last Month window
        # (Only added if <= 10th)
        if current_date_val <= 10:
            if now.month == 1:
                lm_y, lm_m = now.year - 1, 12
            else:
                lm_y, lm_m = now.year, now.month - 1

            lm_start = _month_start(lm_y, lm_m)
            lm_end = _month_end(lm_y, lm_m)
            windows.append((lm_start, lm_end))

        # 2) Current Month window (Always included)
        # Ends at (Today + 1 day) to cover today's transactions in UTC/IST shift
        cur_start = datetime(now.year, now.month, 1)
        cur_end = datetime(now.year, now.month, now.day) + timedelta(days=1)
        windows.append((cur_start, cur_end))

        # Log decision
        logging.info(
            "[Range] Auto-Mode (Day=%d): selected %d windows.",
            current_date_val,
            len(windows)
        )


    return windows


def _cli_manual_run() -> None:
    """Manual CLI runner: connects to Mongo, loads config, and runs all windows.

    This is intentionally self-contained and does **not** depend on Azure Functions
    runtime. It mirrors the behaviour of the timer-triggered main() as closely
    as possible for local debugging.
    """
    logging.info("[CLI] Manual run starting")

    mongo_uri = get_secret(MONGODB_SECRET_NAME)
    if not mongo_uri:
        raise RuntimeError("Mongo connection string not available for leaderboard/core DB.")

    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)

    # Leaderboard DB (already has a default via LEADERBOARD_DB_NAME)
    lb_db = client[LEADERBOARD_DB_NAME]

    # Optional distributed lock (same key as timer job)
    lock_enabled = os.getenv("PLI_LOCK_ENABLED", "1").strip().lower() not in ("0", "false", "no")
    lock_key = os.getenv("PLI_LOCK_KEY", "lumpsum-scorer").strip() or "lumpsum-scorer"
    lock_ttl = int(os.getenv("PLI_LOCK_TTL_SEC", "5400") or "5400")

    lock_acquired = False
    if lock_enabled:
        lock_acquired = acquire_distributed_lock(client, lock_key, ttl_sec=lock_ttl)
        if not lock_acquired:
            logging.warning("[CLI] Another instance holds the lock; exiting.")
            return

    try:
        # Call the new run_net_purchase function with the reusable client
        run_net_purchase(leaderboard_db=lb_db, mongo_client=client)
        logging.info("[Month Done] CLI run complete.")
    except Exception as e:
        logging.error("[CLI] Manual run failed: %s", e, exc_info=True)
        raise
    finally:
        if lock_enabled and lock_acquired:
            release_distributed_lock(client, lock_key)
        try:
            client.close()
        except Exception:
            pass


def main(mytimer: func.TimerRequest) -> None:
    """Azure Function timer entrypoint reusing the manual CLI pipeline."""
    logging.info("[Timer] Lumpsum scorer trigger fired.")
    try:
        _cli_manual_run()
    except Exception as e:
        logging.error("[Timer] Manual run failed: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    _cli_manual_run()

# NOTE Future route for ARIA to handle and to set modifications to incentive through a dashboard. postponed to V2
# def http_since(req: func.HttpRequest) -> func.HttpResponse:
#     """
#     Azure Functions HTTP trigger entry point to run the scorer from a requested
#     month up to the current month MTD+1.

#     Usage (function.json should map entryPoint to 'http_since'):
#       - GET  /?since=YYYY-MM
#       - POST with JSON body: {"since": "YYYY-MM"} or {"from_month": "YYYY-MM"}

#     Behaviour:
#       - Sets an in-process override:
#             range_mode = "since"
#             since_month = <requested month>
#       - Invokes the same pipeline used by the timer/CLI runner so that
#         _compute_month_windows uses the 'since' logic.
#     """
#     logging.info("[HTTP] Lumpsum_Scorer since-month HTTP request received.")
#     try:
#         since = req.params.get("since")
#         if not since:
#             try:
#                 body = req.get_json()
#             except Exception:
#                 body = None
#             if isinstance(body, dict):
#                 since = body.get("since") or body.get("from_month") or body.get("month")

#         if not since:
#             return func.HttpResponse(
#                 json.dumps(
#                     {
#                         "ok": False,
#                         "error": "Missing 'since' parameter. Expected YYYY-MM via query or JSON body.",
#                     }
#                 ),
#                 status_code=400,
#                 mimetype="application/json",
#             )

#         since_str = str(since).strip()

#         # Basic sanity check: expect YYYY-MM
#         try:
#             parts = since_str.split("-")
#             if len(parts) < 2:
#                 raise ValueError("Expected YYYY-MM.")
#             year = int(parts[0])
#             month = int(parts[1])
#             if month < 1 or month > 12:
#                 raise ValueError("Month out of range.")
#         except Exception as e:
#             return func.HttpResponse(
#                 json.dumps(
#                     {
#                         "ok": False,
#                         "error": f"Invalid 'since' month '{since_str}': {e}. Expected YYYY-MM.",
#                     }
#                 ),
#                 status_code=400,
#                 mimetype="application/json",
#             )

#         global RUNTIME_OVERRIDES
#         # Preserve any existing overrides so this HTTP run is non-destructive
#         prev_overrides = dict(RUNTIME_OVERRIDES or {})

#         RUNTIME_OVERRIDES = {
#             "range_mode": "since",
#             "since_month": since_str,
#         }

#         try:
#             logging.info(
#                 "[HTTP] Triggering Lumpsum_Scorer run with range_mode='since', since_month=%s.",
#                 since_str,
#             )
#             # Re-use the same core runner used by Timer/CLI flows
#             main(None)  # type: ignore[arg-type]

#             resp = {
#                 "ok": True,
#                 "range_mode": "since",
#                 "since_month": since_str,
#             }
#             return func.HttpResponse(
#                 json.dumps(resp),
#                 status_code=200,
#                 mimetype="application/json",
#             )
#         finally:
#             # Restore previous overrides so other triggers (Timer/CLI) see the original config
#             RUNTIME_OVERRIDES = prev_overrides
#     except Exception as e:
#         logging.exception("[HTTP] since-month run failed: %s", e)
#         return func.HttpResponse(
#             json.dumps({"ok": False, "error": str(e)}),
#             status_code=500,
#             mimetype="application/json",
#         )
