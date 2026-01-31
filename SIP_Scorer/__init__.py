import os
import re
import sys
import json
import logging
import argparse
import datetime
import datetime as dt
from datetime import datetime, timedelta, timezone
from typing import Tuple, Sequence, Optional, TYPE_CHECKING, Any, cast
import time
import traceback
import uuid
from contextlib import contextmanager

import pymongo
import numpy as np
from pymongo import UpdateOne
from pymongo import ReturnDocument
import hashlib
import hashlib
import azure.functions as func
from ..utils.db_utils import get_db_client

# Let Azure Functions' worker manage handlers. Only set basicConfig if no handlers exist (local CLI / direct run).
_root_logger = logging.getLogger()
if not _root_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

logging.info(
    f"[Log] Level set to {logging.getLevelName(logging.INFO)} (PLI_LOG_LEVEL={logging.INFO})"
)
# --- Structured logging helpers (safe-by-default) ---
LOG_FORMAT_MODE = os.getenv("PLI_LOG_FORMAT", "plain").strip().lower()  # 'plain' | 'json'


def _now_utc():
    return datetime.now(timezone.utc)


def _jsonify(v):
    try:
        import json as _json

        return _json.loads(_json.dumps(v, default=str))
    except Exception:
        return str(v)


def _gen_run_id() -> str:
    rid = os.getenv("RUN_ID")
    if rid and rid.strip():
        return rid.strip()
    return uuid.uuid4().hex[:12].upper()


RUN_ID = _gen_run_id()


def _fmt_kv(msg: str, **kw) -> str:
    try:
        if LOG_FORMAT_MODE == "json":
            payload = {"ts": _now_utc().isoformat(), "msg": msg, "run_id": RUN_ID, **kw}
            return json.dumps(payload, default=str)
        parts = [f"{k}={_jsonify(v)}" for k, v in kw.items()]
        return f"{msg} | run_id={RUN_ID}" + (", " + ", ".join(parts) if parts else "")
    except Exception:
        return f"{msg} | run_id={RUN_ID}"


def log_kv(level: int, msg: str, **kw) -> None:
    logging.log(level, _fmt_kv(msg, **kw))


@contextmanager
def timed(section: str, **kw):
    """Time a code section and always log duration (ms), even on exceptions."""
    t0 = time.perf_counter()
    try:
        yield
    except Exception:
        logging.exception("Section failed: %s | run_id=%s", section, RUN_ID)
        raise
    finally:
        ms = (time.perf_counter() - t0) * 1000.0
        log_kv(logging.INFO, "timing", section=section, ms=round(ms, 2), **kw)


try:
    import pandas as pd
except ImportError:  # pandas might not be available in Azure Functions runtime
    pd = None  # type: ignore

# Type-only import for DataFrame to avoid Pylance errors when pd may be None
if TYPE_CHECKING:
    from pandas import DataFrame  # noqa: F401
else:

    class DataFrame:  # runtime placeholder for type hints
        pass


# Pylance-safe alias: treat pd as Any for attribute access (e.g., Series) while still guarding at runtime.
PD = cast(Any, pd)

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
_AUM_CACHE: dict = {}
_AUM_MISS_SEEN: set[str] = set()


# --- AUM lookup helper: fetch AUM from AUM_Report for RM+month, with cache ---
def _lookup_aum_for_rm_month(db_or_client, rm_name: str, month_key: str) -> Optional[float]:
    """Lookup AUM for a given RM+month from the AUM_Report snapshot.

    Expected schema (per sample):
      {"_id": "2025-04-01_ISHU MAVAR", "Amount": 686520558, "MAIN RM": "ISHU MAVAR", "Month": "2025-04-01"}

    We normalise RM to uppercase and match:
      MAIN RM  (case-insensitive) == rm_name
      Month    == f"{month_key}-01" (e.g. '2025-10-01').

    Uses an in-process cache (_AUM_CACHE) to avoid repeated DB hits.
    """
    try:
        if not rm_name or not month_key or "-" not in str(month_key):
            return None
        rm_clean = " ".join(str(rm_name).strip().split())
        if not rm_clean:
            return None
        cache_key = f"{month_key}|{rm_clean.upper()}"
        if cache_key in _AUM_CACHE:
            return _AUM_CACHE[cache_key]
        if cache_key in _AUM_MISS_SEEN:
            return None

        # Resolve client -> database
        if hasattr(db_or_client, "get_database"):
            client = db_or_client
        elif hasattr(db_or_client, "client"):
            client = db_or_client.client  # type: ignore[attr-defined]
        else:
            _AUM_MISS_SEEN.add(cache_key)
            return None

        # Allow overriding DB/collection via env, but default to iwell.AUM_Report
        db_name = os.getenv("AUM_DB_NAME", "iwell")
        coll_name = os.getenv("AUM_COLL_NAME", "AUM_Report")
        try:
            db = client.get_database(db_name)
            coll = db.get_collection(coll_name)
        except Exception:
            # Fallback: try PLI_Leaderboard.AUM_Report if iwell is not present
            try:
                db = client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))
                coll = db.get_collection(coll_name)
            except Exception:
                _AUM_MISS_SEEN.add(cache_key)
                return None

        # Month in AUM_Report is stored as 'YYYY-MM-01'
        month_val = f"{month_key}-01"
        rm_upper = rm_clean.upper()
        try:
            doc = coll.find_one(
                {
                    "Month": month_val,
                    "MAIN RM": {"$regex": f"^{re.escape(rm_upper)}$", "$options": "i"},
                },
                {"Amount": 1},
            )
        except Exception:
            doc = None

        if not doc or "Amount" not in doc or doc["Amount"] is None:
            _AUM_MISS_SEEN.add(cache_key)
            return None

        aum_val = _coerce_float(doc.get("Amount"))
        if aum_val is None:
            _AUM_MISS_SEEN.add(cache_key)
            return None

        _AUM_CACHE[cache_key] = aum_val
        return aum_val
    except Exception as e:
        logging.log(
            AUM_WARN_LEVEL,
            _fmt_kv("[AUM Lookup] Failed", rm_name=rm_name, month=month_key, error=str(e)),
        )
        return None


# Reused across Azure warm invocations
_GLOBAL_MONGO_CLIENT = None  # Reused across Azure warm invocations

KEY_VAULT_URL = os.getenv("KEY_VAULT_URL", "https://milestonetsl1.vault.azure.net/")
RECON_OK = {"RECONCILED", "RECONCILED_WITH_MINOR"}

# --- SIP Scorer schema & config metadata (aligned with Lumpsum architecture) ---
SKIP_RM_ALIASES: set[str] = {
    # All hardcoded exclusions removed. Everyone is scored.
    # Exclusion happens at Leaderboard API level.
}

# --- SIP Scorer schema & config metadata (aligned with Lumpsum architecture) ---
SCHEMA_VERSION_SIP = "2025-11-13.r1"

SIP_CONFIG_COLL_ENV = "PLI_CONFIG_COLL_SIP"
SIP_CONFIG_DEFAULT_COLL = "config"
SIP_CONFIG_ID_ENV = "PLI_CONFIG_ID_SIP"
SIP_CONFIG_DEFAULT_ID = "Leaderboard_SIP"

SIP_SCHEMA_COLL_ENV = "PLI_SCHEMA_COLL_SIP"
SIP_SCHEMA_DEFAULT_COLL = "Schemas"
SIP_SCHEMA_ID_ENV = "PLI_SCHEMA_ID_SIP"
SIP_SCHEMA_DEFAULT_ID = "SIP_Schema"

# Last effective config hash for this run (stamped into leaderboard/audit docs)
_SIP_LAST_CFG_HASH: Optional[str] = None

# --- Tiered payout constants (finalized) ---
TIER_THRESHOLDS = [
    ("T6", 60000),
    ("T5", 40000),
    ("T4", 25000),
    ("T3", 15000),
    ("T2", 8000),
    ("T1", 2000),
    ("T0", -float("inf")),
]
TIER_MONTHLY_FACTORS = {
    "T0": 0.0,
    "T1": 0.000016667,  # annual 0.020% / 12
    "T2": 0.000020833,  # annual 0.025% / 12
    "T3": 0.000025000,  # annual 0.030% / 12
    "T4": 0.000029167,  # annual 0.035% / 12
    "T5": 0.000033333,  # annual 0.040% / 12
    "T6": 0.000037500,  # annual 0.045% / 12
}
SIP_POINTS_COEFF = 0.03  # points per ₹ of effective Net SIP
SIP_BASE_BPS = 0.0  # Base BPS (e.g. 125.0) overriding the coefficient derivation if set
# SIP_HATTRICK_BPS deprecated in favor of BONUS_SLABS_CONSISTENCY


# --- Lumpsum gate thresholds (Mongo-driven, can override at runtime) ---
SIP_LS_GATE_PCT_DEFAULT = float(os.getenv("PLI_LS_GATE_PCT", "-3.0") or -3.0)
SIP_LS_GATE_MIN_RUPEES_DEFAULT = float(os.getenv("PLI_LS_GATE_MIN_RUPEES", "50000") or 50000)
SIP_LS_GATE_PCT = SIP_LS_GATE_PCT_DEFAULT
SIP_LS_GATE_MIN_RUPEES = SIP_LS_GATE_MIN_RUPEES_DEFAULT

# --- Bonus Slabs (configurable) ---
# Default logic mirrors legacy hardcoded values.
# Ratio: strictly greater (>) logic preserved? No, migrated to standard >= or > check.
# Legacy Ratio was > 0.0005. New Config loop usually implies >= ??
# Code uses `if ratio > thr` if we choose so.
BONUS_SLABS_RATIO = [
    (0.0005, 4.0),
    (0.0004, 3.0),
    (0.0003, 2.0),
    (0.0002, 1.0),
]
BONUS_SLABS_ABS = [
    (300000.0, 3.0),
    (200000.0, 2.0),
    (100000.0, 1.0),
    (50000.0, 0.5),
]
BONUS_SLABS_AVG = [
    (8000.0, 2.0),
    (5000.0, 1.0),
    (3000.0, 0.5),
]
BONUS_SLABS_CONSISTENCY = [] # List of dict: {min_months, min_ratio, min_amount, bps}

# --- SIP range / window behaviour (env-driven; config mirrors this) ---
SIP_RANGE_MODE_DEFAULT = os.getenv("PLI_SIP_RANGE_MODE", "month").strip().lower() or "month"
SIP_FY_MODE_DEFAULT = os.getenv("PLI_SIP_FY_MODE", "FY_APR").strip().upper() or "FY_APR"

# --- SIP/SWP behaviour & horizon defaults (config-driven, but with safe fallbacks) ---
SIP_NET_MODE_DEFAULT = "sip_only"  # "sip_only" or "sip_plus_swp"
SIP_INCLUDE_SWP_IN_NET_DEFAULT = False
SWP_WEIGHTS_DEFAULT = {"registration": -1.0, "cancellation": 1.0}
SIP_HORIZON_MONTHS_DEFAULT = 24

# Penalty defaults
PENALTY_ENABLED = True
PENALTY_SLABS: list[dict] = []

# Scheme weightage defaults
DEFAULT_WEIGHTS: dict[str, Any] = {
    "scheme_rules": []
}

# Scheme weight application control (which transaction types get weighted)
SCHEME_WEIGHT_APPLY_TO: dict[str, bool] = {
    "sip_registration": True,   # Default: enabled
    "sip_cancellation": False,  # Default: disabled
    "swp_registration": False,  # Default: disabled
    "swp_cancellation": False   # Default: disabled
}

# Runtime-effective knobs (populated from Mongo config in _load_runtime_config)
SIP_NET_MODE = SIP_NET_MODE_DEFAULT
SIP_INCLUDE_SWP_IN_NET = SIP_INCLUDE_SWP_IN_NET_DEFAULT
SWP_WEIGHTS = dict(SWP_WEIGHTS_DEFAULT)
SIP_HORIZON_MONTHS = SIP_HORIZON_MONTHS_DEFAULT

# --- Trail leaderboard & VP summary configuration ---
TRAIL_LEADERBOARD_COLL_ENV = "PLI_TRAIL_COLL"
TRAIL_LEADERBOARD_DEFAULT_COLL = "MF_Trail_Leaderboard"
TRAIL_VP_SUMMARY_COLL_ENV = "PLI_TRAIL_VP_SUMMARY_COLL"
TRAIL_VP_SUMMARY_DEFAULT_COLL = "MF_Trail_VP_Summary"

# VP leader metadata (used for 20% credit roll-up)
VP_LEADER_NAME = os.getenv("VP_LEADER_NAME", "Sagar Maini")
VP_LEADER_EMP_ID_ENV = "VP_LEADER_EMP_ID"  # env var that can optionally hold VP employee_id

# --- AUM fallback & logging controls ---
AUM_FALLBACK_DAYS = int(os.getenv("PLI_AUM_FALLBACK_DAYS", "5") or "5")
AUM_FALLBACK_FORCE = os.getenv("PLI_AUM_FALLBACK_PREV", "0").lower() in ("1", "true", "yes")
AUM_WARN_LEVEL_NAME = os.getenv("PLI_AUM_WARN_LEVEL", "INFO").upper()
AUM_WARN_LEVEL = getattr(logging, AUM_WARN_LEVEL_NAME, logging.INFO)


def _fy_bounds(today: dt.date) -> tuple[datetime, datetime]:
    # FY starts Apr 1
    fy_year = today.year if today.month >= 4 else today.year - 1
    start = datetime(fy_year, 4, 1)
    end = datetime.combine(today + dt.timedelta(days=1), datetime.max.time())
    return start, end


def get_secret(name: str, default: str | None = None) -> str | None:
    """
    Fetch secret with **Key Vault priority**:
      1) In-process cache
      2) Azure Key Vault (tries underscore and hyphenated name)
      3) Environment variables (including legacy aliases)
      4) Fallback default

    Caches successful lookups by the original `name`.
    """
    # 0) Cache (fast path)
    if name in _SECRET_CACHE:
        logging.debug("Secrets: cache hit for '%s'", name)
        return _SECRET_CACHE[name]

    # 1) Azure Key Vault first (if SDK and vault URL are available)
    if KEY_VAULT_URL and SecretClient and DefaultAzureCredential:
        lookup_names = [name]
        if "_" in name:
            lookup_names.append(name.replace("_", "-"))
        try:
            cred = DefaultAzureCredential()
            client = SecretClient(vault_url=KEY_VAULT_URL, credential=cred)
            for _nm in lookup_names:
                try:
                    secret = client.get_secret(_nm)
                    val = getattr(secret, "value", None)
                    if isinstance(val, str) and val:
                        _SECRET_CACHE[name] = val
                        return val
                except Exception:
                    # Try next candidate name
                    continue
        except Exception as e:
            logging.warning("Secrets: Key Vault lookup failed for '%s': %s", name, e)

    # 2) Environment variables (fallback after KV)
    env_val = os.getenv(name)
    if env_val:
        _SECRET_CACHE[name] = env_val
        return env_val

    # Back-compat aliases if caller asked for the canonical KV key but env has legacy names
    if name == "MongoDb-Connection-String":
        legacy = os.getenv("MONGO_CONN") or os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
        if legacy:
            _SECRET_CACHE[name] = legacy
            return legacy

    # 3) Final fallback
    return default


def _tx_coll(db_or_client):
    """Return the internal.transactions collection whether you pass a client or a db."""
    try:
        # If user passed a MongoClient
        if hasattr(db_or_client, "get_database"):
            db_internal = db_or_client.get_database("internal")
        elif hasattr(db_or_client, "name") and hasattr(db_or_client, "__getitem__"):
            # If a Database
            if getattr(db_or_client, "name", None) == "internal":
                db_internal = db_or_client
            else:
                db_internal = db_or_client.client.get_database("internal")  # type: ignore[attr-defined]
        else:
            # Fallback to attribute access (older patterns)
            return db_or_client.internal.transactions  # type: ignore[attr-defined]
        if hasattr(db_internal, "get_collection"):
            return db_internal.get_collection("transactions")
        return db_internal["transactions"]  # type: ignore[index]
    except Exception:
        # Final fallback
        return db_or_client.internal.transactions  # type: ignore[attr-defined]


def _pick_validation_date(
    doc: dict, start_date: datetime, end_date: datetime, approved_only: bool = True
) -> Optional[datetime]:
    """
    Return the most recent validations[].validatedAt within [start_date, end_date).
    If approved_only=True, require status == 'APPROVED'.
    """
    vals = doc.get("validations") or []
    best: Optional[datetime] = None
    for v in vals:
        if not isinstance(v, dict):
            continue
        dtv = v.get("validatedAt")
        st = str(v.get("status", "")).upper()
        if not (dtv and (start_date <= dtv < end_date)):
            continue
        if approved_only and st != "APPROVED":
            continue
        if (best is None) or (dtv > best):
            best = dtv
    return best


# --- Helper for fraction-level validation window picking ---
def _pick_fraction_validation_date(
    fr: dict, start_date: datetime, end_date: datetime, approved_only: bool = True
) -> Optional[datetime]:
    """
    Return the most recent transactionFractions.validations[].validatedAt within [start_date, end_date).
    Mirrors _pick_validation_date but operates on a single fraction dict.
    """
    vals = fr.get("validations") or []
    best: Optional[datetime] = None
    for v in vals:
        if not isinstance(v, dict):
            continue
        dtv = v.get("validatedAt")
        st = str(v.get("status", "")).upper()
        if not (dtv and (start_date <= dtv < end_date)):
            continue
        if approved_only and st != "APPROVED":
            continue
        if (best is None) or (dtv > best):
            best = dtv
    return best


# --- New helper: fetch employee_id and Active status robustly ---
def _lookup_employee_active_and_id(
    db_or_client, rm_name: str
) -> tuple[Optional[str], Optional[bool]]:
    """
    Return (employee_id, is_active) for an RM from PLI_Leaderboard.Zoho_Users.
    Tries multiple common field names for ID and Active status. Case-insensitive truthy parsing.
    """
    try:
        if hasattr(db_or_client, "get_database"):
            db_ref = db_or_client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))
        elif hasattr(db_or_client, "client"):
            db_ref = db_or_client.client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))  # type: ignore[attr-defined]
        else:
            return (None, None)
        coll = db_ref.get_collection(os.getenv("ZOHO_USERS_COLL", "Zoho_Users"))
    except Exception:
        return (None, None)

    # Name match: exact then case-insensitive
    doc = coll.find_one(
        {
            "$or": [
                {"Full Name": rm_name},
                {"full_name": rm_name},
                {"name": rm_name},
                {"Name": rm_name},
            ]
        },
        projection=None,
    )
    if not doc:
        rx = {"$regex": f"^{re.escape(rm_name)}$", "$options": "i"}
        doc = coll.find_one(
            {"$or": [{"Full Name": rx}, {"full_name": rx}, {"name": rx}, {"Name": rx}]}
        )

    emp_id = None
    if doc:
        for k in ("employee_id", "employeeId", "employeeID", "emp_id", "zoho_employee_id", "id"):
            if k in doc and doc[k]:
                emp_id = str(doc[k])
                break

    # Active field detection
    is_active: Optional[bool] = None
    if doc:
        active_keys = ("Active", "active", "IsActive", "isActive", "status", "Status")
        val = None
        for k in active_keys:
            if k in doc:
                val = doc[k]
                break
        if isinstance(val, bool):
            is_active = val
        elif isinstance(val, (int, float)):
            is_active = bool(val)
        elif isinstance(val, str):
            v = val.strip().lower()
            if v in ("active", "yes", "true", "1", "y"):
                is_active = True
            elif v in ("inactive", "no", "false", "0", "n"):
                is_active = False
            else:
                is_active = None

    return (emp_id, is_active)


# Inactive eligibility cache: key = (normalized_rm_name, month_key) → bool
_INACTIVE_ELIGIBILITY_CACHE: dict[tuple[str, str], bool] = {}


def _rm_eligible_by_inactive(db_or_client, rm_name: str, month_key: str) -> bool:
    """
    Apply the 6-month inactive rule for an RM:
      - If Zoho_Users has no record for this RM → eligible.
      - If status is not 'inactive' or inactive_since is missing → eligible.
      - If status == 'inactive' and inactive_since is present:
          * Compute month_index for period_month (YYYY-MM).
          * Compute inactive_index from inactive_since.year/month.
          * Eligible ONLY when 0 <= (month_index - inactive_index) < 6.
      - Months before inactive_since are treated as not-eligible when re-running
        old periods for an already-inactive RM (consistent with Lumpsum scorer).

    The lookup is cached per (rm_name, month_key) to avoid repeated DB hits.
    """
    try:
        if db_or_client is None:
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

        # Resolve PLI_Leaderboard.Zoho_Users
        try:
            if hasattr(db_or_client, "get_database"):
                db_lb = db_or_client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))
            elif hasattr(db_or_client, "client"):
                db_lb = db_or_client.client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))  # type: ignore[attr-defined]
            else:
                _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
                return True
            coll_name = os.getenv("ZOHO_USERS_COLL", "Zoho_Users")
            zu_col = db_lb.get_collection(coll_name)
        except Exception:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
            return True

        # Case-insensitive match against Full Name / Name
        try:
            import re as _re

            pat = f"^{_re.escape(rm_clean)}$"
            doc = zu_col.find_one(
                {
                    "$or": [
                        {"Full Name": {"$regex": pat, "$options": "i"}},
                        {"Name": {"$regex": pat, "$options": "i"}},
                        {"full_name": {"$regex": pat, "$options": "i"}},
                        {"name": {"$regex": pat, "$options": "i"}},
                    ]
                },
                {"status": 1, "Status": 1, "inactive_since": 1},
            )
        except Exception:
            doc = None

        # No Zoho mapping → treat as eligible
        if not doc:
            _INACTIVE_ELIGIBILITY_CACHE[cache_key] = True
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
        # Eligible for months in [inactive_month, inactive_month+5]
        eligible = (diff >= 0) and (diff < 6)

        _INACTIVE_ELIGIBILITY_CACHE[cache_key] = bool(eligible)
        if not eligible:
            try:
                iso = (
                    inactive_since.isoformat()
                    if hasattr(inactive_since, "isoformat")
                    else str(inactive_since)
                )
            except Exception:
                iso = str(inactive_since)
            logging.debug(
                "[InactiveGate-SIP] Skipping RM='%s' month='%s' (status=inactive, inactive_since=%s, diff=%s)",
                rm_clean,
                month_key,
                iso,
                diff,
            )
        return bool(eligible)
    except Exception as e:
        logging.warning(
            "[InactiveGate-SIP] Fallback to eligible for RM='%s' month='%s' due to error: %s",
            rm_name,
            month_key,
            e,
        )
        return True


def _tier_from_points(total_points: float) -> str:
    """Map total points to tier per finalized thresholds."""
    try:
        tp = float(total_points or 0.0)
    except Exception:
        tp = 0.0
    for name, thr in TIER_THRESHOLDS:
        if tp >= thr:
            return name
    return "T0"


def _resolve_weight_for_scheme(scheme_name: str, txn_date: datetime) -> float:
    """
    Search DEFAULT_WEIGHTS['scheme_rules'] for a match with scheme_name and txn_date.
    Returns weight multiplier (e.g. 1.0 for 100%, 0.5 for 50%, 1.5 for 150%).
    """
    if not scheme_name or not DEFAULT_WEIGHTS.get("scheme_rules"):
        return 1.0

    scheme_name_upper = str(scheme_name).strip().upper()

    for rule in DEFAULT_WEIGHTS["scheme_rules"]:
        keyword = str(rule.get("keyword", "")).strip().upper()
        if not keyword:
            continue

        match_type = str(rule.get("match_type", "contains")).lower()
        matched = False

        if match_type == "exact":
            matched = (scheme_name_upper == keyword)
        elif match_type == "startswith":
            matched = scheme_name_upper.startswith(keyword)
        else:  # default: contains
            matched = (keyword in scheme_name_upper)

        if matched:
            # Check date bounds if any
            start = rule.get("start_date")
            end = rule.get("end_date")

            # Parse dates properly for comparison
            # Normalize txn_date to date-only for comparison
            txn_date_only = txn_date.date() if hasattr(txn_date, 'date') else txn_date

            def parse_date_flexible(date_val):
                """
                Parse multiple date formats:
                - 'YYYY-MM-DD' (e.g., '2025-12-01')
                - 'Month YYYY' (e.g., 'December 2025')
                - 'Month YY' (e.g., 'December 25')
                - datetime/date objects
                Returns the first day of the month as a date object.
                """
                if not date_val:
                    return None

                if isinstance(date_val, str):
                    date_str = date_val.strip()

                    # Try YYYY-MM-DD format first
                    try:
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        pass

                    # Try 'Month YYYY' format (e.g., 'December 2025')
                    try:
                        return datetime.strptime(date_str, "%B %Y").date()
                    except ValueError:
                        pass

                    # Try 'Month YY' format (e.g., 'December 25')
                    try:
                        parsed = datetime.strptime(date_str, "%B %y").date()
                        # Ensure it's in 2000s (not 1900s)
                        if parsed.year < 2000:
                            parsed = parsed.replace(year=parsed.year + 100)
                        return parsed
                    except ValueError:
                        pass

                    # Try 'Mon YYYY' format (e.g., 'Dec 2025')
                    try:
                        return datetime.strptime(date_str, "%b %Y").date()
                    except ValueError:
                        pass

                    # Try 'Mon YY' format (e.g., 'Dec 25')
                    try:
                        parsed = datetime.strptime(date_str, "%b %y").date()
                        if parsed.year < 2000:
                            parsed = parsed.replace(year=parsed.year + 100)
                        return parsed
                    except ValueError:
                        pass

                    return None

                elif hasattr(date_val, 'date'):
                    return date_val.date()
                elif hasattr(date_val, 'year'):  # Already a date object
                    return date_val

                return None

            if start:
                start_parsed = parse_date_flexible(start)
                if start_parsed and txn_date_only < start_parsed:
                    continue

            if end:
                end_parsed = parse_date_flexible(end)
                if end_parsed:
                    # For end dates, we want to include the entire month
                    # So if end is "December 2025", it should include all of December
                    # We'll use the last day of the month
                    from calendar import monthrange
                    last_day = monthrange(end_parsed.year, end_parsed.month)[1]
                    end_date_inclusive = end_parsed.replace(day=last_day)
                    if txn_date_only > end_date_inclusive:
                        continue

            # Rule matches
            return float(rule.get("weight_pct", 100.0)) / 100.0

    return 1.0


# --- SIP schema + runtime config bootstrap (Mongo-driven, no logic change) ---


def _default_schema_doc(schema_id: str) -> dict:
    """
    Schema registry document for SIP leaderboard. Lives in PLI_Leaderboard.Schemas.
    Carries canonical layout + default templates so tools can introspect.
    """
    now_iso = datetime.utcnow().isoformat()
    return {
        "_id": schema_id,
        "module": "SIP_Scorer",
        "schema": "Leaderboard_SIP",
        "schema_version": SCHEMA_VERSION_SIP,
        "status": "active",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "description": "Schema registry for SIP leaderboard; field layout and default templates.",
        "defaults": {
            "tier_thresholds": TIER_THRESHOLDS,
            "tier_monthly_factors": TIER_MONTHLY_FACTORS,
            "sip_points_coeff": SIP_POINTS_COEFF,
            # "lumpsum_points_coeff": LUMPSUM_POINTS_COEFF, # Removed
            # SIP/SWP behaviour + horizon defaults (documented for introspection)
            "sip_net_mode_default": SIP_NET_MODE_DEFAULT,
            "sip_include_swp_in_net_default": SIP_INCLUDE_SWP_IN_NET_DEFAULT,
            "swp_weights_default": SWP_WEIGHTS_DEFAULT,
            "sip_horizon_months_default": SIP_HORIZON_MONTHS_DEFAULT,
        },
        "keys": {
            "leaderboard_collection": "MF_SIP_Leaderboard",
            "metrics": ["SIP", "Points"],
            "identity_fields": ["employee_id", "rm_name", "employee_name", "month"],
        },
        "meta": {"notes": "Auto-created by SIP runtime. Safe to edit values; keep top-level keys."},
    }


def _ensure_schema_bootstrap(db_leaderboard):
    """
    Ensure a schema-registry doc exists for SIP under PLI_Leaderboard.Schemas.
    Idempotent; updates schema+version and bumps updatedAt.
    """
    try:
        coll_name = os.getenv(SIP_SCHEMA_COLL_ENV, SIP_SCHEMA_DEFAULT_COLL).strip()
        doc_id = os.getenv(SIP_SCHEMA_ID_ENV, SIP_SCHEMA_DEFAULT_ID).strip()
        col = db_leaderboard[coll_name]
        try:
            col.create_index([("schema", 1)])
            col.create_index([("status", 1)])
        except Exception:
            pass

        default_doc = _default_schema_doc(doc_id)
        on_insert = dict(default_doc)
        for k in ("updatedAt", "schema_version", "schema"):
            on_insert.pop(k, None)

        res = col.find_one_and_update(
            {"_id": doc_id},
            {
                "$setOnInsert": on_insert,
                "$set": {
                    "schema": "Leaderboard_SIP",
                    "schema_version": SCHEMA_VERSION_SIP,
                },
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            logging.info(
                "[SIP Schema] Bootstrapped/ensured schema registry: %s/%s",
                coll_name,
                doc_id,
            )
        return res
    except Exception as e:
        logging.warning("[SIP Schema] Bootstrap failed: %s", e)
        return None


def _default_config_doc(config_id: str) -> dict:
    """
    Default runtime-config document for SIP leaderboard.
    Mirrors current env / constant behavior; does NOT change logic.
    """
    now_iso = datetime.utcnow().isoformat()
    # Create UI-friendly tier thresholds list
    tier_thresholds_ui = []
    for t_name, t_val in TIER_THRESHOLDS:
        label = ""
        # Simple heuristic for default labels to match Settings_API
        if t_val == 60000: label = "≥60k"
        elif t_val == 40000: label = "40k–60k"
        elif t_val == 25000: label = "25k–40k"
        elif t_val == 15000: label = "15k–25k"
        elif t_val == 8000: label = "8k–15k"
        elif t_val == 2000: label = "2k–8k"
        elif t_val == -float("inf"): label = "<2k"

        tier_thresholds_ui.append({
            "tier": t_name,
            "min_val": t_val if t_val != -float("inf") else -float("inf"),
            "label": label
        })

    return {
        "_id": config_id,
        "schema": "Leaderboard_SIP",
        "schema_version": SCHEMA_VERSION_SIP,
        "status": "active",
        "createdAt": now_iso,
        "updatedAt": now_iso,
        "options": {
            "range_mode": SIP_RANGE_MODE_DEFAULT,
            "fy_mode": SIP_FY_MODE_DEFAULT,
            "audit_mode": "compact",
            "ls_gate_pct": SIP_LS_GATE_PCT_DEFAULT,
            "ls_gate_min_rupees": SIP_LS_GATE_MIN_RUPEES_DEFAULT,
            "sip_net_mode": SIP_NET_MODE_DEFAULT,
            "sip_include_swp_in_net": SIP_INCLUDE_SWP_IN_NET_DEFAULT,
            "swp_weights": SWP_WEIGHTS_DEFAULT,
            "sip_horizon_months": SIP_HORIZON_MONTHS_DEFAULT,
        },
        "tier_thresholds": tier_thresholds_ui,
        "tier_factors": TIER_MONTHLY_FACTORS,
        "coefficients": {
            "sip_points_per_rupee": SIP_POINTS_COEFF,
            # "lumpsum_points_per_rupee": LUMPSUM_POINTS_COEFF, # Removed
        },
        "meta": {
            "module": "SIP_Scorer",
            "notes": "Auto-created by SIP runtime. Defaults mirror legacy behavior.",
        },
    }


def _ensure_config_bootstrap(db_leaderboard):
    """
    Ensure a runtime-config doc exists for SIP leaderboard.
    Idempotent; if missing, inserts defaults that mirror current behavior.
    """
    try:
        coll_name = os.getenv(SIP_CONFIG_COLL_ENV, SIP_CONFIG_DEFAULT_COLL).strip()
        doc_id = os.getenv(SIP_CONFIG_ID_ENV, SIP_CONFIG_DEFAULT_ID).strip()
        col = db_leaderboard[coll_name]
        try:
            col.create_index([("schema", 1)])
            col.create_index([("status", 1)])
        except Exception:
            pass

        default_doc = _default_config_doc(doc_id)
        on_insert = dict(default_doc)
        for k in ("updatedAt", "schema_version", "schema"):
            on_insert.pop(k, None)

        res = col.find_one_and_update(
            {"_id": doc_id},
            {
                "$setOnInsert": on_insert,
                "$set": {
                    "schema": "Leaderboard_SIP",
                    "schema_version": SCHEMA_VERSION_SIP,
                },
                "$currentDate": {"updatedAt": True},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            logging.info(
                "[SIP Config] Bootstrapped runtime config: %s/%s",
                coll_name,
                doc_id,
            )
        return res
    except Exception as e:
        logging.warning("[SIP Config] Bootstrap failed: %s", e)
        return None


def _effective_config_snapshot(cfg: dict | None) -> dict:
    """Build a compact, normalised snapshot of the config in use.

    This is the single source of truth for:
      * what goes into the config hash, and
      * which runtime options (range_mode, FY, LS gate, SIP/SWP behaviour)
        are actually honoured by the scorer.
    """
    cfg = cfg or {}
    opts_raw = cfg.get("options", {}) if isinstance(cfg, dict) else {}

    # --- Core window / audit options ---
    range_mode = (opts_raw.get("range_mode") or SIP_RANGE_MODE_DEFAULT or "month").strip().lower()
    fy_mode = (opts_raw.get("fy_mode") or SIP_FY_MODE_DEFAULT or "FY_APR").strip().upper()
    audit_mode = opts_raw.get("audit_mode", "compact")

    # Lumpsum gate thresholds (mirror env defaults if missing)
    try:
        ls_gate_pct = float(opts_raw.get("ls_gate_pct", SIP_LS_GATE_PCT_DEFAULT))
    except Exception:
        ls_gate_pct = float(SIP_LS_GATE_PCT_DEFAULT)
    try:
        ls_gate_min_rupees = float(
            opts_raw.get("ls_gate_min_rupees", SIP_LS_GATE_MIN_RUPEES_DEFAULT)
        )
    except Exception:
        ls_gate_min_rupees = float(SIP_LS_GATE_MIN_RUPEES_DEFAULT)

    # --- SIP / SWP behaviour knobs (Shim for UI/Legacy mismatch) ---
    # UI sends 'net_mode', Legacy expects 'sip_net_mode' (prefer UI when both exist)
    net_mode_val = opts_raw.get("net_mode")
    if net_mode_val is None:
        net_mode_val = opts_raw.get("sip_net_mode")
    sip_net_mode = (net_mode_val or "sip_only").strip().lower()
    if sip_net_mode not in {"sip_only", "sip_plus_swp"}:
        sip_net_mode = "sip_only"

    # Boolean mirror for BI / QA
    # UI sends 'include_swp', Legacy expects 'sip_include_swp_in_net' (prefer UI when both exist)
    include_swp_val = opts_raw.get("include_swp")
    if include_swp_val is None:
        include_swp_val = opts_raw.get("sip_include_swp_in_net")

    # Default fallback logic
    sip_include_swp_in_net = bool(
        include_swp_val if include_swp_val is not None else (sip_net_mode == "sip_plus_swp")
    )

    # SWP weights: Registration reduces net SIP; Cancellation increases it.
    swp_weights_raw = opts_raw.get("swp_weights") or {}
    try:
        swp_reg = float(swp_weights_raw.get("registration", -1.0))
    except Exception:
        swp_reg = -1.0
    try:
        swp_cancel = float(swp_weights_raw.get("cancellation", 1.0))
    except Exception:
        swp_cancel = 1.0
    swp_weights = {
        "registration": swp_reg,
        "cancellation": swp_cancel,
    }

    # Horizon in months (Shim: 'sip_horizon_months' vs 'horizon_months')
    try:
        hor_val = opts_raw.get("sip_horizon_months") or opts_raw.get("horizon_months")
        sip_horizon_months = int(hor_val or 24)
    except Exception:
        sip_horizon_months = 24

    options = {
        "range_mode": range_mode,
        "fy_mode": fy_mode,
        "audit_mode": audit_mode,
        "ls_gate_pct": ls_gate_pct,
        "ls_gate_min_rupees": ls_gate_min_rupees,
        "sip_net_mode": sip_net_mode,
        "sip_include_swp_in_net": sip_include_swp_in_net,
        "swp_weights": swp_weights,
        "sip_horizon_months": sip_horizon_months,
    }

    return {
        "schema_version": SCHEMA_VERSION_SIP,
        "options": options,
        "tier_thresholds": cfg.get("tier_thresholds", TIER_THRESHOLDS),
        "tier_monthly_factors": cfg.get("tier_monthly_factors", TIER_MONTHLY_FACTORS),
        "sip_points_coeff": cfg.get("sip_points_coeff", SIP_POINTS_COEFF),
        # FIXED: Return full config sections for Admin visibility
        "bonus_slabs": cfg.get("bonus_slabs", {}),
        "sip_penalty": cfg.get("sip_penalty", {}),
        "weights": cfg.get("weights", {}),
    }


# --- MongoDB client helper (shared, mirrors Lumpsum scorer) ---
def _get_mongo_client(mongo_uri: Optional[str] = None):
    """
    Return a shared MongoClient, resolving the URI via Key Vault / env if needed.
    Mirrors the Lumpsum scorer pattern of a single global client reused across invocations.
    """
    global _GLOBAL_MONGO_CLIENT

    if "_GLOBAL_MONGO_CLIENT" not in globals() or _GLOBAL_MONGO_CLIENT is None:
        # Use centralized DB util
        _GLOBAL_MONGO_CLIENT = get_db_client(serverSelectionTimeoutMS=5000)
        _GLOBAL_MONGO_CLIENT.server_info()  # validate early
        logging.info("Connected to MongoDB (new client).")
    else:
        logging.info("Reusing existing MongoDB client.")
    return _GLOBAL_MONGO_CLIENT


# --- SIP runtime config loader (Mongo-driven options) ---
def _load_runtime_config(client) -> tuple[dict, dict, str]:
    """
    Load or bootstrap the SIP runtime config from MongoDB, update effective
    runtime options (range_mode, fy_mode, gate thresholds), and compute a
    stable config hash for stamping/audit. This mirrors the Lumpsum scorer
    policy of Mongo-driven options.
    """
    db_lb = client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))
    _ensure_schema_bootstrap(db_lb)
    # Discard return value of bootstrap (unsafe read)
    _ensure_config_bootstrap(db_lb)

    # Explicit safe read
    coll_name = os.getenv(SIP_CONFIG_COLL_ENV, SIP_CONFIG_DEFAULT_COLL).strip()
    doc_id = os.getenv(SIP_CONFIG_ID_ENV, SIP_CONFIG_DEFAULT_ID).strip()
    doc = db_lb[coll_name].find_one({"_id": doc_id}) or {}

    # CRITICAL FIX: The Settings API saves the actual config inside a "config" key
    # Handle both flat and nested structures robustly
    cfg = doc.get("config") if (doc.get("config") and isinstance(doc.get("config"), dict)) else doc

    print(f"DEBUG: Loaded cfg from {coll_name}/{doc_id}: {json.dumps(cfg, default=str)}")
    snapshot = _effective_config_snapshot(cfg)
    print(f"DEBUG: effective_snapshot: {json.dumps(snapshot, default=str)}")
    cfg_hash = hashlib.md5(
        json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()

    global _SIP_LAST_CFG_HASH
    _SIP_LAST_CFG_HASH = cfg_hash

    unique_threshold = snapshot.get("unique_sip_threshold", 500.0)

    # [NEW] Load Ignored RMs from Config
    ignored_list = snapshot.get("ignored_rms")
    if ignored_list and isinstance(ignored_list, list):
        # Update global SKIP_RM_ALIASES with config values
        for rm in ignored_list:
            if rm:
                SKIP_RM_ALIASES.add(str(rm).lower().strip())
        logger.info(f"Updated SKIP_RM_ALIASES with {len(ignored_list)} RMs from config, total: {len(SKIP_RM_ALIASES)}")

    opts = snapshot.get("options", {}) or {}

    # Helper for robust float conversion
    def _safe_float(v):
        if isinstance(v, dict) and "$numberDouble" in v:
            val = v["$numberDouble"]
            if val == "-Infinity": return -float('inf')
            if val == "Infinity": return float('inf')
            try: return float(val)
            except: return 0.0
        try:
            return float(v)
        except:
            return 0.0

    global TIER_THRESHOLDS, TIER_MONTHLY_FACTORS, SIP_POINTS_COEFF, BONUS_SLABS_RATIO, BONUS_SLABS_ABS, BONUS_SLABS_AVG, BONUS_SLABS_CONSISTENCY, PENALTY_SLABS, PENALTY_ENABLED, DEFAULT_WEIGHTS, SIP_NET_MODE, SIP_INCLUDE_SWP_IN_NET, SWP_WEIGHTS, SIP_RANGE_MODE_DEFAULT, SIP_FY_MODE_DEFAULT, SIP_HORIZON_MONTHS, SIP_LS_GATE_PCT, SIP_LS_GATE_MIN_RUPEES

    # --- 0. Update Scheme Weights (Whitelist/Blacklist) ---
    weights_bg = snapshot.get("weights", {}) or {}
    scheme_rules_raw = weights_bg.get("scheme_rules")
    if isinstance(scheme_rules_raw, list):
        # Validate and clean rules
        cleaned_rules = []
        for r in scheme_rules_raw:
            if isinstance(r, dict) and r.get("keyword"):
                cleaned_rules.append({
                    "keyword": str(r["keyword"]),
                    "match_type": str(r.get("match_type", "contains")),
                    "weight_pct": float(r.get("weight_pct", 100.0)),
                    "start_date": str(r.get("start_date")) if r.get("start_date") else None,
                    "end_date": str(r.get("end_date")) if r.get("end_date") else None,
                })

        # Merge into DEFAULT_WEIGHTS
        DEFAULT_WEIGHTS["scheme_rules"] = cleaned_rules
        logging.info(f"[SIP Config] Updated DEFAULT_WEIGHTS['scheme_rules']: {len(cleaned_rules)} rules loaded.")

    # Load scheme weight application toggles
    global SCHEME_WEIGHT_APPLY_TO
    weights_config = snapshot.get("weights", {})
    apply_to_config = weights_config.get("apply_to", {})
    if apply_to_config:
        SCHEME_WEIGHT_APPLY_TO = {
            "sip_registration": bool(apply_to_config.get("sip_registration", True)),
            "sip_cancellation": bool(apply_to_config.get("sip_cancellation", False)),
            "swp_registration": bool(apply_to_config.get("swp_registration", False)),
            "swp_cancellation": bool(apply_to_config.get("swp_cancellation", False)),
        }
        logging.info(f"[SIP Config] Scheme weight apply_to: {SCHEME_WEIGHT_APPLY_TO}")

    # 1. Update Tier Thresholds (ListConfig -> Tuples)
    # Expected format: [{"tier": "T6", "min_val": 60000}, ...]
    thresh_raw = snapshot.get("tier_thresholds")
    if isinstance(thresh_raw, list):
        parsed = []
        for item in thresh_raw:
            if isinstance(item, dict):
                t_name = item.get("tier")
                t_val = item.get("min_val")
                if t_name and t_val is not None:
                     parsed.append((str(t_name), _safe_float(t_val)))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                # Backward compat for list of lists
                parsed.append((str(item[0]), _safe_float(item[1])))

        if parsed:
            # Sort descending by value is CRITICAL for _tier_from_points logic
            parsed.sort(key=lambda x: x[1], reverse=True)
            TIER_THRESHOLDS = parsed
            logging.info(f"[SIP Config] Updated TIER_THRESHOLDS: {TIER_THRESHOLDS}")

    # 2. Update Tier Factors
    factors_raw = snapshot.get("tier_factors") or snapshot.get("tier_monthly_factors")
    if isinstance(factors_raw, dict):
        new_factors = {}
        for k, v in factors_raw.items():
            try:
                new_factors[str(k)] = _safe_float(v)
            except:
                pass
        if new_factors:
            TIER_MONTHLY_FACTORS = new_factors

    # Parse and sort bonus slabs
    bonus_slabs = snapshot.get("bonus_slabs") or {}

    # Ratio slabs
    ratio_list = bonus_slabs.get("sip_to_aum")
    if ratio_list and len(ratio_list) > 0:
        BONUS_SLABS_RATIO = [(_safe_float(s["val"]), _safe_float(s["bps"])) for s in ratio_list if isinstance(s, dict)]
        BONUS_SLABS_RATIO.sort(key=lambda x: x[0], reverse=True)
    else:
        BONUS_SLABS_RATIO = []

    # Absolute amount slabs
    abs_list = bonus_slabs.get("absolute_sip")
    if abs_list and len(abs_list) > 0:
        BONUS_SLABS_ABS = [(_safe_float(s["val"]), _safe_float(s["bps"])) for s in abs_list if isinstance(s, dict)]
        BONUS_SLABS_ABS.sort(key=lambda x: x[0], reverse=True)
    else:
        BONUS_SLABS_ABS = []

    # Average ticket slabs
    avg_list = bonus_slabs.get("avg_ticket")
    if avg_list and len(avg_list) > 0:
        BONUS_SLABS_AVG = [(_safe_float(s["val"]), _safe_float(s["bps"])) for s in avg_list if isinstance(s, dict)]
        BONUS_SLABS_AVG.sort(key=lambda x: x[0], reverse=True)
    else:
        BONUS_SLABS_AVG = []

    # Consistency slabs
    cons_list = bonus_slabs.get("consistency")
    if cons_list and len(cons_list) > 0:
        # Expected: {min_months, min_ratio, min_amount, bps}
        # Validate and store
        cleaned = []
        for s in cons_list:
             if isinstance(s, dict):
                 cleaned.append({
                     "min_months": int(s.get("min_months", 0)),
                     "min_ratio": _safe_float(s.get("min_ratio")),
                     "min_amount": _safe_float(s.get("min_amount")),
                     "bps": _safe_float(s.get("bps"))
                 })
        # Sort by min_months descending, then bps desc
        cleaned.sort(key=lambda x: (x["min_months"], x["bps"]), reverse=True)
        BONUS_SLABS_CONSISTENCY = cleaned
    else:
        BONUS_SLABS_CONSISTENCY = []

    # Parse and sort penalty slabs
    penalty_cfg = snapshot.get("sip_penalty") or {}
    PENALTY_ENABLED = bool(penalty_cfg.get("enable", True))
    penalty_list = penalty_cfg.get("slabs") or []
    cleaned_penalties = []
    for s in penalty_list:
        if isinstance(s, dict):
            cleaned_penalties.append({
                "threshold_amount": _safe_float(s.get("threshold_amount") or s.get("max_loss")), # max_loss for backwards compat
                "threshold_ratio": _safe_float(s.get("threshold_ratio") or s.get("max_ratio")),
                "rate_bps": _safe_float(s.get("rate_bps"))
            })
    # Sort by BPS descending to ensure we check the most severe penalties first
    cleaned_penalties.sort(key=lambda x: x["rate_bps"], reverse=True)
    PENALTY_SLABS = cleaned_penalties

    # 3. Update Coefficients
    coeffs_raw = cfg.get("coefficients") or {}
    # Legacy fallbacks keys
    s_coeff = coeffs_raw.get("sip_points_per_rupee") or snapshot.get("sip_points_coeff")

    # New: explicit base BPS override (User Preference)
    s_base_bps = coeffs_raw.get("sip_base_bps")

    global SIP_POINTS_COEFF, SIP_BASE_BPS
    if s_base_bps is not None:
        try:
            SIP_BASE_BPS = float(s_base_bps)
            # Reverse-calculate coeff for consistency/logging if needed, though Base BPS is now authority
            if SIP_HORIZON_MONTHS > 0:
                SIP_POINTS_COEFF = (SIP_BASE_BPS * SIP_HORIZON_MONTHS) / 10000.0
            else:
                SIP_POINTS_COEFF = 0.0
        except Exception:
             SIP_BASE_BPS = 0.0
    elif s_coeff is not None:
        # Legacy path: Derive Base BPS from Coefficient
        SIP_POINTS_COEFF = float(s_coeff)
        if SIP_HORIZON_MONTHS > 0:
            SIP_BASE_BPS = (SIP_POINTS_COEFF * 10000.0) / float(SIP_HORIZON_MONTHS)
        else:
            SIP_BASE_BPS = 0.0
    else:
        # Fallback default
        if SIP_HORIZON_MONTHS > 0 and SIP_POINTS_COEFF > 0:
             SIP_BASE_BPS = (SIP_POINTS_COEFF * 10000.0) / float(SIP_HORIZON_MONTHS)
        else:
             SIP_BASE_BPS = 0.0

    global SIP_HATTRICK_BPS
    # Legacy hattrick support removed, relying on slabs
    # try:
    #     SIP_HATTRICK_BPS = float(coeffs_raw.get("sip_hattrick_bps", 1.0))
    # except:
    #     SIP_HATTRICK_BPS = 1.0

    l_coeff = None # Removed Lumpsum Points Coefficient support.


    SIP_RANGE_MODE_DEFAULT = (
        (opts.get("range_mode") or SIP_RANGE_MODE_DEFAULT or "month").strip().lower()
    )
    SIP_FY_MODE_DEFAULT = (opts.get("fy_mode") or SIP_FY_MODE_DEFAULT or "FY_APR").strip().upper()
    SIP_LS_GATE_PCT = float(opts.get("ls_gate_pct", SIP_LS_GATE_PCT_DEFAULT))
    SIP_LS_GATE_MIN_RUPEES = float(opts.get("ls_gate_min_rupees", SIP_LS_GATE_MIN_RUPEES_DEFAULT))

    # SIP/SWP netting behaviour
    sip_net_mode = (opts.get("sip_net_mode") or SIP_NET_MODE_DEFAULT).strip().lower()
    if sip_net_mode not in {"sip_only", "sip_plus_swp"}:
        sip_net_mode = "sip_only"
    SIP_NET_MODE = sip_net_mode

    SIP_INCLUDE_SWP_IN_NET = bool(
        opts.get("sip_include_swp_in_net", sip_net_mode == "sip_plus_swp")
    )

    swp_raw = opts.get("swp_weights") or {}
    try:
        swp_reg = float(swp_raw.get("registration", SWP_WEIGHTS_DEFAULT["registration"]))
    except Exception:
        swp_reg = SWP_WEIGHTS_DEFAULT["registration"]
    try:
        swp_cancel = float(swp_raw.get("cancellation", SWP_WEIGHTS_DEFAULT["cancellation"]))
    except Exception:
        swp_cancel = SWP_WEIGHTS_DEFAULT["cancellation"]
    SWP_WEIGHTS = {"registration": swp_reg, "cancellation": swp_cancel}

    try:
        SIP_HORIZON_MONTHS = int(opts.get("sip_horizon_months", SIP_HORIZON_MONTHS_DEFAULT))
    except Exception:
        SIP_HORIZON_MONTHS = SIP_HORIZON_MONTHS_DEFAULT

    # Only look up legacy Lumpsum coeff if NOT explicitly configured in SIP config


    print(f"DEBUG: Loaded SIP_POINTS_COEFF = {SIP_POINTS_COEFF}")

    log_kv(
        logging.INFO,
        "[SIP Config] Effective configuration",
        cfg_hash=cfg_hash,
        schema_version=SCHEMA_VERSION_SIP,
        range_mode=SIP_RANGE_MODE_DEFAULT,
        fy_mode=SIP_FY_MODE_DEFAULT,
        SIP_NET_MODE=SIP_NET_MODE,
        SIP_INCLUDE_SWP_IN_NET=SIP_INCLUDE_SWP_IN_NET,
        SWP_WEIGHTS=SWP_WEIGHTS,
        sip_points_coeff=SIP_POINTS_COEFF,
        sip_base_bps=SIP_BASE_BPS,
        sip_horizon_months=SIP_HORIZON_MONTHS
    )

    return cfg, snapshot, cfg_hash


# --- Combined normalized transaction DataFrame ---
def BuildTxnDF(
    start_date: datetime,
    end_date: datetime,
    db_or_client,
    emails: Optional[Sequence[str]] = None,
    include_types: Optional[Sequence[str]] = ("SIP", "SWP"),
    require_reconciled: bool = True,
) -> "DataFrame":
    """
    Return a single normalized DataFrame with one row per effective transaction:
      - For docs WITH fractions: one row per fraction whose latest APPROVED
        validation validatedAt lies within [start_date, end_date). The row date is
        that exec date.
      - For docs WITHOUT fractions: one row per doc whose latest APPROVED
        validations[].validatedAt lies within the window. The row date is that
        exec date.
      - If require_reconciled=True, only include rows where the applicable
        reconciliation status is in RECON_OK.

    Columns: date, amount, txn_type, txn_for, reconcile_status, reconcile_at,
             rm_name, rm_raw, source, parent_id, line_id, registrant_email
    """
    # Normalize to offset-naive datetimes because Mongo typically stores naive datetimes.
    # Mixing tz-aware (from _default_month_window) with naive (from Mongo) causes
    # "can't compare offset-naive and offset-aware datetimes".
    if start_date.tzinfo is not None:
        start_date = start_date.replace(tzinfo=None)
    if end_date.tzinfo is not None:
        end_date = end_date.replace(tzinfo=None)

    if pd is None:
        raise ImportError("pandas is required for BuildTxnDF; please install pandas.")

    coll = _tx_coll(db_or_client)

    # Base match
    types = list(include_types) if include_types else ["SIP", "SWP"]
    base = {
        "category": "systematic",
        "transactionType": {"$in": types},
        "transactionFor": {"$in": ["Registration", "Cancellation"]},
    }
    if emails:
        base["registrantEmail"] = {"$in": list(emails)}

    projection = {
        "_id": 1,
        "relationshipManager": 1,
        "serviceManager": 1,
        "transactionType": 1,
        "transactionFor": 1,
        "category": 1,
        "amount": 1,
        "transactionPreference": 1,
        "hasFractions": 1,
        "transactionFractions": 1,
        "reconciliation": 1,
        "validations": 1,
        "sipSwpStpDate": 1,
        "updatedAt": 1,
        "createdAt": 1,
        "registrantEmail": 1,
        "schemeName": 1,
    }

    # --- WITH FRACTIONS ---
    # --- WITH FRACTIONS ---
    # We no longer filter by transactionFractions.transactionDate in Mongo, because
    # the effective exec date for scoring is the latest APPROVED validation on each
    # fraction. The start/end window is applied using that exec date in Python.
    q_with_fr = {
        **base,
        "$or": [
            {"hasFractions": True},
            {"transactionFractions": {"$exists": True, "$ne": []}},
        ],
    }
    with_fr_docs = list(coll.find(q_with_fr, projection=projection))

    rows: list[dict[str, Any]] = []

    def _rm_raw(doc: dict) -> str:
        # Golden rule: rm_name comes from relationshipManager only.
        return (doc.get("relationshipManager") or "").strip()

    def _rm_name(doc: dict) -> str:
        return _rm_raw(doc)

    for d in with_fr_docs:
        rm_raw = _rm_raw(d)
        rm_name = _rm_name(d)
        for fr in d.get("transactionFractions") or []:
            # Exec date for this fraction = latest APPROVED validation within window
            exec_dt = _pick_fraction_validation_date(fr, start_date, end_date, approved_only=True)
            if not exec_dt:
                continue
            # Accept reconciliation from fraction-level nested or top-level, else fall back to doc-level
            recon = (
                (fr.get("reconciliation") or {}).get("reconcileStatus")
                or fr.get("reconcileStatus")
                or (d.get("reconciliation") or {}).get("reconcileStatus")
                or d.get("reconcileStatus")
            )
            if require_reconciled and str(recon or "").upper() not in RECON_OK:
                continue
            reconcile_at_val = (
                (fr.get("reconciliation") or {}).get("reconciledAt")
                or fr.get("reconciledAt")
                or (d.get("reconciliation") or {}).get("reconciledAt")
                or d.get("reconciledAt")
            )
            pure_amt = fr.get("fractionAmount", 0) or 0
            scheme = d.get("schemeName") or ""
            txn_type = d.get("transactionType")
            txn_for = d.get("transactionFor")

            # Determine if scheme weight should apply based on toggles
            apply_weight = False
            txn_type_upper = str(txn_type).upper()
            txn_for_upper = str(txn_for).upper()

            if txn_type_upper == "SIP" and txn_for_upper == "REGISTRATION":
                apply_weight = SCHEME_WEIGHT_APPLY_TO.get("sip_registration", True)
            elif txn_type_upper == "SIP" and txn_for_upper == "CANCELLATION":
                apply_weight = SCHEME_WEIGHT_APPLY_TO.get("sip_cancellation", False)
            elif txn_type_upper == "SWP" and txn_for_upper == "REGISTRATION":
                apply_weight = SCHEME_WEIGHT_APPLY_TO.get("swp_registration", False)
            elif txn_type_upper == "SWP" and txn_for_upper == "CANCELLATION":
                apply_weight = SCHEME_WEIGHT_APPLY_TO.get("swp_cancellation", False)

            weight = 1.0
            if apply_weight:
                weight = _resolve_weight_for_scheme(scheme, exec_dt)

            amt_weighted = float(pure_amt) * weight

            rows.append(
                {
                    "date": exec_dt,
                    "amount": amt_weighted,
                    "amount_raw": float(pure_amt),  # Unweighted amount for audit
                    "scheme_name": scheme,
                    "weight": weight,
                    "txn_type": txn_type,
                    "txn_for": txn_for,
                    "reconcile_status": recon,
                    "reconcile_at": reconcile_at_val,
                    "rm_name": rm_name,
                    "rm_raw": rm_raw,
                    "source": "fraction",
                    "parent_id": d.get("_id"),
                    "line_id": fr.get("_id"),
                    "registrant_email": d.get("registrantEmail"),
                }
            )

    # --- WITHOUT FRACTIONS ---
    # --- WITHOUT FRACTIONS ---
    # We fetch all non-fraction docs and then apply the window on the latest
    # APPROVED validation timestamp in Python.
    q_no_fr = {
        **base,
        "$and": [
            {
                "$or": [
                    {"hasFractions": {"$in": [False, None]}},
                    {"transactionFractions": {"$exists": False}},
                    {"transactionFractions": {"$size": 0}},
                ]
            },
            {"validations": {"$exists": True, "$ne": []}},
        ],
    }
    if require_reconciled:
        q_no_fr["$or"] = [
            {"reconciliation.reconcileStatus": {"$in": list(RECON_OK)}},
            {"reconcileStatus": {"$in": list(RECON_OK)}},
        ]
    no_fr_docs = list(coll.find(q_no_fr, projection=projection))

    for d in no_fr_docs:
        # Exec date for non-fraction docs = latest APPROVED validation within window
        eff = _pick_validation_date(d, start_date, end_date, approved_only=True)
        if not eff:
            continue
        recon = (d.get("reconciliation") or {}).get("reconcileStatus") or d.get("reconcileStatus")
        if require_reconciled and str(recon or "").upper() not in RECON_OK:
            continue
        rr = (d.get("relationshipManager") or d.get("serviceManager") or "").strip()
        pure_amt = d.get("amount", 0) or 0
        scheme = d.get("schemeName") or ""
        txn_type = d.get("transactionType")
        txn_for = d.get("transactionFor")

        # Determine if scheme weight should apply based on toggles
        apply_weight = False
        txn_type_upper = str(txn_type).upper()
        txn_for_upper = str(txn_for).upper()

        if txn_type_upper == "SIP" and txn_for_upper == "REGISTRATION":
            apply_weight = SCHEME_WEIGHT_APPLY_TO.get("sip_registration", True)
        elif txn_type_upper == "SIP" and txn_for_upper == "CANCELLATION":
            apply_weight = SCHEME_WEIGHT_APPLY_TO.get("sip_cancellation", False)
        elif txn_type_upper == "SWP" and txn_for_upper == "REGISTRATION":
            apply_weight = SCHEME_WEIGHT_APPLY_TO.get("swp_registration", False)
        elif txn_type_upper == "SWP" and txn_for_upper == "CANCELLATION":
            apply_weight = SCHEME_WEIGHT_APPLY_TO.get("swp_cancellation", False)

        weight = 1.0
        if apply_weight:
            weight = _resolve_weight_for_scheme(scheme, eff)

        amt_weighted = float(pure_amt) * weight

        rows.append(
            {
                "date": eff,
                "amount": amt_weighted,
                "amount_raw": float(pure_amt),  # Unweighted amount for audit
                "scheme_name": scheme,
                "weight": weight,
                "txn_type": txn_type,
                "txn_for": txn_for,
                "reconcile_status": recon,
                "reconcile_at": (
                    (d.get("reconciliation") or {}).get("reconciledAt") or d.get("reconciledAt")
                ),
                "rm_name": rr,
                "rm_raw": rr,
                "source": "main",
                "parent_id": d.get("_id"),
                "line_id": d.get("_id"),
                "registrant_email": d.get("registrantEmail"),
            }
        )

    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=[
                "date",
                "amount",
                "scheme_name",
                "weight",
                "txn_type",
                "txn_for",
                "reconcile_status",
                "reconcile_at",
                "rm_name",
                "rm_raw",
                "source",
                "parent_id",
                "line_id",
                "registrant_email",
            ]
        )
    )
    if not df.empty:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["txn_type"] = df["txn_type"].astype(str)
        df["txn_for"] = df["txn_for"].astype(str)
        df = df.sort_values(["date", "line_id"], kind="stable").drop_duplicates(
            subset=["line_id"], keep="last"
        )
    logging.info("BuildTxnDF: rows=%d, window=%s→%s", len(df), start_date, end_date)
    return df


def MonthlyRollups(df_all: "DataFrame", db_or_client) -> "DataFrame":
    """Aggregate normalized SIP/SWP transactions into monthly RM buckets.

    Output is one row per (rm_name, month) with SIP metrics, Lumpsum gate
    information, combined points, and a final tier label. The result is
    ready to be stamped and upserted into MF_SIP_Leaderboard.
    """
    if pd is None:
        raise ImportError("pandas is required for MonthlyRollups; please install pandas.")

    # Empty input → empty rollup with expected key columns
    if df_all is None or df_all.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "rm_name",
                "employee_name",
                "Net SIP",
                "Gross SIP",
                "Cancel SIP",
                "Avg SIP",
                "SIP to AUM %",
                "Lumpsum Net",
                "SIP Rate (bps)",
                "SIP Points",
                "Lumpsum Points",
                "Total Points",
                "Tier",
            ]
        )

    df = df_all.copy()

    # Normalise RM names and drop skipped/internal RMs
    df["rm_name"] = df["rm_name"].astype(str).str.strip()
    mask_blank = df["rm_name"].eq("")
    mask_skip = df["rm_name"].str.lower().isin({n.lower() for n in SKIP_RM_ALIASES})
    df = df[~(mask_blank | mask_skip)]
    if df.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "rm_name",
                "employee_name",
                "Net SIP",
                "Gross SIP",
                "Cancel SIP",
                "Avg SIP",
                "SIP to AUM %",
                "Lumpsum Net",
                "SIP Rate (bps)",
                "SIP Points",
                "Lumpsum Points",
                "Total Points",
                "Tier",
            ]
        )

    # Derive month key as YYYY-MM string from transaction date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    if df.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "rm_name",
                "employee_name",
                "Net SIP",
                "Gross SIP",
                "Cancel SIP",
                "Avg SIP",
                "SIP to AUM %",
                "Lumpsum Net",
                "SIP Rate (bps)",
                "SIP Points",
                "Lumpsum Points",
                "Total Points",
                "Tier",
            ]
        )

    df["month"] = df["date"].dt.strftime("%Y-%m")

    # Normalise type/for for sign computation
    df["txn_type"] = df["txn_type"].astype(str).str.upper()
    df["txn_for"] = df["txn_for"].astype(str).str.upper()

    # Split by transaction type for SIP/SWP handling
    df_sip = df[df["txn_type"] == "SIP"].copy()
    df_swp = df[df["txn_type"] == "SWP"].copy()

    if df_sip.empty and (not SIP_INCLUDE_SWP_IN_NET or df_swp.empty):
        # If we have only SWP and config says not to net SWP into SIP, nothing to score.
        return pd.DataFrame(
            columns=[
                "month",
                "rm_name",
                "employee_name",
                "Net SIP",
                "Gross SIP",
                "Cancel SIP",
                "Avg SIP",
                "SIP to AUM %",
                "Lumpsum Net",
                "SIP Rate (bps)",
                "SIP Points",
                "Lumpsum Points",
                "Total Points",
                "Tier",
            ]
        )

    # Sign: Registration = +1, Cancellation = -1, anything else = 0
    sign_map = {"REGISTRATION": 1.0, "CANCELLATION": -1.0}
    df_sip.loc[:, "_sign"] = df_sip["txn_for"].map(sign_map).fillna(0.0)

    # Pre-fetch streaks for consistency bonus (Streak logic)
    # Map: month ("YYYY-MM") -> { rm_name_lower: streak_count }
    streak_cache = {}
    try:
        key_df = df_sip[["rm_name", "month"]].copy()
        if SIP_INCLUDE_SWP_IN_NET and not df_swp.empty:
            key_df = pd.concat([key_df, df_swp[["rm_name", "month"]]], ignore_index=True)
        unique_months = sorted(key_df["month"].dropna().unique())
        if unique_months:
            # Resolve DB once
            _db_qs = None
            if hasattr(db_or_client, "get_database"):
                _db_qs = db_or_client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2"))
            elif hasattr(db_or_client, "client"):
                 # type: ignore
                _db_qs = db_or_client.client[os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")]

            if _db_qs is not None:
                for m_str in unique_months:
                    try:
                        y, m_num = m_str.split("-")
                        dt_curr = datetime(int(y), int(m_num), 1)
                        # Previous month
                        if m_num == "01" or m_num == "1" or int(m_num) == 1:
                            dt_prev = datetime(int(y) - 1, 12, 1)
                        else:
                            dt_prev = datetime(int(y), int(m_num) - 1, 1)
                        prev_m_str = dt_prev.strftime("%Y-%m")

                        # Populate cache for this month's lookback
                        streak_cache[m_str] = {}

                        # Fetch all RMs for previous month
                        # Projection: rm_name, consecutive_positive_months
                        for doc in _db_qs["MF_SIP_Leaderboard"].find(
                            {"month": prev_m_str},
                            {"rm_name": 1, "consecutive_positive_months": 1}
                        ):
                            r_name = doc.get("rm_name")
                            if r_name:
                                streak_cache[m_str][str(r_name).lower().strip()] = int(doc.get("consecutive_positive_months") or 0)
                    except Exception as e_inner:
                        logging.warning(f"[StreakFetch] Failed for month {m_str}: {e_inner}")

    except Exception as e_streak:
        logging.warning(f"[StreakFetch] Global failure: {e_streak}")

    buckets: list[dict[str, Any]] = []

    key_df = df_sip[["rm_name", "month"]].copy()
    if SIP_INCLUDE_SWP_IN_NET and not df_swp.empty:
        key_df = pd.concat([key_df, df_swp[["rm_name", "month"]]], ignore_index=True)
    key_df = key_df.drop_duplicates()
    sip_groups = {
        (rm, month): g for (rm, month), g in df_sip.groupby(["rm_name", "month"], sort=True)
    }
    swp_groups = (
        {(rm, month): g for (rm, month), g in df_swp.groupby(["rm_name", "month"], sort=True)}
        if not df_swp.empty
        else {}
    )
    group_keys = list(sip_groups.keys())
    # Always process SWP keys so we can report SWP stats even if not netting
    for key in swp_groups.keys():
        if key not in sip_groups:
            group_keys.append(key)
    group_keys.sort()
    empty_sip = df_sip.head(0)

    # Group by RM + month (sorted for streak propagation)
    for rm_name, month in group_keys:
        g = sip_groups.get((rm_name, month), empty_sip)
        # Inactive gating: apply 6-month rule using Zoho_Users status/inactive_since.
        # If not eligible for this month, skip building any SIP bucket for this RM/month.
        try:
            if not _rm_eligible_by_inactive(db_or_client, rm_name, month):
                continue
        except Exception as _e:
            logging.warning(
                "[InactiveGate-SIP] Error while evaluating RM='%s' month='%s': %s",
                rm_name,
                month,
                _e,
            )

        # Basic SIP aggregates
        g_amt = pd.to_numeric(g["amount"], errors="coerce").fillna(0.0)
        g_sign = g["_sign"].astype(float)

        gross_mask = g_sign > 0
        cancel_mask = g_sign < 0

        gross_sip = float(g_amt[gross_mask].sum())
        cancel_sip = float(g_amt[cancel_mask].sum())  # this will be positive magnitude
        # Net SIP (core) is signed: Registration - Cancellation
        net_sip_core = float((g_amt * g_sign).sum())
        # Average SIP ticket size based on positive registration amounts
        pos_amounts = g_amt[gross_mask]
        avg_sip = float(pos_amounts.mean()) if not pos_amounts.empty else 0.0

        # Optional SWP adjustment into Net SIP
        swp_adj_reg = 0.0
        swp_adj_cancel = 0.0
        swp_net_effect = 0.0
        if SIP_INCLUDE_SWP_IN_NET and not df_swp.empty:
            swp_key = (df_swp["rm_name"].astype(str).str.strip() == rm_name) & (
                df_swp["month"].astype(str) == month
            )
            swp_rows = df_swp[swp_key]
            if not swp_rows.empty:
                swp_rows = swp_rows.copy()
                swp_rows["txn_for"] = swp_rows["txn_for"].astype(str).str.upper()
                swp_amt = pd.to_numeric(swp_rows["amount"], errors="coerce").fillna(0.0)
                swp_reg_mask = swp_rows["txn_for"].eq("REGISTRATION")
                swp_cancel_mask = swp_rows["txn_for"].eq("CANCELLATION")

                reg_w = float(SWP_WEIGHTS.get("registration", SWP_WEIGHTS_DEFAULT["registration"]))
                cancel_w = float(
                    SWP_WEIGHTS.get("cancellation", SWP_WEIGHTS_DEFAULT["cancellation"])
                )

                swp_adj_reg = float((swp_amt[swp_reg_mask] * reg_w).sum())
                swp_adj_cancel = float((swp_amt[swp_cancel_mask] * cancel_w).sum())
                swp_net_effect = swp_adj_reg + swp_adj_cancel

        net_sip = net_sip_core + swp_net_effect

        # Look up employee_id + active flag for Lumpsum gate & AUM attribution
        emp_id, _is_active = _lookup_employee_active_and_id(db_or_client, rm_name)

        # Lumpsum gate + primary AUM sourcing from Lumpsum leaderboard
        gate = _lumpsum_gate_check(db_or_client, rm_name, emp_id, month)
        ls_net = _coerce_float(gate.get("ls_net_purchase")) or 0.0
        aum_start = _coerce_float(gate.get("ls_aum_start"))

        # If Lumpsum leaderboard has no AUM (ls_doc_not_found/no_aum), fall back to AUM_Report
        if aum_start is None or aum_start <= 0:
            try:
                aum_fallback = _lookup_aum_for_rm_month(db_or_client, rm_name, month)
            except Exception:
                aum_fallback = None
            if aum_fallback is not None and aum_fallback > 0:
                aum_start = aum_fallback

        # Normalise missing/invalid AUM to 0.0 so downstream math never produces NaN
        if (
            aum_start is None
            or (isinstance(aum_start, float) and not np.isfinite(aum_start))
            or aum_start <= 0
        ):
            aum_start = 0.0

        # SIP-to-AUM ratio (dimensionless); 0.0 if no usable AUM
        try:
            sip_to_aum = float(net_sip) / float(aum_start) if aum_start > 0 else 0.0
        except Exception:
            sip_to_aum = 0.0

        # Consistency Streak
        prev_streak = 0
        try:
             # streak_cache keys are strings
             prev_streak = streak_cache.get(str(month), {}).get(str(rm_name).lower().strip(), 0)
        except:
             pass

        # Increment if positive Net SIP, else reset
        if net_sip > 0:
            curr_streak = prev_streak + 1
        else:
            curr_streak = 0

        # Propagate streak to next month in memory (for robust batch/re-aggregation runs)
        try:
            y_str, m_str_n = month.split("-")
            y_val, m_val = int(y_str), int(m_str_n)
            if m_val == 12:
                next_m = f"{y_val + 1}-01"
            else:
                next_m = f"{y_val}-{m_val + 1:02d}"

            if next_m not in streak_cache:
                streak_cache[next_m] = {}
            streak_cache[next_m][str(rm_name).lower().strip()] = curr_streak
        except Exception:
            pass


        # SIP incentive computation
        inc = _compute_sip_incentive(
            net_sip=net_sip,
            sip_to_aum=sip_to_aum,
            avg_sip=avg_sip,
            consec_positive_months=curr_streak,
            horizon_months=SIP_HORIZON_MONTHS,
        )
        sip_points = float(inc.get("points") or 0.0)
        sip_rate_bps = inc.get("rate_bps")
        sip_effective_rate = inc.get("effective_rate")
        sip_rate_components = inc.get("rate_components_bps")
        sip_rate_capped = inc.get("rate_capped")
        sip_rate_cap_reason = inc.get("cap_reason")

        # If Lumpsum gate is triggered, POSITIVE SIP points are zeroed out.
        # Penalties (negative points) must persist.
        if gate.get("applied") and sip_points > 0:
            sip_points = 0.0

        # Lumpsum points: NP × rate (from Lumpsum_Scorer's growth slabs)
        # Rate is determined by NP:AUM growth % via Lumpsum rate_slabs
        try:
            ls_rate = float(gate.get("ls_rate_used") or 0.0)
            if ls_rate > 0:
                # Use rate from Lumpsum_Scorer (e.g., 0.0015 for 71% growth)
                ls_points = float(ls_net) * ls_rate
            else:
                # No fallback - Lumpsum Scorer is sole authority.
                ls_points = 0.0

            # Cap negative lumpsum points to -5000 (Max Penalty) to allow negatives but prevent unbounded drag
            # User Feedback: "should give negative points... but with capping"
            ls_points = max(-5000.0, ls_points)
        except Exception:
            ls_points = 0.0

        total_points = sip_points + ls_points
        tier = _tier_from_points(total_points)

        # --- Audit Section: Raw Amounts & Scheme Bonus ---
        # Calculate unweighted amounts for audit trail
        g_amt_raw = pd.to_numeric(g.get("amount_raw", g["amount"]), errors="coerce").fillna(0.0)

        # Raw aggregates by type
        gross_sip_raw = float(g_amt_raw[gross_mask].sum())
        cancel_sip_raw = float(g_amt_raw[cancel_mask].sum())
        net_sip_raw = float((g_amt_raw * g_sign).sum())

        # SWP raw amounts
        swp_adj_reg_raw = 0.0
        swp_adj_cancel_raw = 0.0
        if SIP_INCLUDE_SWP_IN_NET and not df_swp.empty:
            swp_key = (df_swp["rm_name"].astype(str).str.strip() == rm_name) & (
                df_swp["month"].astype(str) == month
            )
            swp_rows = df_swp[swp_key]
            if not swp_rows.empty:
                swp_rows = swp_rows.copy()
                swp_rows["txn_for"] = swp_rows["txn_for"].astype(str).str.upper()
                swp_amt_raw = pd.to_numeric(swp_rows.get("amount_raw", swp_rows["amount"]), errors="coerce").fillna(0.0)
                swp_reg_mask = swp_rows["txn_for"].eq("REGISTRATION")
                swp_cancel_mask = swp_rows["txn_for"].eq("CANCELLATION")

                reg_w = float(SWP_WEIGHTS.get("registration", SWP_WEIGHTS_DEFAULT["registration"]))
                cancel_w = float(SWP_WEIGHTS.get("cancellation", SWP_WEIGHTS_DEFAULT["cancellation"]))

                swp_adj_reg_raw = float((swp_amt_raw[swp_reg_mask] * reg_w).sum())
                swp_adj_cancel_raw = float((swp_amt_raw[swp_cancel_mask] * cancel_w).sum())

        # Calculate Scheme Bonus (weighted - unweighted)
        scheme_bonus = (gross_sip - gross_sip_raw) + (cancel_sip_raw - cancel_sip) + (swp_adj_reg - swp_adj_reg_raw) + (swp_adj_cancel - swp_adj_cancel_raw)

        # Build Audit ByType array
        audit_by_type = [
            {"type": "SIP Registration", "sum": gross_sip_raw},
            {"type": "SIP Cancellation", "sum": cancel_sip_raw},
            {"type": "SWP Registration", "sum": swp_adj_reg_raw},
            {"type": "SWP Cancellation", "sum": swp_adj_cancel_raw},
            {"type": "Net SIP", "sum": net_sip_raw + swp_adj_reg_raw + swp_adj_cancel_raw},
        ]

        bucket: dict[str, Any] = {
            "month": month,
            "rm_name": rm_name,
            "employee_name": rm_name,
            # SIP metrics (both Title Case and snake_case for compatibility)
            "Net SIP": net_sip,
            "net_sip": net_sip,
            "Net SIP (Core)": net_sip_core,
            "net_sip_core": net_sip_core,
            "SWP Net Effect": swp_net_effect,
            "swp_net_effect": swp_net_effect,
            "swp_adj_registration": swp_adj_reg,
            "swp_adj_cancellation": swp_adj_cancel,
            "Gross SIP": gross_sip,
            "gross_sip": gross_sip,
            "Cancel SIP": cancel_sip,
            "cancel_sip": cancel_sip,
            "Avg SIP": avg_sip,
            "avg_sip": avg_sip,
            "SIP to AUM %": sip_to_aum,
            "sip_to_aum": sip_to_aum,
            # Lumpsum + AUM context
            "Lumpsum Net": ls_net,
            "net_lumpsum": ls_net,
            "aum_start": aum_start,
            "ls_gate_applied": bool(gate.get("applied")),
            "ls_gate_reason": gate.get("reason"),
            "ls_growth_pct": gate.get("ls_growth_pct"),
            # SIP incentive details
            "SIP Rate (bps)": sip_rate_bps,
            "sip_rate_bps": sip_rate_bps,
            "sip_effective_rate": sip_effective_rate,
            "sip_rate_components_bps": sip_rate_components,
            "sip_rate_capped": sip_rate_capped,
            "sip_rate_cap_reason": sip_rate_cap_reason,
            "consecutive_positive_months": curr_streak,
            "Consecutive Positive Months": curr_streak,
            "SIP Points": sip_points,
            "sip_points": sip_points,
            # Lumpsum + combined points
            "Lumpsum Points": ls_points,
            "lumpsum_points": ls_points,
            "Total Points": total_points,
            "total_points": total_points,
            # Final tier
            "Tier": tier,
            "tier": tier,
            # Audit section
            "Audit": {
                "ByType": audit_by_type,
                "SchemeBonus": scheme_bonus,
            },
        }
        buckets.append(bucket)

    if not buckets:
        return pd.DataFrame(
            columns=[
                "month",
                "rm_name",
                "employee_name",
                "Net SIP",
                "Gross SIP",
                "Cancel SIP",
                "Avg SIP",
                "SIP to AUM %",
                "Lumpsum Net",
                "SIP Rate (bps)",
                "SIP Points",
                "Lumpsum Points",
                "Total Points",
                "Tier",
            ]
        )

    df_roll = pd.DataFrame(buckets)
    return df_roll


def run_pipeline(
    start_date: datetime,
    end_date: datetime,
    emails_list: Optional[Sequence[str]] = None,
    mongo_uri: Optional[str] = None,
) -> tuple[int, int, int]:
    """
    Orchestrates the end-to-end run:
      1) Connect to MongoDB (reusing client on warm starts).
      2) Build normalized transactions (reconciled-only).
      3) Build monthly rollups (with AUM/employee enrichment).
      4) Upsert to leaderboard.

    Returns: (normalized_rows, upserts, buckets)
    """
    client = _get_mongo_client(mongo_uri)

    # --- SIP schema + config bootstrap (Mongo-driven runtime options, aligned with Lumpsum) ---
    try:
        _load_runtime_config(client)
    except Exception as e:
        logging.warning("[SIP Config] Failed to bootstrap config/schema: %s", e)

    df_all = BuildTxnDF(
        start_date,
        end_date,
        client,
        emails=emails_list,
        include_types=("SIP", "SWP"),
        require_reconciled=True,
    )
    logging.info("Normalized TXNs: rows=%d", len(df_all))

    df_roll = MonthlyRollups(df_all, client)
    logging.info("Monthly rollups ready: %d buckets", len(df_roll))

    # Trail aggregation: for each month in the SIP rollup, sync trail leaderboard + VP summary
    try:
        if df_roll is not None and not df_roll.empty and "month" in df_roll.columns:
            month_keys = sorted(set(df_roll["month"].astype(str)))
            for m in month_keys:
                AggregateTrailRates(client, m)
    except Exception as e:
        logging.warning("[Trail Aggregator] Failed during aggregation: %s", e)

    # Ensure pandas is available for vectorized stamping
    if pd is None:
        raise ImportError("pandas is required for SIP stamping operations; please install pandas.")

    # Schema/config metadata stamping via assign (avoids Series[Any] literal-type-check warnings)
    df_roll = df_roll.assign(
        schema_version=SCHEMA_VERSION_SIP,
        config_hash=_SIP_LAST_CFG_HASH,
        module="SIP_Scorer",
        updated_at=_now_utc(),
    )

    # Vectorized lookup for (employee_id, is_active) — map to tuples, then expand to a DataFrame
    pairs = df_roll["rm_name"].astype(str).map(lambda n: _lookup_employee_active_and_id(client, n))
    df_roll[["employee_id", "is_active"]] = PD.DataFrame(
        list(pairs), index=df_roll.index, columns=["employee_id", "is_active"]
    )

    upserts, buckets = UpsertMonthlyRollups(client, df_roll)
    logging.info("Upserted leaderboard records: %d (buckets=%d)", upserts, buckets)
    return (len(df_all), upserts, buckets)


# --- Helper: Coerce float ---
def _coerce_float(val: object) -> Optional[float]:
    """Leniently convert common numeric shapes (ints, floats, '1,234.56') to float."""
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            s = val.replace(",", "").strip()
            if s == "":
                return None
            return float(s)
        # Handle common MongoDB numeric representations
        if isinstance(val, dict):
            for key in ("$numberDouble", "$numberDecimal", "$numberInt", "value", "amount"):
                if key in val and val[key] is not None:
                    try:
                        s = str(val[key]).replace(",", "").strip()
                        if s == "":
                            return None
                        return float(s)
                    except Exception:
                        continue
    except Exception:
        return None
    return None


    # _resolve_lumpsum_points_coeff removed
    return 0.0


# --- SIP Incentive computation (rates in basis points; final capped at [-3, +9] bps) ---
def _compute_sip_incentive(
    net_sip: float,
    sip_to_aum: float | None,
    avg_sip: float | None,
    consec_positive_months: int | None = None,
    horizon_months: int | None = None,
) -> dict:
    """
    Returns a dict with:
      - rate_bps: final applied rate in basis points (can be negative for penalties)
      - rate_components_bps: breakdown dict
      - rate_capped: bool (True if cap/floor applied)
      - incentive_points: net_sip * (rate_bps/10000.0)
    Design:
      * Positive months: base 2 bps + (ratio bonus up to +4) + (amount bonus up to +3) + (avg_sip up to +2) + (consistency +1)
        No artificial positive cap; total is the sum of slabs.
      * Negative months: penalty -1 / -2 / -3 bps depending on severity; floor -3 bps.
    """
    # Defaults / guards
    try:
        ns = float(net_sip or 0.0)
    except Exception:
        ns = 0.0
    ratio = float(sip_to_aum) if sip_to_aum is not None else 0.0
    avg = float(avg_sip) if avg_sip is not None else 0.0
    positive = ns >= 0

    try:
        horizon = int(horizon_months) if horizon_months is not None else int(SIP_HORIZON_MONTHS)
    except Exception:
        horizon = int(SIP_HORIZON_MONTHS)
    if horizon <= 0:
        horizon = int(SIP_HORIZON_MONTHS_DEFAULT)

    if not positive:
        # Penalty slabs on negative month - now configurable
        # Check if penalty is enabled
        if not PENALTY_ENABLED:
            # If disabled, return zero penalty
            return {
                "rate_bps": 0.0,
                "effective_rate": 0.0,
                "rate_components_bps": {"penalty_bps": 0.0},
                "rate_capped": False,
                "cap_reason": "penalty_disabled",
                "points_raw": 0.0,
                "incentive_points": 0.0,
                "points": 0.0,
            }

        severity = abs(ns)
        penalty = 0.0

        # Find matching slab (sorted descending by severity/BPS)
        # Apply "OR" logic: if it crosses EITHER the amount threshold OR the ratio threshold, it triggers the penalty.
        for s in PENALTY_SLABS:
            thr_amt = s.get("threshold_amount", 0.0)
            thr_ratio = s.get("threshold_ratio", 0.0)

            if severity >= thr_amt or (ratio <= thr_ratio and ratio < 0):
                penalty = -float(s.get("rate_bps", 0.0))
                break

        # Respect configured slabs; only floor if no slabs matched and default is needed
        rate_bps = penalty
        effective_rate = rate_bps / 10000.0
        cap_reason = None
        raw_points = ns * abs(effective_rate)  # ns is negative -> payout negative
        points_scaled = raw_points * float(horizon)
        return {
            "rate_bps": rate_bps,
            "effective_rate": effective_rate,
            "rate_components_bps": {
                "penalty_bps": rate_bps,
            },
            "rate_capped": cap_reason is not None,
            "cap_reason": cap_reason,
            "points_raw": raw_points,
            "incentive_points": points_scaled,  # backward-compat key
            "points": points_scaled,
        }

    # Policy 2025 V1: Base earning rate
    # Use global SIP_BASE_BPS (which is either explicitly configured or derived from legacy coeff for standard horizon)
    base_bps = float(SIP_BASE_BPS)

    # Ratio (SIP/AUM) bonuses
    ratio_bonus = 0.0
    for thr, bps in BONUS_SLABS_RATIO:
        # Legacy used strictly greater (>). Preserving that behavior for Ratio.
        if ratio > thr:
            ratio_bonus = bps
            break

    # Absolute Net SIP bonuses
    amt_bonus = 0.0
    for thr, bps in BONUS_SLABS_ABS:
        if ns >= thr:
            amt_bonus = bps
            break

    # Average SIP ticket bonuses
    avg_bonus = 0.0
    for thr, bps in BONUS_SLABS_AVG:
        if avg >= thr:
            avg_bonus = bps
            break

    # Consistency bonus (+1 bp if 3+ consecutive positive months) — optional; default off
    cons_bonus = 0.0
    streak = int(consec_positive_months) if consec_positive_months is not None else 0
    if streak > 0:
        for s in BONUS_SLABS_CONSISTENCY:
            # 1. Streak Requirement
            if streak >= s.get("min_months", 0):
                # 2. Secondary Criteria (Ratio OR Amount)
                min_r = s.get("min_ratio", 0.0)
                min_a = s.get("min_amount", 0.0)

                has_criteria = (min_r > 0) or (min_a > 0)

                if not has_criteria:
                    # Pure streak bonus
                    cons_bonus = s.get("bps", 0.0)
                    break
                else:
                    # Must meet at least one configured secondary condition
                    pass_r = (min_r > 0 and ratio >= min_r)
                    pass_a = (min_a > 0 and ns >= min_a)
                    if pass_r or pass_a:
                        cons_bonus = s.get("bps", 0.0)
                        break

    raw_bps = base_bps + ratio_bonus + amt_bonus + avg_bonus + cons_bonus
    rate_bps = raw_bps  # no artificial positive cap
    effective_rate = rate_bps / 10000.0
    raw_points = ns * effective_rate
    points_scaled = raw_points * float(horizon)
    return {
        "rate_bps": rate_bps,
        "effective_rate": effective_rate,
        "rate_components_bps": {
            "base_bps": base_bps,
            "ratio_bonus_bps": ratio_bonus,
            "amount_bonus_bps": amt_bonus,
            "avg_sip_bonus_bps": avg_bonus,
            "consistency_bonus_bps": cons_bonus,
        },
        "rate_capped": False,
        "cap_reason": None,
        "points_raw": raw_points,
        "incentive_points": points_scaled,  # backward-compat key
        "points": points_scaled,
    }


# --- SIP eligibility gate based on Lumpsum monthly performance ---
def _lumpsum_gate_check(
    client, rm_name: str, employee_id: Optional[str], period_month: str
) -> dict:
    """
    SIP eligibility gate based on Lumpsum monthly performance.
    Looks up PLI_Leaderboard.<LEADERBOARD_LUMPSUM_COLL> for (employee_id, month) else (rm_name, month).

    Returns keys: applied, reason, ls_growth_pct, ls_net_purchase, ls_aum_start,
                  threshold_pct, min_rupees, source_coll, match_key.
    """
    try:
        db = client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard"))
        coll_name = os.getenv("LEADERBOARD_LUMPSUM_COLL", "Leaderboard_Lumpsum")
        coll = db.get_collection(coll_name)

        doc = None
        if employee_id:
            doc = coll.find_one({"employee_id": employee_id, "month": period_month})
        if doc is None:
            doc = coll.find_one({"rm_name": rm_name, "month": period_month})
        if not doc:
            return {"applied": False, "reason": "ls_doc_not_found", "source_coll": coll_name}

        def pick(d: dict, *keys):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return None

        aum = _coerce_float(
            pick(doc, "aum_start", "AUM (Start of Month)", "aum_first", "aum", "AUM")
        )
        net = _coerce_float(
            pick(doc, "net_purchase", "Net Purchase (Formula)", "net", "np", "net_purchase_rupees")
        )
        rate = _coerce_float(pick(doc, "rate_used", "rate"))

        if aum is None or aum <= 0:
            return {
                "applied": False,
                "reason": "no_aum",
                "ls_net_purchase": float(net or 0.0),
                "ls_rate_used": float(rate or 0.0),
                "source_coll": coll_name,
                "match_key": ("employee_id" if employee_id else "rm_name"),
            }
        if net is None:
            return {
                "applied": False,
                "reason": "no_net_purchase",
                "ls_aum_start": float(aum or 0.0),
                "ls_rate_used": float(rate or 0.0),
                "source_coll": coll_name,
                "match_key": ("employee_id" if employee_id else "rm_name"),
            }

        growth_pct = (float(net) / float(aum)) * 100.0
        thr = float(SIP_LS_GATE_PCT)
        min_amt = float(SIP_LS_GATE_MIN_RUPEES)
        applied = (growth_pct <= thr) and (abs(float(net)) >= min_amt)
        return {
            "applied": bool(applied),
            "reason": ("gate_triggered" if applied else "ok"),
            "ls_growth_pct": float(round(growth_pct, 4)),
            "ls_net_purchase": float(net),
            "ls_aum_start": float(aum),
            "ls_rate_used": float(rate or 0.0),
            "threshold_pct": float(thr),
            "min_rupees": float(min_amt),
            "source_coll": coll_name,
            "match_key": ("employee_id" if employee_id else "rm_name"),
        }
    except Exception as e:
        return {"applied": False, "reason": f"error:{e.__class__.__name__}"}


# --- Helper: Resolve VP employee id (env override first, then Zoho_Users) ---
def _resolve_vp_employee_id(db_or_client) -> Optional[str]:
    """
    Resolve VP employee_id using env override first, then Zoho_Users based on VP_LEADER_NAME.
    Env (VP_LEADER_EMP_ID) has priority; Zoho lookup is a fallback so we still work
    even when the env is not populated.
    """
    # 1) Explicit env override wins for safety/back-compat
    vp_emp_id_env = os.getenv(VP_LEADER_EMP_ID_ENV)
    if vp_emp_id_env:
        return str(vp_emp_id_env)

    # 2) Fallback: look up by VP_LEADER_NAME in Zoho_Users
    try:
        emp_id, _is_active = _lookup_employee_active_and_id(db_or_client, VP_LEADER_NAME)
        if emp_id:
            return str(emp_id)
    except Exception:
        # Silent fallback: caller will see a None and can ignore the employee_id key
        pass

    return None


def AggregateTrailRates(client, month: str) -> tuple[int, float]:
    """Join Lumpsum + SIP leaderboards for a month and compute final trail.

    For each RM row in MF_SIP_Leaderboard for the given month:
      * Look up the matching Lumpsum leaderboard row by (employee_id, month) then (rm_name, month).
      * Derive AUM at start of month (preferring Lumpsum, falling back to SIP aum_start).
      * Use the SIP tier (T0–T6) and TIER_MONTHLY_FACTORS to compute monthly and annual trail rate
        and the rupee trail on AUM.
      * Preserve vp_points_credit from SIP leaderboard.

    Results are upserted into PLI_Leaderboard.<TRAIL_LEADERBOARD_DEFAULT_COLL> and a single
    VP summary doc into <TRAIL_VP_SUMMARY_DEFAULT_COLL>.

    Returns: (trail_upserts_count, vp_points_total).
    """
    db_lb = client.get_database(os.getenv("PLI_DB_NAME", "PLI_Leaderboard"))
    coll_sip = db_lb.get_collection("MF_SIP_Leaderboard")
    coll_ls = db_lb.get_collection(os.getenv("LEADERBOARD_LUMPSUM_COLL", "Leaderboard_Lumpsum"))

    trail_coll_name = os.getenv(TRAIL_LEADERBOARD_COLL_ENV, TRAIL_LEADERBOARD_DEFAULT_COLL)
    coll_trail = db_lb.get_collection(trail_coll_name)

    vp_coll_name = os.getenv(TRAIL_VP_SUMMARY_COLL_ENV, TRAIL_VP_SUMMARY_DEFAULT_COLL)
    coll_vp = db_lb.get_collection(vp_coll_name)

    cursor = coll_sip.find({"month": month})

    ops_trail: list[UpdateOne] = []
    vp_points_total: float = 0.0

    for doc_sip in cursor:
        rm_name = doc_sip.get("rm_name") or doc_sip.get("employee_name")
        if not rm_name:
            continue
        rm_name = str(rm_name).strip()
        if not rm_name:
            continue

        emp_id = doc_sip.get("employee_id")

        doc_ls = None
        if emp_id:
            doc_ls = coll_ls.find_one({"employee_id": emp_id, "month": month})
        if not doc_ls:
            doc_ls = coll_ls.find_one({"rm_name": rm_name, "month": month})

        def _pick(d: dict | None, *keys):
            if not isinstance(d, dict):
                return None
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return None

        aum_start = _coerce_float(
            _pick(
                doc_ls,
                "aum_start",
                "AUM (Start of Month)",
                "aum_first",
                "aum",
                "AUM",
            )
        )
        if aum_start is None:
            aum_start = _coerce_float(doc_sip.get("aum_start"))

        # Normalise missing/invalid AUM to 0.0 so trail_amount_month never becomes NaN
        if aum_start is None or (isinstance(aum_start, float) and not np.isfinite(aum_start)):
            aum_start = 0.0

        total_points_raw = doc_sip.get("Total Points", doc_sip.get("total_points"))
        try:
            total_points = float(total_points_raw or 0.0)
        except Exception:
            total_points = 0.0

        tier = doc_sip.get("Tier") or doc_sip.get("tier") or _tier_from_points(total_points)
        factor_raw = TIER_MONTHLY_FACTORS.get(str(tier), 0.0)
        try:
            monthly_factor = float(factor_raw or 0.0)
        except Exception:
            monthly_factor = 0.0

        annual_factor = monthly_factor * 12.0
        try:
            trail_amount_month: float | None = float(aum_start) * monthly_factor
        except Exception:
            trail_amount_month = 0.0

        sip_points = doc_sip.get("SIP Points", doc_sip.get("sip_points"))
        lumpsum_points = doc_sip.get("Lumpsum Points", doc_sip.get("lumpsum_points"))

        vp_pc_raw = doc_sip.get("vp_points_credit") or 0.0
        try:
            vp_pc = float(vp_pc_raw or 0.0)
        except Exception:
            vp_pc = 0.0
        vp_points_total = vp_points_total + vp_pc

        # Structured per-RM-per-month trail log (aligned with Lumpsum trail logging)
        try:
            log_kv(
                logging.INFO,
                "[Trail RM] Aggregated monthly trail",
                month=month,
                rm_name=rm_name,
                employee_id=emp_id,
                tier=tier,
                total_points=total_points,
                sip_points=sip_points,
                lumpsum_points=lumpsum_points,
                aum_start=aum_start,
                monthly_factor=monthly_factor,
                annual_factor=annual_factor,
                trail_amount_month=trail_amount_month,
                vp_points_credit=vp_pc,
            )
        except Exception:
            logging.warning(
                "[Trail RM] Failed to log trail row for rm_name=%s month=%s",
                rm_name,
                month,
            )

        trail_doc: dict[str, Any] = {
            "month": month,
            "rm_name": rm_name,
            "employee_name": doc_sip.get("employee_name") or rm_name,
            "employee_id": emp_id,
            "tier": tier,
            "total_points": total_points,
            "sip_points": sip_points,
            "lumpsum_points": lumpsum_points,
            "aum_start": aum_start,
            "monthly_factor": monthly_factor,
            "annual_factor": annual_factor,
            "trail_amount_month": trail_amount_month,
            "vp_points_credit": vp_pc,
            "schema_version": SCHEMA_VERSION_SIP,
            "config_hash": _SIP_LAST_CFG_HASH,
            "module": "MF_Trail_Aggregator",
            "updated_at": _now_utc(),
        }

        filter_: dict[str, Any] = {"month": month, "rm_name": rm_name}
        if emp_id:
            filter_["employee_id"] = emp_id

        ops_trail.append(UpdateOne(filter_, {"$set": trail_doc}, upsert=True))

    trail_upserts = 0
    if ops_trail:
        res = coll_trail.bulk_write(ops_trail, ordered=False)
        trail_upserts = res.upserted_count + res.modified_count

    # Resolve VP employee_id: env override first, then Zoho_Users via VP_LEADER_NAME
    vp_emp_id = _resolve_vp_employee_id(client)
    vp_doc: dict[str, Any] = {
        "month": month,
        "vp_name": VP_LEADER_NAME,
        "vp_employee_id": vp_emp_id,
        "vp_points_total": vp_points_total,
        "schema_version": SCHEMA_VERSION_SIP,
        "config_hash": _SIP_LAST_CFG_HASH,
        "module": "MF_Trail_Aggregator",
        "updated_at": _now_utc(),
    }
    try:
        # Key by month + vp_name, and also by vp_employee_id when available so
        # downstream aggregates can join on a stable employee id.
        vp_filter: dict[str, Any] = {"month": month, "vp_name": VP_LEADER_NAME}
        if vp_emp_id:
            vp_filter["vp_employee_id"] = vp_emp_id
        coll_vp.update_one(vp_filter, {"$set": vp_doc}, upsert=True)
    except Exception as e:
        logging.warning("[Trail Aggregator] Failed to upsert VP summary: %s", e)

    logging.info(
        "[Trail Aggregator] Aggregated trail | month=%s, upserts=%d, vp_points_total=%s",
        month,
        trail_upserts,
        vp_points_total,
    )
    return trail_upserts, vp_points_total


def UpsertMonthlyRollups(client, df_rollups: "DataFrame") -> tuple[int, int]:
    """
    Upsert monthly rollups into PLI_Leaderboard.MF_SIP_Leaderboard.
    Returns (upserts_count, buckets_count).
    """
    # Get collections
    db_name = os.getenv("PLI_DB_NAME") or os.getenv("DB_NAME") or "PLI_Leaderboard"
    db_lb = client.get_database(db_name)
    coll_lb = db_lb.get_collection("MF_SIP_Leaderboard")
    coll_audit = db_lb.get_collection("SIP_audit")

    # Prepare bulk upserts and audit docs
    ops: list[UpdateOne] = []
    audit_docs: list[dict] = []

    if pd is None or df_rollups is None or df_rollups.empty:
        return (0, 0)

    for idx, row in df_rollups.iterrows():
        # Convert the Series to a plain dict so Pylance doesn't treat it as a DataFrame/Series
        row_dict = row.to_dict()
        doc: dict[str, Any] = dict(row_dict)
        # --- SIP schema/config stamping ---
        doc["schema_version"] = SCHEMA_VERSION_SIP
        doc["config_hash"] = _SIP_LAST_CFG_HASH
        doc["module"] = "SIP_Scorer"
        doc["updated_at"] = _now_utc()
        # Ensure employee_id and is_active are present via a clean string rm_name
        rm_name_val = doc.get("rm_name") or doc.get("employee_name")
        rm_name: str = str(rm_name_val) if rm_name_val is not None else ""
        emp_id: Optional[str] = None
        is_active: Optional[bool] = None
        if rm_name:
            emp_id, is_active = _lookup_employee_active_and_id(client, rm_name)
        if emp_id and not doc.get("employee_id"):
            doc["employee_id"] = emp_id
        if is_active is not None and "is_active" not in doc:
            doc["is_active"] = bool(is_active)
        # --- VP credit: 20% of total points (audit-only field per RM row) ---
        total_pts_raw = (
            doc.get("Total Points") if "Total Points" in doc else doc.get("total_points")
        )
        try:
            total_pts = float(total_pts_raw or 0.0)
        except Exception:
            total_pts = 0.0
        vp_points_credit = total_pts * 0.20
        doc["vp_points_credit"] = vp_points_credit
        # Prepare upsert operation (keyed by RM/month)
        filter_ = {"rm_name": doc.get("rm_name"), "month": doc.get("month")}
        ops.append(UpdateOne(filter_, {"$set": doc}, upsert=True))

        # Structured per-RM-per-month SIP rollup log (mirrors Lumpsum-style buckets)
        try:
            log_kv(
                logging.INFO,
                "[SIP Rollup] RM monthly bucket",
                month=doc.get("month"),
                rm_name=doc.get("rm_name"),
                employee_id=doc.get("employee_id"),
                net_sip=doc.get("Net SIP") if "Net SIP" in doc else doc.get("net_sip"),
                gross_sip=(doc.get("Gross SIP") if "Gross SIP" in doc else doc.get("gross_sip")),
                cancel_sip=(
                    doc.get("Cancel SIP") if "Cancel SIP" in doc else doc.get("cancel_sip")
                ),
                avg_sip=doc.get("Avg SIP") if "Avg SIP" in doc else doc.get("avg_sip"),
                sip_to_aum=(
                    doc.get("SIP to AUM %") if "SIP to AUM %" in doc else doc.get("sip_to_aum")
                ),
                net_lumpsum=(
                    doc.get("Lumpsum Net") if "Lumpsum Net" in doc else doc.get("net_lumpsum")
                ),
                sip_points=(
                    doc.get("SIP Points") if "SIP Points" in doc else doc.get("sip_points")
                ),
                lumpsum_points=(
                    doc.get("Lumpsum Points")
                    if "Lumpsum Points" in doc
                    else doc.get("lumpsum_points")
                ),
                total_points=(
                    doc.get("Total Points") if "Total Points" in doc else doc.get("total_points")
                ),
                tier=doc.get("Tier") or doc.get("tier"),
                vp_points_credit=vp_points_credit,
            )
        except Exception:
            logging.warning(
                "[SIP Rollup] Failed to log bucket for rm_name=%s month=%s",
                doc.get("rm_name"),
                doc.get("month"),
            )

        # --- SIP audit doc ---
        audit_docs.append(
            {
                "month": doc.get("month"),
                "employee_name": doc.get("employee_name") or doc.get("rm_name"),
                "employee_id": doc.get("employee_id"),
                "rm_name": doc.get("rm_name"),
                "metrics": {
                    "net_sip": doc.get("Net SIP") if "Net SIP" in doc else doc.get("net_sip"),
                    # Add the newly calculated values to the metrics dictionary
                    "net_sip_core": doc.get("net_sip_core"),
                    "swp_adj_registration": doc.get("swp_adj_registration"),
                    "swp_adj_cancellation": doc.get("swp_adj_cancellation"),
                    "swp_net_effect": doc.get("swp_net_effect"),
                    "net_sip_val": doc.get("net_sip_val"),
                    "sip_scheme_bonus_val": doc.get("sip_scheme_bonus_val"),
                    "gross_sip_raw": doc.get("gross_sip_raw"),
                    "cancel_sip_raw": doc.get("cancel_sip_raw"),
                    "swp_adj_registration_raw": doc.get("swp_adj_registration_raw"),
                    "swp_adj_cancellation_raw": doc.get("swp_adj_cancellation_raw"),
                    "swp_net_effect_raw": doc.get("swp_net_effect_raw"),
                    "net_sip_raw": doc.get("net_sip_raw"),
                    "scheme_bonus": doc.get("scheme_bonus"),
                    # Existing metrics continue below
                    "net_sip_core": (
                        doc.get("Net SIP (Core)")
                        if "Net SIP (Core)" in doc
                        else doc.get("net_sip_core")
                    ),
                    "swp_net_effect": (
                        doc.get("SWP Net Effect")
                        if "SWP Net Effect" in doc
                        else doc.get("swp_net_effect")
                    ),
                    "swp_adj_registration": doc.get("swp_adj_registration"),
                    "swp_adj_cancellation": doc.get("swp_adj_cancellation"),
                    "gross_sip": (
                        doc.get("Gross SIP") if "Gross SIP" in doc else doc.get("gross_sip")
                    ),
                    "cancel_sip": (
                        doc.get("Cancel SIP") if "Cancel SIP" in doc else doc.get("cancel_sip")
                    ),
                    "sip_to_aum": (
                        doc.get("SIP to AUM %") if "SIP to AUM %" in doc else doc.get("sip_to_aum")
                    ),
                    "avg_sip": doc.get("Avg SIP") if "Avg SIP" in doc else doc.get("avg_sip"),
                    "net_lumpsum": (
                        doc.get("Lumpsum Net") if "Lumpsum Net" in doc else doc.get("net_lumpsum")
                    ),
                    "sip_rate_bps": (
                        doc.get("SIP Rate (bps)")
                        if "SIP Rate (bps)" in doc
                        else doc.get("sip_rate_bps")
                    ),
                    "sip_effective_rate": doc.get("sip_effective_rate"),
                    "sip_rate_components_bps": doc.get("sip_rate_components_bps"),
                    "sip_rate_capped": doc.get("sip_rate_capped"),
                    "sip_rate_cap_reason": doc.get("sip_rate_cap_reason"),
                    "total_points": (
                        doc.get("Total Points")
                        if "Total Points" in doc
                        else doc.get("total_points")
                    ),
                    "tier": doc.get("Tier"),
                    "vp_points_credit": doc.get("vp_points_credit"),
                },
                "config_hash": _SIP_LAST_CFG_HASH,
                "schema_version": SCHEMA_VERSION_SIP,
                "module": "SIP_Scorer",
                "created_at": _now_utc(),
            }
        )

    upserts_count = 0
    if ops:
        res = coll_lb.bulk_write(ops, ordered=False)
        upserts_count = res.upserted_count + res.modified_count
    # Insert audit docs (never break main write)
    if audit_docs:
        try:
            coll_audit.insert_many(audit_docs, ordered=False)
        except Exception as e:
            logging.warning("[SIP Audit] Failed to insert some audit docs: %s", e)
    return (upserts_count, len(df_rollups))


# --- Azure Timer Trigger entrypoint + local CLI runner ---


# --- Helper: Resolve scoring window from range_mode/fy_mode ---
def _resolve_window_from_mode(
    range_mode: Optional[str] = None,
    fy_mode: Optional[str] = None,
) -> tuple[datetime, datetime]:
    """
    Resolve the scoring window based on a simple range mode:
      - 'month' or unknown: previous fully completed calendar month.
      - 'last5' or 'last10': dynamic recent-window mode (5- or 10-day lookback):
        * Compute lastN_date = today - N days (UTC).
        * If lastN_date's month != current month, cover previous full month + current month MTD+1.
        * If lastN_date's month == current month, cover current month MTD+1 only.
      - 'fy': financial-year-to-date window based on fy_mode (currently FY_APR).

    This is intentionally coarse-grained: the DataFrame still groups by 'month'
    internally, so multi-month windows simply emit multiple monthly buckets.
    """
    mode = (range_mode or SIP_RANGE_MODE_DEFAULT or "month").strip().lower()
    fy = (fy_mode or SIP_FY_MODE_DEFAULT or "FY_APR").strip().upper()

    # Default behaviour: previous completed month
    if mode not in {"last5", "last10", "fy"}:
        return _default_month_window()

    now = datetime.now(timezone.utc)

    if mode == "fy":
        # Currently only FY starting April is supported; fy_mode is kept for future use.
        start, end = _fy_bounds(now.date())
        # _fy_bounds returns naive datetimes; make them UTC-aware for consistency.
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return start, end

    # 'last5' / 'last10' → dynamic "recent N days aware" behaviour
    # We look at the date N days ago (N = 5 or 10):
    #   - If it falls in the previous calendar month, we cover:
    #       previous full month + current month MTD (up to tomorrow, exclusive).
    #   - If it falls in the current month, we cover:
    #       current month MTD (up to tomorrow, exclusive).
    if mode in {"last5", "last10"}:
        window_days = 5 if mode == "last5" else 10
        today_utc = now.astimezone(timezone.utc).date()
        lastN_date = (now.astimezone(timezone.utc) - timedelta(days=window_days)).date()

        # Compute first day of current month (UTC) and "tomorrow" (exclusive end)
        first_curr_month = datetime(
            today_utc.year,
            today_utc.month,
            1,
            tzinfo=timezone.utc,
        )
        tomorrow = datetime.combine(
            today_utc + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )

        # If the "N days ago" date is in a different month/year, include previous full month
        if lastN_date.year != today_utc.year or lastN_date.month != today_utc.month:
            prev_year = today_utc.year
            prev_month = today_utc.month - 1
            if prev_month <= 0:
                prev_month = 12
                prev_year -= 1
            start = datetime(prev_year, prev_month, 1, tzinfo=timezone.utc)
            end = tomorrow
            return start, end

        # Otherwise, we are comfortably in the same month; only run current MTD+1
        start = first_curr_month
        end = tomorrow
        return start, end

    # Defensive fallback: should be unreachable, but keeps type checkers happy
    return _default_month_window()


def _default_month_window() -> tuple[datetime, datetime]:
    """
    Return (start, end) for the last fully completed calendar month in UTC.
    Example: if today is 2025-11-13, this returns:
      start = 2025-10-01T00:00:00Z
      end   = 2025-11-01T00:00:00Z
    """
    now = datetime.now(timezone.utc)
    first_this_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    last_prev_month = first_this_month - timedelta(days=1)
    first_prev_month = datetime(
        last_prev_month.year,
        last_prev_month.month,
        1,
        tzinfo=timezone.utc,
    )
    return first_prev_month, first_this_month


def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Functions timer-trigger entrypoint.

    Runs SIP scorer for the **previous completed month** and logs a compact summary.
    """
    try:
        # Resolve window based on Mongo-driven runtime options (mirrors Lumpsum policy).
        client = _get_mongo_client(None)
        try:
            _load_runtime_config(client)
        except Exception as e:
            logging.warning("[SIP Timer] Failed to load runtime config from Mongo: %s", e)

        start_date, end_date = _resolve_window_from_mode()
        log_kv(
            logging.INFO,
            "[SIP Timer] Starting SIP run",
            trigger_status=getattr(mytimer, "past_due", None),
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            range_mode=SIP_RANGE_MODE_DEFAULT,
            fy_mode=SIP_FY_MODE_DEFAULT,
        )
        normalized_rows, upserts, buckets = run_pipeline(
            start_date=start_date,
            end_date=end_date,
            emails_list=None,
            mongo_uri=None,
        )
        log_kv(
            logging.INFO,
            "[SIP Timer] Completed run",
            normalized_rows=normalized_rows,
            upserts=upserts,
            buckets=buckets,
        )
    except Exception:
        logging.exception("[SIP Timer] Run failed")
        # Re-raise so Azure marks the invocation as failed
        raise


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SIP_Scorer pipeline locally.")
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (inclusive) in YYYY-MM-DD. Defaults to start of previous full month.",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (exclusive) in YYYY-MM-DD. Defaults to start of current month.",
    )
    parser.add_argument(
        "--emails",
        type=str,
        nargs="*",
        help="Optional list of registrant emails to filter on.",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        help="Optional MongoDB connection string override. "
        "If omitted, uses Key Vault / env via get_secret().",
    )
    return parser.parse_args()


def _cli_main() -> None:
    args = _parse_cli_args()

    # Resolve date window
    if args.start or args.end:
        if not args.start or not args.end:
            raise SystemExit("Both --start and --end must be provided together, or neither.")
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError as e:
            raise SystemExit(f"Invalid date format: {e}. Use YYYY-MM-DD.") from e
        range_mode = "custom"
        fy_mode = SIP_FY_MODE_DEFAULT
    else:
        # Mongo-driven default: month / last5 / fy, aligned with Lumpsum scorer.
        client = _get_mongo_client(args.mongo_uri)
        try:
            _load_runtime_config(client)
        except Exception as e:
            logging.warning("[SIP CLI] Failed to load runtime config from Mongo: %s", e)
        start_date, end_date = _resolve_window_from_mode()
        range_mode = SIP_RANGE_MODE_DEFAULT
        fy_mode = SIP_FY_MODE_DEFAULT

    emails_list = args.emails if args.emails else None

    log_kv(
        logging.INFO,
        "[SIP CLI] Starting run",
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        emails=emails_list,
        range_mode=range_mode,
        fy_mode=fy_mode,
    )
    try:
        normalized_rows, upserts, buckets = run_pipeline(
            start_date=start_date,
            end_date=end_date,
            emails_list=emails_list,
            mongo_uri=args.mongo_uri,
        )
    except Exception:
        logging.exception("[SIP CLI] Run failed")
        raise

    log_kv(
        logging.INFO,
        "[SIP CLI] Completed run",
        normalized_rows=normalized_rows,
        upserts=upserts,
        buckets=buckets,
    )
    print(
        f"SIP_Scorer run complete | normalized_rows={normalized_rows}, "
        f"upserts={upserts}, buckets={buckets}"
    )


if __name__ == "__main__":
    _cli_main()
