import os
import sys
import logging
import ast
import datetime as dt
from datetime import datetime, timezone
from typing import Any, Dict, cast

import requests
import pandas as pd
import pymongo
import azure.functions as func
from pymongo import UpdateOne
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure, BulkWriteError

# === Insurance Bonuses (Quarterly & Annual) – now computed inside scorer ===
# • Basis: ONLY fresh-to-company premium (pre-GST); renewal premium does NOT count.
# • Units: 1 Rupee fresh premium = 1 point for bonus computations.
# • This scorer now computes bonus *points* and surfaces them on the monthly leaderboard rows:
#     - bonus_quarterly_points: credited only in quarter-end months (Jun, Sep, Dec, Mar)
#     - bonus_annual_points: credited only in March for the financial year (Apr–Mar)
# • We still maintain per-policy fields for traceability:
#     - fresh_premium_eligible (float), 0.0 for non-fresh
#     - period_month ('YYYY-MM')
# • Rupee_Incentives aggregator may convert these points to rupees (1:1) and apply leader adjustments/audits.

# --- Robust import of pli_common.skiplist ---
try:
    from pli_common.skiplist import should_skip, SKIP_RM_NAMES  # type: ignore
except Exception:
    import pathlib, importlib.util

    here = pathlib.Path(__file__).resolve()
    ROOT = here.parent.parent  # project root (contains pli_common and InsuranceScore)

    # 1) Try by adding project root to sys.path
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from pli_common.skiplist import should_skip, SKIP_RM_NAMES  # type: ignore
    except Exception:
        # 2) Try direct file-load if package exists on disk
        mod_path = ROOT / "pli_common" / "skiplist.py"
        if mod_path.exists():
            spec = importlib.util.spec_from_file_location("pli_common.skiplist", str(mod_path))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore[attr-defined]
                should_skip = getattr(module, "should_skip")
                SKIP_RM_NAMES = getattr(module, "SKIP_RM_NAMES")
            else:
                raise ImportError(f"Could not load module spec for {mod_path}")
        else:
            # 3) Ultimate fallback: define local skip list and helper so the run never breaks
            logging.warning(
                "pli_common.skiplist not found at %s; using in-file fallback skip list.",
                mod_path,
            )
            SKIP_RM_ALIASES: set[str] = {
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
                "neha sharma",
                "shrasti gupta",
            }
            SKIP_RM_NAMES: set[str] = set(SKIP_RM_ALIASES)

            _TOKEN_RULES = [
                {"vilakshan", "bhutani"},
                {"pramod", "bhutani"},
                {"manisha", "tendulkar"},
            ]

            def should_skip(name: str) -> bool:  # type: ignore
                s = " ".join(str(name or "").lower().split())
                if s in SKIP_RM_NAMES:
                    return True
                tokens = set(s.split())
                return any(rule.issubset(tokens) for rule in _TOKEN_RULES)


# --- Skip by Zoho user profile ---
# Default: skip Operations and Administrator profiles; do NOT skip Mutual Funds (we adjust tiers for it instead)
SKIP_ZOHO_PROFILES = set(
    s.strip().lower()
    for s in (os.getenv("PLI_SKIP_ZOHO_PROFILES", "Operations,Administrator").split(","))
    if s.strip()
)


# Configure logging (works locally and in Azure Functions)
IS_AZURE_FUNC = bool(os.getenv("FUNCTIONS_WORKER_RUNTIME"))
_root_logger = logging.getLogger()

# In Azure Functions, let the worker manage handlers to avoid duplicate console lines.
# Locally, attach one stdout handler (plus optional App Insights).
if not IS_AZURE_FUNC:
    _handlers = [logging.StreamHandler(sys.stdout)]

    # Optional: forward logs to Application Insights if connection string is present
    try:
        from opencensus.ext.azure.log_exporter import AzureLogHandler  # type: ignore

        _ai_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING") or os.getenv(
            "APPINSIGHTS_INSTRUMENTATIONKEY"
        )
        if _ai_conn:
            # Accept either full connection string or bare instrumentation key
            if "InstrumentationKey=" in _ai_conn or ";" in _ai_conn:
                _handlers.append(AzureLogHandler(connection_string=_ai_conn))
            else:
                _handlers.append(
                    AzureLogHandler(connection_string=f"InstrumentationKey={_ai_conn}")
                )
    except Exception:
        pass

    # Only set basicConfig in local/dev (root has no handlers)
    if not _root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    # Attach built handlers (stdout + optional App Insights)
    try:
        for _h in _handlers:
            # avoid duplicate attachments
            if _h not in _root_logger.handlers:
                _root_logger.addHandler(_h)
    except Exception:
        pass
else:
    # Azure Functions environment: do not add any extra handlers
    pass

LOG_LEVEL = (os.getenv("PLI_LOG_LEVEL", "INFO") or "INFO").upper()
try:
    _root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logging.info(f"[Log] Effective level -> {LOG_LEVEL}")
except Exception:
    _root_logger.setLevel(logging.INFO)

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:  # ImportError or any runtime import issue
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE = {}
# Cache for Zoho profiles used by monthly tier adjustments (Mutual Funds rules)
_PROFILES_BY_ID: Dict[str, str] = {}


# --- Inactive-gate helpers (Insurance scorer scope) --------------------------
def _last6_month_labels(inactive_since: datetime):
    """
    Return list of YYYY-MM labels covering the last 6 months (inclusive)
    counting BACK from the inactive_since month.
    """
    if not inactive_since:
        return []
    if inactive_since.tzinfo is None:
        inactive_since = inactive_since.replace(tzinfo=timezone.utc)
    base = datetime(inactive_since.year, inactive_since.month, 1, tzinfo=timezone.utc)
    out: list[str] = []
    for i in range(5, -1, -1):
        y = base.year + (base.month - 1 - i) // 12
        m = (base.month - 1 - i) % 12 + 1
        out.append(f"{y:04d}-{m:02d}")
    return out


# def _apply_inactive_block(db, employee_id: str, period_month: str, payload: dict):
#     """
#     Mutates `payload` to gate payout eligibility if the employee is inactive
#     AND the given `period_month` ('YYYY-MM') falls on or after the month of
#     Zoho_Users.inactive_since.

#     Behaviour:
#       • If Zoho_Users has no record → do nothing (payout_eligible stays True by default).
#       • If status != 'inactive' or inactive_since is missing → payout_eligible=True.
#       • If status == 'inactive' AND period_month >= inactive_since month-label → payout_eligible=False.
#     """
#     try:
#         if db is None:
#             payload.setdefault("payout_eligible", True)
#             return _sanitize_doc(payload)

#         u = db.Zoho_Users.find_one({"id": employee_id}, {"status": 1, "inactive_since": 1})
#         if not u:
#             payload.setdefault("payout_eligible", True)
#             return _sanitize_doc(payload)

#         status = (u.get("status") or "").lower()
#         inactive_since = u.get("inactive_since")

#         # Default: if we cannot compute anything sensible, stay eligible
#         payload.setdefault("payout_eligible", True)

#         if status != "inactive" or not inactive_since:
#             return _sanitize_doc(payload)

#         # Build 'YYYY-MM' labels for comparison
#         try:
#             per_label = str(period_month) if period_month is not None else ""
#             if not per_label or "-" not in per_label:
#                 # If period_month is malformed, treat as block to be safe
#                 should_block = True
#             else:
#                 y = inactive_since.year
#                 m = inactive_since.month
#                 inactive_label = f"{y:04d}-{m:02d}"
#                 # Lexicographic compare is safe for 'YYYY-MM'
#                 should_block = per_label >= inactive_label
#         except Exception:
#             # On any parse error, fail safe by blocking
#             should_block = True

#         if should_block:
#             payload["payout_eligible"] = False
#             payload["payout_blocked_by_inactive"] = True
#             payload["payout_block_reason"] = "inactive_from_exit_month_onwards"
#             payload["ins_payout_blocked_by_inactive"] = True
#             payload.setdefault("audit", {})["inactive_block"] = {
#                 "inactive_since": inactive_since,
#                 "period_month": period_month,
#                 "applied_at": datetime.now(timezone.utc),
#             }

#         return _sanitize_doc(payload)
#     except Exception as e:
#         # Never let gating crash the scorer
#         logging.exception(
#             "Inactive gate (insurance) failed for %s %s: %s", employee_id, period_month, e
#         )
#         return _sanitize_doc(payload)


def _apply_inactive_block(zoho_users_coll, employee_id: str, period_month: str, payload: dict):
    """
    Mutates `payload` to gate payout eligibility if the employee is inactive
    AND the given `period_month` ('YYYY-MM') falls on or after the month of
    Zoho_Users.inactive_since.

    `zoho_users_coll` is expected to be the Zoho_Users collection
    (connect_to_mongo("Zoho_Users")), but the function fails-safe when it's None or invalid.
    """
    try:
        # No collection → do nothing, keep payouts eligible
        if zoho_users_coll is None or callable(zoho_users_coll):
            payload.setdefault("payout_eligible", True)
            return _sanitize_doc(payload)

        u = zoho_users_coll.find_one(
            {"id": employee_id},
            {"status": 1, "inactive_since": 1},
        )
        if not u:
            payload.setdefault("payout_eligible", True)
            return _sanitize_doc(payload)

        status = (u.get("status") or "").lower()
        inactive_since = u.get("inactive_since")

        # Default: if we cannot compute anything sensible, stay eligible
        payload.setdefault("payout_eligible", True)

        if status != "inactive" or not inactive_since:
            return _sanitize_doc(payload)

        try:
            per_label = str(period_month) if period_month is not None else ""
            if not per_label or "-" not in per_label:
                # If period_month is malformed, treat as block to be safe
                should_block = True
            else:
                y = inactive_since.year
                m = inactive_since.month
                inactive_label = f"{y:04d}-{m:02d}"
                # Lexicographic compare is safe for 'YYYY-MM'
                should_block = per_label >= inactive_label
        except Exception:
            # On any parse error, fail safe by blocking
            should_block = True

        if should_block:
            payload["payout_eligible"] = False
            payload["payout_blocked_by_inactive"] = True
            payload["payout_block_reason"] = "inactive_from_exit_month_onwards"
            payload["ins_payout_blocked_by_inactive"] = True
            payload.setdefault("audit", {})["inactive_block"] = {
                "inactive_since": inactive_since,
                "period_month": period_month,
                "applied_at": datetime.now(timezone.utc),
            }

        return _sanitize_doc(payload)
    except Exception as e:
        logging.exception(
            "Inactive gate (insurance) failed for %s %s: %s",
            employee_id,
            period_month,
            e,
        )
        return _sanitize_doc(payload)


# -----------------------------------------------------------------------------
# If you resolve employees here, stop restricting to only active users
# and include status/inactive_since for the gate:
# (example before)
# user = db.Zoho_Users.find_one({"id": employee_id, "status": "active"}, {"id":1, "name":1})
# (example after)
# user = db.Zoho_Users.find_one({"id": employee_id}, {"id":1, "name":1, "status":1, "inactive_since":1})
# Similarly for name-based resolve:
# user = db.Zoho_Users.find_one({"name": emp_name}, {"id":1, "name":1, "status":1, "inactive_since":1})


# --- Helper: MF profile detection from in-memory map ---
def _is_mf_profile_from_map(emp_id: str | None, profiles_by_id: dict[str, str] | None) -> bool:
    """Return True if the employee's profile maps to an MF-like profile.
    We keep detection permissive (contains 'mf' or 'mutual'+'fund') but prefer exact label 'mutual funds' when present.
    """
    if not emp_id or not profiles_by_id:
        return False
    prof = str(profiles_by_id.get(str(emp_id)) or "").strip().lower()
    return (prof == "mutual funds") or ("mf" in prof) or ("mutual" in prof and "fund" in prof)


# Global holders populated by load_secrets()
ZOHO_CLIENT_ID = None
ZOHO_CLIENT_SECRET = None
ZOHO_REFRESH_TOKEN = None
CONNECTIONSTRING = None

KV_SECRET_ZOHO_CLIENT_ID = os.getenv("KV_SECRET_ZOHO_CLIENT_ID", "Zoho-client-id-vilakshan-account")
KV_SECRET_ZOHO_CLIENT_SECRET = os.getenv(
    "KV_SECRET_ZOHO_CLIENT_SECRET", "Zoho-client-secret-vilakshan-account"
)
KV_SECRET_ZOHO_REFRESH_TOKEN = os.getenv(
    "KV_SECRET_ZOHO_REFRESH_TOKEN", "Zoho-refresh-token-vilakshan-account"
)
KV_SECRET_MONGO_CONNSTRING = os.getenv("KV_SECRET_MONGO_CONNSTRING", "MongoDb-Connection-String")


# --- Mongo target database (override with env MONGO_DB_NAME) ---
DB_NAME = "PLI_Leaderboard"

collection_associate = "Associate_Payout"
collection_DirClient = "DirClient_Payout"

# --- Schema + runtime config (golden architecture, aligned with SIP/Lumpsum) ---
SCHEMA_COLL_NAME = "Schemas"
SCHEMA_DOC_ID = "Insurance_Schema"

INS_CONFIG_COLL_NAME = "config"
INS_CONFIG_KEY = "Leaderboard_Insurance"
INS_SCHEMA_VERSION = "2025-11-15.r1"

INS_CONFIG_DEFAULT = {
    "module": "Insurance_scorer",
    "schema_version": INS_SCHEMA_VERSION,
    # Indian FY settings – same defaults as SIP/Lumpsum
    "range_mode": "fy",  # use financial year ranges by default
    "fy_mode": "FY_APR",  # FY = Apr–Mar
    # Periodic bonus controls (kept for future use; safe no-ops today)
    "periodic_bonus_enable": False,
    "periodic_bonus_apply": True,
    # Audit/logging verbosity for downstream aggregators
    "audit_mode": "compact",
    # Default Premium->Points conversion (hardcoded logic migrated to config)
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
    # Company-specific overrides (Whitelist/Blacklist logic)
    "company_rules": [],
}


# --- Filtering Helper Functions ---

def _infer_company_column(df: pd.DataFrame) -> str | None:
    """Detect the Company Name column."""
    candidates = [
        "Company Name", "Company", "Insurer", "Provider",
        "COMPANY", "INSURER"
    ]
    cols = {c.strip() for c in df.columns}
    for c in candidates:
        if c in cols:
            return c
        # Case insensitive check
        c_lower = c.lower()
        for df_c in cols:
            if df_c.lower() == c_lower:
                return df_c
    return None

def _infer_date_column(df: pd.DataFrame) -> str | None:
    """Detect the date column for filtering."""
    candidates = [
        "TRXN_DATE", "Transaction Date", "Date", "DATE",
        "TrxnDate", "trxn_date", "date", "Login Date", "Issue Date"
    ]
    cols = {c.strip() for c in df.columns}
    for c in candidates:
        if c in cols:
            return c
    return None


def _apply_company_weights(df: pd.DataFrame, label: str, config: dict) -> pd.DataFrame:
    """
    Apply company-based weightage multipliers to the 'Amount' column.
    Modifies the dataframe in-place (or returns modified copy).

    Rules are fetched from config['company_rules'].
    Each rule: { "keyword": "...", "match_type": "contains"|"exact", "weight_pct": 120 }
    """
    if df is None or df.empty:
        return df

    company_rules = config.get("company_rules", [])
    if not company_rules:
        return df

    company_col = _infer_company_column(df)
    if not company_col:
        logging.debug(f"[Window] {label}: No company column found; skipping company weights.")
        return df

    logging.info(f"[Window] {label}: Applying {len(company_rules)} company rules using col='{company_col}'")

    # Ensure Company Name is string
    df[company_col] = df[company_col].astype(str).fillna("")
    company_series = df[company_col].str.strip().str.lower()

    # Store original amount for auditing if needed
    amount_col = None
    if "Amount" in df.columns:
        amount_col = "Amount"
    elif "this_year_premium" in df.columns:
        amount_col = "this_year_premium"

    if not amount_col:
         return df

    if "Amount_Orig" not in df.columns:
        df["Amount_Orig"] = df[amount_col]

    # Date handling
    date_col = _infer_date_column(df)
    date_series = None
    if date_col:
        date_series = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')

    # Track which rows have been modified to enforce "First matching rule wins"
    processed_mask = pd.Series(False, index=df.index)

    for rule in company_rules:
        # 1. Parse Rule Metadata
        kw = str(rule.get("keyword", "")).strip().lower()
        if not kw: continue

        wt_pct = float(rule.get("weight_pct", 100))
        match_type = str(rule.get("match_type", "exact")).strip().lower()

        # Date Range (optional)
        start_date_str = rule.get("start_date")
        end_date_str = rule.get("end_date")

        # 2. Build Company Match Mask
        if match_type == "exact":
            rule_match = (company_series == kw)
        else:
            rule_match = company_series.str.contains(kw, regex=False)

        # 3. Apply Date Filter (if configured)
        if date_series is not None and (start_date_str or end_date_str):
            date_mask = pd.Series(True, index=df.index)
            if start_date_str:
                try:
                    sd = pd.to_datetime(start_date_str)
                    date_mask &= (date_series >= sd)
                except: pass
            if end_date_str:
                try:
                    ed = pd.to_datetime(end_date_str)
                    date_mask &= (date_series <= ed)
                except: pass
            rule_match &= date_mask

        # 4. Filter out already processed rows
        active_match = rule_match & (~processed_mask)

        if not active_match.any():
            continue

        # 5. Apply Weightage
        multiplier = wt_pct / 100.0

        # Apply to Amount
        df.loc[active_match, amount_col] *= multiplier

        # Log application
        df.loc[active_match, "weight_applied_pct"] = wt_pct
        df.loc[active_match, "weight_rule"] = kw

        # Mark as processed
        processed_mask |= active_match

    return df


def ensure_schema_registry():
    """
    Ensure a minimal schema registry document exists for Insurance scorer.
    Mirrors the 'Schemas/…' pattern used by SIP and Lumpsum scorers.
    """
    try:
        coll = connect_to_mongo(SCHEMA_COLL_NAME)
        if coll is None:
            logging.warning("[Schema] Schemas collection unavailable; registry bootstrap skipped.")
            return None

        now = dt.datetime.utcnow()
        base = {
            "_id": SCHEMA_DOC_ID,
            "metric": "Insurance",
            "module": "Insurance_scorer",
            "schema_version": INS_SCHEMA_VERSION,
        }

        existing = coll.find_one({"_id": SCHEMA_DOC_ID})
        if existing:
            # Merge to keep any extra fields that may have been added manually
            merged = {**existing, **base}
            merged["updated_at"] = now
            coll.replace_one(
                {"_id": SCHEMA_DOC_ID},
                cast(Dict[str, Any], _sanitize_doc(merged)),
                upsert=True,
            )
        else:
            doc = {**base, "created_at": now, "updated_at": now}
            coll.insert_one(_sanitize_doc(doc))

        logging.info(
            "[Schema] Bootstrapped/ensured schema registry: %s/%s",
            SCHEMA_COLL_NAME,
            SCHEMA_DOC_ID,
        )
        return coll
    except Exception as e:
        logging.warning("[Schema] Registry ensure skipped due to error: %s", e)
        return None


def load_insurance_runtime_config() -> dict:
    """
    Load the Insurance scorer runtime config from Mongo, defaulting to
    INS_CONFIG_DEFAULT if not present or if Mongo is unavailable.

    Shape is intentionally aligned with SIP/Lumpsum:
      • range_mode / fy_mode
      • periodic_bonus_enable / periodic_bonus_apply
      • audit_mode
    """
    cfg = dict(INS_CONFIG_DEFAULT)
    try:
        coll = connect_to_mongo(INS_CONFIG_COLL_NAME)
        if coll is None:
            logging.warning(
                "[Config] Config collection '%s' unavailable; using in-code defaults.",
                INS_CONFIG_COLL_NAME,
            )
            return cfg

        now = dt.datetime.utcnow()
        doc = coll.find_one({"_id": INS_CONFIG_KEY})

        if not doc:
            # First‑time bootstrap: create a config row with defaults
            to_store = {
                "_id": INS_CONFIG_KEY,
                **INS_CONFIG_DEFAULT,
                "created_at": now,
                "updated_at": now,
            }
            coll.insert_one(_sanitize_doc(to_store))
            logging.info(
                "[Config] Bootstrapped default runtime config (exists or created): %s/%s",
                INS_CONFIG_COLL_NAME,
                INS_CONFIG_KEY,
            )
            cfg = to_store
        else:
            # Merge stored config over defaults to remain backward compatible
            merged = {**INS_CONFIG_DEFAULT, **doc}
            merged["_id"] = INS_CONFIG_KEY
            merged.setdefault("schema_version", INS_SCHEMA_VERSION)
            merged["updated_at"] = now
            coll.replace_one(
                {"_id": INS_CONFIG_KEY},
                cast(Dict[str, Any], _sanitize_doc(merged)),
                upsert=True,
            )
            logging.info(
                "[Config] Loaded runtime config from Mongo: %s/%s",
                INS_CONFIG_COLL_NAME,
                INS_CONFIG_KEY,
            )
            cfg = merged

        # Load company whitelist/blacklist rules
        weights = doc.get("weights") or {}
        cfg["company_rules"] = weights.get("company_rules", [])

        try:
            logging.info(
                "[Config] Options: range_mode=%s fy_mode=%s periodic_bonus_enable=%s periodic_bonus_apply=%s audit_mode=%s",
                cfg.get("range_mode"),
                cfg.get("fy_mode"),
                cfg.get("periodic_bonus_enable"),
                cfg.get("periodic_bonus_apply"),
                cfg.get("audit_mode"),
            )
        except Exception:
            # Logging should never break execution
            pass

        return cfg
    except Exception as e:
        logging.warning(
            "[Config] Runtime config load failed; falling back to in-code defaults: %s",
            e,
        )
        return dict(INS_CONFIG_DEFAULT)


# --- Key Vault secret loader ---
_kv_loaded = False

KEY_VAULT_URL = "https://milestonetsl1.vault.azure.net/"


def get_secret(name: str, default: str | None = None) -> str | None:
    """Return secret value from environment if present; otherwise fetch from Azure Key Vault.
    Falls back to `default` if neither source is available. Values are cached per-process.
    Supports KV names that disallow underscores by trying hyphenated variants.
    """
    # 1) Env precedence (easy local override for dev/testing)
    if name in os.environ and os.environ[name]:
        return os.environ[name]

    # Back-compat alias: if code asks for the KV key but env only provides legacy name
    if name == "MongoDb-Connection-String":
        legacy = os.getenv("MONGO_CONN")
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
                    _SECRET_CACHE[name] = secret.value
                    return secret.value
                except Exception:
                    continue
            # If none of the lookup names worked, warn once and fall back
            logging.warning(
                "Secrets: '%s' not found in Key Vault (tried: %s). Using default if provided.",
                name,
                ", ".join(lookup_names),
            )
            return default
        except Exception as e:
            # Don't crash the pipeline if KV is unreachable; rely on default
            logging.warning("Secrets: failed to fetch '%s' from Key Vault: %s", name, e)
            return default

    # 4) Fallback
    return default


def connect_to_mongo(collection_name, db_name: str | None = None):
    # Special safety: disable live Mongo writes/reads for the legacy "Leaderboard" collection
    # from inside Insurance_scorer unless explicitly re‑enabled via env.
    # This prevents stale/incorrect monthly aggregate data from being written or read.
    if str(collection_name).lower() == "leaderboard" and str(
        os.getenv("PLI_DISABLE_LEADERBOARD", "1")
    ).lower() in ("1", "true", "yes"):

        class _NullCollection:
            """Minimal no-op collection stub for disabled Leaderboard writes."""

            def __getattr__(self, name):
                def _noop(*args, **kwargs):
                    logging.info(
                        "[Leaderboard-disabled] %s called on NullCollection; skipping.",
                        name,
                    )
                    return None

                return _noop

        logging.info(
            "[Leaderboard-disabled] connect_to_mongo(%r) returning NullCollection stub "
            "because PLI_DISABLE_LEADERBOARD is enabled.",
            collection_name,
        )
        return _NullCollection()
    mongo_uri = get_secret("MongoDb-Connection-String")
    if not mongo_uri:
        logging.error("MongoDB CONNECTIONSTRING not loaded from Key Vault.")
        return None
    try:
        # Ensure the MongoDB connection string is securely managed and correctly formatted
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)

        # Attempt to retrieve the server information to verify the connection
        client.server_info()  # This will raise an exception if the connection fails

        target_db = db_name or DB_NAME
        db = client[target_db]
        logging.info(
            f"Successfully connected to MongoDB database: {target_db}, Collection: {collection_name}"
        )
        return db[collection_name]

    except ServerSelectionTimeoutError as sste:
        logging.error(f"Connection timed out: {sste}", exc_info=True)
        return None
    except ConnectionFailure as cf:
        logging.error(f"MongoDB connection failed: {cf}", exc_info=True)
        return None
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while connecting to MongoDB: {e}",
            exc_info=True,
        )
        return None


def ensure_monthly_leaderboard_index(monthly_col):
    """
    Ensure the Leaderboard collection has the correct unique index on (employee_id, period_month)
    and drop any legacy indexes that involve 'month'.
    """
    if monthly_col is None:
        return

    # Handle both real collections and the NullCollection stub used when
    # PLI_DISABLE_LEADERBOARD is enabled. The stub's list_indexes() returns
    # None, so we must guard against iterating over a None value.
    try:
        idx_cur = monthly_col.list_indexes()
        if not idx_cur:
            # NullCollection or driver returned nothing; nothing to inspect/drop.
            return
        idx_info = list(idx_cur)
        for ix in idx_info:
            try:
                key_data = ix.get("key")
                if isinstance(key_data, dict):
                    keys = list(key_data.keys())
                else:
                    keys = [k for k, _ in key_data] if key_data else []
            except Exception:
                keys = []
            if "month" in keys and "period_month" not in keys:
                try:
                    monthly_col.drop_index(ix["name"])
                    logging.info(
                        "Dropped legacy Leaderboard index on 'month': %s",
                        ix.get("name"),
                    )
                except Exception as _e_drop:
                    logging.warning(
                        "Could not drop legacy index %s: %s",
                        ix.get("name"),
                        _e_drop,
                    )
    except Exception as e:
        logging.warning("Could not inspect/drop legacy Leaderboard indexes: %s", e)

    # Always try to ensure the canonical upsert index; this is a no-op when
    # using the NullCollection stub as its create_index() simply logs and returns.
    try:
        monthly_col.create_index(
            [("employee_id", pymongo.ASCENDING), ("period_month", pymongo.ASCENDING)],
            unique=True,
            name="upsert_key",
        )
    except Exception as e:
        logging.warning("Leaderboard index (employee_id, period_month) create skipped: %s", e)


def reset_monthly_leaderboard(monthly_col):
    """
    Hard reset the Leaderboard collection and recreate the canonical index.
    Controlled via env WIPE_MONTHLY_LEADERBOARD=1/true/yes.
    """
    if monthly_col is None:
        return monthly_col
    try:
        monthly_col.drop()
        logging.info("[MonthlyLB] Dropped Leaderboard collection per WIPE_MONTHLY_LEADERBOARD.")
    except Exception as e:
        logging.warning("[MonthlyLB] Drop failed (continuing): %s", e)
    new_col = connect_to_mongo("Leaderboard")
    ensure_monthly_leaderboard_index(new_col)
    return new_col


def refresh_mongo_collection(collection, data_df):
    try:
        if not isinstance(data_df, pd.DataFrame):
            raise ValueError("data_df must be a pandas DataFrame")

        collection.delete_many({})
        records = data_df.to_dict("records")

        if records:
            collection.insert_many(records)
            logging.info(f"Successfully refreshed the collection with {len(records)} records.")
        else:
            logging.warning("No records to insert; collection was cleared.")

    except ValueError as ve:
        logging.error(f"Data validation error: {ve}")
    except BulkWriteError as bwe:
        logging.error(f"Error writing data to MongoDB: {bwe.details}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def get_access_token(retries: int = 3) -> str:
    """Obtain a Zoho **access token** using refresh token from Key Vault.
    Uses accounts.zoho.com and does not attempt any grant-code flow.
    """
    import time

    global HEADERS
    logging.info(
        "get_access_token(): using refresh-token flow via https://accounts.zoho.com/oauth/v2/token"
    )

    client_id = get_secret(KV_SECRET_ZOHO_CLIENT_ID)
    client_secret = get_secret(KV_SECRET_ZOHO_CLIENT_SECRET)
    refresh_token = get_secret(KV_SECRET_ZOHO_REFRESH_TOKEN)

    if not all([client_id, client_secret, refresh_token]):
        raise Exception(
            "Zoho OAuth secrets missing. Ensure client id/secret and refresh token exist in Key Vault."
        )

    token_endpoint = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(token_endpoint, data=payload, timeout=20)
        except Exception as e:
            last_err = {"exception": str(e)}
            logging.warning(
                "Access token request error; attempt %d/%d will retry: %s",
                attempt,
                retries,
                last_err,
            )
            time.sleep(min(2**attempt, 8))
            continue

        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:300]}

        if resp.status_code == 200 and isinstance(body, dict) and body.get("access_token"):
            token = body["access_token"]
            HEADERS = {
                "Authorization": f"Zoho-oauthtoken {token}",
                "Content-Type": "application/json",
            }
            logging.info("Successfully retrieved Zoho access token via refresh token.")
            return token

        last_err = {"status": resp.status_code, "body": str(body)[:400]}
        logging.warning(
            "Zoho token endpoint did not return access_token (attempt %d/%d): %s",
            attempt,
            retries,
            last_err,
        )
        time.sleep(min(2**attempt, 8))

    logging.error("Failed to obtain Zoho access token after retries. Last error: %s", last_err)
    raise Exception(f"Failed to obtain Zoho access token: {last_err}")


MANUAL_RM_EMAIL_MAP = {
    "DILIP KUMAR SINGH": "dilip@niveshonline.com",
    "ISHU MAVAR": "ishu@niveshonline.com",
    "KAWAL SINGH": "kawal@niveshonline.com",
    "MANISHA P TENDULKAR": "manisha@niveshonline.com",
    "PRAMOD BHUTANI": "pramod@niveshonline.com",
    "RUBY": "ruby@niveshonline.com",
    "SAGAR MAINI": "sagar@niveshonline.com",
    "YATIN MUNJAL": "yatin@niveshonline.com",
}

# --- Monthly payout slabs (points → payout rates) ---
# Table (Total Score → Fresh %, Renewal %, Bonus ₹):
# < 500 → 0%, 0%, ₹0
# 500–999 → 0.5%, 0%, ₹0
# 1000–1499 → 1.0%, 0.2%, ₹0
# 1500–1999 → 1.25%, 0.4%, ₹0
# 2000–2499 → 1.5%, 0.5%, ₹0
# 2500+ → 1.75%, 0.75%, ₹2,000
PAYOUT_SLABS = [
    {
        "min": 0,
        "max": 499.9999,
        "fresh_pct": 0.0000,
        "renew_pct": 0.0000,
        "bonus": 0,
        "label": "<500",
    },
    {
        "min": 500,
        "max": 999.9999,
        "fresh_pct": 0.0050,
        "renew_pct": 0.0000,
        "bonus": 0,
        "label": "500–999",
    },
    {
        "min": 1000,
        "max": 1499.9999,
        "fresh_pct": 0.0100,
        "renew_pct": 0.0020,
        "bonus": 0,
        "label": "1000–1499",
    },
    {
        "min": 1500,
        "max": 1999.9999,
        "fresh_pct": 0.0125,
        "renew_pct": 0.0040,
        "bonus": 0,
        "label": "1500–1999",
    },
    {
        "min": 2000,
        "max": 2499.9999,
        "fresh_pct": 0.0150,
        "renew_pct": 0.0050,
        "bonus": 0,
        "label": "2000–2499",
    },
    {
        "min": 2500,
        "max": float("inf"),
        "fresh_pct": 0.0175,
        "renew_pct": 0.0075,
        "bonus": 2000,
        "label": "2500+",
    },
]


def _apply_payout_slab(score: float) -> dict:
    """Return the payout slab dict for a given monthly score (rounded to nearest int for banding)."""
    try:
        s = float(score)
    except Exception:
        s = 0.0
    # banding uses the integer score for human-aligned thresholds
    band_score = int(round(s))
    for slab in PAYOUT_SLABS:
        if slab["min"] <= band_score <= slab["max"]:
            return slab
    return PAYOUT_SLABS[0]


def _build_insurance_leaderboard_doc(
    *,
    period_month: str,
    employee_id: str | None,
    employee_name: str | None,
    profile: str | None,
    fresh_premium: float,
    renewal_premium: float,
    points_policy: float,
    points_bonus: float,
    points_total: float,
    is_active: bool | None,
    payout_eligible: bool | None = None,
    ins_payout_blocked_by_inactive: bool | None = None,
) -> dict:
    """
    Construct a canonical monthly insurance leaderboard row.
    """
    try:
        fresh_p = float(fresh_premium)
    except Exception:
        fresh_p = 0.0
    try:
        renew_p = float(renewal_premium)
    except Exception:
        renew_p = 0.0
    try:
        pts_pol = float(points_policy)
    except Exception:
        pts_pol = 0.0
    try:
        pts_bonus = float(points_bonus)
    except Exception:
        pts_bonus = 0.0
    try:
        pts_total = float(points_total)
    except Exception:
        pts_total = pts_pol + pts_bonus

    doc = {
        "period_month": str(period_month) if period_month is not None else None,
        "employee_id": str(employee_id) if employee_id is not None else None,
        "employee_name": employee_name,
        "profile": (profile or "").strip() or None,
        "fresh_premium": fresh_p,
        "renewal_premium": renew_p,
        "points_policy": pts_pol,
        "points_bonus": pts_bonus,
        "points_total": pts_total,
        "is_active": bool(is_active) if is_active is not None else None,
        "payout_eligible": True if payout_eligible is None else bool(payout_eligible),
        "ins_payout_blocked_by_inactive": (
            bool(ins_payout_blocked_by_inactive)
            if ins_payout_blocked_by_inactive is not None
            else False
        ),
        "updated_at": dt.datetime.utcnow(),
    }
    return cast(dict, _sanitize_doc(doc))


def generate_email_from_name(rm_name):
    # Fallback: use first part of name, lowercase, remove spaces, '@niveshonline.com'
    # e.g. "John Doe" -> "john@niveshonline.com"
    if not rm_name or not isinstance(rm_name, str):
        return ""
    first = rm_name.strip().split()[0].lower()
    return f"{first}@niveshonline.com"


def get_zoho_user_id_from_email(rm_name, email_to_id_map):
    """
    Looks up Zoho user ID using RM name.
    First tries a manual mapping, then falls back to default logic.
    """
    # --- Manual mapping for known RMs (use full email addresses) ---
    manual_email_map = {
        "ISHU MAVAR": "ishu.mavar@niveshonline.com",
        "KAWAL SINGH": "kawal.singh@niveshonline.com",
        "MANISHA P TENDULKAR": "manisha.tendulkar@niveshonline.com",
        "PRAMOD BHUTANI": "pramod.bhutani@niveshonline.com",
        "RUBY": "ruby.kaur@niveshonline.com",
        "SAGAR MAINI": "sagar.maini@niveshonline.com",
        "YATIN MUNJAL": "yatin.munjal@niveshonline.com",
    }
    import logging

    key = (rm_name or "").upper().strip()
    # Use manual_email_map first, fallback to dynamic construction
    email = manual_email_map.get(key, generate_email_from_name(rm_name))
    return email_to_id_map.get(email)


def get_pli_records(access_token):
    HEADERS = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    url = "https://www.zohoapis.com/crm/v6/Insurance_Leads"
    params = {
        "cvid": "2969103000498919061",
        "per_page": 200,
        "page": 1,  # Initialize the page parameter
    }

    all_users = []  # List to store all users across pages
    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Zoho header received (page {params['page']})")
            all_users.extend(data["data"])

            if not data["info"]["more_records"]:
                break

            params["page"] += 1
        else:
            try:
                err = response.json()
            except Exception:
                err = {"raw": response.text[:300]}
            if response.status_code == 401:
                logging.error(
                    "Zoho API returned 401 Unauthorized. Your access token may be expired. Update the 'Zoho-access-token' secret in Key Vault."
                )
            logging.error(f"Failed to fetch Zoho CRM users: {err}")
            raise Exception(f"Failed to fetch Zoho CRM users: {err}")

    df_users = pd.DataFrame(all_users)
    df_users.rename(columns={"Name": "Insurance_Lead_Name"}, inplace=True)
    logging.debug(f"Fetched columns from Zoho: {df_users.columns.tolist()}")
    if "Discount_Payout_Released" in df_users.columns:
        df_users = df_users[df_users["Discount_Payout_Released"] == False]
    else:
        logging.debug(
            "'Discount_Payout_Released' column not found in data. Proceeding without filtering."
        )
    # print("-=======================")
    # print(df_users[df_users["Lead_ID"] == "MIB9013"]["Associate_Payout"].isna())
    # print("-=======================")

    if "Referral_Fee" in df_users.columns:
        df_users["Merged_Referral_Fee"] = df_users["Referral_Fee1"].combine_first(
            df_users["Referral_Fee"]
        )
    else:
        df_users["Merged_Referral_Fee"] = df_users["Referral_Fee1"]
        logging.debug(
            "'Referral_Fee' column not found. Using only 'Referral_Fee1' for Merged_Referral_Fee."
        )

    df_referral_fee = df_users[df_users["Referral_Fee1"].notna()]
    df_associate_payout = df_users[df_users["Associate_Payout"].notna()].copy()

    if "Associate_Payout" in df_associate_payout.columns:
        df_associate_payout.loc[:, "Associate_id"] = (
            df_associate_payout["Associate_Payout"]
            .apply(convert_str_to_dict)
            .apply(lambda x: x.get("id") if "id" in x else None)
        )
    else:
        df_associate_payout["Associate_id"] = None
        logging.warning("'Associate_Payout' column not found. Setting 'Associate_id' as None.")
    # print("After Filtering:")
    logging.debug(df_associate_payout.to_string(index=False))

    return df_users, df_associate_payout, df_referral_fee


# --- Helper: fetch active Zoho user dicts (raw, for upsert/sync) ---
def _fetch_active_zoho_users(access_token):
    """
    Fetch ActiveUsers from Zoho CRM (v6) and return the raw list of user dicts.
    Used by fetch_active_employee_ids() and Zoho_Users collection sync.
    """
    users = []
    url = "https://www.zohoapis.com/crm/v6/users"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    page = 1
    while True:
        params = {
            "type": "ActiveUsers",
            "page": page,
            "per_page": 200,
        }
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text[:300]}
            logging.warning("Could not fetch Active users from Zoho (page %d): %s", page, err)
            break
        data = resp.json()
        users.extend(data.get("users", []))
        if not data.get("info", {}).get("more_records"):
            break
        page += 1
    return users


# --- New: fetch AllUsers and upsert Zoho_Users with inactive_since stamping --------------------
def _fetch_all_zoho_users(access_token):
    """
    Fetch AllUsers from Zoho CRM (v6) and return the raw list of user dicts.
    Includes both active and inactive users.
    """
    users = []
    url = "https://www.zohoapis.com/crm/v6/users"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    page = 1
    while True:
        params = {
            "type": "AllUsers",
            "page": page,
            "per_page": 200,
        }
        resp = requests.get(url, headers=headers, params=params, timeout=(5, 30))
        if resp.status_code != 200:
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text[:300]}
            logging.warning("Could not fetch AllUsers from Zoho (page %d): %s", page, err)
            break
        data = resp.json()
        users.extend(data.get("users", []))
        if not data.get("info", {}).get("more_records"):
            break
        page += 1
    return users


def sync_zoho_users_all(access_token, zoho_users_collection=None):
    """
    Upsert ALL Zoho users into Mongo (collection 'Zoho_Users') with:
      • unique index on id
      • secondary index on status
      • stamp inactive_since at the FIRST observed active→inactive transition
    Returns (upserts_cnt, modified_cnt, total_processed).
    """
    try:
        users = _fetch_all_zoho_users(access_token)
        if not users:
            logging.warning("AllUsers sync: no users returned.")
            return (0, 0, 0)

        coll = zoho_users_collection or connect_to_mongo("Zoho_Users")
        if coll is None:
            logging.warning("AllUsers sync: Zoho_Users collection unavailable.")
            return (0, 0, 0)

        # Indexes: unique id, and status for quick filters
        try:
            coll.create_index([("id", pymongo.ASCENDING)], unique=True)
        except Exception:
            pass
        try:
            coll.create_index([("status", pymongo.ASCENDING)], name="status_idx")
        except Exception:
            pass

        now = dt.datetime.utcnow()
        upserts_cnt = 0
        modified_cnt = 0
        total = 0

        for u in users:
            try:
                cur_id = str(u.get("id")) if u.get("id") is not None else None
                if not cur_id:
                    continue
                cur_status = str(u.get("status") or "").lower()

                doc = {
                    "id": cur_id,
                    "full_name": u.get("full_name"),
                    "email": u.get("email"),
                    "status": u.get("status"),
                    "role": (
                        (u.get("role") or {}).get("name")
                        if isinstance(u.get("role"), dict)
                        else u.get("role")
                    ),
                    "profile": (
                        (u.get("profile") or {}).get("name")
                        if isinstance(u.get("profile"), dict)
                        else u.get("profile")
                    ),
                    "last_fetched_at": now,
                }

                existing = coll.find_one({"id": cur_id}, {"status": 1, "inactive_since": 1})
                prev_status = str((existing or {}).get("status") or "").lower()
                prev_inactive_since = (existing or {}).get("inactive_since")

                # --- inactive_since transition policy ---
                # 1) Active -> Inactive  : set inactive_since = now
                # 2) Inactive -> Active  : clear inactive_since (None)
                # 3) First time seen and currently Inactive: set inactive_since = now
                # 4) No status change    : carry forward previous stamp if present
                if prev_status != cur_status:
                    if prev_status == "active" and cur_status == "inactive":
                        doc["inactive_since"] = now
                    elif prev_status in ("inactive", "deactivated") and cur_status == "active":
                        # Reactivation: clear prior stamp so next deactivation gets a fresh date
                        doc["inactive_since"] = None
                    else:
                        # Other transitions; keep previous if present
                        if prev_inactive_since is not None:
                            doc["inactive_since"] = prev_inactive_since
                else:
                    # No status change
                    if prev_inactive_since is not None:
                        doc["inactive_since"] = prev_inactive_since

                # If no prior doc and current status is inactive, initialize the stamp
                if not existing and cur_status == "inactive" and "inactive_since" not in doc:
                    doc["inactive_since"] = now

                res = coll.update_one({"id": cur_id}, {"$set": _sanitize_doc(doc)}, upsert=True)
                total += 1
                if getattr(res, "upserted_id", None) is not None:
                    upserts_cnt += 1
                else:
                    try:
                        modified_cnt += int(getattr(res, "modified_count", 0) or 0)
                    except Exception:
                        pass
            except Exception as e:
                logging.warning("AllUsers sync: failed upsert for id=%s: %s", cur_id, e)

        logging.info(
            "AllUsers sync complete: upserts=%d, modified=%d, total=%d",
            upserts_cnt,
            modified_cnt,
            total,
        )
        return (upserts_cnt, modified_cnt, total)
    except Exception as e:
        logging.warning("AllUsers sync failed: %s", e)
        return (0, 0, 0)


# --- Helper: fetch active Zoho employee IDs and optionally upsert Zoho_Users ---
def fetch_active_employee_ids(access_token, zoho_users_collection=None):
    """
    Return a *set* of employee (Zoho User) IDs that are marked **Active**
    in Zoho CRM. If `zoho_users_collection` is provided, also upsert a
    minimal user document into Milestone.Zoho_Users (no raw payload).
    """
    # Fetch all active users once
    users = _fetch_active_zoho_users(access_token)
    active_ids = set(str(u.get("id")) for u in users if u.get("id"))
    logging.info("Fetched %d active employees from Zoho.", len(active_ids))

    # Optional: sync to MongoDB collection "Zoho_Users"
    if zoho_users_collection is not None and users:
        # Ensure a unique index on "id"
        try:
            zoho_users_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
        except Exception:
            pass

        upserts_cnt = 0
        modified_cnt = 0
        total = 0
        now = dt.datetime.utcnow()

        for u in users:
            try:
                doc = {
                    "id": str(u.get("id")) if u.get("id") is not None else None,
                    "full_name": u.get("full_name"),
                    "email": u.get("email"),
                    "status": u.get("status"),
                    "role": (
                        (u.get("role") or {}).get("name")
                        if isinstance(u.get("role"), dict)
                        else u.get("role")
                    ),
                    "profile": (
                        (u.get("profile") or {}).get("name")
                        if isinstance(u.get("profile"), dict)
                        else u.get("profile")
                    ),
                    "last_fetched_at": now,
                }
                if not doc["id"]:
                    continue  # skip if no id
                res = zoho_users_collection.update_one(
                    {"id": doc["id"]},
                    {"$set": doc},
                    upsert=True,
                )
                total += 1
                if getattr(res, "upserted_id", None) is not None:
                    upserts_cnt += 1
                else:
                    try:
                        modified_cnt += int(getattr(res, "modified_count", 0) or 0)
                    except Exception:
                        pass
            except Exception as e:
                logging.warning("Failed to upsert Zoho user: %s", e)

        logging.info(
            "Zoho_Users sync: upserts=%d, modified=%d, total=%d",
            upserts_cnt,
            modified_cnt,
            total,
        )

    return active_ids


def convert_str_to_dict(s):
    try:
        if s is None:
            # Return an empty dictionary if the input is None
            return {}

        # Check if input is already a dictionary (in case of rerun)
        if isinstance(s, dict):
            return s

        # Handle float/int edge case
        if isinstance(s, (int, float)):
            return {}
        # Safely evaluate the string as a dictionary
        return ast.literal_eval(s)

    except (ValueError, SyntaxError, TypeError) as e:
        logging.error(f"Error converting {s}: {e}", exc_info=True)
        return {}


def days_before_due(start, end):
    """
    Return signed day difference between *start* (conversion/issue date)
    and *end* (the previous policy-end date).

    * If either date is missing, returns None.
    * Positive → renewed early; Negative → renewed late.
    """
    if pd.isna(start) or pd.isna(end):
        return None
    delta = (pd.to_datetime(end) - pd.to_datetime(start)).days
    return int(delta)


def classify_term(start_date, end_date):
    """
    Return policy term in whole years with tolerance.
      • If either date is missing → 1
      • If end-start ≤ 370 days → 1 year (human error tolerance)
      • Else ceil((end-start)/365)
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return 1
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
    except Exception:
        return 1
    delta_days = (end - start).days
    if delta_days <= 0:
        return 1
    if delta_days <= 370:
        return 1
    years = (delta_days + 364) // 365
    return max(int(years), 1)


def _match_slab_points(value, slabs, min_key, max_key):
    """
    Helper to find points from a list of slabs where min <= value < max.
    Handles None boundaries as -inf (min) or +inf (max).
    """
    if value is None:
        return 0
    val_f = float(value)
    for s in slabs:
        mn = s.get(min_key)
        mx = s.get(max_key)

        # Check lower bound (inclusive)
        if mn is not None and val_f < float(mn):
            continue
        # Check upper bound (exclusive)
        if mx is not None and val_f >= float(mx):
            continue

        return s.get("points", 0)
    return 0


def compute_points(row, config=None):
    """
    Finalized per-policy point logic (pre-weights), now configurable via 'conversion' block.
    """
    # Load conversion config (fallback to default if missing)
    if config is None:
        config = INS_CONFIG_DEFAULT

    conversion = config.get("conversion") or INS_CONFIG_DEFAULT.get("conversion") or {}
    fresh_slabs = conversion.get("fresh_slabs") or []
    renew_slabs = conversion.get("renew_slabs") or []
    upsell_div = float(conversion.get("upsell_divisor") or 500.0)
    if upsell_div <= 0:
        upsell_div = 500.0

    pts = {
        "base": 0,
        "upsell": 0,
        "early_renew": 0,
        "term_bonus": 0,
        "deductible_bonus": 0,
        "slab_bonus": 0,
    }

    # Extract basics
    policy_type = str(row.get("policy_type") or "").lower()
    conv_status = str(row.get("conversion_status") or "").lower()
    dtr = row.get("days_to_renewal")
    term_years = int(row.get("term_years") or 1)
    this_prem = float(row.get("this_year_premium") or 0)
    last_prem = float(row.get("last_year_premium") or 0)

    # Detect portability (textual)
    is_port = ("portability" in conv_status) or ("portability" in policy_type)

    # Category flags
    pol = policy_type

    def any_in(words):
        return any(w in pol for w in words)

    is_ulip = any_in(["ulip"]) or any_in(["traditional"])  # zero weight later
    is_term = any_in(["term insurance"]) and not is_ulip

    # -------------------------------
    # Portability reclassification
    # -------------------------------
    has_ren_date = pd.notna(row.get("Renewal_Date"))
    row["has_renewal_date"] = bool(has_ren_date)
    port_reclass = None

    if is_port:
        if not has_ren_date:
            # Portability but no Renewal_Date → treat as Fresh to company
            is_renewal = False
            port_reclass = "port→fresh_no_renew_date"
        else:
            if last_prem > 0:
                # Portability with Renewal_Date and last premium → Renewal (upsell eligible if this>last)
                is_renewal = True
                port_reclass = "port→renew_with_last"
            else:
                # Portability with Renewal_Date but no last premium → Renewal (no upsell)
                is_renewal = True
                port_reclass = "port→renew_no_last"
    else:
        # Fallback to existing behavior when not portability-like
        is_health_pa = any_in(["health", "personal accident"]) and not is_term
        if dtr is None:
            is_renewal = is_health_pa
        else:
            is_renewal = ("renewal" in conv_status) or ("renewal" in policy_type)

    row["port_reclass"] = port_reclass
    is_fresh_or_port = not is_renewal

    # Persist a classification for later weights
    row["policy_classification"] = "renewal" if is_renewal else "fresh"

    # Track fresh-to-company premium explicitly for bonus calculation (quarterly/annual)
    # Rule: bonuses count ONLY 'fresh' (incl. port→fresh) premium; renewals do not count.
    try:
        row["fresh_premium_eligible"] = float(this_prem) if not is_renewal else 0.0
    except Exception:
        row["fresh_premium_eligible"] = 0.0

    # Attach an ISO 'YYYY-MM' period for easy monthly aggregation
    try:
        _cd = row.get("conversion_date")
        if isinstance(_cd, pd.Timestamp):
            row["period_month"] = _cd.to_period("M").strftime("%Y-%m")
        elif isinstance(_cd, (dt.datetime, dt.date)):
            row["period_month"] = pd.to_datetime(_cd).to_period("M").strftime("%Y-%m")
        else:
            row["period_month"] = None
    except Exception:
        row["period_month"] = None

    # -----------------
    # Renewal base bands (Configurable)
    # -----------------
    if is_renewal:
        if dtr is None:
            base = 0  # no penalty when missing
            # Check for fallback "None" slab if configured?
            # Current legacy logic was explicitly 0 if None.
            # My config has 'min_dtr': None for -inf, not for NoneType dtr.
            # So standard '0' is fine unless we want to map None explicitly.
        else:
            # Use slab lookup
            base = _match_slab_points(dtr, renew_slabs, "min_dtr", "max_dtr")

        # Upsell on renewals (annualized)
        ups = 0
        if this_prem > last_prem and last_prem > 0 and term_years > 0:
            annualized_delta = (this_prem - last_prem) / term_years
            ups = int(annualized_delta // upsell_div)

        pts["base"] = base
        pts["upsell"] = ups

    # -----------------------
    # Fresh / Portability base (Configurable)
    # -----------------------
    else:
        avg_annual = this_prem / max(1, term_years)
        # Use slab lookup
        base = _match_slab_points(avg_annual, fresh_slabs, "min_val", "max_val")

        pts["base"] = base
        pts["upsell"] = 0  # upsell not applicable to fresh

    row["points"] = pts
    row["total_points"] = pts["base"] + pts["upsell"]
    row["is_portability"] = bool(is_port)
    row["is_term"] = bool(is_term)
    row["is_ulip"] = bool(is_ulip)
    return row


#
# --- helpers: sanitize values for Mongo (convert pandas NaT/Timestamp, tz-aware datetimes, dates) ---
def _sanitize_value(v):
    # Handle None / NaN / NaT first
    try:
        if v is None or pd.isna(v):
            return None
    except Exception:
        if v is None:
            return None
    # pandas Timestamp -> naive datetime
    if isinstance(v, pd.Timestamp):
        dt_obj = v.to_pydatetime()
        if getattr(dt_obj, "tzinfo", None) is not None:
            dt_obj = dt_obj.replace(tzinfo=None)
        return dt_obj
    # tz-aware datetime -> naive
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            return v.replace(tzinfo=None)
        return v
    # date -> datetime
    if isinstance(v, dt.date):
        return dt.datetime.combine(v, dt.time())
    # leave other types as-is
    return v


def _sanitize_doc(obj):
    if isinstance(obj, dict):
        return {k: _sanitize_doc(_sanitize_value(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_doc(_sanitize_value(x)) for x in obj]
    # Scalars
    return _sanitize_value(obj)


#
# --- Helpers: Indian FY quarter labelling for bonus audits (used by aggregators/logs) ---
def _quarter_label_from_date(dt_value: dt.datetime | dt.date | None) -> str | None:
    """
    Return 'FYYYYY-Qn' using Indian financial year (Apr–Mar).
    Example: May 2025 -> 'FY2026-Q1'; March 2026 -> 'FY2026-Q4'.
    """
    if dt_value is None:
        return None
    try:
        if isinstance(dt_value, pd.Timestamp):
            y, m = int(dt_value.year), int(dt_value.month)
        elif isinstance(dt_value, (dt.datetime, dt.date)):
            y, m = int(dt_value.year), int(dt_value.month)
        else:
            return None
        if m >= 4:
            fy_end = y + 1
            q = 1 if m in (4, 5, 6) else 2 if m in (7, 8, 9) else 3
        else:
            fy_end = y
            q = 4
        return f"FY{fy_end}-Q{q}"
    except Exception:
        return None


# --- Helper: quarter and FY bounds for bonus logic ---
def _bounds_for_quarter_and_fy(period_month: str):
    """
    Given 'YYYY-MM', return:
      {
        'q_start': datetime,
        'q_end': datetime,         # exclusive
        'is_q_end': bool,          # True if period_month is a quarter-end month
        'fy_start': datetime,      # FY = Apr 1 .. Apr 1 (exclusive)
        'fy_end': datetime,
        'is_fy_end': bool          # True if period_month is March
      }
    """
    if not period_month or not isinstance(period_month, str) or "-" not in period_month:
        today = dt.datetime.utcnow()
        return {
            "q_start": today,
            "q_end": today,
            "is_q_end": False,
            "fy_start": today,
            "fy_end": today,
            "is_fy_end": False,
        }
    try:
        y, m = [int(x) for x in period_month.split("-")[:2]]
    except Exception:
        today = dt.datetime.utcnow()
        return {
            "q_start": today,
            "q_end": today,
            "is_q_end": False,
            "fy_start": today,
            "fy_end": today,
            "is_fy_end": False,
        }

    # Quarter bounds (Indian fiscal grouping)
    if m in (4, 5, 6):
        q_start = dt.datetime(y, 4, 1)
        q_end = dt.datetime(y, 7, 1)
        is_q_end = m == 6
    elif m in (7, 8, 9):
        q_start = dt.datetime(y, 7, 1)
        q_end = dt.datetime(y, 10, 1)
        is_q_end = m == 9
    elif m in (10, 11, 12):
        q_start = dt.datetime(y, 10, 1)
        q_end = dt.datetime(y + 1, 1, 1)
        is_q_end = m == 12
    else:  # Jan–Mar
        q_start = dt.datetime(y, 1, 1)
        q_end = dt.datetime(y, 4, 1)
        is_q_end = m == 3

    # Financial year (Apr–Mar)
    if m >= 4:
        fy_start = dt.datetime(y, 4, 1)
        fy_end = dt.datetime(y + 1, 4, 1)
    else:
        fy_start = dt.datetime(y - 1, 4, 1)
        fy_end = dt.datetime(y, 4, 1)
    is_fy_end = m == 3

    return {
        "q_start": q_start,
        "q_end": q_end,
        "is_q_end": is_q_end,
        "fy_start": fy_start,
        "fy_end": fy_end,
        "is_fy_end": is_fy_end,
    }


# --- Helper: compute monthly bonus maps (quarterly & annual) from scored detail frame ---
def _bonus_maps_from_scored(df_scored: pd.DataFrame):
    """
    Build lookup maps keyed by (employee_id, period_month) to support monthly upsert enrichment.

    Inputs:
      df_scored: per-policy scored DataFrame (after compute_points). Must contain:
        ['employee_id','period_month','fresh_premium_eligible','conversion_date']

    Returns tuple of dicts:
      (
        bonus_q_points_map,     # (emp, YYYY-MM) -> int points to credit this month for quarter (only at q-end)
        bonus_a_points_map,     # (emp, YYYY-MM) -> int points to credit this month for annual (only in March)
        month_basis_map,        # (emp, YYYY-MM) -> float fresh-to-company premium in that month
        q_basis_map,            # (emp, YYYY-MM) -> float fresh premium QTD (sum of months in quarter)
        fy_basis_map,           # (emp, YYYY-MM) -> float fresh premium FYTD (Apr..Mar)
        q_end_flag_map,         # (emp, YYYY-MM) -> bool is quarter-end
        fy_end_flag_map         # (emp, YYYY-MM) -> bool is FY-end (March)
      )
    """
    try:
        if df_scored is None or df_scored.empty:
            return ({}, {}, {}, {}, {}, {}, {})

        work = df_scored.copy()
        work["employee_id"] = work["employee_id"].astype(str)
        work["period_month"] = work["period_month"].astype(str)
        # Ensure a Series source (not a scalar) before to_numeric/fillna
        if "fresh_premium_eligible" in work.columns:
            _fpe_src = work["fresh_premium_eligible"]
        else:
            _fpe_src = pd.Series(0.0, index=work.index, dtype="float64")
        work["fresh_premium_eligible"] = pd.to_numeric(_fpe_src, errors="coerce").fillna(0.0)

        # Monthly fresh basis per employee
        month_grp = (
            work.groupby(["employee_id", "period_month"], dropna=False)["fresh_premium_eligible"]
            .sum()
            .reset_index(name="fresh_month")
        )
        if month_grp.empty:
            return ({}, {}, {}, {}, {}, {}, {})

        # Parse month & derive FY/Quarter labels
        month_grp["_per_date"] = pd.to_datetime(
            month_grp["period_month"].astype(str) + "-01", errors="coerce"
        )

        def _fy_end_year(dt: pd.Timestamp) -> int:
            y, m = int(dt.year), int(dt.month)
            return y + 1 if m >= 4 else y

        month_grp["_q_label"] = month_grp["_per_date"].apply(_quarter_label_from_date)
        month_grp["_fy_label"] = month_grp["_per_date"].apply(
            lambda d: f"FY{_fy_end_year(d)}" if pd.notna(d) else None
        )

        # QTD / FYTD sums
        q_grp = (
            month_grp.groupby(["employee_id", "_q_label"], dropna=False)["fresh_month"]
            .sum()
            .reset_index()
            .rename(columns={"fresh_month": "fresh_qtd"})
        )
        fy_grp = (
            month_grp.groupby(["employee_id", "_fy_label"], dropna=False)["fresh_month"]
            .sum()
            .reset_index()
            .rename(columns={"fresh_month": "fresh_fy"})
        )

        q_map = {
            (r["employee_id"], r["_q_label"]): float(r["fresh_qtd"]) for _, r in q_grp.iterrows()
        }
        fy_map = {
            (r["employee_id"], r["_fy_label"]): float(r["fresh_fy"]) for _, r in fy_grp.iterrows()
        }

        # Flags
        month_grp["_m"] = month_grp["_per_date"].dt.month
        month_grp["_is_q_end"] = month_grp["_m"].isin([6, 9, 12, 3])
        month_grp["_is_fy_end"] = month_grp["_m"].eq(3)

        # Compose maps
        month_basis_map: dict[tuple[str, str], float] = {}
        q_basis_map: dict[tuple[str, str], float] = {}
        fy_basis_map: dict[tuple[str, str], float] = {}
        q_end_flag_map: dict[tuple[str, str], bool] = {}
        fy_end_flag_map: dict[tuple[str, str], bool] = {}
        bonus_q_points_map: dict[tuple[str, str], int] = {}
        bonus_a_points_map: dict[tuple[str, str], int] = {}

        rows = list(month_grp.iterrows())
        for _, r in rows:
            emp = str(r["employee_id"])
            per = str(r["period_month"])
            ql = r["_q_label"]
            fyl = r["_fy_label"]

            fresh_m = float(r["fresh_month"] or 0.0)
            is_qe = bool(r["_is_q_end"])
            is_fye = bool(r["_is_fy_end"])

            month_basis_map[(emp, per)] = fresh_m
            q_basis = float(q_map.get((emp, ql), 0.0)) if ql else 0.0
            fy_basis = float(fy_map.get((emp, fyl), 0.0)) if fyl else 0.0
            q_basis_map[(emp, per)] = q_basis
            fy_basis_map[(emp, per)] = fy_basis
            q_end_flag_map[(emp, per)] = is_qe
            fy_end_flag_map[(emp, per)] = is_fye

            # PDF-spec reversion (2025 incentive policy):
            # Quarterly and Annual bonuses are NOT point credits for Insurance (and MF Annual is INR too).
            # INR payouts are computed in the payout layer; do not contaminate monthly points here.
            # Keep zero point credit at quarter-end / FY-end.
            bonus_q_points_map[(emp, per)] = 0
            bonus_a_points_map[(emp, per)] = 0

        return (
            bonus_q_points_map,
            bonus_a_points_map,
            month_basis_map,
            q_basis_map,
            fy_basis_map,
            q_end_flag_map,
            fy_end_flag_map,
        )
    except Exception as e:
        logging.warning("_bonus_maps_from_scored failed: %s", e)
        return ({}, {}, {}, {}, {}, {}, {})


def upsert_insurance_mf_leaders(
    df: pd.DataFrame, profiles_by_id: dict[str, str] | None = None
) -> None:
    """Credit 20% of monthly points to leaders, split by employee profile bucket.

    • Non‑MF points → env `Insurance_Leader_ins` (default: 'Sumit Chakraborty') with bucket 'INS'
    • MF points     → env `Insurance_Leader_mf`  (default: 'Sagar Maini') with bucket 'MF'

    Writes to collection named by env `MF_SIP_LEADERS_COLL` (default: 'MF_Leaders').
    Upsert key: (rm_name, period_month)
    """
    try:
        if df is None or df.empty:
            return
        logging.info(
            "[Leader20] Enter: df_rows=%s cols=%s",
            len(df) if df is not None else 0,
            list(df.columns) if hasattr(df, "columns") else None,
        )
        # --- Surface incoming period distribution for diagnostics ---
        try:
            if "conversion_date" in df.columns:
                _months = (
                    pd.to_datetime(df["conversion_date"], errors="coerce")
                    .dt.to_period("M")
                    .astype(str)
                    .value_counts()
                    .to_dict()
                )
                logging.info(
                    "[Leader20] Incoming rows by month (from conversion_date): %s", _months
                )
        except Exception:
            pass

        leaders_coll_name = os.getenv("MF_SIP_LEADERS_COLL", "MF_Leaders")
        leader_name_ins = os.getenv("Insurance_Leader_ins", "Sumit Chakraborty")
        leader_name_mf = os.getenv("Insurance_Leader_mf", "Sagar Maini")
        # Backward compatibility: if config/env still points to Sahil, shift MF leader credit to Sagar
        if isinstance(leader_name_mf, str) and leader_name_mf.strip().lower() in {
            "sahil gupta",
            "sahil",
        }:
            logging.info(
                "[Leader20] Insurance_Leader_mf configured as '%s'; remapping MF leader credit to 'Sagar Maini'.",
                leader_name_mf,
            )
            leader_name_mf = "Sagar Maini"

        leaders_coll = connect_to_mongo(leaders_coll_name)
        if leaders_coll is None:
            logging.warning(
                "Leaders collection '%s' unavailable; skipping leader allocation.",
                leaders_coll_name,
            )
            return
        logging.info("[Leader20] Connected to leaders collection: %s", leaders_coll_name)
        try:
            leaders_coll.create_index(
                [("rm_name", pymongo.ASCENDING), ("period_month", pymongo.ASCENDING)], unique=True
            )
        except Exception:
            pass
        # --- Add unique index for (source, period_month, bucket) for idempotency ---
        try:
            leaders_coll.create_index(
                [
                    ("source", pymongo.ASCENDING),
                    ("period_month", pymongo.ASCENDING),
                    ("bucket", pymongo.ASCENDING),
                ],
                unique=True,
                name="uniq_source_month_bucket",
            )
        except Exception:
            pass

        # --- Ensure we have a profiles map (fallback to Mongo Zoho_Users if not provided) ---
        if not profiles_by_id:
            try:
                zu = connect_to_mongo("Zoho_Users")
                if zu is not None:
                    # Ensure unique index on user id
                    try:
                        zu.create_index([("id", pymongo.ASCENDING)], unique=True)
                    except Exception:
                        pass
                    # Ensure secondary index on status for quick filtering
                    try:
                        zu.create_index([("status", pymongo.ASCENDING)], name="status_idx")
                    except Exception:
                        pass

                    # If Zoho_Users looks sparse, auto-sync AllUsers from Zoho first
                    try:
                        cur_cnt = int(zu.estimated_document_count() or 0)
                    except Exception:
                        cur_cnt = 0
                    min_needed = int(os.getenv("ZOHO_USERS_MIN", "25"))
                    if cur_cnt < min_needed:
                        logging.info(
                            "[Leader20] Zoho_Users has %d docs (<%d); syncing AllUsers from Zoho (with inactive_since stamps).",
                            cur_cnt,
                            min_needed,
                        )
                        try:
                            token = get_access_token()
                            # Full AllUsers sync including inactive_since stamping
                            sync_zoho_users_all(token, zoho_users_collection=zu)
                            try:
                                cur_cnt = int(zu.estimated_document_count() or 0)
                            except Exception:
                                pass
                            logging.info("[Leader20] Zoho_Users post-sync count=%s", cur_cnt)
                        except Exception as _e_sync:
                            logging.warning(
                                "[Leader20] Zoho_Users AllUsers sync failed (continuing with existing docs): %s",
                                _e_sync,
                            )

                    # Build in-memory map {id -> profile}
                    _map = {}
                    cursor = zu.find({}, {"id": 1, "profile": 1})
                    if cursor is not None:
                        for doc in cursor:
                            if doc.get("id") is None:
                                continue
                            _map[str(doc["id"])] = str(doc.get("profile") or "")
                    profiles_by_id = _map
                    logging.info(
                        "[Leader20] Loaded %d profiles from Zoho_Users for MF bucket mapping.",
                        len(profiles_by_id),
                    )
                else:
                    logging.warning(
                        "[Leader20] Zoho_Users collection unavailable; MF bucket mapping may be inaccurate."
                    )
            except Exception as _e:
                logging.warning(
                    "[Leader20] Failed to load profiles from Zoho_Users; MF/INS split may be off: %s",
                    _e,
                )

        # Month-aware de-duplication at policy granularity to avoid double-crediting
        try:
            # Ensure period_month exists (string 'YYYY-MM') for reliable de-dup by month
            if "conversion_date" in df.columns and "period_month" not in df.columns:
                df = df.copy()
                df["period_month"] = (
                    pd.to_datetime(df["conversion_date"], errors="coerce")
                    .dt.to_period("M")
                    .astype(str)
                )
            if {"lead_id", "policy_number", "period_month"}.issubset(df.columns):
                df = df.drop_duplicates(subset=["lead_id", "policy_number", "period_month"])
            elif {"employee_id", "conversion_date", "total_points"}.issubset(df.columns):
                # minimal fallback
                df = df.drop_duplicates(subset=["employee_id", "conversion_date", "total_points"])
        except Exception:
            pass

        # Minimal fields required: employee_id, conversion_date, total_points
        cols_needed = ["employee_id", "conversion_date", "total_points"]
        for c in cols_needed:
            if c not in df.columns:
                logging.warning(
                    "Leader allocation skipped: missing column '%s' in scored dataframe.", c
                )
                return

        logging.info("[Leader20] Required columns present; proceeding to filter non-null rows.")
        work = df.loc[df["employee_id"].notna() & df["conversion_date"].notna(), cols_needed].copy()
        if work.empty:
            logging.info(
                "[Leader20] Workframe empty after non-null filter (employee_id & conversion_date). Skipping."
            )
            return
        logging.info("[Leader20] Workframe rows=%d", len(work))

        # Month and MF bucket classification
        work["period_month"] = (
            pd.to_datetime(work["conversion_date"], errors="coerce").dt.to_period("M").astype(str)
        )
        work["is_mf"] = work["employee_id"].apply(
            lambda eid: _is_mf_profile_from_map(eid, profiles_by_id)
        )
        logging.info(
            "[Leader20] Classified MF vs non-MF; sample=%s",
            work[["employee_id", "period_month", "is_mf"]].head(5).to_dict("records"),
        )

        # Aggregate monthly totals by bucket
        agg = (
            work.groupby(["period_month", "is_mf"], dropna=False)["total_points"]
            .sum()
            .reset_index()
        )
        if agg.empty:
            logging.info("[Leader20] Aggregation empty; no monthly totals to credit.")
            return
        logging.info("[Leader20] Aggregation rows=%d: %s", len(agg), agg.head(5).to_dict("records"))

        # Purge existing insurance leader rows for all months we might touch (cover legacy rows too)
        try:
            # months seen in this aggregation
            months = set(str(x) for x in agg["period_month"].unique())

            # union with months present in Insurance_Policy_Scoring for the current FY
            try:
                ips = connect_to_mongo("Insurance_Policy_Scoring")
                if ips is not None:
                    _now = dt.datetime.utcnow()
                    if _now.month >= 4:
                        _fy_start = dt.datetime(_now.year, 4, 1)
                        _fy_end = dt.datetime(_now.year + 1, 3, 31, 23, 59, 59)
                    else:
                        _fy_start = dt.datetime(_now.year - 1, 4, 1)
                        _fy_end = dt.datetime(_now.year, 3, 31, 23, 59, 59)
                    _cursor = ips.aggregate(
                        [
                            {"$match": {"conversion_date": {"$gte": _fy_start, "$lte": _fy_end}}},
                            {
                                "$group": {
                                    "_id": {
                                        "$dateToString": {
                                            "format": "%Y-%m",
                                            "date": "$conversion_date",
                                        }
                                    }
                                }
                            },
                            {"$project": {"_id": 0, "m": "$_id"}},
                        ]
                    )
                    _months_docs: list[dict] = list(_cursor) if _cursor is not None else []
                    for d in _months_docs:
                        m_val = d.get("m")
                        if m_val:
                            months.add(m_val)
            except Exception as _e2:
                logging.warning(
                    "[Leader20] Could not extend purge months from Insurance_Policy_Scoring: %s",
                    _e2,
                )

            # also include any months already present in leaders collection for our leaders (covers legacy/no-source rows)
            try:
                existing_months = set()
                for leader in (leader_name_ins, leader_name_mf):
                    existing_months |= set(
                        leaders_coll.distinct("period_month", {"rm_name": leader}) or []
                    )
                months |= {m for m in existing_months if m}
            except Exception as _e3:
                logging.warning(
                    "[Leader20] Could not read existing months from leaders_coll: %s", _e3
                )

            months = sorted(m for m in months if m)
            if months:
                del_filter = {
                    "source": "insurance_leader_allocation_v1",
                    "period_month": {"$in": months},
                    "bucket": {"$in": ["INS", "MF"]},
                }
                del_res = leaders_coll.delete_many(del_filter)
                logging.info(
                    "[Leader20] Purged %s prior leader rows for months=%s by month×bucket×source.",
                    getattr(del_res, "deleted_count", "?"),
                    months,
                )
            else:
                logging.info("[Leader20] No months to purge before leader allocation.")
        except Exception as _e:
            logging.warning("[Leader20] Purge step failed (continuing): %s", _e)

        leader_rate = 0.20
        now = dt.datetime.utcnow()
        ops: list[UpdateOne] = []
        for _, r in agg.iterrows():
            per = str(r["period_month"])  # 'YYYY-MM'
            base_total = float(r.get("total_points") or 0.0)
            is_mf = bool(r.get("is_mf"))
            leader = leader_name_mf if is_mf else leader_name_ins
            bucket = "MF" if is_mf else "INS"

            filt = {
                "source": "insurance_leader_allocation_v1",
                "period_month": per,
                "bucket": bucket,
            }
            doc = {
                "rm_name": leader,
                "period_month": per,
                "bucket": bucket,
                "leader_bonus_points": int(round(base_total * leader_rate)),
                "leader_bonus_rate": leader_rate,
                "base_points_total_others": int(round(base_total)),
                "computed_at": now,
                "source": "insurance_leader_allocation_v1",
            }
            doc = _sanitize_doc(doc)
            ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))

        logging.info(
            "[Leader20] Prepared leader ops=%d (months=%s)",
            len(ops),
            sorted(set(str(x) for x in agg["period_month"].unique())),
        )
        if ops:
            try:
                res = leaders_coll.bulk_write(ops, ordered=False)
                logging.info(
                    "Insurance leader allocation upserts: upserted=%s modified=%s matched=%s ops=%d",
                    getattr(res, "upserted_count", 0),
                    getattr(res, "modified_count", 0),
                    getattr(res, "matched_count", 0),
                    len(ops),
                )
                try:
                    by_bucket = (
                        agg.assign(bucket=agg["is_mf"].map({True: "MF", False: "INS"}))
                        .groupby(["period_month", "bucket"])["total_points"]
                        .sum()
                        .reset_index()
                        .to_dict("records")
                    )
                    logging.info("[Leader20] Totals by month×bucket: %s", by_bucket)
                except Exception:
                    pass
                # --- Reconciliation: verify expected (20% of base) equals credited for touched months ---
                try:
                    exp_rows = (
                        agg.assign(bucket=agg["is_mf"].map({True: "MF", False: "INS"}))
                        .groupby(["period_month", "bucket"], dropna=False)["total_points"]
                        .sum()
                        .reset_index()
                    )
                    exp_rows["expected"] = exp_rows["total_points"].apply(
                        lambda x: int(round(float(x) * leader_rate))
                    )
                    months_set = sorted(set(exp_rows["period_month"].astype(str)))
                    cursor = leaders_coll.aggregate(
                        [
                            {
                                "$match": {
                                    "source": "insurance_leader_allocation_v1",
                                    "period_month": {"$in": months_set},
                                }
                            },
                            {
                                "$group": {
                                    "_id": {"m": "$period_month", "b": "$bucket"},
                                    "credited": {"$sum": "$leader_bonus_points"},
                                }
                            },
                        ]
                    )
                    cred_cur: list[dict]
                    if cursor is not None:
                        cred_cur = list(cursor)
                    else:
                        cred_cur = []
                    cred_map = {
                        (d["_id"]["m"], d["_id"]["b"]): int(d.get("credited") or 0)
                        for d in cred_cur
                    }
                    bad = []
                    for _, rr in exp_rows.iterrows():
                        key = (str(rr["period_month"]), str(rr["bucket"]))
                        credited = cred_map.get(key, 0)
                        delta = credited - int(rr["expected"])
                        if delta != 0:
                            bad.append(
                                {
                                    "month": key[0],
                                    "bucket": key[1],
                                    "expected": int(rr["expected"]),
                                    "credited": credited,
                                    "delta": delta,
                                }
                            )
                    if bad:
                        logging.error(
                            "[Leader20][Reconcile] Non-zero deltas after write: %s", bad[:10]
                        )
                    else:
                        logging.info(
                            "[Leader20][Reconcile] expected == credited for months=%s", months_set
                        )
                    # --- Persist audit snapshot (expected vs credited) per month×bucket ---
                    try:
                        audit_coll_name = os.getenv("LEADER_AUDIT_COLL", "Leader_Audit")
                        audit_coll = connect_to_mongo(audit_coll_name)
                        if audit_coll is not None:
                            try:
                                # optional idempotency index: (source, period_month, bucket)
                                # --- Search for any other unsafe list(cursor) or set(distinct()) patterns in this function ---
                                # (No additional occurrences in this function)
                                audit_coll.create_index(
                                    [
                                        ("source", pymongo.ASCENDING),
                                        ("period_month", pymongo.ASCENDING),
                                        ("bucket", pymongo.ASCENDING),
                                    ],
                                    unique=True,
                                    name="uniq_source_month_bucket",
                                )
                            except Exception:
                                pass

                            audit_ops: list[UpdateOne] = []
                            for _, rr in exp_rows.iterrows():
                                per = str(rr["period_month"])
                                bucket = str(rr["bucket"])
                                expected = int(rr["expected"])
                                credited = int(cred_map.get((per, bucket), 0))
                                doc = {
                                    "source": "insurance_leader_allocation_v1",
                                    "period_month": per,
                                    "bucket": bucket,
                                    "expected_points": expected,
                                    "credited_points": credited,
                                    "delta": credited - expected,
                                    "reconcile_status": (
                                        "ok" if credited == expected else "mismatch"
                                    ),
                                    "computed_at": now,
                                }
                                doc = _sanitize_doc(doc)
                                audit_ops.append(
                                    UpdateOne(
                                        {
                                            "source": "insurance_leader_allocation_v1",
                                            "period_month": per,
                                            "bucket": bucket,
                                        },
                                        {"$set": doc},
                                        upsert=True,
                                    )
                                )
                            if audit_ops:
                                try:
                                    audit_res = audit_coll.bulk_write(audit_ops, ordered=False)
                                    logging.info(
                                        "[Leader20][Audit] wrote %s audit rows to %s",
                                        len(audit_ops),
                                        audit_coll_name,
                                    )
                                except Exception as _e_audit:
                                    logging.warning(
                                        "[Leader20][Audit] bulk_write failed: %s", _e_audit
                                    )
                        else:
                            logging.warning(
                                "[Leader20][Audit] audit collection unavailable; skipping write."
                            )
                    except Exception as _e_outer_audit:
                        logging.warning(
                            "[Leader20][Audit] skipped due to error: %s", _e_outer_audit
                        )
                except Exception as _e:
                    logging.warning("[Leader20][Reconcile] skipped due to error: %s", _e)
            except Exception as e:
                logging.error("Leader allocation bulk_write failed: %s", e, exc_info=True)
    except Exception as e:
        logging.error("Leader allocation failed: %s", e, exc_info=True)


def process_and_upsert(
    df_raw,
    mongo_collection,
    profiles_by_id: dict | None = None,
    skip_profiles: set[str] | None = None,
):
    # Ensure schema + runtime config (aligned with SIP/Lumpsum; idempotent each run)
    ensure_schema_registry()
    ins_runtime_cfg = load_insurance_runtime_config()
    logging.info(
        "[Config] Insurance runtime config: range_mode=%s fy_mode=%s audit_mode=%s",
        ins_runtime_cfg.get("range_mode"),
        ins_runtime_cfg.get("fy_mode"),
        ins_runtime_cfg.get("audit_mode"),
    )

    # 1. Pre-process date fields (handle whichever variants are present)
    date_cols = [
        "Conversion/Lost Date",
        "Conversion_Lost_Date",
        "Policy Start Date",
        "Policy_Start_Date1",
        "Policy End Date",
        "Policy_End_Date",
        "Renewal_Date",
        "Eldest Member Age",
        "Eldest_Member_Age",
        "Policy_Start_Date",
    ]
    for col in date_cols:
        if col in df_raw.columns:
            df_raw[col] = pd.to_datetime(df_raw[col], errors="coerce")

    # 2. Rename columns – support both space‑separated and underscore variants
    df = df_raw.rename(
        columns={
            # premium
            "Premium B/f GST": "this_year_premium",
            "Premium_B/f_GST": "this_year_premium",
            "Premium_B_f_GST": "this_year_premium",
            "Last Year Premium": "last_year_premium",
            "Last_Year_Premium": "last_year_premium",
            # renewal notice
            "Renewal Notice Premium": "renewal_notice_premium",
            "Renewal_Notice_Premium": "renewal_notice_premium",
            # eldest member DOB
            "Eldest Member Age": "eldest_member_dob",
            "Eldest_Member_Age": "eldest_member_dob",
            # basic identity
            "Insurance Lead Name": "client_name",
            "Insurance_Lead_Name": "client_name",
            "Policy Number": "policy_number",
            "Policy_Number": "policy_number",
            # dates
            "Conversion/Lost Date": "conversion_date",
            "Conversion_Lost_Date": "conversion_date",
            "Policy Start Date": "policy_start",
            "Policy_Start_Date": "policy_start",
            "Policy_Start_Date1": "policy_start",
            "Policy End Date": "policy_end",
            "Policy_End_Date": "policy_end",
            # misc
            "Lead_ID": "lead_id",
            "Insurance_Type": "policy_type",
            "Conversion_Status": "conversion_status",
            "Processing_User": "processing_user",
            "Processing User": "processing_user",
            # ensure Direct_Associate and product columns
            "Direct Associate": "Direct_Associate",
            "Product": "product",
        }
    )

    # --- Coalesce duplicate columns produced by renaming multiple sources to the same target ---
    # Example: "Policy Start Date", "Policy_Start_Date", "Policy_Start_Date1" → "policy_start"
    # After rename, pandas can hold duplicate column labels; coalesce them into a single Series.
    if df.columns.duplicated().any():
        dupe_labels = df.columns[df.columns.duplicated()].unique()
        for label in dupe_labels:
            # Collect all duplicate columns for this label (keeps left-to-right order)
            cols_df = df.loc[:, df.columns == label]
            # Take first non-null across duplicates
            merged = cols_df.bfill(axis=1).iloc[:, 0]
            # Drop duplicates (keep first occurrence), then assign merged back
            df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()
            df[label] = merged

    # --- Strong guard: force single-Series for key date columns even if labels are still duplicated ---
    for label in ["policy_start", "policy_end", "conversion_date", "Renewal_Date"]:
        if label in df.columns:
            _cols = df.loc[:, df.columns == label]
            if isinstance(_cols, pd.DataFrame) and _cols.shape[1] > 1:
                _merged = pd.to_datetime(_cols.bfill(axis=1).iloc[:, 0], errors="coerce")
                # drop all occurrences of `label` then add back one merged series
                df = df.loc[:, df.columns != label].copy()
                df[label] = _merged
            else:
                df[label] = pd.to_datetime(df[label], errors="coerce")

    # 3. Apply Company Whitelist/Blacklist Rules
    # check if company_rules exists in config
    if ins_runtime_cfg.get("company_rules"):
        df = _apply_company_weights(df, "MainPipeline", ins_runtime_cfg)

    # Re-ensure datetime dtypes for critical date fields post-coalescing
    for _c in ["policy_start", "policy_end", "conversion_date", "Renewal_Date"]:
        if _c in df.columns:
            s = df.loc[:, _c]  # ensure a Series, not Any|None
            df.loc[:, _c] = pd.to_datetime(cast(pd.Series, s), errors="coerce")

    # --- Ensure normalized monthly and quarterly tags for downstream aggregation ---
    if "conversion_date" in df.columns:
        _conv = pd.to_datetime(df["conversion_date"], errors="coerce")
        df["period_month"] = _conv.dt.to_period("M").astype(str)
        df["_quarter_label"] = _conv.apply(_quarter_label_from_date)
    else:
        df["period_month"] = None
        df["_quarter_label"] = None

    # ensure processing_user column exists for employee extraction
    if "processing_user" not in df.columns:
        df["processing_user"] = None

    # ---- derive explicit employee name / id columns from processing_user ----
    df["employee_name"] = df["processing_user"].apply(
        lambda v: v.get("name") if isinstance(v, dict) else v
    )
    df["employee_id"] = df["processing_user"].apply(
        lambda v: v.get("id") if isinstance(v, dict) else None
    )
    # --- Skip RM names via central skiplist (only your aliases; no env unions) ---
    try:
        _before = len(df)
        df["_emp_name_lc"] = df["employee_name"].astype(str).str.strip().str.lower()
        _keep_mask = ~df["_emp_name_lc"].apply(should_skip)
        _skipped = int(_before - int(_keep_mask.sum()))
        if _skipped > 0:
            try:
                _uniq = int(df.loc[~_keep_mask, "_emp_name_lc"].nunique())
            except Exception:
                _uniq = _skipped
            logging.info(
                "[SkipRMs] Insurance: excluded %d row(s) for %d unique RM name(s) via skip list.",
                _skipped,
                _uniq,
            )
        df = df.loc[_keep_mask].copy()
    finally:
        df.drop(columns=["_emp_name_lc"], inplace=True, errors="ignore")

    # --- Skip rows by Zoho user profile (e.g., Operations, Administrator) ---
    if profiles_by_id:
        try:
            _before_prof = len(df)
            # map profile (lowercased) for each row using employee_id
            df["_emp_profile_lc"] = df["employee_id"].apply(
                lambda v: str(profiles_by_id.get(str(v))) if v is not None else ""
            )
            df["_emp_profile_lc"] = df["_emp_profile_lc"].astype(str).str.strip().str.lower()
            _skip_prof = skip_profiles or set(SKIP_ZOHO_PROFILES)
            _keep_mask_prof = ~df["_emp_profile_lc"].isin(_skip_prof)
            _skipped_prof = int(_before_prof - int(_keep_mask_prof.sum()))
            if _skipped_prof > 0:
                try:
                    _uniq_prof = int(df.loc[~_keep_mask_prof, "_emp_profile_lc"].nunique())
                except Exception:
                    _uniq_prof = _skipped_prof
                logging.info(
                    "[SkipProfile] Insurance: excluded %d row(s) for %d unique profile(s): %s",
                    _skipped_prof,
                    _uniq_prof,
                    ", ".join(
                        sorted(set(df.loc[~_keep_mask_prof, "_emp_profile_lc"].dropna().unique()))
                    )[:120],
                )
            df = df.loc[_keep_mask_prof].copy()
        finally:
            df.drop(columns=["_emp_profile_lc"], inplace=True, errors="ignore")

    # 3. Ensure required columns exist even if missing in source
    required_cols = {
        "this_year_premium": 0,
        "last_year_premium": 0,
        "renewal_notice_premium": 0,
        "policy_type": None,
        "policy_start": pd.NaT,
        "policy_end": pd.NaT,
        "conversion_date": pd.NaT,
        "Direct_Associate": "",
        "product": None,
    }
    for col, default in required_cols.items():
        if col not in df.columns:
            df[col] = default

    # --- Ensure numeric columns are proper floats ---
    numeric_cols = ["this_year_premium", "last_year_premium", "renewal_notice_premium"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # --- deductible flag (dropdown text ➜ boolean) ---
    # Accept any variant of the column name (case / underscore / spacing)
    deductible_col = None
    for col in df.columns:
        if col.strip().lower() == "deductible_in_policy":
            deductible_col = col
            break

    if deductible_col:
        df["deductible_added"] = (
            df[deductible_col].fillna("").astype(str).str.strip().str.lower().str.startswith("yes")
        )
    else:
        df["deductible_added"] = False

    df["premium_delta"] = df["this_year_premium"].fillna(0) - df["last_year_premium"].fillna(0)
    # Add upsell tracking columns
    # Mark as upsell only if last‑year premium is available (>0) to avoid false positives
    df["is_upsell"] = (
        (df["premium_delta"] > 0) & (df["policy_type"] == "Renewal") & (df["last_year_premium"] > 0)
    )
    df["upsell_amount"] = df["premium_delta"].where(df["is_upsell"], 0)

    # --- Fresh-to-Company basis used by QTD/FY bonuses & external aggregations ---
    class_norm = (
        df.get("policy_classification", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .str.lower()
    )
    is_fresh = class_norm.eq("fresh")
    is_port = df.get("is_portability", pd.Series([False] * len(df))).fillna(False).astype(bool)
    has_ren = df.get("has_renewal_date", pd.Series([False] * len(df))).fillna(False).astype(bool)
    upsell_amt = pd.to_numeric(df.get("upsell_amount", 0), errors="coerce").fillna(0)
    df["fresh_premium_eligible"] = df["this_year_premium"].where(
        is_fresh | (is_port & ~has_ren), 0
    ) + upsell_amt.clip(lower=0)

    # --- derive “previous policy end” (one day before current start) ---
    # Ensure prev_policy_end uses a single datetime Series even if policy_start has duplicate-labeled columns
    _ps_obj = df.get("policy_start")
    if isinstance(_ps_obj, pd.DataFrame):
        _ps_series = pd.to_datetime(_ps_obj.bfill(axis=1).iloc[:, 0], errors="coerce")
    else:
        _ps_series = pd.to_datetime(_ps_obj, errors="coerce")
    df["prev_policy_end"] = _ps_series - pd.Timedelta(days=1)
    df["days_to_renewal"] = df.apply(
        lambda r: (
            (r["Renewal_Date"] - r["conversion_date"]).days
            if pd.notnull(r.get("Renewal_Date")) and pd.notnull(r.get("conversion_date"))
            else None
        ),
        axis=1,
    )
    # Persist renewal-date presence and value for downstream logic & writes
    df["has_renewal_date"] = df["Renewal_Date"].notna()
    df["renewal_date"] = df["Renewal_Date"]

    # --- Optional fallback: use policy_start to approximate days_to_renewal when Renewal_Date is missing ---
    # Controlled by env PLI_ENABLE_DTR_FALLBACK in {true,1,yes,on} (case-insensitive). Default: off.
    try:
        _enable_dtr_fb_raw = os.getenv("PLI_ENABLE_DTR_FALLBACK", "false") or "false"
        _ENABLE_DTR_FALLBACK = str(_enable_dtr_fb_raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        _ENABLE_DTR_FALLBACK = False
    if _ENABLE_DTR_FALLBACK:
        # Apply only to obvious renewals with both dates available
        _mask_fb = (
            df["days_to_renewal"].isna()
            & df["policy_start"].notna()
            & df["conversion_date"].notna()
            & df["conversion_status"].astype(str).str.contains("renewal", case=False, na=False)
        )
        if _mask_fb.any():
            df.loc[_mask_fb, "days_to_renewal"] = (
                df.loc[_mask_fb, "policy_start"] - df.loc[_mask_fb, "conversion_date"]
            ).dt.days
            logging.info(
                "DTR fallback applied on %d renewal row(s) using policy_start (PLI_ENABLE_DTR_FALLBACK).",
                int(_mask_fb.sum()),
            )

    df["term_years"] = df.apply(lambda r: classify_term(r["policy_start"], r["policy_end"]), axis=1)
    try:
        logging.debug(
            "Direct_Associate uniques: %s", df["Direct_Associate"].dropna().unique().tolist()
        )
    except Exception:
        pass

    # --- Defensive fallback: if policy_start is missing, fall back to conversion_date for FY gating ---
    if "policy_start" in df.columns and "conversion_date" in df.columns:
        try:
            _ps = pd.to_datetime(df["policy_start"], errors="coerce")
            _cd = pd.to_datetime(df["conversion_date"], errors="coerce")
            missing_ps = _ps.isna()
            if missing_ps.any():
                fill_cnt = int(missing_ps.sum())
                df.loc[missing_ps, "policy_start"] = _cd[missing_ps]
                logging.debug(
                    "Filled %d missing policy_start with conversion_date for FY filter.", fill_cnt
                )
        except Exception:
            pass

    # --- Financial Year filter (India: Apr 1 to Mar 31) ---
    today = pd.Timestamp.today()
    if today.month >= 4:
        fy_start = pd.Timestamp(today.year, 4, 1)
        fy_end = pd.Timestamp(today.year + 1, 3, 31, 23, 59, 59)
        fy_label_start, fy_label_end = today.year, today.year + 1
    else:
        fy_start = pd.Timestamp(today.year - 1, 4, 1)
        fy_end = pd.Timestamp(today.year, 3, 31, 23, 59, 59)
        fy_label_start, fy_label_end = today.year - 1, today.year
    before_cnt = len(df)
    df = df[df["policy_start"].between(fy_start, fy_end, inclusive="both")].copy()
    after_cnt = len(df)
    logging.info(
        "Financial Year filter applied: FY %d-%d; kept %d of %d rows (policy_start within FY).",
        fy_label_start,
        fy_label_end,
        after_cnt,
        before_cnt,
    )
    df = df.reset_index(drop=True)

    # 3. Calculate points (passing loaded config)
    df = df.apply(lambda row: compute_points(row, config=ins_runtime_cfg), axis=1)

    # Safety fill: ensure bonus-ready fields exist for every row
    if "fresh_premium_eligible" not in df.columns:
        df["fresh_premium_eligible"] = 0.0
    if "period_month" not in df.columns:
        if "conversion_date" in df.columns:
            df["period_month"] = (
                pd.to_datetime(df["conversion_date"], errors="coerce").dt.to_period("M").astype(str)
            )
        else:
            df["period_month"] = None

    # (Optional) Audit log: sample monthly fresh-premium sums for bonus basis
    try:
        _bonus_probe = df.loc[
            :, ["employee_id", "period_month", "policy_classification", "fresh_premium_eligible"]
        ].copy()
        _bonus_probe["fresh_premium_eligible"] = pd.to_numeric(
            _bonus_probe["fresh_premium_eligible"], errors="coerce"
        ).fillna(0.0)
        month_summary = (
            _bonus_probe.groupby(["employee_id", "period_month"], dropna=False)[
                "fresh_premium_eligible"
            ]
            .sum()
            .reset_index()
            .sort_values(["period_month", "employee_id"])
            .head(10)
            .to_dict("records")
        )
        logging.info(
            "[INS Bonus Basis] sample monthly fresh-premium sums (first 10): %s", month_summary
        )
    except Exception as _e_bonus_log:
        logging.debug("Skip INS bonus-basis sample log: %s", _e_bonus_log)

    # Keep a lowercase copy for convenience in downstream writes (audit uses 'renewal_date')
    if "Renewal_Date" in df.columns:
        df["renewal_date"] = df["Renewal_Date"]
    else:
        df["renewal_date"] = pd.NaT

    # --- Portability reclassification summary (for diagnostics) ---
    try:
        if "port_reclass" in df.columns:
            v = df["port_reclass"].value_counts(dropna=False).to_dict()
            pf = int(v.get("port→fresh_no_renew_date", 0))
            prl = int(v.get("port→renew_with_last", 0))
            prn = int(v.get("port→renew_no_last", 0))
            logging.info(
                "Portability reclass summary: port→fresh(no Renewal_Date)=%d, port→renew(with last premium)=%d, port→renew(no last premium)=%d",
                pf,
                prl,
                prn,
            )
    except Exception:
        pass

    # --- explode the `points` dict into separate columns for detailed view ---
    # Build points breakout with index aligned to df to avoid misalignment after filters
    point_cols_df = pd.json_normalize(df["points"]).fillna(0)
    point_cols_df.index = df.index
    # Ensure all six keys exist
    for col in ["base", "upsell", "early_renew", "term_bonus", "deductible_bonus", "slab_bonus"]:
        if col not in point_cols_df.columns:
            point_cols_df[col] = 0
    point_cols_df.rename(
        columns={
            "base": "base_points",
            "upsell": "upsell_points",
            "early_renew": "early_renew_points",
            "term_bonus": "term_bonus_points",
            "deductible_bonus": "deductible_bonus_points",
            "slab_bonus": "slab_bonus_points",
        },
        inplace=True,
    )

    # Drop any previously-created breakout columns to avoid duplication on rerun
    df.drop(
        columns=[
            c
            for c in [
                "base_points",
                "upsell_points",
                "early_renew_points",
                "term_bonus_points",
                "deductible_bonus_points",
            ]
            if c in df.columns
        ],
        inplace=True,
        errors="ignore",
    )

    # Concatenate the breakout columns back into the main DataFrame (index will align)
    df = pd.concat([df, point_cols_df], axis=1)

    # --- Sanity fix: no -200 when days_to_renewal is missing (any classification) ---
    if {"days_to_renewal", "base_points"}.issubset(df.columns):
        _bp_num = pd.to_numeric(df["base_points"], errors="coerce")
        mask_violation = df["days_to_renewal"].isna() & (_bp_num == -200)
        if mask_violation.any():
            offenders = df.loc[
                mask_violation,
                [
                    "lead_id",
                    "policy_number",
                    "conversion_status",
                    "policy_type",
                    "policy_classification",
                ],
            ]
            logging.info(
                "Sanity fix: %d rows had base_points=-200 with days_to_renewal=None. Resetting base_points to 0.",
                int(mask_violation.sum()),
            )
            logging.debug("Offenders (sample): %s", offenders.head(10).to_dict("records"))
            df.loc[mask_violation, "base_points"] = 0
            _total = pd.to_numeric(df.loc[mask_violation, "base_points"], errors="coerce").fillna(
                0
            ) + pd.to_numeric(df.loc[mask_violation, "upsell_points"], errors="coerce").fillna(0)
            df.loc[mask_violation, "total_points"] = _total

    # --- Repair: ensure renewal base_points reflect days_to_renewal bands when accidentally left at 0 ---
    try:
        bp_num = pd.to_numeric(df["base_points"], errors="coerce").fillna(0)
        dtr = pd.to_numeric(df["days_to_renewal"], errors="coerce")
        class_norm = (
            df.get("policy_classification", pd.Series([""] * len(df), index=df.index))
            .astype(str)
            .str.lower()
        )
        mask = (class_norm == "renewal") & (bp_num == 0) & dtr.notna()
        if mask.any():

            def _renewal_base(x):
                if x > 30:
                    return 175
                if 30 >= x > 14:
                    return 100
                if 14 >= x > 7:
                    return 50
                if 7 >= x >= -1:
                    return 35
                if -2 >= x >= -7:
                    return 20
                if -8 >= x >= -15:
                    return -100
                if -16 >= x >= -29:
                    return -150
                return -200  # ≤ -30

            df.loc[mask, "base_points"] = dtr[mask].apply(_renewal_base).astype(int)
            # NOTE: Do not recompute total_points here; weight_factor is computed later and
            # total_points is finalized afterwards to avoid KeyError/ordering issues.
    except Exception:
        pass

    # --- Auto-correct: FRESH rows should never have renewal-like slabs ---
    try:
        renewal_like = {175, 50, 35, 20, -100, -150, -200}
        # Robust access for type-checkers and missing columns
        _class_series = (
            df["policy_classification"]
            if "policy_classification" in df.columns
            else pd.Series([""] * len(df), index=df.index)
        )
        class_norm = _class_series.astype(str).str.strip().str.lower()

        _bp_series = (
            df["base_points"]
            if "base_points" in df.columns
            else pd.Series([0] * len(df), index=df.index)
        )
        bp_num = pd.to_numeric(_bp_series, errors="coerce")
        if {"this_year_premium", "term_years"}.issubset(df.columns):
            mask_fresh_mismatch = (class_norm == "fresh") & (bp_num.isin(list(renewal_like)))
            if mask_fresh_mismatch.any():
                fix_rows = df.loc[mask_fresh_mismatch, ["this_year_premium", "term_years"]].copy()
                avg_annual = fix_rows["this_year_premium"].astype(float) / fix_rows[
                    "term_years"
                ].replace(0, 1).astype(float)
                import numpy as _np

                conds = [
                    avg_annual < 25_000,
                    (avg_annual >= 25_000) & (avg_annual < 75_000),
                    (avg_annual >= 75_000) & (avg_annual < 200_000),
                    avg_annual >= 200_000,
                ]
                choices = [40, 100, 250, 350]
                corrected = _np.select(conds, choices, default=40)
                df.loc[mask_fresh_mismatch, "base_points"] = corrected
                _tp_corr = pd.to_numeric(
                    df.loc[mask_fresh_mismatch, "base_points"], errors="coerce"
                ).fillna(0) + pd.to_numeric(
                    df.loc[mask_fresh_mismatch, "upsell_points"], errors="coerce"
                ).fillna(
                    0
                )
                df.loc[mask_fresh_mismatch, "total_points"] = _tp_corr
                logging.warning(
                    "Auto-corrected %d FRESH rows that had renewal-like base slabs to fresh slabs (40/100/250/350).",
                    int(mask_fresh_mismatch.sum()),
                )
    except Exception:
        pass

    # Diagnostic: fresh rows showing renewal-like base slabs (should not happen)
    try:
        renewal_like = {175, 50, 35, 20, -100, -150, -200}
        _class_series = (
            df["policy_classification"]
            if "policy_classification" in df.columns
            else pd.Series([""] * len(df), index=df.index)
        )
        class_norm = _class_series.astype(str).str.strip().str.lower()

        _bp_series = (
            df["base_points"]
            if "base_points" in df.columns
            else pd.Series([0] * len(df), index=df.index)
        )
        bp_num = pd.to_numeric(_bp_series, errors="coerce")
        mask_fresh_mismatch = (class_norm == "fresh") & (bp_num.isin(list(renewal_like)))
        if mask_fresh_mismatch.any():
            bad = df.loc[
                mask_fresh_mismatch,
                [
                    "lead_id",
                    "conversion_status",
                    "policy_type",
                    "this_year_premium",
                    "term_years",
                    "days_to_renewal",
                    "base_points",
                ],
            ]
            logging.warning(
                "Found %d FRESH rows with renewal-like base points. Check classification / slabs.",
                int(mask_fresh_mismatch.sum()),
            )
            logging.debug("Examples: %s", bad.head(10).to_dict("records"))
    except Exception:
        pass

    # Classification already set in compute_points; ensure columns exist
    if "policy_classification" not in df.columns:
        df["policy_classification"] = None

    # Apply weights per finalized rules
    def _weight(row):
        # Short-circuit for ULIP/Traditional
        if row.get("is_ulip"):
            return 0.0

        policy_type_lower = str(row.get("policy_type") or "").lower()
        classification = row.get("policy_classification")  # 'renewal' or 'fresh'

        # --- Tenure weight ---
        _bp = row.get("base_points")
        try:
            base_pts = int(_bp) if pd.notna(_bp) else 0
        except Exception:
            base_pts = 0
        _ty = row.get("term_years")
        try:
            term_years = int(_ty) if pd.notna(_ty) and int(_ty) > 0 else 1
        except Exception:
            term_years = 1
        if classification == "renewal":
            if base_pts >= 0:
                tenure_w = (
                    1.00
                    if term_years == 1
                    else (
                        1.10
                        if term_years == 2
                        else 1.25 if term_years == 3 else 1.35 if term_years == 4 else 1.50
                    )
                )
            else:
                tenure_w = (
                    1.00
                    if term_years == 1
                    else (
                        0.90
                        if term_years == 2
                        else 0.75 if term_years == 3 else 0.65 if term_years == 4 else 0.50
                    )
                )
        else:  # fresh/port
            tenure_w = (
                1.00
                if term_years == 1
                else (
                    1.20
                    if term_years == 2
                    else 1.60 if term_years == 3 else 1.75 if term_years == 4 else 2.00
                )
            )

        # --- Deductible weight (fresh only) ---
        deductible_w = 1.0
        if classification == "fresh" and (row.get("deductible_added") is True):
            deductible_w = 1.15

        # --- Portability weight (no penalty) ---
        port_w = 1.0

        # --- Category weight ---
        cat_w = 1.0
        if any(
            k in policy_type_lower
            for k in ["motor", "fire", "burglary", "burgulary", "marine", "misc"]
        ):
            cat_w = 0.40
        if any(k in policy_type_lower for k in ["gmc"]) and ("otc" not in policy_type_lower):
            cat_w = 0.20
        if "gpa" in policy_type_lower:
            cat_w = 0.20
        if "gmc-otc" in policy_type_lower or "gmc otc" in policy_type_lower:
            cat_w = 0.50
        if "term insurance" in policy_type_lower:
            cat_w = 1.00

        # --- Associate weight via Direct_Associate ---
        da_text = str(row.get("Direct_Associate") or "").strip().lower()
        associate_w = 0.25 if da_text == "associate client" else 1.00

        # --- Cashback weight (Referral_Fee1 % precedence) ---
        def _as_float(v, default=None):
            try:
                if v is None:
                    return default
                if isinstance(v, str):
                    v = v.strip().replace("%", "")
                    if v == "":
                        return default
                return float(v)
            except Exception:
                return default

        def _get_percent(row, keys):
            for k in keys:
                if k in row and row.get(k) is not None:
                    val = _as_float(row.get(k), None)
                    if val is not None:
                        return max(0.0, val)
            return None

        _tp = row.get("this_year_premium")
        try:
            this_prem = float(_tp) if pd.notna(_tp) else 0.0
        except Exception:
            this_prem = 0.0
        is_term = bool(row.get("is_term")) if row.get("is_term") is not None else False

        cb_percent = _get_percent(row, ["Referral_Fee1"])  # primary
        cashback_source = None
        if cb_percent is not None and cb_percent > 0:
            cashback_source = "Referral_Fee1_percent"
        else:
            cb_percent = _get_percent(
                row,
                [
                    "Cashback %",
                    "cashback %",
                    "Discount %",
                    "Discount",
                    "cashback_percent",
                    "Cashback_Percentage",
                    "Cashback",
                    "Discount_Perc",
                    "Cashback_Perc",
                    "Referral_Fee_%",
                    "Referral_Fee_Percentage",
                    "Merged_Referral_Fee_%",
                ],
            )
            if cb_percent is None or cb_percent == 0:
                # last resort: convert Merged_Referral_Fee amount to % of premium
                rf_amt = _as_float(row.get("Merged_Referral_Fee"), None)
                if rf_amt is not None and rf_amt > 0 and this_prem > 0:
                    cb_percent = (rf_amt / this_prem) * 100.0
                    cashback_source = "merged_referral_fee_amount_to_percent"
            else:
                cashback_source = "cashback_or_discount_percent"

        cashback_w = 1.0
        if cb_percent is not None and cb_percent > 0:
            if is_term:
                cashback_w = (
                    0.80
                    if 0 < cb_percent <= 5
                    else 0.50 if 5 < cb_percent <= 10 else 0.25 if 10 < cb_percent <= 15 else 0.00
                )
            else:
                cashback_w = (
                    0.80
                    if 0 < cb_percent <= 4
                    else 0.50 if 4 < cb_percent <= 8 else 0.25 if 8 < cb_percent <= 10 else 0.00
                )

        total_w = tenure_w * deductible_w * port_w * cat_w * associate_w * cashback_w
        return round(float(total_w), 3)

    if "base_points" in df.columns:
        df["base_points"] = pd.to_numeric(df["base_points"], errors="coerce").fillna(0)
    df["weight_factor"] = df.apply(_weight, axis=1)
    df["total_points"] = (
        (df["base_points"].fillna(0) + df["upsell_points"].fillna(0)) * df["weight_factor"]
    ).round(2)

    # Add verification log for the -200 rule
    if "days_to_renewal" in df.columns and "base_points" in df.columns:
        _dtr_num = pd.to_numeric(df["days_to_renewal"], errors="coerce")
        _bp_num = pd.to_numeric(df["base_points"], errors="coerce")
        v1 = int(((_dtr_num <= -30) & (_bp_num == -200)).sum())
        v2 = int((df["days_to_renewal"].isna() & (_bp_num == -200)).sum())
        logging.info(
            "Verification: -200 only when days_to_renewal<=-30 → matches=%d, NONE→-200 violations=%d",
            v1,
            v2,
        )

    # Column harden: ensure 'deductible_in_policy' exists for projection
    if "deductible_in_policy" not in df.columns:
        df["deductible_in_policy"] = None

    # Prepare the final dataframe to persist
    df_out = df[
        [
            "lead_id",
            "policy_number",
            "conversion_status",
            "policy_classification",
            "base_points",
            "upsell_points",
            "weight_factor",
            "total_points",
            "premium_delta",
            "days_to_renewal",
            "term_years",
            "deductible_in_policy",
            "deductible_added",
            "this_year_premium",
            "last_year_premium",
            "is_portability",
            "has_renewal_date",
            "renewal_date",
            "upsell_amount",
            "policy_start",
            "policy_end",
            "conversion_date",
            "policy_type",
            "product",
            "Direct_Associate",
            "processing_user",
            "employee_name",
            "employee_id",
            "client_name",
            "fresh_premium_eligible",
            # period_month is derived from conversion_date earlier in the pipeline
            "period_month",
        ]
    ].copy()

    # Persist to MongoDB: upsert by (lead_id, policy_number)
    if mongo_collection is not None:
        try:
            ops = []
            skipped_missing_keys = 0
            now = dt.datetime.utcnow()

            for _, r in df_out.iterrows():
                lead_id = r.get("lead_id")
                policy_number = r.get("policy_number")

                if lead_id in (None, "") or policy_number in (None, ""):
                    skipped_missing_keys += 1
                    continue

                row_dict = r.to_dict()

                # --- Force key identity fields + bonus basis helpers ---
                period_month = row_dict.get("period_month")
                if not period_month:
                    conv_raw = row_dict.get("conversion_date")
                    conv_dt = (
                        pd.to_datetime(conv_raw, errors="coerce")
                        if conv_raw not in (None, "")
                        else None
                    )
                    if conv_dt is not None and pd.notna(conv_dt):
                        period_month = conv_dt.to_period("M").strftime("%Y-%m")

                row_dict.update(
                    {
                        "lead_id": str(r.get("lead_id") or ""),
                        "policy_number": r.get("policy_number"),
                        "employee_id": str(r.get("employee_id") or ""),
                        "employee_name": r.get("employee_name"),
                        "conversion_date": r.get("conversion_date"),
                        "base_points": int(r.get("base_points") or 0),
                        "upsell_points": int(r.get("upsell_points") or 0),
                        "total_points": float(r.get("total_points") or 0.0),
                        # --- Persisted bonus basis for Q/FY aggregation ---
                        "policy_classification": str(r.get("policy_classification") or ""),
                        "fresh_premium_eligible": float(r.get("fresh_premium_eligible") or 0.0),
                        "period_month": str(r.get("period_month") or period_month),
                        "days_to_renewal": (
                            int(r.get("days_to_renewal"))
                            if pd.notna(r.get("days_to_renewal"))
                            else None
                        ),
                    }
                )

                doc = _sanitize_doc(row_dict)
                if not isinstance(doc, dict):
                    # Defensive: _sanitize_doc should return a dict when given a dict;
                    # if not, coerce to empty dict to avoid type/subscript issues.
                    doc = {}
                doc["updated_at"] = now
                doc = _sanitize_doc(doc)

                ops.append(
                    UpdateOne(
                        {"lead_id": lead_id, "policy_number": policy_number},
                        {"$set": doc},
                        upsert=True,
                    )
                )

            upserts_cnt = 0
            modified_cnt = 0
            total_calls = len(ops)

            if ops:
                res = mongo_collection.bulk_write(ops, ordered=False)
                try:
                    modified_cnt = int(getattr(res, "modified_count", 0) or 0)
                except Exception:
                    modified_cnt = 0
                try:
                    upserts_cnt = len(getattr(res, "upserted_ids", {}) or {})
                except Exception:
                    upserts_cnt = 0

            logging.info(
                "Insurance_Policy_Scoring writes: upserts=%d, modified=%d, total_update_calls=%d, skipped_missing_keys=%d",
                upserts_cnt,
                modified_cnt,
                total_calls,
                skipped_missing_keys,
            )
        except Exception as e:
            logging.error("Error upserting Insurance_Policy_Scoring: %s", e, exc_info=True)

    # --- Insurance_Audit mirror (row-wise audit with period_month for reporting) ---
    try:
        audit_coll = connect_to_mongo("Insurance_audit")
        if audit_coll is not None and not df_out.empty:
            try:
                audit_coll.create_index(
                    [
                        ("lead_id", pymongo.ASCENDING),
                        ("policy_number", pymongo.ASCENDING),
                    ],
                    name="uniq_lead_policy",
                    unique=False,
                )
            except Exception:
                pass

            audit_ops = []
            skipped_zero = 0
            skipped_inactive = 0
            skipped_by_name = 0
            skipped_by_profile = 0
            now_utc = dt.datetime.utcnow()

            for _, r in df_out.iterrows():
                lead_id = r.get("lead_id")
                policy_number = r.get("policy_number")

                if lead_id in (None, "") or policy_number in (None, ""):
                    skipped_zero += 1
                    continue

                conv_raw = r.get("conversion_date")
                conv_raw_any: Any = conv_raw
                try:
                    conv_dt = pd.to_datetime(conv_raw_any, errors="coerce")
                except Exception:
                    conv_dt = pd.NaT

                if conv_dt is not None and pd.notna(conv_dt):
                    period_month = conv_dt.to_period("M").strftime("%Y-%m")
                else:
                    period_month = None

                doc = {
                    "lead_id": str(lead_id),
                    "policy_number": policy_number,
                    "conversion_date": conv_dt,
                    "period_month": period_month,
                    "employee_id": str(r.get("employee_id") or ""),
                    "employee_name": r.get("employee_name"),
                    "client_name": r.get("client_name"),
                    "policy_classification": r.get("policy_classification"),
                    "base_points": int(r.get("base_points") or 0),
                    "upsell_points": int(r.get("upsell_points") or 0),
                    "total_points": float(r.get("total_points") or 0.0),
                    "premium_delta": float(r.get("premium_delta") or 0.0),
                    "this_year_premium": float(r.get("this_year_premium") or 0.0),
                    "last_year_premium": float(r.get("last_year_premium") or 0.0),
                    "is_portability": bool(r.get("is_portability") or False),
                    "has_renewal_date": bool(r.get("has_renewal_date") or False),
                    "renewal_date": r.get("renewal_date"),
                    "upsell_amount": float(r.get("upsell_amount") or 0.0),
                    "policy_start": r.get("policy_start"),
                    "policy_end": r.get("policy_end"),
                    "policy_type": r.get("policy_type"),
                    "product": r.get("product"),
                    "Direct_Associate": r.get("Direct_Associate"),
                    "fresh_premium_eligible": float(r.get("fresh_premium_eligible") or 0.0),
                    "updated_at": now_utc,
                }

                # Sanitize for MongoDB: handle NaT, tz-aware datetimes, NaN, etc.
                doc = _sanitize_doc(doc)

                audit_ops.append(
                    UpdateOne(
                        {"lead_id": lead_id, "policy_number": policy_number},
                        {"$set": doc},
                        upsert=True,
                    )
                )

            upserts_a = 0
            modified_a = 0
            total_a = len(audit_ops)
            if audit_ops:
                res_a = audit_coll.bulk_write(audit_ops, ordered=False)
                try:
                    modified_a = int(getattr(res_a, "modified_count", 0) or 0)
                except Exception:
                    modified_a = 0
                try:
                    upserts_a = len(getattr(res_a, "upserted_ids", {}) or {})
                except Exception:
                    upserts_a = 0

            logging.info(
                "Insurance_Audit writes: upserts=%d, modified=%d, total_update_calls=%d, skipped_zero=%d, skipped_inactive=%d, skipped_by_name=%d, skipped_by_profile=%d, total_rows=%d",
                upserts_a,
                modified_a,
                total_a,
                skipped_zero,
                skipped_inactive,
                skipped_by_name,
                skipped_by_profile,
                len(df_out),
            )
    except Exception as e:
        logging.error("Error upserting Insurance_audit: %s", e, exc_info=True)

    # --- 20% leader credit (INS vs MF bucket) ---
    try:
        upsert_insurance_mf_leaders(df_out, profiles_by_id)
    except Exception:
        logging.exception("Leader 20% credit step failed")
    return df_out


# --- leaderboard helper ---
def update_leaderboard(df: pd.DataFrame, leaderboard_col, active_ids):
    """
    Upsert a per‑lead entry in the *Leaderboard* collection.

    Schema
    ------
    employee_name        – Processing_User.name (or str if already a string)
    lead_id              – CRM Lead_ID
    points               – integer (can be negative)
    justification        – human‑readable breakup of points
    updated_at           – UTC timestamp of this run
    """
    if leaderboard_col is None:
        logging.warning("Leaderboard collection handle is None – skipping leaderboard update.")
        return

    now = dt.datetime.utcnow()

    for _, row in df.iterrows():
        # --- employee name / id ---
        processing_usr = row.get("processing_user")
        if isinstance(processing_usr, dict):
            employee_name = processing_usr.get("name")
            employee_id = processing_usr.get("id")
        else:
            employee_name = processing_usr
            employee_id = None

        # Skip & purge entries for inactive employees
        if not employee_id or str(employee_id) not in active_ids:
            if employee_id:
                leaderboard_col.delete_many({"employee_id": employee_id})
            continue

        # --- justification string (include only non‑zero buckets) ---
        parts = []
        if row.get("base_points", 0):
            parts.append(f"{row['base_points']} pts for premium")
        if row.get("upsell_points", 0):
            parts.append(f"{row['upsell_points']} pts for upsell")
        if row.get("early_renew_points", 0):
            parts.append(f"{row['early_renew_points']} pts for early renewal")
        if row.get("term_bonus_points", 0):
            parts.append(f"{row['term_bonus_points']} pts for term")
        if row.get("deductible_bonus_points", 0):
            parts.append(f"{row['deductible_bonus_points']} pts for deductible")
        if row.get("slab_bonus_points", 0):
            parts.append(f"{row['slab_bonus_points']} pts for high‑premium slab")

        justification = "; ".join(parts) if parts else "0 pts"

        doc = {
            "employee_name": employee_name,
            "employee_id": employee_id,
            "lead_id": row["lead_id"],
            "points": int(row["total_points"] or 0),
            "justification": justification,
            "weight_factor": row["weight_factor"],
            "updated_at": now,
        }

        # upsert on lead_id + employee_id for uniqueness
        leaderboard_col.update_one(
            {"lead_id": row["lead_id"], "employee_id": employee_id},
            {"$set": doc},
            upsert=True,
        )


# ---------------------------------------------------------------------
# Monthly ≥3 L fresh/portability bonus  +  quarterly “hat‑trick” bonus
# ---------------------------------------------------------------------
def award_monthly_quarterly_bonus(df: pd.DataFrame, leaderboard_col):
    """
    • For each employee, for each calendar‑month, add **2 000 pts**
      if their Fresh / Portability premium > ₹3 lakh.
    • If an employee achieves that target for **three consecutive
      months** (a hat‑trick) within the same rolling window, add an
      **extra 5 000 pts** (once per hat‑trick sequence).

    Two separate rows are written to *Leaderboard*:
      reason = "Monthly 3L Bonus"           (2 000‑pt rows)
      reason = "Quarterly Hattrick Bonus"   (5 000‑pt rows)
    """
    if leaderboard_col is None or df.empty:
        return

    # ------------------------------------------------------------
    # 1)  isolate fresh / portability rows and bucket by Month
    # ------------------------------------------------------------
    fresh_mask = df["conversion_status"].str.contains("fresh", case=False, na=False) | df[
        "conversion_status"
    ].str.contains("port", case=False, na=False)
    sub = df.loc[
        fresh_mask, ["employee_id", "employee_name", "conversion_date", "this_year_premium"]
    ].copy()

    # ignore rows without an employee_id
    sub = sub[sub["employee_id"].notna()]
    if sub.empty:
        return

    _sub_conv = cast(pd.Series, sub.loc[:, "conversion_date"])
    sub["month"] = pd.to_datetime(_sub_conv, errors="coerce").dt.to_period("M")

    monthly = (
        sub.groupby(["employee_id", "employee_name", "month"])["this_year_premium"]
        .sum()
        .reset_index()
    )

    # ------------------------------------------------------------
    # 2)  2 000‑pt monthly bonuses
    # ------------------------------------------------------------
    monthly_bonus_rows = monthly[monthly["this_year_premium"] > 300_000]
    for _, r in monthly_bonus_rows.iterrows():
        doc = {
            "employee_name": r["employee_name"],
            "employee_id": r["employee_id"],
            "lead_id": None,
            "points": 2000,
            "reason": "Monthly 3L Bonus",
            "justification": f"₹{int(r['this_year_premium']):,} fresh/port premium in {r['month']}",
            "updated_at": dt.datetime.utcnow(),
        }
        leaderboard_col.update_one(
            {
                "employee_id": r["employee_id"],
                "reason": "Monthly 3L Bonus",
                "justification": doc["justification"],
            },
            {"$set": doc},
            upsert=True,
        )

    # ------------------------------------------------------------
    # 3)  Hat‑trick + extended‑streak bonuses
    #     • first 3‑month run  → +5 000 once
    #     • every *additional* consecutive month beyond the hat‑trick → +2 000
    # ------------------------------------------------------------
    target_months = monthly_bonus_rows.groupby("employee_id")["month"].apply(
        lambda s: sorted(list(s))
    )

    for emp_id, months in target_months.items():
        if not months:
            continue

        emp_name = monthly_bonus_rows.loc[
            monthly_bonus_rows["employee_id"] == emp_id, "employee_name"
        ].iloc[0]

        streak = 0
        prev_m = None
        hattrick_awarded = False

        for m in months:
            # consecutive‑month tracking
            if prev_m is not None and prev_m + 1 == m:
                streak += 1
            else:
                streak = 1  # reset streak
            prev_m = m

            # --- bonuses ---
            if streak == 3 and not hattrick_awarded:
                # hat‑trick → 5 000 (only once per sequence)
                justification = (
                    f"Hat‑trick: ≥3 L fresh/port premium for "
                    f"{months[months.index(m)-2]}, {months[months.index(m)-1]}, {m}"
                )
                doc = {
                    "employee_name": emp_name,
                    "employee_id": emp_id,
                    "lead_id": None,
                    "points": 5000,
                    "reason": "Quarterly Hattrick Bonus",
                    "justification": justification,
                    "updated_at": dt.datetime.utcnow(),
                }
                leaderboard_col.update_one(
                    {
                        "employee_id": emp_id,
                        "reason": "Quarterly Hattrick Bonus",
                        "justification": justification,
                    },
                    {"$set": doc},
                    upsert=True,
                )
                hattrick_awarded = True

            elif streak > 3:
                # every month beyond hat‑trick → +2 000
                justification = (
                    f"Extended streak: ≥3 L fresh/port premium for {m} " f"(streak {streak} months)"
                )
                doc = {
                    "employee_name": emp_name,
                    "employee_id": emp_id,
                    "lead_id": None,
                    "points": 2000,
                    "reason": "Extended 3L Streak Bonus",
                    "justification": justification,
                    "updated_at": dt.datetime.utcnow(),
                }
                leaderboard_col.update_one(
                    {
                        "employee_id": emp_id,
                        "reason": "Extended 3L Streak Bonus",
                        "justification": justification,
                    },
                    {"$set": doc},
                    upsert=True,
                )


def upsert_monthly_leaderboard(
    df: pd.DataFrame, monthly_col, active_ids: set[str] | set | None = None
):
    """
    V1: Public Leaderboard is KPI-only.
        • Key = (employee_id, period_month)
        • Do NOT persist payout %, amounts, tiers, or insurance MF bonus amounts here.
    Aggregate per employee per calendar month and compute payout using the finalized slab table.
    Sources:
      • Points: df['total_points'] (already weighted) per policy's conversion month
      • Premiums: sum of this_year_premium split by policy_classification (fresh vs renewal)
      • Bonus points: re-computed here:
          - +2,000 for months with Fresh/Portability premium > ₹3L
          - +5,000 on the third consecutive qualifying month (hat-trick)
          - +2,000 for each additional consecutive month beyond the hat-trick
    Writes to: PLI_Leaderboard.Leaderboard with upsert key (employee_id, period_month)
    """
    if monthly_col is None or df is None or df.empty:
        return

    db_handle = getattr(monthly_col, "database", None)

    # Ensure the minimal fields exist
    base = df.copy()
    _conv_series = cast(pd.Series, base.loc[:, "conversion_date"])
    base["month"] = pd.to_datetime(_conv_series, errors="coerce").dt.to_period("M")
    base = base[base["employee_id"].notna() & base["month"].notna()].copy()
    base["period_month"] = base["month"].astype(str)  # canonical 'YYYY-MM' for logs/audit

    # Keep all employees; only tag active/inactive for the final docs
    active_set = {str(x) for x in active_ids} if active_ids else None

    # Split premiums by classification
    class_norm = (
        base.get("policy_classification", pd.Series([""] * len(base))).astype(str).str.lower()
    )
    is_fresh = class_norm.eq("fresh")

    is_port = base.get("is_portability", pd.Series([False] * len(base))).fillna(False).astype(bool)
    has_ren = (
        base.get("has_renewal_date", pd.Series([False] * len(base))).fillna(False).astype(bool)
    )
    port_no_ren = is_port & ~has_ren

    if "upsell_amount" in base.columns:
        upsell_src = base["upsell_amount"]
    else:
        upsell_src = pd.Series([0] * len(base), index=base.index)
    upsell_amt = pd.to_numeric(upsell_src, errors="coerce").fillna(0)

    base["fresh_premium_component"] = base["this_year_premium"].where(is_fresh, 0)
    base["renewal_premium_component"] = base["this_year_premium"].where(~is_fresh, 0)

    # Fresh-to-Company = Fresh + Port(no Renewal_Date) + positive upsell
    base["fresh_to_company_component"] = base["this_year_premium"].where(
        is_fresh | port_no_ren, 0
    ) + upsell_amt.clip(lower=0)

    # Core monthly aggregation
    grp = (
        base.groupby(["employee_id", "employee_name", "month", "period_month"], dropna=False)
        .agg(
            points_policy=("total_points", "sum"),
            fresh_premium=("fresh_premium_component", "sum"),
            renewal_premium=("renewal_premium_component", "sum"),
            fresh_to_company_premium=("fresh_to_company_component", "sum"),
        )
        .reset_index()
    )
    # --- Build bonus lookup maps (credit *points* only at period-ends; basis is fresh-to-company) ---
    try:
        # Prepare a minimal detail frame for the helper (YYYY-MM string month)
        _detail = base.copy()
        _detail["period_month"] = _detail["month"].astype(str)
        _detail["fresh_premium_eligible"] = _detail["fresh_to_company_component"]
        (
            bonus_q_points_map,
            bonus_a_points_map,
            month_basis_map,
            q_basis_map,
            fy_basis_map,
            q_end_flag_map,
            fy_end_flag_map,
        ) = _bonus_maps_from_scored(
            _detail[["employee_id", "period_month", "fresh_premium_eligible", "conversion_date"]]
        )
    except Exception as _e_bonus_map:
        logging.warning("Bonus map build failed; continuing without bonus fields: %s", _e_bonus_map)
        bonus_q_points_map = bonus_a_points_map = {}
        month_basis_map = q_basis_map = fy_basis_map = {}
        q_end_flag_map = fy_end_flag_map = {}
    # --- Fiscal Year/Quarter (India: Apr–Mar) and Quarterly/Annual bonus tables ---
    grp["_month_ts"] = grp["month"].dt.to_timestamp()
    grp["fy"] = grp["_month_ts"].dt.to_period("Y-MAR")
    grp["fq"] = grp["_month_ts"].dt.to_period("Q-MAR")

    def _quarterly_bonus_amount(total: float) -> int:
        x = float(total or 0)
        if 1_500_000 <= x < 1_700_000:
            return 3200
        if 1_700_000 <= x < 2_000_000:
            return 9000
        if 2_000_000 <= x < 2_500_000:
            return 17_500
        if x >= 2_500_000:
            return 31_000
        return 0

    def _annual_bonus_amount(total: float) -> int:
        x = float(total or 0)
        if 6_000_000 <= x < 7_500_000:
            return 20_000
        if 7_500_000 <= x < 9_000_000:
            return 50_000
        if 9_000_000 <= x < 10_000_000:
            return 75_000
        if x >= 10_000_000:
            return 100_000
        return 0

    # --- Test overrides for bonus crediting (env-driven; easy to toggle and revert) ---
    try:
        _Q_SCALE = float(os.getenv("PLI_BONUS_Q_SCALE", "1") or "1")
    except Exception:
        _Q_SCALE = 1.0
    try:
        _A_SCALE = float(os.getenv("PLI_BONUS_A_SCALE", "1") or "1")
    except Exception:
        _A_SCALE = 1.0

    # 'all' or a specific 'YYYY-MM' to force that month to behave like a quarter/FY-end
    _FORCE_QEND = (os.getenv("PLI_BONUS_FORCE_QEND", "") or "").strip().lower()
    _FORCE_FYEND = (os.getenv("PLI_BONUS_FORCE_FYEND", "") or "").strip().lower()
    # Aggregate fresh-to-company totals per fiscal quarter/year
    q_totals = (
        grp.groupby(["employee_id", "fq"], dropna=False)["fresh_to_company_premium"]
        .sum()
        .reset_index()
    )
    a_totals = (
        grp.groupby(["employee_id", "fy"], dropna=False)["fresh_to_company_premium"]
        .sum()
        .reset_index()
    )

    # Credit the quarterly bonus in the quarter-end month; annual bonus in FY-end month (March)
    quarter_bonus_by_month: dict[tuple[str, str], int] = {}
    for _, rq in q_totals.iterrows():
        amt = _quarterly_bonus_amount(rq["fresh_to_company_premium"])
        if amt:
            q_end_month = rq["fq"].asfreq("M", "end")
            key = (str(rq["employee_id"]), str(q_end_month))  # e.g., ('2969...','2025-09')
            quarter_bonus_by_month[key] = quarter_bonus_by_month.get(key, 0) + int(amt)

    annual_bonus_by_month: dict[tuple[str, str], int] = {}
    for _, ra in a_totals.iterrows():
        amt = _annual_bonus_amount(ra["fresh_to_company_premium"])
        if amt:
            fy_end_month = ra["fy"].asfreq("M", "end")  # March for Y-MAR
            key = (str(ra["employee_id"]), str(fy_end_month))
            annual_bonus_by_month[key] = annual_bonus_by_month.get(key, 0) + int(amt)

    # --- Build QTD / FYTD basis maps for forcing and audit ---
    _grp_q = grp.sort_values(["employee_id", "fq", "month"]).copy()
    _grp_q["qtd_fresh_to_company"] = _grp_q.groupby(["employee_id", "fq"], dropna=False)[
        "fresh_to_company_premium"
    ].cumsum()
    qtd_map = {
        (str(r.employee_id), str(r.period_month)): float(
            getattr(r, "qtd_fresh_to_company", 0.0) or 0.0
        )
        for r in _grp_q.itertuples(index=False)
    }

    _grp_a = grp.sort_values(["employee_id", "fy", "month"]).copy()
    _grp_a["fytd_fresh_to_company"] = _grp_a.groupby(["employee_id", "fy"], dropna=False)[
        "fresh_to_company_premium"
    ].cumsum()
    fytd_map = {
        (str(r.employee_id), str(r.period_month)): float(
            getattr(r, "fytd_fresh_to_company", 0.0) or 0.0
        )
        for r in _grp_a.itertuples(index=False)
    }

    # Re-compute monthly+streak bonus points
    grp.sort_values(["employee_id", "month"], inplace=True)
    grp["points_bonus"] = 0
    for emp_id, sub in grp.groupby("employee_id", sort=False):
        idxs = list(sub.index)
        months = list(sub["month"])
        qualifies = (sub["fresh_to_company_premium"] > 300_000).tolist()
        bonuses = [0] * len(sub)
        streak = 0
        prev_m = None
        for i, q in enumerate(qualifies):
            if q:
                bonuses[i] += 2000  # monthly bonus
                m = months[i]
                if prev_m is not None and (prev_m + 1) == m:
                    streak += 1
                else:
                    streak = 1
                prev_m = m
                if streak == 3:
                    bonuses[i] += 5000  # hat-trick month
                elif streak > 3:
                    bonuses[i] += 2000  # extended streak month
            else:
                streak = 0
                prev_m = months[i]
        grp.loc[idxs, "points_bonus"] = bonuses

    grp["points_total"] = (grp["points_policy"].fillna(0) + grp["points_bonus"].fillna(0)).round(0)

    # Apply payout slabs and compute payout amounts
    out_docs = []
    now = dt.datetime.utcnow()
    for _, r in grp.iterrows():
        points_total = int(round(float(r["points_total"] or 0)))
        emp_id = str(r.get("employee_id"))
        month_str = str(r["month"])  # 'YYYY-MM'
        points_total = int(round(float(r["points_total"] or 0)))
        emp_id = str(r.get("employee_id"))
        month_str = str(r["month"])
        # --- Bonus points & audit basis (safe: defaults to zero/False) ---
        try:
            bq_pts = int(bonus_q_points_map.get((emp_id, month_str), 0))
            ba_pts = int(bonus_a_points_map.get((emp_id, month_str), 0))
            basis_m = float(month_basis_map.get((emp_id, month_str), 0.0))
            basis_q = float(q_basis_map.get((emp_id, month_str), 0.0))
            basis_f = float(fy_basis_map.get((emp_id, month_str), 0.0))
            is_qe = bool(q_end_flag_map.get((emp_id, month_str), False))
            is_fye = bool(fy_end_flag_map.get((emp_id, month_str), False))
        except Exception:
            bq_pts = ba_pts = 0
            basis_m = basis_q = basis_f = 0.0
            is_qe = is_fye = False
        raw_period_month = r.get("period_month")
        if raw_period_month is None or (
            isinstance(raw_period_month, float) and pd.isna(raw_period_month)
        ):
            period_month = month_str
        else:
            period_month = str(raw_period_month)
        # guard: never write rows with empty month → would create period_month=None collisions
        if not period_month:
            logging.warning(
                "[MonthlyLB] Skipping row for employee_id=%s due to empty period_month.",
                emp_id,
            )
            continue

        # Fetch the profile for this employee_id
        profile = str(_PROFILES_BY_ID.get(emp_id, "")).strip()
        prof = profile.lower()

        # --- scheme metadata immediately after computing prof ---
        scheme = "MF" if prof == "mutual funds" else "Standard"
        tier_floor = 100 if scheme == "MF" else 500
        tier_rule = (
            "MF fresh-only tiers (no renewal/bonus)"
            if scheme == "MF"
            else "Standard slabs (fresh+renewal, bonus per table)"
        )

        if prof == "mutual funds":
            # Mutual Funds: fresh-only tiers, no renewal %, no bonus
            if points_total < 100:
                fresh_pct, label = 0.0, "MF 0–99"
            elif points_total < 500:
                fresh_pct, label = 0.005, "MF 100–499"
            elif points_total < 1000:
                fresh_pct, label = 0.010, "MF 500–999"
            elif points_total < 1500:
                fresh_pct, label = 0.0125, "MF 1000–1499"
            elif points_total < 2000:
                fresh_pct, label = 0.0150, "MF 1500–1999"
            else:  # ≥ 2000
                fresh_pct, label = 0.0175, "MF 2000+"
            renew_pct = 0.0
            bonus_amt = 0
        else:
            slab = _apply_payout_slab(points_total)
            fresh_pct = float(slab["fresh_pct"])
            renew_pct = float(slab["renew_pct"])
            bonus_amt = int(slab["bonus"])
            label = slab["label"]

        fresh_amt = float(r.get("fresh_premium", 0.0)) * fresh_pct
        renew_amt = float(r.get("renewal_premium", 0.0)) * renew_pct

        # add Quarterly/Annual bonuses (credited only in the period-end months)
        # add Quarterly/Annual bonuses
        q_extra = int(quarter_bonus_by_month.get((emp_id, month_str), 0))
        a_extra = int(annual_bonus_by_month.get((emp_id, month_str), 0))

        # Optional test forcing: treat this month as q/fy end to validate flows
        force_q = (_FORCE_QEND == "all") or (_FORCE_QEND == period_month.lower())
        force_a = (_FORCE_FYEND == "all") or (_FORCE_FYEND == period_month.lower())

        if force_q and q_extra == 0:
            qtd_basis = float(qtd_map.get((emp_id, period_month), 0.0))
            q_extra = int(_quarterly_bonus_amount(qtd_basis))
        if force_a and a_extra == 0:
            fytd_basis = float(fytd_map.get((emp_id, period_month), 0.0))
            a_extra = int(_annual_bonus_amount(fytd_basis))

        # Apply scaling for safe testing (e.g., 0.01)
        try:
            q_extra = int(round(q_extra * _Q_SCALE))
            a_extra = int(round(a_extra * _A_SCALE))
        except Exception:
            pass

        # Mutual Funds rows do not take insurance quarterly/annual bonuses
        if scheme == "MF":
            q_extra = 0
            a_extra = 0

        payout_amount = round(fresh_amt + renew_amt + bonus_amt + q_extra + a_extra)

        # Public Leaderboard V1 (KPI-only) – key = (employee_id, period_month)
        if not emp_id or not month_str:
            continue  # cannot satisfy unique key

        is_active = (str(emp_id) in active_set) if active_set is not None else True
        doc = {
            "employee_id": str(r["employee_id"]),
            "employee_name": r["employee_name"],
            "period_month": period_month,
            # KPI fields only
            "points_policy": float(r.get("points_policy", 0) or 0),
            "points_bonus": int(r.get("points_bonus", 0) or 0),
            "points_total": int(round(float(r.get("points_total", 0) or 0))),
            "fresh_premium": float(r.get("fresh_premium", 0) or 0),
            "renewal_premium": float(r.get("renewal_premium", 0) or 0),
            "profile": profile,
            "is_active": bool(is_active),
            "updated_at": now,
        }
        # Defaults—gate may flip these off if within last-6 months of inactivation
        doc.setdefault("payout_eligible", True)
        doc.setdefault("ins_payout_blocked_by_inactive", False)
        if db_handle is not None:
            # Enforce inactive-employee payout gate (no deductions, just eligibility off)
            doc = _apply_inactive_block(db_handle, emp_id, period_month, doc)
        doc = _sanitize_doc(doc)
        out_docs.append(doc)

    # Upsert all docs (one per employee_id×month)
    if out_docs:
        ensure_monthly_leaderboard_index(monthly_col)

        ops = []
        for d in out_docs:
            # Public Leaderboard V1 key
            filt = {"employee_id": d["employee_id"], "period_month": d["period_month"]}
            ops.append(UpdateOne(filt, {"$set": d}, upsert=True))

        if ops:
            # Strip accidental 'month' keys before write to keep schema clean
            for _op in ops:
                if isinstance(_op, UpdateOne):
                    payload = getattr(_op, "_doc", {}).get("u")
                    if isinstance(payload, dict):
                        set_doc = payload.get("$set")
                        if isinstance(set_doc, dict):
                            set_doc.pop("month", None)

            res = monthly_col.bulk_write(ops, ordered=False)
            logging.info(
                "Monthly leaderboard writes: upserts=%s modified=%s total_ops=%s",
                getattr(res, "upserted_count", 0),
                getattr(res, "modified_count", 0),
                len(ops),
            )


def process_investment_leads(access_token, leaderboard_collection, active_ids):
    # Placeholder for finalized logic for processing investment leads
    # Replace the following line with the actual implementation as needed
    logging.info(
        "Processing investment leads with access_token, leaderboard_collection, and active_ids."
    )


def Run_insurance_Score():
    logging.info("Starting Insurance Score computation")
    access_token = get_access_token()
    logging.info("Access token loaded; fetching active Zoho employees and CRM leads...")

    # Open Zoho_Users collection and sync active users (no raw payload stored)
    zoho_users_collection = connect_to_mongo("Zoho_Users")
    if zoho_users_collection is not None:
        logging.info("Zoho_Users collection ready; syncing active users...")
    active_ids = fetch_active_employee_ids(access_token, zoho_users_collection)

    profiles_by_id: Dict[str, str] = {}
    try:
        if zoho_users_collection is not None:
            cursor = zoho_users_collection.find({}, {"id": 1, "profile": 1})
            if cursor is not None:
                for doc in cursor:
                    _id = str(doc.get("id")) if doc.get("id") is not None else None
                    if not _id:
                        continue
                    _prof = doc.get("profile")
                    if _prof is not None:
                        profiles_by_id[_id] = str(_prof)
        # cache profiles globally for monthly leaderboard logic
        global _PROFILES_BY_ID
        _PROFILES_BY_ID = dict(profiles_by_id)
        if profiles_by_id:
            logging.info(
                "[Profile] Loaded %d profile mappings. Active profile-skip set: %s",
                len(profiles_by_id),
                ", ".join(sorted(SKIP_ZOHO_PROFILES))[:120],
            )
        else:
            logging.info("[Profile] No profiles loaded.")
    except Exception as e:
        logging.warning("[Profile] Failed to load from Zoho_Users: %s", e)

    df_users, df_associate_payout, df_referral_fee = get_pli_records(access_token)
    mongo_collection = connect_to_mongo("Insurance_Policy_Scoring")
    leaderboard_collection = connect_to_mongo("Insurance_audit")
    if mongo_collection is not None:
        df_result = process_and_upsert(
            df_users,
            mongo_collection,
            profiles_by_id=profiles_by_id,
            skip_profiles=SKIP_ZOHO_PROFILES,
        )
        # Custom leaderboard insert: Only insert if points != 0
        if leaderboard_collection is not None:
            from datetime import datetime

            # --- visibility counters ---
            total_rows = len(df_result)
            writes = 0
            upserts_cnt = 0
            modified_cnt = 0
            skipped_zero = 0
            skipped_inactive = 0
            skipped_by_name = 0
            skipped_by_profile = 0

            for _, row in df_result.iterrows():
                # Hard guard: skip any rows where employee_name matches skip list
                _ename_lc = str(row.get("employee_name") or "").strip().lower()
                if should_skip(_ename_lc):
                    skipped_by_name += 1
                    continue

                # Profile-based guard: skip employees whose Zoho profile is in the configured skip set
                _emp_id = row.get("employee_id")
                if _emp_id is not None and profiles_by_id:
                    _prof = str(profiles_by_id.get(str(_emp_id)) or "").strip().lower()
                    if _prof in SKIP_ZOHO_PROFILES:
                        skipped_by_profile += 1
                        continue

                points = int(row["total_points"] or 0)
                if points == 0:
                    skipped_zero += 1
                    continue

                # Ignore / purge inactive users
                if not row["employee_id"] or str(row["employee_id"]) not in active_ids:
                    if row["employee_id"]:
                        leaderboard_collection.delete_many({"employee_id": row["employee_id"]})
                    skipped_inactive += 1
                    continue

                # Build justification string
                parts = []
                if row.get("base_points", 0):
                    parts.append(f"{row['base_points']} pts for premium")
                if row.get("upsell_points", 0):
                    parts.append(f"{row['upsell_points']} pts for upsell")
                if row.get("early_renew_points", 0):
                    parts.append(f"{row['early_renew_points']} pts for early renewal")
                if row.get("term_bonus_points", 0):
                    parts.append(f"{row['term_bonus_points']} pts for term")
                if row.get("deductible_bonus_points", 0):
                    parts.append(f"{row['deductible_bonus_points']} pts for deductible")
                justification = "; ".join(parts) if parts else "0 pts"

                # --- sanitize document for MongoDB (handle NaT, tz-aware, etc.) ---
                set_doc = {
                    "employee_name": row["employee_name"],
                    "points": points,
                    "justification": justification,
                    "weight_factor": row["weight_factor"],
                    # Use UTC and sanitize any date-like objects (NaT → None, tz-aware → naive)
                    "updated_at": dt.datetime.utcnow(),
                    "renewal_date": row.get("renewal_date"),
                }
                set_doc = _sanitize_doc(set_doc)

                # Filter must be a plain Mapping[str, Any]; keep it string-only to avoid NaT/tz issues
                lead_id_str = "" if pd.isna(row.get("lead_id")) else str(row.get("lead_id") or "")
                emp_id_str = (
                    "" if pd.isna(row.get("employee_id")) else str(row.get("employee_id") or "")
                )

                q: Dict[str, Any] = {
                    "lead_id": lead_id_str,
                    "employee_id": emp_id_str,
                    "reason": "Insurance Score",
                }

                res = leaderboard_collection.update_one(
                    q,
                    {"$set": set_doc},
                    upsert=True,
                )

                writes += 1
                # Count true upserts vs updates (matched/modified)
                if getattr(res, "upserted_id", None) is not None:
                    upserts_cnt += 1
                else:
                    # modified_count can be 0 if nothing changed
                    try:
                        modified_cnt += int(getattr(res, "modified_count", 0) or 0)
                    except Exception:
                        pass

            # Debug: entries for a specific lead
            logging.debug("Queried leaderboard for lead_id MIB11426")
            debug_cursor = leaderboard_collection.find({"lead_id": "MIB11426"})
            debug_rows = list(debug_cursor) if debug_cursor is not None else []
            df_leaderboard = pd.DataFrame(debug_rows)
            if not df_leaderboard.empty:
                logging.debug(f"\n{df_leaderboard.to_string(index=False)}")
            else:
                logging.debug("No entries found for lead_id MIB11426")

            # --- print current leaderboard (debug) ---
            full_cursor = leaderboard_collection.find({}, {"_id": 0})
            full_rows = list(full_cursor) if full_cursor is not None else []
            lb_df = pd.DataFrame(full_rows)
            logging.debug("Queried full leaderboard")
            if not lb_df.empty:
                logging.debug(f"\n{lb_df.to_string(index=False)}")
            else:
                logging.debug("Leaderboard is currently empty")

            # --- concise visibility log ---
            logging.info(
                "Insurance_Audit writes: upserts=%d, modified=%d, total_update_calls=%d, skipped_zero=%d, skipped_inactive=%d, skipped_by_name=%d, skipped_by_profile=%d, total_rows=%d",
                upserts_cnt,
                modified_cnt,
                writes,
                skipped_zero,
                skipped_inactive,
                skipped_by_name,
                skipped_by_profile,
                total_rows,
            )
        # ── show full DataFrame in console ──
        pd.set_option("display.max_columns", None)  # never hide columns
        pd.set_option("display.width", 0)  # auto‑wrap lines
        pd.set_option("display.max_colwidth", None)  # don't truncate strings
        logging.info("Insurance score calculation complete. Sample rows:")
        logging.info(f"\n{df_result.head().to_string(index=False)}")

        # --- monthly / quarterly bonuses ---
        award_monthly_quarterly_bonus(df_result, leaderboard_collection)
        # --- actual monthly leaderboard (payout) ---
        monthly_leaderboard_collection = connect_to_mongo("Leaderboard")
        wipe_flag = os.getenv("WIPE_MONTHLY_LEADERBOARD", "0").strip().lower()
        if wipe_flag in {"1", "true", "yes"}:
            monthly_leaderboard_collection = reset_monthly_leaderboard(
                monthly_leaderboard_collection
            )
        else:
            ensure_monthly_leaderboard_index(monthly_leaderboard_collection)
        upsert_monthly_leaderboard(df_result, monthly_leaderboard_collection, active_ids)
    # --- Call process_investment_leads logic ---
    process_investment_leads(access_token, leaderboard_collection, active_ids)


# Only Run_insurance_Score() should be called at the top level.


if __name__ == "__main__":
    logging.info("CLI mode detected; invoking Run_insurance_Score()")
    Run_insurance_Score()

# -----------------------------
# Azure Functions timer trigger
# -----------------------------


def main(mytimer: func.TimerRequest) -> None:
    """Azure Functions timer entrypoint.
    This wraps the production insurance scoring pipeline.
    """
    try:
        start_iso = dt.datetime.utcnow().isoformat()
        logging.info("[Timer] Insurance scorer timer fired at %s (UTC)", start_iso)
        Run_insurance_Score()
        logging.info("[Timer] Insurance scorer completed successfully")
    except Exception as e:
        logging.exception("[Timer] Insurance scorer failed: %s", e)
        # Re-raise so Azure marks the invocation as failed (for retry/alerting)
        raise
