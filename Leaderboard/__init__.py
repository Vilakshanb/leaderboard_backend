from __future__ import annotations
print("DEBUG: Module Loaded")
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:  # pragma: no cover - typing aid
    import azure.functions as func
else:  # pragma: no cover - azure runtime only
    try:
        import azure.functions as func
    except ImportError:
        func = None  # type: ignore[assignment]

from pymongo import MongoClient
from ..utils.db_utils import get_db_client

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE: dict[str, str] = {}

# Key Vault URL can be configured via env; default to Milestone vault
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL", "https://milestonetsl1.vault.azure.net/")

# --- Leaderboard config ---
CONFIG_COLLECTION = "config"

_DEFAULT_CONFIG: dict[str, object] = {
    "range_mode": "twomonths",  # single / twomonths / fy
}

_config_cache: dict[str, object] | None = None


def load_leaderboard_config(
    mongo_uri: str | None = None, db_name: str | None = None
) -> dict[str, object]:
    """
    Load leaderboard config from Mongo in a SIP_Schema-style layout.
    Bootstraps a Leaderboard_Schema document with defaults if missing.
    """
    global _config_cache

    if _config_cache is not None:
        return _config_cache

    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        raise RuntimeError(
            "Mongo connection string not found in env (MongoDb-Connection-String / MONGODB_URI)."
        )
    db_name = db_name or DB_NAME

    client = get_db_client(mongo_uri)
    db = client[db_name]

    # Expect a schema-style document similar to SIP_Schema, e.g.:
    # {
    #   _id: "Leaderboard_Schema",
    #   module: "Leaderboard",
    #   schema: "Leaderboard_Rupee",
    #   defaults: { range_mode: "twomonths", ... },
    #   keys: { ... },
    #   meta: { ... },
    #   ...
    # }
    doc = db[CONFIG_COLLECTION].find_one({"_id": "Leaderboard_Schema"})

    if not doc:
        now_iso = datetime.now(timezone.utc).isoformat()
        base_defaults: dict[str, object] = dict(_DEFAULT_CONFIG)

        schema_doc: dict[str, object] = {
            "_id": "Leaderboard_Schema",
            "module": "Leaderboard",
            "schema": "Leaderboard_Rupee",
            "schema_version": "2025-11-15.r1",
            "status": "active",
            "description": "Schema registry for Leaderboard & Rupee incentives; runtime config and defaults.",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "defaults": base_defaults,
            "keys": {
                "leaderboard_collection": "Rupee_Incentives",
                "metrics": ["incentive_rupees_total"],
                "identity_fields": ["employee_id", "rm_name", "period_month"],
            },
            "meta": {
                "notes": "Auto-created by Leaderboard runtime. Safe to edit values under `defaults`; keep top-level keys.",
            },
        }

        db[CONFIG_COLLECTION].insert_one(schema_doc)
        doc = schema_doc

    defaults_raw = doc.get("defaults") or {}
    if not isinstance(defaults_raw, dict):
        defaults_raw = {}
    defaults: dict[str, object] = defaults_raw

    cfg: dict[str, object] = {**_DEFAULT_CONFIG, **defaults}

    _config_cache = cfg
    return cfg


def resolve_months_for_range(anchor_month: str, cfg: dict[str, object]) -> list[str]:
    mode = str(cfg.get("range_mode", "twomonths")).lower()

    if mode == "single":
        return [anchor_month]

    if mode == "twomonths":
        return [prev_month(anchor_month), anchor_month]

    if mode == "fy":
        return [m for m in fy_months_for(anchor_month) if m <= anchor_month]

    return [anchor_month]


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
    if name == "MONGODB_CONNECTION_STRING":
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
                        continue
                except Exception:
                    continue
        except Exception as e:
            logging.warning("Secrets: failed to fetch '%s' from Key Vault: %s", name, e)

    # 4) Fallback
    return default


# ---------- Config ----------
# Database name:
# Prefer explicit env overrides so all scorers / leaderboards can share the same DB:
#   - PLI_DB_NAME (primary)
#   - MONGO_DB_NAME (generic)
# Fallback to the previous default "PLI_Leaderboard" if nothing is set.
DB_NAME = os.getenv("PLI_DB_NAME") or os.getenv("MONGO_DB_NAME") or "PLI_Leaderboard_v2"

MONGO_URI = (
    get_secret("MONGODB_CONNECTION_STRING")
    or os.getenv("MONGO_CONN")
    or os.getenv("MONGO_URI")
    or os.getenv("MONGODB_URI")
)

# Special-case regex (case-insensitive) for leader adjustments:
# Insurance slab boosted by INS leader points for Sumit C
INS_LEADER_EMP_REGEX = os.getenv(
    "PLI_INS_LEADER_EMP_REGEX", r"(?i)^sumit\s+c"
)  # e.g., "Sumit Ch..."
# MF tier boosted by INV leader points for Sagar M
MF_LEADER_EMP_REGEX = os.getenv("PLI_MF_LEADER_EMP_REGEX", r"(?i)^sagar\s+maini")

# Prefer Zoho employee-id based leader boosts (regex is fallback)
INS_LEADER_EMP_ID = os.getenv("PLI_INS_LEADER_EMP_ID")  # e.g., "2969103000154276001" (Sumit C)
MF_LEADER_EMP_ID = os.getenv("PLI_MF_LEADER_EMP_ID")  # e.g., "2969103000000183019" (Sagar M)


# Scoring Config IDs
SCORING_CONFIG_ID_INSURANCE = "Leaderboard_Insurance"
SCORING_CONFIG_ID_REFERRAL = "Leaderboard_Referral"

# Default Referral Config (Mirrors Settings_API)
DEFAULT_REFERRAL_CONFIG = {
    "points": {
        "insurance_points": 0,
        "investment_points": 0
    },
    "gamification": {
        "badges": [
            {"id": "shield", "name": "Guardian Shield", "icon": "Shield", "color": "#3b82f6", "description": "Protected 10 families", "condition_metric": "policies_active", "condition_operator": "gte", "condition_value": 10},
            {"id": "star", "name": "Rising Star", "icon": "Star", "color": "#eab308", "description": "Top performer this month", "condition_metric": "total_points", "condition_operator": "gte", "condition_value": 1000},
            {"id": "trophy", "name": "Champion", "icon": "Trophy", "color": "#f97316", "description": "Consistency Award", "condition_metric": "consistency_score", "condition_operator": "gte", "condition_value": 90}
        ]
    }
}

def _resolve_target_month(now: datetime | None = None) -> str:
    """
    Determine which YYYY-MM window to process.
    Priority: explicit env override -> current UTC month.
    """
    override = (
        os.getenv("PLI_LEADERBOARD_MONTH") or os.getenv("LEADERBOARD_MONTH") or os.getenv("MONTH")
    )
    if override:
        return override
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y-%m")


# ---------- Helpers ----------
def month_window(month: str) -> Tuple[datetime, datetime]:
    """
    month: 'YYYY-MM'
    returns [start_utc, end_utc)
    """
    y, m = map(int, month.split("-"))
    start = datetime(y, m, 1, 0, 0, 0, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        end = datetime(y, m + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return start, end


def prev_month(month: str) -> str:
    """Return previous month (YYYY-MM) for a given YYYY-MM."""
    y, m = map(int, month.split("-"))
    if m == 1:
        return f"{y-1:04d}-12"
    return f"{y:04d}-{m-1:02d}"


def fy_months_for(anchor_month: str) -> list[str]:
    """Return the list of 'YYYY-MM' months for the FY (Apr–Mar) containing anchor_month."""
    y, m = map(int, anchor_month.split("-"))
    # Financial year starts in April; if month is Jan–Mar, FY started previous calendar year
    fy_start_year = y if m >= 4 else y - 1
    months: list[str] = []
    for offset in range(12):
        mm = 4 + offset
        yy = fy_start_year
        if mm > 12:
            mm -= 12
            yy += 1
        months.append(f"{yy:04d}-{mm:02d}")
    return months


# ---------- Pipeline 1: Public Leaderboard (no leader points) ----------
def build_public_leaderboard_pipeline(month: str, start: datetime, end: datetime):
    return [
        # Base: MF points (MF_SIP_Leaderboard, using Total Points as MF points)
        {
            "$match": {
                "$and": [
                    {"$or": [{"period_month": month}, {"month": month}]},
                    {"module": "SIP_Scorer"},
                ]
            }
        },
        {
            "$project": {
                # Normalise period_month: prefer explicit field, then month, then anchor
                "period_month": {
                    "$ifNull": [
                        "$period_month",
                        {"$ifNull": ["$month", month]},
                    ]
                },
                "rm_name": 1,
                "employee_id": {"$toString": "$employee_id"},
                "bucket": {"$literal": "MF"},
                # MF points = Total Points from SIP_Scorer (already includes SIP + Lumpsum logic)
                "pts": {
                    "$toDouble": {
                        "$ifNull": [
                            "$Total Points",
                            {"$ifNull": ["$total_points", 0]},
                        ]
                    }
                },
                "sip_gross": {"$toDouble": {"$ifNull": ["$Gross SIP", {"$ifNull": ["$gross_sip", 0]}]}},
                "sip_net": {"$toDouble": {"$ifNull": ["$Net SIP", {"$ifNull": ["$net_sip", 0]}]}},
                "sip_cancel": {"$toDouble": {"$ifNull": ["$Cancel SIP", {"$ifNull": ["$cancel_sip", 0]}]}},
                "sip_id": "$_id",
                "sip_swp_reg": {"$toDouble": {"$ifNull": ["$swp_adj_registration", 0]}},
                "sip_swp_canc": {"$toDouble": {"$ifNull": ["$swp_adj_cancellation", 0]}},
                "sip_updated_at": "$updated_at",
                # [NEW] Point splits
                "mf_sip_points": {"$toDouble": {"$ifNull": ["$sip_points", 0]}},
                "mf_lumpsum_points": {"$toDouble": {"$ifNull": ["$lumpsum_points", 0]}},
                # [NEW] Hierarchy fields
                "team_id": "$team_id",
                "reporting_manager_id": "$reporting_manager_id",
            }
        },
        # (Lumpsum points are now included in MF points above; unionWith for LS removed)
        # + Insurance points aggregated from Insurance_Policy_Scoring (per RM, per month)
        {
            "$unionWith": {
                "coll": "Insurance_Policy_Scoring",
                "pipeline": [
                    {
                        "$match": {
                            "conversion_date": {"$gte": start, "$lt": end},
                            "employee_id": {"$ne": None},
                        }
                    },
                    {
                        "$addFields": {
                            "employee_id": {"$toString": "$employee_id"},
                            "period_month": {
                                "$dateToString": {
                                    "format": "%Y-%m",
                                    "date": "$conversion_date",
                                }
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "employee_id": "$employee_id",
                                "period_month": "$period_month",
                                "rm_name": "$employee_name",
                            },
                            "pts": {
                                "$sum": {
                                    "$ifNull": [
                                        "$total_points",
                                        {"$ifNull": ["$points_policy", 0]},
                                    ]
                                }
                            },
                            "fresh_premium": {
                                "$sum": {
                                    "$toDouble": {"$ifNull": ["$fresh_premium_eligible", 0]}
                                }
                            },
                            "renewal_premium": {
                                "$sum": {
                                    "$subtract": [
                                        {"$toDouble": {"$ifNull": ["$this_year_premium", 0]}},
                                        {"$toDouble": {"$ifNull": ["$fresh_premium_eligible", 0]}}
                                    ]
                                }
                            },
                            "renewal_lost_premium": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$regexMatch": {
                                                "input": {"$ifNull": ["$status", ""]},
                                                "regex": "lapsed|surrendered|cancelled|lost",
                                                "options": "i"
                                            }
                                        },
                                        {"$toDouble": {"$ifNull": ["$this_year_premium", 0]}},
                                        0
                                    ]
                                }
                            },
                            "avg_dtr": {"$avg": "$days_to_renewal"},
                            "policy_count": {"$sum": 1},
                            "ins_updated_at": {"$max": "$updated_at"},
                            # [NEW] Capture hierarchy (assuming consistent per employee-month)
                            "team_id_first": {"$first": "$team_id"},
                            "reporting_manager_id_first": {"$first": "$reporting_manager_id"},
                        }
                    },
                    {
                        "$project": {
                            "period_month": "$_id.period_month",
                            "rm_name": "$_id.rm_name",
                            "employee_id": "$_id.employee_id",
                            "bucket": {"$literal": "INS"},
                            "pts": "$pts",
                            "fresh_premium": "$fresh_premium",
                            "renewal_premium": "$renewal_premium",
                            "renewal_lost_premium": "$renewal_lost_premium",
                            "avg_dtr": "$avg_dtr",
                            "policy_count": "$policy_count",
                            "ins_updated_at": "$ins_updated_at",
                            # [NEW] Hierarchy fields (grab first encountered for the group)
                            "team_id": "$team_id_first",
                            "reporting_manager_id": "$reporting_manager_id_first",
                        }
                    },
                ],
            }
        },
        # + Referrals (support both legacy and new collection names)
        {
            "$unionWith": {
                "coll": "referralLeaderboard",
                "pipeline": [
                    {
                        "$match": {
                            "period_month": month,
                            "employee_id": {"$ne": None},
                        }
                    },
                    {
                        "$addFields": {
                            "employee_id": {"$toString": "$employee_id"},
                            "period_month": {
                                "$dateToString": {
                                    "format": "%Y-%m",
                                    "date": "$updated_at",
                                }
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "employee_id": "$employee_id",
                                "period_month": "$period_month",
                                "rm_name": {
                                    "$ifNull": ["$employee_name", "$rm_name"],
                                },
                            },
                            "pts": {
                                "$sum": {
                                    "$ifNull": ["$points", 0],
                                }
                            },
                            "ref_updated_at": {"$max": "$updated_at"},
                            # [NEW] Hierarchy
                            "team_id_first": {"$first": "$team_id"},
                            "reporting_manager_id_first": {"$first": "$reporting_manager_id"},
                        }
                    },
                    {
                        "$project": {
                            "period_month": "$_id.period_month",
                            "rm_name": "$_id.rm_name",
                            "employee_id": "$_id.employee_id",
                            "bucket": {"$literal": "REF"},
                            "pts": "$pts",
                            "ref_updated_at": "$ref_updated_at",
                            # [NEW] Pass through
                            "team_id": "$team_id_first",
                            "reporting_manager_id": "$reporting_manager_id_first",
                        }
                    },
                ],
            }
        },
        {
            "$unionWith": {
                "coll": "Referral_Incentives",
                "pipeline": [
                    {"$match": {"$or": [{"period_month": month}, {"month": month}]}},
                    {
                        "$project": {
                            "period_month": 1,
                            "rm_name": 1,
                            "employee_id": {"$toString": "$employee_id"},
                            "bucket": {"$literal": "REF"},
                            "pts": {"$toInt": {"$ifNull": ["$points", 0]}},
                            # [NEW] Pass through
                            "team_id": "$team_id",
                            "reporting_manager_id": "$reporting_manager_id",
                        }
                    },
                ],
            }
        },
        # Aggregate per RM
        {
            "$group": {
                "_id": {"rm_name": "$rm_name", "employee_id": "$employee_id", "m": "$period_month"},
                "mf_points": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$pts", 0]}},
                "mf_sip_points": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$mf_sip_points", 0]}},
                "mf_lumpsum_points": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$mf_lumpsum_points", 0]}},
                "sip_gross": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_gross", 0]}},
                "sip_net": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_net", 0]}},
                "sip_cancel": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_cancel", 0]}},
                "sip_swp_reg": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_swp_reg", 0]}},
                "sip_swp_canc": {"$sum": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_swp_canc", 0]}},
                "ins_points": {"$sum": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$pts", 0]}},
                "ref_points": {"$sum": {"$cond": [{"$eq": ["$bucket", "REF"]}, "$pts", 0]}},
                "ins_fresh_premium": {"$sum": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$fresh_premium", 0]}},
                "ins_renewal_premium": {"$sum": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$renewal_premium", 0]}},
                "ins_renewal_lost_premium": {"$sum": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$renewal_lost_premium", 0]}},
                "avg_dtr": {"$avg": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$avg_dtr", None]}},
                "ins_policy_count": {"$sum": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$policy_count", 0]}},
                "sip_updated_at": {"$max": {"$cond": [{"$eq": ["$bucket", "MF"]}, "$sip_updated_at", None]}},
                "ins_updated_at": {"$max": {"$cond": [{"$eq": ["$bucket", "INS"]}, "$ins_updated_at", None]}},
                "ref_updated_at": {"$max": {"$cond": [{"$eq": ["$bucket", "REF"]}, "$ref_updated_at", None]}},
                # [NEW] Hierarchy (from any bucket that has it)
                "team_id": {"$max": "$team_id"},
                "reporting_manager_id": {"$max": "$reporting_manager_id"},
            }
        },
        # Lookup Lumpsum details to enrich the row
        {
            "$lookup": {
                "from": "Leaderboard_Lumpsum",
                "let": {"emp": {"$toString": "$_id.employee_id"}, "m": "$_id.m"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": [{"$toString": "$month"}, "$$m"]},
                                    {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "gross_purchase": {"$ifNull": ["$Breakdown.Additions.Total Purchase (100%)", 0]},
                            "redemption": {"$ifNull": ["$Breakdown.Subtractions.Redemption (100%)", 0]},
                            # Switch In: sum all percentage variations (90%, 100%, 120%)
                            "switch_in": {
                                "$add": [
                                    {"$ifNull": ["$Breakdown.Additions.Switch In (90%)", 0]},
                                    {"$ifNull": ["$Breakdown.Additions.Switch In (100%)", 0]},
                                    {"$ifNull": ["$Breakdown.Additions.Switch In (120%)", 0]},
                                ]
                            },
                            # Switch Out: sum all percentage variations (100%, 120%)
                            "switch_out": {
                                "$add": [
                                    {"$ifNull": ["$Breakdown.Subtractions.Switch Out (100%)", 0]},
                                    {"$ifNull": ["$Breakdown.Subtractions.Switch Out (120%)", 0]},
                                ]
                            },
                            # COB In: support 50% and 55% variants (raw value, no weight adjustment)
                            "cob_in": {
                                "$add": [
                                    {"$divide": [{"$ifNull": ["$Breakdown.Additions.Change Of Broker In - TICOB (50%)", 0]}, 0.5]},
                                    {"$divide": [{"$ifNull": ["$Breakdown.Additions.Change Of Broker In - TICOB (55%)", 0]}, 0.55]},
                                ]
                            },
                            "cob_out": {
                                "$divide": [
                                    {"$ifNull": ["$Breakdown.Subtractions.Change Of Broker Out - TOCOB (120%)", 0]},
                                    1.2,
                                ]
                            },
                            "ls_id": "$_id",
                            "ls_points": {"$ifNull": ["$final_incentive", 0]},
                            "updatedAt": 1,
                        }
                    }
                ],
                "as": "ls_stats"
            }
        },
        {
            "$addFields": {
                "ls_stats": {"$arrayElemAt": ["$ls_stats", 0]}
            }
        },
        {
            "$addFields": {
                "lumpsum_gross_purchase": {"$ifNull": ["$ls_stats.gross_purchase", 0]},
                "lumpsum_redemption": {"$ifNull": ["$ls_stats.redemption", 0]},
                "lumpsum_switch_in": {"$ifNull": ["$ls_stats.switch_in", 0]},
                "lumpsum_switch_out": {"$ifNull": ["$ls_stats.switch_out", 0]},
                "lumpsum_cob_in": {"$ifNull": ["$ls_stats.cob_in", 0]},
                "lumpsum_cob_out": {"$ifNull": ["$ls_stats.cob_out", 0]},
                # [REMOVED] Do not overwrite mf_lumpsum_points; blindly trust MF_SIP_Leaderboard (unified scorer)
                # "mf_lumpsum_points": {"$ifNull": ["$ls_stats.ls_points", 0]},
            }
        },
        {
            "$addFields": {
                # Recalculate Total MF Points with the fresh Lumpsum points
                "mf_points": {
                    "$add": [
                        {"$ifNull": ["$mf_sip_points", 0]},
                        {"$ifNull": ["$mf_lumpsum_points", 0]},
                    ]
                }
            }
        },
        # is_active and eligibility window from Zoho_Users
        {
            "$lookup": {
                "from": "Zoho_Users",
                "let": {"emp": {"$toString": "$_id.employee_id"}},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": [{"$toString": "$id"}, "$$emp"]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "status": "$status",
                            "Status": "$Status",
                            "active": "$active",
                            "is_active": "$is_active",
                            "IsActive": "$IsActive",
                            "inactive_since": "$inactive_since",
                            "employee_id": "$employee_id",
                            "Employee ID": "$Employee ID",
                            "full": "$Full Name",
                            "alt": "$Name",
                        }
                    },
                ],
                "as": "zu",
            }
        },
        {
            "$addFields": {
                "has_zoho_user": {"$gt": [{"$size": "$zu"}, 0]},
                "is_active": {
                    "$let": {
                        "vars": {
                            "st": {
                                "$toLower": {
                                    "$ifNull": [
                                        {"$first": "$zu.status"},
                                        {"$first": "$zu.Status"},
                                        "",
                                    ]
                                }
                            },
                            "a1": {"$first": "$zu.active"},
                            "a2": {"$first": "$zu.is_active"},
                            "a3": {"$first": "$zu.IsActive"},
                        },
                        "in": {
                            "$or": [
                                {"$eq": ["$$st", "active"]},
                                {"$eq": ["$$a1", True]},
                                {"$eq": ["$$a2", True]},
                                {"$eq": ["$$a3", True]},
                            ]
                        },
                    }
                },
                "skip_by_inactive_no_empid": {
                    "$let": {
                        "vars": {
                            "st": {
                                "$toLower": {
                                    "$ifNull": [
                                        {"$first": "$zu.status"},
                                        {"$first": "$zu.Status"},
                                        "",
                                    ]
                                }
                            },
                            "empid": {
                                "$ifNull": [
                                    {"$first": "$zu.employee_id"},
                                    {"$first": "$zu.Employee ID"},
                                    "",
                                ]
                            },
                        },
                        "in": {
                            "$and": [
                                {"$eq": ["$$st", "inactive"]},
                                {
                                    "$eq": [
                                        {"$trim": {"input": {"$toString": "$$empid"}}},
                                        "",
                                    ]
                                },
                            ]
                        },
                    }
                },
                "inactive_since_raw": {"$first": "$zu.inactive_since"},
                "rm_name_final": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$_id.rm_name", None]},
                                {"$ne": ["$_id.rm_name", ""]},
                            ]
                        },
                        "$_id.rm_name",
                        {
                            "$let": {
                                "vars": {"z": {"$first": "$zu"}},
                                "in": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$ne": ["$$z.full", None]},
                                                {"$ne": ["$$z.full", ""]},
                                            ]
                                        },
                                        "$$z.full",
                                        {
                                            "$cond": [
                                                {
                                                    "$and": [
                                                        {"$ne": ["$$z.alt", None]},
                                                        {"$ne": ["$$z.alt", ""]},
                                                    ]
                                                },
                                                "$$z.alt",
                                                {
                                                    "$concat": [
                                                        "Unmapped-",
                                                        {"$toString": "$_id.employee_id"},
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                },
                            }
                        },
                    ]
                },
                # Convert period month 'YYYY-MM' into a date (first of month)
                "period_date": {
                    "$dateFromString": {
                        "dateString": {"$concat": ["$_id.m", "-01"]},
                        "format": "%Y-%m-%d",
                    }
                },
                # End of 6-month eligibility window after inactive_since
                "inactive_until": {
                    "$cond": [
                        {"$ne": ["$inactive_since_raw", None]},
                        {
                            "$dateAdd": {
                                "startDate": "$inactive_since_raw",
                                "unit": "month",
                                "amount": 6,
                            }
                        },
                        None,
                    ]
                },
                # Final eligibility flag: active OR (within 6 months after inactive_since)
                "eligible_by_inactive": {
                    "$cond": [
                        {
                            "$or": [
                                "$is_active",
                                {"$eq": ["$inactive_since_raw", None]},
                            ]
                        },
                        True,
                        {
                            "$and": [
                                {"$gte": ["$period_date", "$inactive_since_raw"]},
                                {"$lt": ["$period_date", "$inactive_until"]},
                            ]
                        },
                    ]
                },
            }
        },
        {
            "$match": {
                "rm_name_final": {"$nin": [None, ""]},
                # "has_zoho_user": True,  <-- REMOVED to fix Epic E (missing historical data)
                # Hard filter: Zoho user is inactive AND has no employee_id → skip from leaderboard
                "skip_by_inactive_no_empid": {"$ne": True},
                # No eligibility gate here – public board is factual; eligibility is enforced in Rupee_Incentives.
            }
        },
        # Final doc
        {
            "$project": {
                "_id": 0,
                "period_month": "$_id.m",
                "rm_name": "$rm_name_final",
                "employee_id": "$_id.employee_id",
                "is_active": {"$ifNull": ["$is_active", True]},
                "mf_points": 1,
                "is_active": {"$ifNull": ["$is_active", True]},
                "mf_points": 1,
                "mf_sip_points": 1,
                "mf_lumpsum_points": 1,
                "ins_points": 1,
                "ref_points": 1,
                "sip_gross": 1,
                "sip_net": 1,
                "sip_cancel": 1,
                "sip_swp_reg": 1,
                "sip_swp_canc": 1,
                "total_points_public": {"$add": ["$mf_points", "$ins_points", "$ref_points"]},
                "ins_fresh_premium": 1,
                "ins_renewal_premium": 1,
                "ins_renewal_lost_premium": 1,
                "ins_policy_count": 1,
                "lumpsum_gross_purchase": 1,
                "lumpsum_redemption": 1,
                "lumpsum_switch_in": 1,
                "lumpsum_switch_out": 1,
                "lumpsum_cob_in": 1,
                "lumpsum_cob_in": 1,
                "lumpsum_cob_out": 1,
                # [NEW] Hierarchy
                "team_id": 1,
                "reporting_manager_id": 1,
                "audit": {
                    "buckets": {
                        "mf_points": "$mf_points",
                        "ins_points": "$ins_points",
                        "ref_points": "$ref_points",
                        "total_points_public": {
                            "$add": ["$mf_points", "$ins_points", "$ref_points"]
                        },
                    },
                    "sources": {
                        "sip": {"$literal": "MF_SIP_Leaderboard (Total Points via SIP+LS)"},
                        "lumpsum": {"$literal": "Leaderboard_Lumpsum"},
                        "insurance": {"$literal": "Insurance_Policy_Scoring"},
                        "referrals": {"$literal": "referralLeaderboard + Referral_Incentives"},
                    },
                },
                "updated_at": {
                    "$ifNull": [
                        {
                            "$max": [
                                "$sip_updated_at",
                                "$ins_updated_at",
                                "$ref_updated_at",
                                "$ls_stats.updatedAt",
                            ]
                        },
                        "$$NOW"
                    ]
                },
                "updated_at_audit": {
                    "$let": {
                        "vars": {
                            "max_ts": {
                                "$max": [
                                    "$sip_updated_at",
                                    "$ins_updated_at",
                                    "$ref_updated_at",
                                    "$ls_stats.updatedAt",
                                ]
                            }
                        },
                        "in": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$eq": ["$$max_ts", "$ls_stats.updatedAt"]}, "then": {"collection": "Public_Leaderboard", "doc_id": "$ls_stats.ls_id"}},
                                    {"case": {"$eq": ["$$max_ts", "$sip_updated_at"]}, "then": {"collection": "Public_Leaderboard", "doc_id": "$sip_id"}},
                                    {"case": {"$eq": ["$$max_ts", "$ins_updated_at"]}, "then": {"collection": "Public_Leaderboard", "doc_id": "aggregated"}},
                                    {"case": {"$eq": ["$$max_ts", "$ref_updated_at"]}, "then": {"collection": "Public_Leaderboard", "doc_id": "aggregated"}},
                                ],
                                "default": {"collection": "Public_Leaderboard", "reason": "System Fallback"}
                            }
                        }
                    }
                },
            }
        },
        {
            "$merge": {
                "into": "Public_Leaderboard",
                "on": ["rm_name", "period_month"],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


# ---------- Pipeline 2: Rupee Incentives (with audit) ----------
def load_sip_config(mongo_uri: str | None = None) -> dict[str, object] | None:
    """
    Load SIP/Unified Scoring configuration from the V2 database.
    Required for dynamic tier calculations in the incentive pipeline.
    """
    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        logging.warning("Mongo URI missing, skipping SIP config load.")
        return None

    # Determine V2 DB Name (Fallback to PLI_Leaderboard_v2)
    db_name_v2 = os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name_v2]
        # Collection is 'config', ID is 'Leaderboard_SIP'
        doc = db["config"].find_one({"_id": "Leaderboard_SIP"})
        if doc:
            return doc
    except Exception as e:
        logging.warning(f"Failed to load SIP config: {e}")

    return None


def load_insurance_config(mongo_uri: str | None = None) -> dict[str, object] | None:
    """
    Load Insurance Scoring configuration from the V2 database.
    """
    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        logging.warning("Mongo URI missing, skipping Insurance config load.")
        return None

    # Determine V2 DB Name (Fallback to PLI_Leaderboard_v2)
    db_name_v2 = os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name_v2]
        # Collection is 'config', ID is 'Leaderboard_Insurance'
        doc = db["config"].find_one({"_id": SCORING_CONFIG_ID_INSURANCE})
        if doc and "config" in doc:
            return doc["config"]
        # Fallback if doc exists but structure is flat (older schema?) or just return doc
        # Actually API returns doc['config']. Let's stick to returning the inner config dict if possible,
        # or the whole doc if the caller expects it.
        # load_sip_config returns `doc`.
        # In API we accessed `sip_config.get("tier_thresholds")`.
        # Let's return only the inner config if 'config' key exists, else doc.
        if doc:
             return doc.get("config", doc)
    except Exception as e:
        logging.warning(f"Failed to load Insurance config: {e}")

    return None

def load_referral_config(mongo_uri: str | None = None) -> dict[str, object] | None:
    """
    Load Referral/Gamification Scoring configuration.
    """
    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        return None

    db_name_v2 = os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name_v2]
        doc = db["config"].find_one({"_id": SCORING_CONFIG_ID_REFERRAL})
        # Merge with defaults to ensure gamification keys exist
        merged = DEFAULT_REFERRAL_CONFIG.copy()
        if doc:
             # Deep merge for gamification if needed, but simple update is okay for now
             # Since 'gamification' is a top-level key, we can merge sections
             for k in ["insurance", "investment", "gating", "gamification"]:
                 if k in doc:
                     merged[k] = {**merged.get(k, {}), **doc[k]}
        return merged
    except Exception as e:
        logging.warning(f"Failed to load Referral config: {e}")
        return DEFAULT_REFERRAL_CONFIG

def build_rupee_incentives_pipeline(month: str, start: datetime, end: datetime, sip_config: dict = None, ins_config: dict = None, ref_config: dict = None):
    """
    Build Rupee_Incentives from the already-written Public_Leaderboard.

    Design:
      - Public_Leaderboard is the single source of truth for points per RM/month
        (sip_points, ls_points, ins_points, ref_points, rm_name, employee_id, is_active).
      - Rupee_Incentives sits "on top" of that board to:
          * add leader bonuses (MF_Leaders),
          * derive insurance slabs and rupee payouts,
          * derive MF tier and rupee payouts from AUM,
          * compute eligibility_by_inactive for payout gating.
      - This guarantees that every RM visible on the public board for a month
        has a corresponding Rupee_Incentives row for that month (T0 included),
        regardless of inactive status. Eligibility only affects payout, not
        presence in the audit table.
    """

    # Defaults (Fallback if config missing)
    default_thresholds = [
        {"tier": "T6", "min_val": 60000},
        {"tier": "T5", "min_val": 40000},
        {"tier": "T4", "min_val": 25000},
        {"tier": "T3", "min_val": 15000},
        {"tier": "T2", "min_val": 8000},
        {"tier": "T1", "min_val": 2000},
        {"tier": "T0", "min_val": -float('inf')},
    ]
    default_factors = {
        "T6": 0.000037500,
        "T5": 0.000033333,
        "T4": 0.000029167,
        "T3": 0.000025000,
        "T2": 0.000020833,
        "T1": 0.000016667,
        "T0": 0.0,
    }

    # Helpers to generate JS
    def make_js(thry_list, fact_dict):
        # Sort desc by min_val
        safe_thr = []
        if isinstance(thry_list, list):
            safe_thr = sorted(
                [dict(t, min_val=t.get("min_val", 0)) for t in thry_list],
                key=lambda x: x["min_val"],
                reverse=True
            )

        safe_fac = fact_dict if isinstance(fact_dict, dict) else {}

        # Tier JS
        t_js = "function(points) { "
        for t in safe_thr:
            tn = t.get("tier", "T0")
            mv = t.get("min_val", 0)
            if mv == -float('inf'):
                t_js += f"return '{tn}'; "
            else:
                t_js += f"if (points >= {mv}) return '{tn}'; "
        t_js += "return 'T0'; }"

        # Factor JS
        f_js = "function(tier) { switch(tier) { "
        for tc, r in safe_fac.items():
            f_js += f"case '{tc}': return {r}; "
        f_js += "default: return 0.0; } }"

        return t_js, f_js

    # Determine Mode
    scoring_mode = "unified"
    if sip_config and "scoring_mode" in sip_config:
        scoring_mode = sip_config["scoring_mode"]

    # Generate JS bodies
    if scoring_mode == "individual":
        # SIP
        sip_thr = sip_config.get("tier_thresholds", default_thresholds)
        sip_fac = sip_config.get("tier_factors", default_factors)
        sip_tier_js, sip_factor_js = make_js(sip_thr, sip_fac)

        # Lump
        lump_thr = sip_config.get("lumpsum_tier_thresholds", default_thresholds)
        lump_fac = sip_config.get("lumpsum_tier_factors", default_factors)
        lump_tier_js, lump_factor_js = make_js(lump_thr, lump_fac)

        # Unified (Fallback)
        unified_tier_js, unified_factor_js = sip_tier_js, sip_factor_js

    else:
        # Unified
        uni_thr = default_thresholds
        uni_fac = default_factors
        if sip_config:
            uni_thr = sip_config.get("tier_thresholds", default_thresholds)
            uni_fac = sip_config.get("tier_factors", default_factors)

        unified_tier_js, unified_factor_js = make_js(uni_thr, uni_fac)
        sip_tier_js, sip_factor_js = unified_tier_js, unified_factor_js
        lump_tier_js, lump_factor_js = unified_tier_js, unified_factor_js

    # Logic for MF Rupees
    # Unified: AUM * Factor
    # Individual: SIP_Rupees + Lump_Rupees
    mf_rupees_expr = {"$round": [{"$multiply": ["$aum_first", "$mf_factor"]}, 2]}

    if scoring_mode == "individual":
        mf_rupees_expr = {
            "$add": ["$mf_sip_rupees", "$mf_lumpsum_rupees"]
        }


    # ---- Insurance Logic Generation ----
    # Default Slabs (Fallback) matching hardcoded logic
    default_ins_slabs = [
         {"min_points": 0, "max_points": 500, "fresh_pct": 0.0, "renew_pct": 0.0, "bonus_rupees": 0, "label": "<500"},
         {"min_points": 500, "max_points": 1000, "fresh_pct": 0.0050, "renew_pct": 0.0, "bonus_rupees": 0, "label": "500–999"},
         {"min_points": 1000, "max_points": 1500, "fresh_pct": 0.0100, "renew_pct": 0.0020, "bonus_rupees": 0, "label": "1000–1499"},
         {"min_points": 1500, "max_points": 2000, "fresh_pct": 0.0125, "renew_pct": 0.0040, "bonus_rupees": 0, "label": "1500–1999"},
         {"min_points": 2000, "max_points": 2500, "fresh_pct": 0.0150, "renew_pct": 0.0050, "bonus_rupees": 0, "label": "2000–2499"},
         {"min_points": 2500, "max_points": None, "fresh_pct": 0.0175, "renew_pct": 0.0075, "bonus_rupees": 2000, "label": "2500+"},
    ]

    use_ins_slabs = default_ins_slabs
    if ins_config and "slabs" in ins_config:
        use_ins_slabs = ins_config["slabs"]

    # Sort ASC by min_points
    use_ins_slabs.sort(key=lambda x: x.get("min_points", 0))

    # Build branches
    ins_label_branches = []
    ins_fresh_branches = []
    ins_renew_branches = []
    ins_bonus_branches = []

    ins_label_default = "<500"
    ins_fresh_default = 0.0
    ins_renew_default = 0.0
    ins_bonus_default = 0

    for i, s in enumerate(use_ins_slabs):
        mx = s.get("max_points")
        if mx is None:
            ins_label_default = s.get("label", "")
            ins_fresh_default = s.get("fresh_pct", 0.0)
            ins_renew_default = s.get("renew_pct", 0.0)
            ins_bonus_default = s.get("bonus_rupees", 0)
        else:
            cond = {"$lt": ["$ins_points_effective", mx]}

            ins_label_branches.append({"case": cond, "then": s.get("label", "")})
            ins_fresh_branches.append({"case": cond, "then": s.get("fresh_pct", 0.0)})
            ins_renew_branches.append({"case": cond, "then": s.get("renew_pct", 0.0)})
            ins_bonus_branches.append({"case": cond, "then": s.get("bonus_rupees", 0)})



    # ---- Generate Dynamic Badge Conditions ----
    badge_logic = []
    badges_conf = []

    if ref_config and "gamification" in ref_config:
         badges_conf = ref_config["gamification"].get("badges", [])
    elif "gamification" in DEFAULT_REFERRAL_CONFIG:
         badges_conf = [
             {"id": "referral_novice", "label": "Referral Novice", "icon": "UserPlus", "color": "orange", "description": "First successful referral!", "condition_type": "min_points", "condition_field": "ref_points", "threshold": 1},
             {"id": "referral_pro", "label": "Referral Pro", "icon": "Users", "color": "orange", "description": "Consistent referrer.", "condition_type": "min_points", "condition_field": "ref_points", "threshold": 100},
             {"id": "insurance_titan", "label": "Insurance Titan", "icon": "ShieldCheck", "color": "purple", "description": "Achieved the highest insurance slab.", "condition_type": "min_points", "condition_field": "ins_points_effective", "threshold": 2500},
             {"id": "sip_master", "label": "SIP Master", "icon": "TrendingUp", "color": "blue", "description": "Top tier SIP performance.", "condition_type": "equals", "condition_field": "mf_sip_tier", "threshold": "T6"},
             {"id": "club_500", "label": "Club 500", "icon": "Award", "color": "yellow", "description": "Earned 500+ total points in a month.", "condition_type": "min_points", "condition_field": "total_effective_points", "threshold": 500},
        ]

    for b in badges_conf:
        # Map fields (Support both new and old keys)
        field_key = b.get("condition_metric") or b.get("condition_field")
        val = b.get("condition_value") if "condition_value" in b else b.get("threshold")

        field_expr = None
        if field_key == "ref_points":
            field_expr = "$ref_points"
        elif field_key in ["ins_points_effective", "insurance_points"]:
            field_expr = "$ins_points_effective"
        elif field_key == "mf_sip_tier":
            field_expr = "$mf_sip_tier"
        elif field_key in ["total_effective_points", "total_points"]:
            # Approximate total if not available directly in this stage
            field_expr = {"$add": ["$mf_points_effective", "$ins_points_effective", "$ref_points"]}
        elif field_key == "policies_active":
             # Placeholder if not available in aggregator, or map to insurance count if available
             # For now, skip if we can't evaluate
             logging.warning(f"Skipping badge {b.get('id')} - metric {field_key} not available in aggregation.")
             continue
        elif field_key == "consistency_score":
             logging.warning(f"Skipping badge {b.get('id')} - metric {field_key} not available in aggregation.")
             continue

        if not field_expr:
            continue

        # Determine operator
        op_key = b.get("condition_operator") or b.get("condition_type")
        mongo_op = "$gte" # default
        if op_key in ["equals", "eq"]:
            mongo_op = "$eq"
        elif op_key in ["min_points", "gte"]:
             mongo_op = "$gte"

        # Build condition: { $cond: [ { op: [field, val] }, badge_obj, null ] }
        badge_cond = {
            "$cond": {
                "if": {mongo_op: [field_expr, val]},
                "then": b,
                "else": None
            }
        }
        badge_logic.append(badge_cond)


    return [
        # Base spine: one row per RM from the public leaderboard for the month
        {
            "$match": {
                "period_month": month,
            }
        },
        {
            "$project": {
                "period_month": 1,
                "rm_name": 1,
                "employee_id": {"$toString": "$employee_id"},
                "is_active_public": {"$ifNull": ["$is_active", True]},
                "mf_points": {"$ifNull": ["$mf_points", 0]},
                "mf_sip_points": {"$ifNull": ["$mf_sip_points", 0]},
                "mf_lumpsum_points": {"$ifNull": ["$mf_lumpsum_points", 0]},
                "ins_points": {"$ifNull": ["$ins_points", 0]},
                "ref_points": {"$ifNull": ["$ref_points", 0]},
                # aum_first will be brought from MF_SIP_Leaderboard
                "aum_first": {"$literal": 0.0},
            }
        },
        # Bring AUM for MF payout: best-effort from MF_SIP_Leaderboard for that month/employee
        {
            "$lookup": {
                "from": "MF_SIP_Leaderboard",
                "let": {"emp": "$employee_id", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {
                                        "$eq": [
                                            {
                                                "$ifNull": [
                                                    "$period_month",
                                                    {"$ifNull": ["$month", "$$m"]},
                                                ]
                                            },
                                            "$$m",
                                        ]
                                    },
                                    {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "aum_first": {
                                "$let": {
                                    "vars": {"v": {"$toDouble": {"$ifNull": ["$aum_start", 0]}}},
                                    "in": {
                                        "$cond": [
                                            {"$eq": ["$$v", "$$v"]},
                                            "$$v",
                                            0.0,
                                        ]
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "sip_aum_col",
            }
        },
        # Bring Lumpsum AUM from Leaderboard_Lumpsum
        {
            "$lookup": {
                "from": "Leaderboard_Lumpsum",
                "let": {"emp": "$employee_id", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$month", "$$m"]},
                                    {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "lump_aum": {
                                "$let": {
                                    "vars": {"v": {"$toDouble": {"$ifNull": ["$AUM (Start of Month)", 0]}}},
                                    "in": {
                                        "$cond": [
                                            {"$eq": ["$$v", "$$v"]},
                                            "$$v",
                                            0.0,
                                        ]
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "lump_aum_col",
            }
        },
        {
            "$addFields": {
                "aum_first": {
                    "$ifNull": [
                        {"$max": "$sip_aum_col.aum_first"},
                        0.0,
                    ]
                },
                "lump_aum_raw": {
                    "$ifNull": [
                        {"$max": "$lump_aum_col.lump_aum"},
                        0.0,
                    ]
                }
            }
        },
        {
             "$addFields": {
                 "sip_aum_derived": {
                     "$max": [0.0, {"$subtract": ["$aum_first", "$lump_aum_raw"]}]
                 }
             }
        },
        # Bring leader bonuses (INS & INV) for the month
        {
            "$lookup": {
                "from": "MF_Leaders",
                "let": {"rm": "$rm_name", "m": "$period_month"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$period_month", "$$m"]},
                                    {"$eq": ["$rm_name", "$$rm"]},
                                    {"$in": ["$bucket", ["INS", "MF"]]},
                                ]
                            }
                        },
                    },
                    {"$project": {"_id": 0, "bucket": 1, "leader_bonus_points": 1}},
                ],
                "as": "leaders",
            }
        },
        {
            "$addFields": {
                "leader_ins_points": {
                    "$ifNull": [
                        {
                            "$first": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$leaders",
                                            "as": "l",
                                            "cond": {"$eq": ["$$l.bucket", "INS"]},
                                        }
                                    },
                                    "as": "x",
                                    "in": "$$x.leader_bonus_points",
                                }
                            }
                        },
                        0,
                    ]
                },
                "leader_inv_points": {
                    "$ifNull": [
                        {
                            "$first": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$leaders",
                                            "as": "l",
                                            "cond": {"$eq": ["$$l.bucket", "INV"]},
                                        }
                                    },
                                    "as": "x",
                                    "in": "$$x.leader_bonus_points",
                                }
                            }
                        },
                        0,
                    ]
                },
            }
        },
        {
            "$addFields": {
                "rm_lower": {"$toLower": {"$ifNull": ["$rm_name", ""]}},
                # Base MF points = mf_points
                "mf_points_base": {"$ifNull": ["$mf_points", 0]},
                "is_ins_leader_empid": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": [INS_LEADER_EMP_ID, None]},
                                {"$ne": [INS_LEADER_EMP_ID, ""]},
                                {"$eq": ["$employee_id", INS_LEADER_EMP_ID]},
                            ]
                        },
                        True,
                        False,
                    ]
                },
                "is_mf_leader_empid": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": [MF_LEADER_EMP_ID, None]},
                                {"$ne": [MF_LEADER_EMP_ID, ""]},
                                {"$eq": ["$employee_id", MF_LEADER_EMP_ID]},
                            ]
                        },
                        True,
                        False,
                    ]
                },
            }
        },
        # Apply leader adjustments (ID-based, with regex fallback)
        {
            "$addFields": {
                "ins_points_effective": {
                    "$add": [
                        "$ins_points",
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        "$is_ins_leader_empid",
                                        {
                                            "$regexMatch": {
                                                "input": "$rm_lower",
                                                "regex": INS_LEADER_EMP_REGEX,
                                            }
                                        },
                                    ]
                                },
                                {"$ifNull": ["$leader_ins_points", 0]},
                                0,
                            ]
                        },
                    ]
                },
                "mf_points_effective": {
                    "$add": [
                        "$mf_points_base",
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        "$is_mf_leader_empid",
                                        {
                                            "$regexMatch": {
                                                "input": "$rm_lower",
                                                "regex": MF_LEADER_EMP_REGEX,
                                            }
                                        },
                                    ]
                                },
                                {"$ifNull": ["$leader_inv_points", 0]},
                                0,
                            ]
                        },
                    ]
                },
            }
        },
        {
            "$addFields": {
                "ins_slab_label": {
                    "$switch": {
                        "branches": ins_label_branches,
                        "default": ins_label_default,
                    }
                },
                "ins_fresh_pct": {
                    "$switch": {
                        "branches": ins_fresh_branches,
                        "default": ins_fresh_default,
                    }
                },
                "ins_renew_pct": {
                    "$switch": {
                        "branches": ins_renew_branches,
                        "default": ins_renew_default,
                    }
                },
                "ins_bonus_rupees": {
                    "$switch": {
                        "branches": ins_bonus_branches,
                        "default": ins_bonus_default,
                    }
                },
            }
        },
        # Gather monthly fresh/renew premium from Insurance_Policy_Scoring (best-effort schema)
        {
            "$lookup": {
                "from": "Insurance_Policy_Scoring",
                "let": {"emp": "$employee_id"},
                "pipeline": [
                    {
                        "$match": {
                            "conversion_date": {"$gte": start, "$lt": end},
                        }
                    },
                    {
                        "$match": {
                            "$expr": {"$eq": [{"$toString": "$employee_id"}, "$$emp"]},
                        }
                    },
                    {
                        "$project": {
                            "this_year_premium": {
                                "$toDouble": {"$ifNull": ["$this_year_premium", 0]}
                            },
                            "renewal_notice_premium": {
                                "$toDouble": {"$ifNull": ["$renewal_notice_premium", 0]}
                            },
                            "last_year_premium": {
                                "$toDouble": {"$ifNull": ["$last_year_premium", 0]}
                            },
                            "policy_classification": {
                                "$toLower": {"$ifNull": ["$policy_classification", ""]}
                            },
                            "conversion_status": {
                                "$toLower": {"$ifNull": ["$conversion_status", ""]}
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "renew_flag": {
                                "$or": [
                                    {"$in": ["$policy_classification", ["renewal", "renew"]]},
                                    {
                                        "$regexMatch": {
                                            "input": "$conversion_status",
                                            "regex": "renew",
                                        }
                                    },
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "fresh_prem": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$renew_flag", False]},
                                        {"$ifNull": ["$this_year_premium", 0]},
                                        0,
                                    ]
                                }
                            },
                            "renew_prem": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$renew_flag", True]},
                                        {
                                            "$ifNull": [
                                                {
                                                    "$ifNull": [
                                                        "$renewal_notice_premium",
                                                        "$last_year_premium",
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        0,
                                    ]
                                }
                            },
                        }
                    },
                ],
                "as": "prem",
            }
        },
        {
            "$addFields": {
                "fresh_premium": {"$ifNull": [{"$first": "$prem.fresh_prem"}, 0]},
                "renew_premium": {"$ifNull": [{"$first": "$prem.renew_prem"}, 0]},
            }
        },
        {
            "$addFields": {
                "ins_rupees_from_fresh": {
                    "$round": [{"$multiply": ["$ins_fresh_pct", "$fresh_premium"]}, 2]
                },
                "ins_rupees_from_renew": {
                    "$round": [{"$multiply": ["$ins_renew_pct", "$renew_premium"]}, 2]
                },
                "ins_rupees_total": {
                    "$add": [
                        "$ins_bonus_rupees",
                        {"$round": [{"$multiply": ["$ins_fresh_pct", "$fresh_premium"]}, 2]},
                        {"$round": [{"$multiply": ["$ins_renew_pct", "$renew_premium"]}, 2]},
                    ]
                },
            }
        },
        # ---- Mutual Fund tier & payout (Combined & Split) ----
        {
            "$addFields": {
                # Helper macro for Tier Calculation
                # Standard Tiers
                "mf_tier_calc": {
                    "$function": {
                        "body": unified_tier_js,
                        "args": ["$mf_points_effective"],
                        "lang": "js"
                    }
                },
                 "mf_tier_sip_calc": {
                    "$function": {
                        "body": sip_tier_js,
                        "args": ["$mf_sip_points"],
                        "lang": "js"
                    }
                },
                 "mf_tier_lump_calc": {
                    "$function": {
                        "body": lump_tier_js,
                        "args": ["$mf_lumpsum_points"],
                        "lang": "js"
                    }
                }
            }
        },
        {
            "$addFields": {
                "mf_tier": "$mf_tier_calc",
                "mf_sip_tier": "$mf_tier_sip_calc",
                "mf_lump_tier": "$mf_tier_lump_calc"
            }
        },
        # Helper: Factor from Tier
        {
            "$addFields": {
               "factor_lookup": {
                    "$function": {
                        "body": unified_factor_js,
                        "args": ["$mf_tier"],
                        "lang": "js"
                    }
               },
               "factor_sip_lookup": {
                    "$function": {
                        "body": sip_factor_js,
                        "args": ["$mf_sip_tier"],
                        "lang": "js"
                    }
               },
               "factor_lump_lookup": {
                    "$function": {
                        "body": lump_factor_js,
                        "args": ["$mf_lump_tier"],
                        "lang": "js"
                    }
               }
            }
        },
        {
            "$addFields": {
                "mf_factor": "$factor_lookup",
                "mf_sip_factor": "$factor_sip_lookup",
                "mf_lumpsum_factor": "$factor_lump_lookup",
            }
        },
        {
             "$addFields": {
                 "mf_rupees": mf_rupees_expr,
                 "mf_sip_rupees": {
                     "$round": [{"$multiply": ["$sip_aum_derived", "$mf_sip_factor"]}, 2]
                 },
                 "mf_lumpsum_rupees": {
                    "$round": [{"$multiply": ["$lump_aum_raw", "$mf_lumpsum_factor"]}, 2]
                 },
             }
        },
        # ---- Referral Logic (No changes requested) ----
        {
            "$addFields": {
                "ref_rupees": {
                    # Non-monetary incentive policy (User Request 2025-12-27)
                    "$literal": 0
                }
            }
        },
        # ---- Final Total ----
        {
            "$addFields": {
                "total_incentive": {
                    "$add": [
                        {"$ifNull": ["$ins_rupees_total", 0]},
                        {"$ifNull": ["$mf_rupees", 0]},
                        {"$ifNull": ["$ref_rupees", 0]},
                    ]
                },
                "audit": {
                    "tier": "$mf_tier",
                    "rate": "$mf_factor",
                    "ins_slab": "$ins_slab_label",
                    "unified_logic": True
                }
            }
        },
        # ---- Gamification / Badge System ----
        {
            "$addFields": {
                "badges": {
                    "$filter": {
                        "input": badge_logic,
                        "as": "b",
                        "cond": {"$ne": ["$$b", None]}
                    }
                }
            }
        },

        # Write to Rupee_Incentives
        {
            "$project": {"_id": 0}
        },
        {
            "$merge": {
                "into": "Rupee_Incentives",
                "on": ["rm_name", "period_month"],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


# ---------- Extra Debug Logging Helpers ----------
# (Moved here from top of file to follow import and config section)
def _log_insurance_debug(db, period_month: str, start: datetime, end: datetime) -> None:
    """
    Extra debug: show how insurance points are distributed per RM for the month.
    """
    try:
        pipeline = [
            {
                "$match": {
                    "conversion_date": {"$gte": start, "$lt": end},
                    "employee_id": {"$ne": None},
                }
            },
            {
                "$addFields": {
                    "employee_id": {"$toString": "$employee_id"},
                }
            },
            {
                "$group": {
                    "_id": {
                        "employee_id": "$employee_id",
                        "employee_name": "$employee_name",
                        "period_month": {
                            "$dateToString": {
                                "format": "%Y-%m",
                                "date": "$conversion_date",
                            }
                        },
                    },
                    "total_points": {
                        "$sum": {
                            "$ifNull": [
                                "$total_points",
                                {"$ifNull": ["$points_policy", 0]},
                            ]
                        }
                    },
                    "fresh_premium": {
                        "$sum": {
                            "$toDouble": {
                                "$ifNull": ["$this_year_premium", 0],
                            }
                        }
                    },
                    "renewal_premium": {
                        "$sum": {
                            "$toDouble": {
                                "$ifNull": ["$last_year_premium", 0],
                            }
                        }
                    },
                }
            },
            {
                "$match": {
                    "_id.period_month": period_month,
                }
            },
            {"$sort": {"total_points": -1}},
            {"$limit": 10},
        ]
        rows = list(db.Insurance_Policy_Scoring.aggregate(pipeline))
        logging.debug(
            "[Leaderboard][Debug][INS] Month=%s top insurance rows (max 10): %s",
            period_month,
            rows,
        )
    except Exception:
        logging.exception(
            "[Leaderboard][Debug][INS] Failed to aggregate insurance points for %s",
            period_month,
        )


def _log_public_leaderboard_debug(db, period_month: str) -> None:
    """
    Extra debug: show top public leaderboard rows for the month.
    """
    try:
        cursor = (
            db.Public_Leaderboard.find({"period_month": period_month})
            .sort("total_points_public", -1)
            .limit(10)
        )
        rows = list(cursor)
        logging.debug(
            "[Leaderboard][Debug][Public] Month=%s top public rows (max 10): %s",
            period_month,
            rows,
        )
    except Exception:
        logging.exception(
            "[Leaderboard][Debug][Public] Failed to read public leaderboard rows for %s",
            period_month,
        )


# ---------- Runner ----------
def run(
    month: str,
    mongo_uri: str | None = None,
    db_name: str | None = None,
    process_full_fy: bool = False,
):
    """Run public leaderboard and rupee incentive pipelines.

    If process_full_fy=False (default), only the given `month` (YYYY-MM) is processed.
    If process_full_fy=True, the full FY (Apr–Mar) containing `month` is processed.
    """
    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        raise RuntimeError(
            "Mongo connection string not found in env (MongoDb-Connection-String / MONGODB_URI)."
        )
    db_name = db_name or DB_NAME

    logging.info("[Leaderboard] Connecting to MongoDB database '%s'", db_name)
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # Ensure helpful indexes (idempotent)
    for coll, spec in [
        ("Public_Leaderboard", [("rm_name", 1), ("period_month", 1)]),
        ("Public_Leaderboard", [("period_month", 1)]),
        ("Public_Leaderboard", [("employee_id", 1)]),
        ("Rupee_Incentives", [("rm_name", 1), ("period_month", 1)]),
        ("Rupee_Incentives", [("period_month", 1)]),
        ("Rupee_Incentives", [("employee_id", 1)]),
    ]:
        try:
            db[coll].create_index(spec, unique=(spec == [("rm_name", 1), ("period_month", 1)]))
        except Exception:
            pass

    # Decide which months to process
    if process_full_fy:
        months = fy_months_for(month)
        logging.info(
            "[Leaderboard] Processing FULL FY (Apr–Mar) anchored at %s; months=%s → %s",
            month,
            months[0],
            months[-1],
        )
    else:
        months = [month]
        logging.info("[Leaderboard] Processing SINGLE month=%s", month)

    # Log the full list of months that will be processed in this run
    logging.info("[Leaderboard] Months in this run: %s", ", ".join(months))

    # Execute pipelines for each month
    for m in months:
        start, end = month_window(m)
        logging.info(
            "[Leaderboard] Running pipelines for period_month=%s (start=%s, end=%s)",
            m,
            start.isoformat(),
            end.isoformat(),
        )

        # Debug: source collection counts for this month
        try:
            sip_count = db.MF_SIP_Leaderboard.count_documents({"period_month": m})
            ls_count = db.Leaderboard_Lumpsum.count_documents({"period_month": m})
            ins_raw = db.Insurance_Policy_Scoring.count_documents(
                {"conversion_date": {"$gte": start, "$lt": end}, "employee_id": {"$ne": None}}
            )
            ref1 = db.referralLeaderboard.count_documents({"period_month": m})
            ref2 = db.Referral_Incentives.count_documents({"period_month": m})
            logging.debug(
                "[Leaderboard][Debug] Source counts for %s -> SIP=%d, LS=%d, INS=%d, REF1=%d, REF2=%d",
                m,
                sip_count,
                ls_count,
                ins_raw,
                ref1,
                ref2,
            )
            _log_insurance_debug(db, m, start, end)
        except Exception:
            logging.exception("[Leaderboard][Debug] Failed to read source counts for %s", m)

        pub_pipe = build_public_leaderboard_pipeline(m, start, end)

        # Load Configs
        sip_config = load_sip_config()
        ins_config = load_insurance_config()
        ref_config = load_referral_config()
        inc_pipe = build_rupee_incentives_pipeline(m, start, end, sip_config=sip_config, ins_config=ins_config, ref_config=ref_config)

        # Public leaderboard pipeline is anchored on MF_SIP_Leaderboard
        # (it uses $unionWith to pull in other buckets).
        list(db.MF_SIP_Leaderboard.aggregate(pub_pipe, allowDiskUse=True))

        # Rupee incentives pipeline must be anchored on Public_Leaderboard,
        # since it expects one input row per RM/month already written there.
        list(db.Public_Leaderboard.aggregate(inc_pipe, allowDiskUse=True))

        logging.info("[Leaderboard] Finished pipelines for period_month=%s", m)

        # Debug: output collection counts for this month
        try:
            pub_count = db.Public_Leaderboard.count_documents({"period_month": m})
            inc_count = db.Rupee_Incentives.count_documents({"period_month": m})
            logging.debug(
                "[Leaderboard][Debug] Output counts for %s -> Public_Leaderboard=%d, Rupee_Incentives=%d",
                m,
                pub_count,
                inc_count,
            )
            _log_public_leaderboard_debug(db, m)
        except Exception:
            logging.exception("[Leaderboard][Debug] Failed to read output counts for %s", m)

    return True


# ---------- FY-to-date Runner ----------
def run_fy_to_date(
    anchor_month: str,
    mongo_uri: str | None = None,
    db_name: str | None = None,
) -> None:
    """Run single-month pipelines for all months in the FY up to `anchor_month`.

    FY is Apr–Mar. For example, if anchor_month='2025-11', this will run
    Apr 2025 through Nov 2025 (inclusive), one month at a time, using the
    existing `run` function in single-month mode.
    """

    # Resolve connection defaults similarly to `run`
    if not mongo_uri:
        mongo_uri = MONGO_URI
    if not mongo_uri:
        raise RuntimeError(
            "Mongo connection string not found in env (MongoDb-Connection-String / MONGODB_URI)."
        )
    db_name = db_name or DB_NAME

    # Compute all FY months and trim to <= anchor_month
    all_fy_months = fy_months_for(anchor_month)
    months_upto_anchor = [m for m in all_fy_months if m <= anchor_month]

    logging.info(
        "[Leaderboard] FY-to-date run anchored at %s; months=%s",
        anchor_month,
        ", ".join(months_upto_anchor),
    )

    for m in months_upto_anchor:
        logging.info("[Leaderboard][FY-TD] Running month=%s", m)
        run(m, mongo_uri=mongo_uri, db_name=db_name, process_full_fy=False)
        logging.info("[Leaderboard][FY-TD] Finished month=%s", m)


def run_for_configured_range(
    anchor_month: str, mongo_uri: str | None = None, db_name: str | None = None
) -> None:
    cfg = load_leaderboard_config(mongo_uri=mongo_uri, db_name=db_name)
    months = resolve_months_for_range(anchor_month, cfg)

    logging.info(
        "[Leaderboard] Config range_mode=%s; months to run: %s",
        cfg.get("range_mode", "twomonths"),
        ", ".join(months),
    )

    for m in months:
        logging.info("[Leaderboard] Triggering run() for month=%s", m)
        run(m, mongo_uri=mongo_uri, db_name=db_name, process_full_fy=False)


def main(mytimer: func.TimerRequest) -> None:
    """Azure Functions timer entrypoint."""
    trigger_time = datetime.now(timezone.utc).isoformat()
    logging.info("[Leaderboard] Timer fired at %s", trigger_time)
    if getattr(mytimer, "past_due", False):
        logging.warning("[Leaderboard] Timer is running past due.")

    anchor_month = _resolve_target_month()
    logging.info("[Leaderboard] Anchor month resolved as %s", anchor_month)

    try:
        run_for_configured_range(anchor_month)
    except Exception:
        logging.exception(
            "[Leaderboard] Leaderboard pipeline failed for anchor_month=%s", anchor_month
        )
        raise

    logging.info("[Leaderboard] Completed leaderboard pipelines for anchor_month=%s", anchor_month)


if __name__ == "__main__":
    # Example:
    #   export MongoDb-Connection-String="mongodb+srv://..."
    #   python -m Leaderboard

    # Configure logging for local CLI runs so INFO logs are visible
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    anchor_month = _resolve_target_month()
    logging.info("[Leaderboard-CLI] Anchor month resolved as %s", anchor_month)

    cfg = load_leaderboard_config()
    months = resolve_months_for_range(anchor_month, cfg)
    logging.info(
        "[Leaderboard-CLI] Config range_mode=%s; months to run: %s",
        cfg.get("range_mode", "twomonths"),
        ", ".join(months),
    )

    for m in months:
        logging.info("[Leaderboard-CLI] Triggering run() for month=%s", m)
        run(m, process_full_fy=False)

    print(f"Done for {', '.join(months)}")
