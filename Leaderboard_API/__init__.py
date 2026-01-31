import logging
import azure.functions as func
import json
import os
import pymongo
import pymongo
from datetime import datetime, timezone
from ..utils import rbac
from ..utils.db_utils import get_db
import concurrent.futures
import sys
import os

# Adjust path to import otel_setup from root/Shared
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from otel_setup import setup_telemetry
    tracer, meter = setup_telemetry("pli-leaderboard-api")
except Exception as e:
    logging.warning(f"Failed to initialize OpenTelemetry: {e}")
else:
    logging.info(f"OpenTelemetry Initialized. Tracer: {tracer}, Meter: {meter}")



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Leaderboard_API processed a request.')

    # 1. Routing
    # Route format differs by host but we use route parameter support "leaderboard/{*route}"
    route_params = req.route_params
    subpath = route_params.get("route", "")

    # Dispatch
    if subpath == "" or subpath == "/": # list all
        return get_leaderboard(req)
    elif subpath == "me":
        return get_me(req)
    elif subpath == "me/breakdown":
        return get_me_breakdown(req)
    elif subpath == "breakdown":
        return get_all_breakdown(req)
    # New Admin Routes
    elif subpath.startswith("user/"):
        parts = subpath.split("/")
        # format: user/{id} or user/{id}/breakdown
        target_id = parts[1]
        # Handle rm_name: prefix for inactive RMs without employee_id
        if target_id.startswith("rm_name:"):
            target_id = target_id.replace("rm_name:", "")
        if len(parts) == 2:
            return get_user(req, target_id)
        elif len(parts) == 3 and parts[2] == "breakdown":
            return get_user_breakdown(req, target_id)
    elif subpath == "team-view":
        return get_team_view(req)
    elif subpath == "team-view/members":
        return get_team_view_members(req)
    elif subpath == "health":
        return func.HttpResponse(
            json.dumps({"status": "ok", "service": "leaderboard-api"}),
            mimetype="application/json"
        )
    elif subpath == "debug":
        uri = os.getenv("MongoDb-Connection-String")
        db_name = os.getenv("PLI_DB_NAME", os.getenv("DB_NAME", "PLI_Leaderboard_v2"))
        # Mask URI
        masked_uri = uri.split("@")[1] if "@" in uri else "Hidden"
        return func.HttpResponse(json.dumps({
            "db_name_in_use": db_name,
            "mongo_host_redacted": masked_uri,
            "collections_confirmed": ["Public_Leaderboard", "MF_SIP_Leaderboard", "Insurance_Policy_Scoring", "Config"]
        }), mimetype="application/json")

    return func.HttpResponse("Not Found", status_code=404)

# def get_db(): # Replaced by utils
#     uri = os.getenv("MongoDb-Connection-String")
#     client = pymongo.MongoClient(uri)
#     db_name = os.getenv("PLI_DB_NAME", os.getenv("DB_NAME", "PLI_Leaderboard_v2"))
#     return client[db_name]

def sanitize_for_json(obj):
    """Recursively replace NaN and Infinity with None in nested structures"""
    import math
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    return obj

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    import math
    # Handle NaN and Infinity
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, datetime):
        iso = obj.isoformat()
        if obj.tzinfo is None and 'Z' not in iso and '+' not in iso:
             return iso + 'Z'
        return iso
    return str(obj)

def get_leaderboard(req):
    try:
        month = req.params.get("month")
        if not month:
            # Default to current month
            month = datetime.utcnow().strftime("%Y-%m")

        view_mode = req.params.get("view") # 'MTD' (default) or 'YTD'
        db = get_db()

        # 1. Fetch Ignored RMs (Pre-fetch for DB filtering)
        # We can cache this or fetch once per request. Optimized to fetch only necessary fields.
        ignored_set = set()
        try:
            configs = list(db.config.find(
                {"_id": {"$in": ["Leaderboard_Lumpsum", "Leaderboard_SIP", "Leaderboard_Insurance"]}},
                {"ignored_rms": 1}
            ))
            for cfg in configs:
                for r in cfg.get("ignored_rms", []):
                    ignored_set.add(str(r).strip().lower())
        except Exception as e:
            logging.warning(f"Failed to fetch ignored RMs: {e}")

        # 2. Pipeline Construction
        pipeline = []

        # Timebox Logic
        if view_mode == "YTD":
            try:
                sy, sm = map(int, month.split('-'))
                start_year = sy if sm >= 4 else sy - 1
                start_month_str = f"{start_year}-04"
            except:
                start_month_str = f"{datetime.now().year}-04"

            logging.info(f"Fetching YTD Aggregation: {start_month_str} to {month}")

            # Match Range
            pipeline.append({
                "$match": {
                    "period_month": {"$gte": start_month_str, "$lte": month}
                }
            })

            # Filter Ignored RMs (Early Filter)
            if ignored_set:
                 pipeline.append({
                    "$match": {
                        "rm_name": {"$nin": list(ignored_set)}, # Case-sensitive exact match usually, lower logic in app?
                        # Ideally we normalize checks. The DB usually has correct case.
                        # If we need case-insensitive, we strictly need regex which is slow.
                        # Assuming 'ignored_rms' matches DB 'rm_name' casing roughly or we accept minor mismatch.
                        # For strictly Case-Insensitive filter in Aggregation without Regex:
                        # We can use $toLower in $expr.
                         "$expr": {
                             "$not": {
                                 "$in": [{"$toLower": "$rm_name"}, list(ignored_set)]
                             }
                        }
                    }
                })

            # Group (YTD Sums)
            pipeline.append({
                "$group": {
                    "_id": "$employee_id",
                    "total_points_public": {"$sum": {"$toDouble": "$total_points_public"}},
                    "ins_points": {"$sum": {"$toDouble": {"$ifNull": ["$ins_points", 0]}}},
                    "mf_points": {"$sum": {"$toDouble": {"$ifNull": ["$mf_points", 0]}}},
                    "ref_points": {"$sum": {"$toDouble": {"$ifNull": ["$ref_points", 0]}}},
                    # Keep metadata from first doc
                    "rm_name": {"$first": "$rm_name"},
                    "name": {"$first": "$name"},
                    "employee_id": {"$first": "$employee_id"}
                }
            })

            # YTD Adjustments Lookup
            pipeline.append({
                "$lookup": {
                    "from": "Leaderboard_Adjustments",
                    "let": {"eid": "$employee_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$employee_id", "$$eid"]},
                                        {"$eq": ["$status", "APPROVED"]},
                                        {"$gte": ["$month", start_month_str]},
                                        {"$lte": ["$month", month]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "adjustments_data"
                }
            })

        else:
            # MTD
            pipeline.append({
                "$match": {
                    "period_month": month
                }
            })

            # Ignored RMs Filter
            if ignored_set:
                 pipeline.append({
                    "$match": {
                         "$expr": {
                             "$not": {
                                 "$in": [{"$toLower": "$rm_name"}, list(ignored_set)]
                             }
                        }
                    }
                })

            # MTD Adjustments Lookup
            pipeline.append({
                "$lookup": {
                    "from": "Leaderboard_Adjustments",
                    "let": {"eid": "$employee_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$employee_id", "$$eid"]},
                                        {"$eq": ["$status", "APPROVED"]},
                                        {"$eq": ["$month", month]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "adjustments_data"
                }
            })

            # MTD Incentives Lookup
            pipeline.append({
                 "$lookup": {
                    "from": "Rupee_Incentives",
                    "localField": "employee_id",
                    "foreignField": "employee_id",
                    "pipeline": [
                        {"$match": {"period_month": month}}
                    ],
                    "as": "incentive_data"
                }
            })

        # Calculate Final Points & Format Adjustments
        # Strategy:
        # 1. Unwind adjustments (keep array but sum value) not really, better map/reduce inside project?
        # 2. Use $reduce to sum adjustment values.
        pipeline.append({
            "$addFields": {
                "adj_total": {
                    "$reduce": {
                        "input": "$adjustments_data",
                        "initialValue": 0,
                        "in": {"$add": ["$$value", {"$toDouble": {"$ifNull": ["$$this.value", 0]}}]}
                    }
                },
                "adjustments": {
                    "$map": {
                        "input": "$adjustments_data",
                        "as": "adj",
                        "in": {
                            "id": {"$toString": "$$adj._id"},
                            "reason": "$$adj.reason",
                            "val": {"$toDouble": "$$adj.value"},
                            "type": "$$adj.adjustment_type"
                        }
                    }
                }
            }
        })

        # Add Adjustments to Total
        pipeline.append({
            "$addFields": {
                "total_points_final": {
                    "$add": [
                        {"$toDouble": "$total_points_public"},
                        "$adj_total"
                    ]
                }
            }
        })

        # Incentive formatting (MTD only)
        if view_mode != "YTD":
            pipeline.append({
                "$addFields": {
                    "rupee_incentive": {
                        "$ifNull": [{"$arrayElemAt": ["$incentive_data", 0]}, {"total_incentive": 0}]
                    }
                }
            })

            # Calculate Slab (in-app logic moved to post-processing for logic reuse or complex slab logic?)
            # Or use $switch logic?
            # For strict speed, fetching slab config + $switch in aggregation is superior but complex to maintain.
            # Let's keep Slab Calculation in Python for flexibility, but it's O(N) fast loop.
        else:
            # YTD: No incentives shown typically, or sum? Requirement usually MTD.
             pipeline.append({
                "$addFields": {
                    "rupee_incentive": {"total_incentive": 0}
                }
            })

        # Sort
        pipeline.append({"$sort": {"total_points_final": -1}})

        # Execute
        leaderboard = list(db.Public_Leaderboard.aggregate(pipeline))

        # Post-Processing: Slab Calculation (Fast in-memory)
        # Fetch config once
        incentive_slabs = []
        if view_mode != "YTD":
             sip_config = db.config.find_one({"_id": "Leaderboard_SIP_Config"}) or {}
             incentive_slabs = sip_config.get("incentive_slabs", [])
             incentive_slabs.sort(key=lambda x: x.get("min", 0), reverse=True)

        for row in leaderboard:
            # Fix IDs
            if "_id" in row and not isinstance(row["_id"], str):
                 row["_id"] = str(row["_id"])

            # Remove helper fields
            row.pop("adjustments_data", None)
            row.pop("incentive_data", None)
            row.pop("adj_total", None)

            # Slab Logic
            if view_mode != "YTD":
                current_total = float(row.get("rupee_incentive", {}).get("total_incentive", 0))
                slab_label = "S1"; slab_name = "₹0 - ₹9,999"
                if incentive_slabs:
                    for s in incentive_slabs:
                        if current_total >= s.get("min", 0):
                            slab_label = s.get("label"); slab_name = s.get("name")
                            break
                if "rupee_incentive" not in row: row["rupee_incentive"] = {}
                row["rupee_incentive"]["current_slab"] = {"label": slab_label, "name": slab_name}

        return func.HttpResponse(
            json.dumps(leaderboard, default=json_serial),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error in get_leaderboard: {e}", exc_info=True)
        return func.HttpResponse("Internal Server Error", status_code=500)

    except Exception as e:
        logging.error(f"Error in get_leaderboard: {e}", exc_info=True)
        return func.HttpResponse("Internal Server Error", status_code=500)

def get_me(req):
    email = rbac.get_user_email(req)
    if not email:
        return func.HttpResponse("Unauthorized", status_code=401)

    # Lookup Employee ID from Email via Zoho_Users
    db = get_db()
    user = db.Zoho_Users.find_one({"email": email}) # Case insensitive?
    if not user:
         # Try regex case insensitive
         user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})

    if not user:
        return func.HttpResponse("User not linked to Employee ID", status_code=403)

    eid = user.get("id")
    return fetch_user_stats(req, eid)

def get_user(req, employee_id):
    # NOTE: Authorization Check should happen at Gateway Level via RBAC Service.
    # But as defense in depth, we could check headers here if forwarded.
    # For now, we trust the Gateway to only let Admins hit this.
    return fetch_user_stats(req, employee_id)

def fetch_user_stats(req, eid):
    db = get_db()
    month = req.params.get("month", datetime.utcnow().strftime("%Y-%m"))

    # Reuse logic but filter
    # Ideally optimize query
    row = db.Public_Leaderboard.find_one({"employee_id": eid, "period_month": month})

    # Fallback: If not found by employee_id, try by rm_name (for inactive RMs without employee_id)
    if not row:
        # Check if eid is non-numeric (likely an rm_name)
        try:
            int(eid)
        except (ValueError, TypeError):
            # eid is not numeric, try querying by rm_name
            row = db.Public_Leaderboard.find_one({"rm_name": eid, "period_month": month})

    if not row:
        return func.HttpResponse(json.dumps({}), mimetype="application/json")

    # Adjustments - try both employee_id and rm_name
    adjs = list(db.Leaderboard_Adjustments.find({"employee_id": eid, "month": month, "status": "APPROVED"}))
    if not adjs and row.get("rm_name"):
        adjs = list(db.Leaderboard_Adjustments.find({"rm_name": row.get("rm_name"), "month": month, "status": "APPROVED"}))

    pts = float(row.get("total_points_public", 0))
    applied = []
    for a in adjs:
        val = float(a.get("value", 0))
        if a.get("adjustment_type") == "Points":
            pts += val
        applied.append({"reason": a.get("reason"), "val": val, "type": a.get("adjustment_type")})

    out = dict(row)
    out["total_points_final"] = pts
    out["adjustments"] = applied
    out.pop("_id", None)

    return func.HttpResponse(json.dumps(out, default=json_serial), mimetype="application/json")

def get_me_breakdown(req):
    # Authenticate
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    db = get_db()
    user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})
    if not user: return func.HttpResponse("Forbidden", status_code=403)

    eid = user.get("id")
    return fetch_user_breakdown(req, eid)

def get_user_breakdown(req, employee_id):
    # Trust Gateway AuthZ
    res = fetch_user_breakdown(req, employee_id)
    if res is None:
        logging.error(f"[Breakdown] fetch_user_breakdown returned None for {employee_id}")
        return func.HttpResponse(json.dumps({"error": "Internal Error: None Result"}), status_code=500, mimetype="application/json")
    return res

def fetch_user_breakdown(req, eid):
    month = req.params.get("month", datetime.utcnow().strftime("%Y-%m"))
    db = get_db()

    # Fetch Intermediate Datas
    # Fetch Intermediate Datas
    # Pre-fetch Name for fallback (as some collections miss employee_id)
    user_doc = db.Zoho_Users.find_one({"id": eid})
    if not user_doc:
        # Try int fallback
        try:
            user_doc = db.Zoho_Users.find_one({"id": int(eid)})
        except:
            pass

    name_fallback = None
    if user_doc:
        logging.info(f"[Breakdown] Found User Doc. Keys: {list(user_doc.keys())}")
        # Try various name fields
        name_fallback = user_doc.get("Name") or user_doc.get("name") or user_doc.get("Full_Name") or user_doc.get("full_name")
        if not name_fallback:
             # Construct from parts
             fname = user_doc.get("First_Name") or user_doc.get("first_name") or ""
             lname = user_doc.get("Last_Name") or user_doc.get("last_name") or ""
             if fname or lname:
                 name_fallback = f"{fname} {lname}".strip()

    # EPIC E2 FIX: If Zoho Lookup Failed (Historical User), try Public Leaderboard which is Authoritative
    if not name_fallback:
        pub_row = db.Public_Leaderboard.find_one({"employee_id": eid}, sort=[("period_month", -1)])
        if pub_row:
             name_fallback = pub_row.get("rm_name")
             logging.info(f"[Breakdown] Recovered Name from Public Leaderboard: {name_fallback}")

    # NEW: If still no name_fallback and eid looks like a name (contains space or is not numeric), treat it as rm_name
    if not name_fallback:
        try:
            int(eid)  # If this succeeds, it's a numeric ID
        except (ValueError, TypeError):
            # eid is not numeric, likely an rm_name passed directly
            name_fallback = eid
            logging.info(f"[Breakdown] Treating eid as rm_name directly: {name_fallback}")

    logging.info(f"[Breakdown] Fetching for ID: {eid}, Fallback Name: {name_fallback}")

    logging.info(f"[Breakdown] Fetching for ID: {eid}, Fallback Name: {name_fallback}")

    # parallel fetch
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        f_sip = executor.submit(lambda: db.MF_SIP_Leaderboard.find_one({
            "$or": [{"period_month": month}, {"month": month}],
            "employee_id": eid
        }) or (db.MF_SIP_Leaderboard.find_one({
             "$or": [{"period_month": month}, {"month": month}],
             "rm_name": name_fallback
        }) if name_fallback else None))

        f_lumpsum = executor.submit(lambda: db.Leaderboard_Lumpsum.find_one({"month": month, "employee_id": eid}) or (
            db.Leaderboard_Lumpsum.find_one({"month": month, "employee_name": name_fallback}) if name_fallback else None
        ))

        f_ins = executor.submit(lambda: list(db.Insurance_Policy_Scoring.find({"period_month": month, "employee_id": eid})) or (
             list(db.Insurance_Policy_Scoring.find({"period_month": month, "employee_name": name_fallback})) if name_fallback else []
        ))

        f_ref = executor.submit(lambda: list(db.referralLeaderboard.find({"period_month": month, "employee_id": eid})) or (
             list(db.referralLeaderboard.find({"period_month": month, "referrer_name": name_fallback})) if name_fallback else []
        ))

        f_inc = executor.submit(lambda: db.Rupee_Incentives.find_one({"period_month": month, "employee_id": eid}) or (
             db.Rupee_Incentives.find_one({"period_month": month, "rm_name": name_fallback}) if name_fallback else None
        ))

        f_sum = executor.submit(lambda: db.Public_Leaderboard.find_one({"employee_id": eid, "period_month": month}) or (
            db.Public_Leaderboard.find_one({"rm_name": name_fallback, "period_month": month}) if name_fallback else None
        ))

        sip = f_sip.result()
        lumpsum = f_lumpsum.result()
        insurance = f_ins.result()
        ref = f_ref.result()
        rupee_incentive = f_inc.result()
        summary_row = f_sum.result()

    res = {
        "employee_id": eid,
        "month": month,
        "sip": sip,
        "lumpsum": lumpsum,
        "insurance_policies": insurance,
        "referral": ref,
        "rupee_incentive": rupee_incentive,
        "summary": summary_row
    }

    # [FIX] On-the-fly Rupee Incentive Calculation for Inactive/Missing RMs
    # If rupee_incentive was not found in the DB, try to calculate it on-the-fly
    if not res.get("rupee_incentive") or res["rupee_incentive"] == {"total_incentive": 0}:
        try:
            from .incentive_logic import build_rupee_incentives_pipeline
            # datetime and timezone are available globally

            # We need the period_month (YYYY-MM) and window for calculation
            # m comes from the argument
            # We need start/end datetimes for the pipeline
            y_int, m_int = map(int, month.split("-"))
            start_dt = datetime(y_int, m_int, 1, 0, 0, 0, tzinfo=timezone.utc)
            if m_int == 12:
                end_dt = datetime(y_int + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            else:
                end_dt = datetime(y_int, m_int + 1, 1, 0, 0, 0, tzinfo=timezone.utc) # Corrected hour to 0

            # Determine the correct name/ID filter
            match_stage = {"period_month": month}
            if eid and str(eid).isdigit(): # Ensure eid is treated as string for comparison
                 match_stage["employee_id"] = eid
            elif name_fallback: # Use name_fallback if eid is not numeric
                 match_stage["rm_name"] = name_fallback
            else:
                # If neither ID nor name fallback is available, we can't calculate
                raise ValueError("Cannot determine employee identifier for incentive calculation.")

            # Fetch the SIP config to pass to the pipeline
            config_doc = None
            try:
                # Connect to V2 DB for config
                v2_db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard_v2")
                # Reuse UR from get_db() context or environment
                # Reuse UR from get_db() context or environment
                db_v2 = get_db(default_db=v2_db_name)
                config_doc = db_v2.config.find_one({"_id": "Leaderboard_SIP"})
            except Exception as e:
                logging.warning(f"Failed to fetch SIP config from V2 DB: {e}")

            # 1. Fetch the base row from Public_Leaderboard to seed the pipeline
            # The pipeline 'build_rupee_incentives_pipeline' expects to run on Public_Leaderboard
            # We will run it, but restricted to this user.

            pipeline = build_rupee_incentives_pipeline(month, start_dt, end_dt, sip_config=config_doc)

            # Prepend a match step to limit to just this user
            pipeline.insert(0, {"$match": match_stage})

            # Execute on Public_Leaderboard
            calc_res = list(db.Public_Leaderboard.aggregate(pipeline))

            if calc_res:
                # We found and calculated data!
                calculated_doc = calc_res[0]
                res["rupee_incentive"] = calculated_doc # Update the singular key
                # Ensure we have the audit trail
                if "audit" not in res["rupee_incentive"]:
                     res["rupee_incentive"]["audit"] = {}
                res["rupee_incentive"]["audit"]["calculated_at_runtime"] = True

        except Exception as ex:
            logging.error(f"Failed to calculate on-the-fly incentives for {eid}/{name_fallback}: {ex}", exc_info=True)
            # Non-critical, just continue without bounty data
            pass

    # [FIX] On-the-fly Quarterly/Annual Bonus Projection (if missing)
    # Check if this is a quarter-end month
    try:
        y_str, m_str = month.split("-")
        req_dt = datetime(int(y_str), int(m_str), 15)
        # Determine Quarter Ends (hardcoded checking standard quarters)
        # Simplest: check if month is 3, 6, 9, 12.
        # But depends on FY_MODE? Assuming FY_APR or CAL, QE months are same: Mar, Jun, Sep, Dec.
        is_qe_month = int(m_str) in (3, 6, 9, 12)

        # Condition: Is QE month AND (Lumpsum missing OR bonus_projected missing)
        need_projection = is_qe_month and (
            not res.get("lumpsum") or not res["lumpsum"].get("bonus_projected")
        )

        if need_projection:
             if not res.get("lumpsum"):
                 res["lumpsum"] = {} # Initialize if missing

             # Fetch defaults from config
             # We reuse the logic from rupee incentive block, fetch Config if not loaded
             if "config_doc" not in locals() or not config_doc:
                 try:
                    db_v2 = get_db()
                    config_doc = db_v2.config.find_one({"_id": "Leaderboard_Lumpsum"})
                 except:
                    config_doc = {}

             # Load templates
             q_tmpl = config_doc.get("qtr_bonus_template") or {
                "slabs": [
                    {"min_np": 0, "bonus_rupees": 0},
                    {"min_np": 1000000, "bonus_rupees": 0},
                    {"min_np": 2500000, "bonus_rupees": 0},
                    {"min_np": 5000000, "bonus_rupees": 0}
                ],
                "min_positive_months": 2
             }
             a_tmpl = config_doc.get("annual_bonus_template") or {
                "slabs": [
                    {"min_np": 0, "bonus_rupees": 0},
                    {"min_np": 3000000, "bonus_rupees": 0},
                    {"min_np": 7500000, "bonus_rupees": 0},
                    {"min_np": 12000000, "bonus_rupees": 0}
                ],
                 "min_positive_months": 6
             }

             # Calculate Quarter Bounds
             qs, qe, q_label = _get_quarter_bounds_api(req_dt) # default FY_APR
             # Calculate FY Bounds
             fys, fye, fy_label = _get_fy_bounds_api(req_dt)

             # Format for MongoDB query YYYY-MM
             def format_month(dt): return dt.strftime("%Y-%m")

             # Generate list of months in Quarter and FY up to current
             q_months = []
             curr = qs
             while curr <= qe:
                 q_months.append(format_month(curr))
                 # next month
                 if curr.month == 12: curr = datetime(curr.year + 1, 1, 1)
                 else: curr = datetime(curr.year, curr.month + 1, 1)

             fy_months = []
             curr = fys
             while curr <= fye:
                 fy_months.append(format_month(curr))
                 if curr.month == 12: curr = datetime(curr.year + 1, 1, 1)
                 else: curr = datetime(curr.year, curr.month + 1, 1)

             # Fetch Lumpsum records for this user for these periods
             # Query: { month: { $in: fy_months }, employee_id: eid }
             # Filter duplicates? No, Leaderboard_Lumpsum is one per user per month.
             match_q = {"month": {"$in": q_months}}
             match_a = {"month": {"$in": fy_months}}

             if eid and str(eid).isdigit():
                 match_q["employee_id"] = eid
                 match_a["employee_id"] = eid
             elif name_fallback:
                 match_q["rm_name"] = name_fallback
                 match_a["rm_name"] = name_fallback

             # Aggregate Query
             # Since q_months is subset of fy_months (usually), we can just fetch fy_months and filter in memory
             recs = list(db.Leaderboard_Lumpsum.find(match_a))

             # Compute Aggregates
             q_np = 0.0
             q_pos = 0
             a_np = 0.0
             a_pos = 0

             for r in recs:
                 m_key = r.get("month")
                 # Net Purchase (Formula) is what Scorer uses?
                 # Or use "Net Purchase"? "Net Purchase" in Breakdown Totals is final.
                 # Let's check root "net_lumpsum"? No, Lumpsum doc has "NetPurchase" object usually or at root?
                 # Inspecting schema: root has "net_purchase" (float)? No, usually structured.
                 # Let's rely on Breakdown.Totals['Net Purchase (Formula)'] if present, else fallback
                 np_val = 0.0
                 try:
                     np_val = float(r.get("Breakdown", {}).get("Totals", {}).get("Net Purchase (Formula)", 0))
                 except:
                     np_val = 0.0

                 # Positive Month?
                 is_pos = 1 if np_val > 0 else 0

                 # Add to Annual
                 a_np += np_val
                 a_pos += is_pos

                 # Add to Quarter if in q_months
                 if m_key in q_months:
                     q_np += np_val
                     q_pos += is_pos

             # Calculate Bonuses
             q_bonus_val = _select_np_slab_bonus_api(q_np, q_tmpl)
             a_bonus_val = _select_np_slab_bonus_api(a_np, a_tmpl)

             q_min = int(q_tmpl.get("min_positive_months", 2))
             a_min = int(a_tmpl.get("min_positive_months", 6))

             q_qual = (q_pos >= q_min)
             a_qual = (a_pos >= a_min)

             # Populate Projection
             res["lumpsum"]["bonus_projected"] = {
                "quarterly": {
                    "period": q_label,
                    "net_purchase_qtd": q_np,
                    "positive_months": q_pos,
                    "projected_amount": q_bonus_val if q_qual else 0.0,
                    "potential_amount": q_bonus_val,
                    "is_qualified": q_qual,
                    "min_positive_months_req": q_min
                },
                "annual": {
                    "period": fy_label,
                    "net_purchase_ytd": a_np,
                    "positive_months": a_pos,
                    "projected_amount": a_bonus_val if a_qual else 0.0,
                    "potential_amount": a_bonus_val,
                    "is_qualified": a_qual,
                    "min_positive_months_req": a_min
                }
             }

             # Mark as runtime calculated
             res["lumpsum"]["bonus_projected_runtime"] = True

    except Exception as ex:
        logging.warning(f"Failed on-the-fly bonus projection: {ex}")
        pass

    # Sanitize NaN/Infinity values before JSON serialization
    res = sanitize_for_json(res)

    logging.info("[Breakdown] Returning HTTP Response")
    return func.HttpResponse(json.dumps(res, default=json_serial), mimetype="application/json")

def get_team_view(req):
    """
    GET /api/leaderboard/team-view?month=YYYY-MM
    Returns aggregated team data for a given month.
    Uses group_type + group_key design to handle different grouping strategies.
    RBAC: Requires admin or superadmin role.
    """
    # RBAC Check
    user_email = rbac.get_user_email(req)
    if not rbac.is_admin(user_email):
        # Dev-only debug information
        is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"
        error_response = {"error": "Forbidden: Admin access required"}
        if is_dev:
            error_response["debug_user_email"] = user_email or "(none detected)"
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=403,
            mimetype="application/json"
        )

    try:
        month = req.params.get("month")
        if not month:
            month = datetime.utcnow().strftime("%Y-%m")

        logging.info(f"get_team_view: month={month}, user_email={user_email}")

        db = get_db()
        logging.info(f"Connected to database: {db.name}")

        # Ensure indexes exist for performance
        try:
            db.Public_Leaderboard.create_index([("period_month", 1), ("team_id", 1)])
            db.Public_Leaderboard.create_index([("period_month", 1), ("reporting_manager_id", 1)])
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")

        # Aggregate teams by team_id
        pipeline = [
            {"$match": {"period_month": month}},
            {
                "$group": {
                    "_id": "$team_id",
                    "manager_id": {"$first": "$reporting_manager_id"},
                    "manager_name": {"$first": "$RM_Name"},
                    "member_count": {"$sum": 1},
                    "total_points": {"$sum": {"$ifNull": ["$total_points_public", 0]}},
                    "total_mf_points": {"$sum": {"$ifNull": ["$mf_points", 0]}},
                    "total_ins_points": {"$sum": {"$ifNull": ["$ins_points", 0]}},
                    "total_ref_points": {"$sum": {"$ifNull": ["$ref_points", 0]}}
                }
            },
            {"$sort": {"total_points": -1}}
        ]

        logging.info(f"Executing aggregation pipeline for month: {month}")
        results = list(db.Public_Leaderboard.aggregate(pipeline))
        logging.info(f"Aggregation returned {len(results)} teams")

        teams = []
        for r in results:
            team_id = r.get("_id")

            # Determine group_type and group_key
            if team_id and team_id != "":
                group_type = "team"
                group_key = team_id
                team_name = team_id
            elif r.get("manager_id"):
                group_type = "manager"
                group_key = r.get("manager_id")
                team_name = f"{r.get('manager_name', 'Unknown')}'s Team"
            else:
                group_type = "unassigned"
                group_key = "UNASSIGNED"
                team_name = "Unassigned"

            total = r.get("total_points", 0)
            count = r.get("member_count", 0)
            avg = total / count if count > 0 else 0

            teams.append({
                "group_type": group_type,
                "group_key": group_key,
                "team_name": team_name,
                "manager_id": r.get("manager_id"),
                "manager_name": r.get("manager_name"),
                "member_count": count,
                "total_points": round(total, 2),
                "total_mf_points": round(r.get("total_mf_points", 0), 2),
                "total_ins_points": round(r.get("total_ins_points", 0), 2),
                "total_ref_points": round(r.get("total_ref_points", 0), 2),
                "avg_points": round(avg, 2)
            })

        res = {"month": month, "teams": teams}
        return func.HttpResponse(json.dumps(res, default=json_serial), mimetype="application/json")

    except Exception as e:
        logging.exception(f"ERROR in get_team_view: {str(e)}")
        is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"

        if is_dev:
            error_response = {
                "error": "Internal Server Error",
                "detail": str(e),
                "type": type(e).__name__,
                "month": req.params.get("month"),
                "user_email": user_email
            }
        else:
            error_response = {"error": "Internal Server Error"}

        return func.HttpResponse(json.dumps(error_response), status_code=500, mimetype="application/json")

    """
    GET /api/leaderboard/team-view?month=YYYY-MM
    Returns aggregated team data for a given month.
    Uses group_type + group_key design to handle different grouping strategies.
    RBAC: Requires admin or superadmin role.
    """
    # RBAC Check
    user_email = rbac.get_user_email(req)
    if not rbac.is_admin(user_email):
        # Dev-only debug information
        is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"
        error_response = {"error": "Forbidden: Admin access required"}
        if is_dev:
            error_response["debug_user_email"] = user_email or "(none detected)"
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=403,
            mimetype="application/json"
        )

    try:
        month = req.params.get("month")
        if not month:
            month = datetime.utcnow().strftime("%Y-%m")

        logging.info(f"get_team_view: month={month}, user_email={user_email}")

        db = get_db()

        # Log database name for debugging
        logging.info(f"Connected to database: {db.name}")

        # Ensure indexes exist for performance
        try:
            db.Public_Leaderboard.create_index([("period_month", 1), ("team_id", 1)])
            db.Public_Leaderboard.create_index([("period_month", 1), ("reporting_manager_id", 1)])
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")

        # Aggregate teams by team_id (based on preflight: 100% coverage)
        pipeline = [
            {"$match": {"period_month": month}},
            {
                "$group": {
                    "_id": "$team_id",
                    "team_name": {"$first": "$team_name"},
                    "team_leader": {"$first": "$team_leader"},
                    "total_points": {"$sum": {"$ifNull": ["$total_points_final", 0]}},
                    "member_count": {"$sum": 1},
                    "avg_points": {"$avg": {"$ifNull": ["$total_points_final", 0]}}
                }
            },
            {"$sort": {"total_points": -1}}
        ]

        logging.info(f"Executing aggregation pipeline for month: {month}")
        teams_cursor = db.Public_Leaderboard.aggregate(pipeline)
        teams = list(teams_cursor)

        logging.info(f"Aggregation returned {len(teams)} teams")

        # Transform to match frontend expectations
        result = []
        for team in teams:
            result.append({
                "group_type": "team",
                "group_key": team["_id"],
                "name": team.get("team_name") or team["_id"] or "UNASSIGNED",
                "leader": team.get("team_leader"),
                "total_points": round(team.get("total_points", 0), 2),
                "member_count": team.get("member_count", 0),
                "avg_points": round(team.get("avg_points", 0), 2)
            })

        res = {
            "month": month,
            "teams": result
        }

        return func.HttpResponse(
            json.dumps(res, default=json_serial),
            mimetype="application/json"
        )

    except Exception as e:
        # Log full traceback
        logging.exception(f"ERROR in get_team_view: {str(e)}")

        # Determine if dev/test mode
        is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"

        if is_dev:
            # Dev/test: Include details
            error_response = {
                "error": "Internal Server Error",
                "detail": str(e),
                "type": type(e).__name__,
                "month": req.params.get("month"),
                "user_email": user_email
            }
        else:
            # Production: Generic error only
            error_response = {"error": "Internal Server Error"}

        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )




def get_team_view_members(req):
    """
    GET /api/leaderboard/team-view/members?month=YYYY-MM&group_type=team|manager|unassigned&group_key=XXX
    Returns members for a specific team/group for a given month.
    CRITICAL: Must branch by group_type to avoid incorrect OR queries.
    RBAC: Requires admin or superadmin role.
    """
    # RBAC Check
    user_email = rbac.get_user_email(req)
    if not rbac.is_admin(user_email):
        # Dev-only debug information
        is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"
        error_response = {"error": "Forbidden: Admin access required"}
        if is_dev:
            error_response["debug_user_email"] = user_email or "(none detected)"
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=403,
            mimetype="application/json"
        )

    try:
        month = req.params.get("month")
        group_type = req.params.get("group_type")
        group_key = req.params.get("group_key")

        if not month:
            return func.HttpResponse(
                json.dumps({"error": "month parameter required"}),
                status_code=400,
                mimetype="application/json"
            )

        if not group_type or not group_key:
            return func.HttpResponse(
                json.dumps({"error": "group_type and group_key parameters required"}),
                status_code=400,
                mimetype="application/json"
            )

        db = get_db()

        # Build filter based on group_type (CRITICAL: no OR across different fields)
        if group_type == "team":
            filter_query = {"period_month": month, "team_id": group_key}
        elif group_type == "manager":
            filter_query = {"period_month": month, "reporting_manager_id": group_key}
        elif group_type == "unassigned":
            filter_query = {
                "period_month": month,
                "team_id": {"$in": [None, ""]},
                "reporting_manager_id": {"$in": [None, ""]}
            }
        else:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid group_type: {group_type}. Must be team, manager, or unassigned"}),
                status_code=400,
                mimetype="application/json"
            )

        # Query members
        members_cursor = db.Public_Leaderboard.find(filter_query).sort("total_points_public", -1)

        members = []
        rank = 1
        for doc in members_cursor:
            # Calculate MF Net Sales (Lumpsum Net + SIP Net)
            sip_net = doc.get("sip_net", 0) or 0

            # Lumpsum Net Calculation from breakdown fields
            ls_gross = doc.get("lumpsum_gross_purchase", 0) or 0
            ls_red = doc.get("lumpsum_redemption", 0) or 0
            ls_sw_in = doc.get("lumpsum_switch_in", 0) or 0
            ls_sw_out = doc.get("lumpsum_switch_out", 0) or 0
            ls_cob_in = doc.get("lumpsum_cob_in", 0) or 0
            ls_cob_out = doc.get("lumpsum_cob_out", 0) or 0

            # Net Purchase = Gross - Redemption + Switch In - Switch Out + COB In - COB Out
            ls_net = ls_gross - ls_red + ls_sw_in - ls_sw_out + ls_cob_in - ls_cob_out

            mf_net_sales = sip_net + ls_net

            members.append({
                "employee_id": doc.get("employee_id"),
                "name": doc.get("NameOfEmp", doc.get("RM_Name", doc.get("rm_name", "Unknown"))),
                "points": doc.get("total_points_public", 0),
                "mf_points": doc.get("mf_points", 0),
                "ins_points": doc.get("ins_points", 0),
                "ref_points": doc.get("ref_points", 0),

                # Eagle-Eye Metrics
                "mf_net_sales": mf_net_sales,
                "ins_fresh_premium": doc.get("ins_fresh_premium", 0),
                "ref_conversion_rate": 0, # Placeholder until derived
                "meetings_count": 0, # Placeholder: Ideally fetch from Investor_Meetings_Data join
                "avg_dtr": doc.get("avg_dtr", 0),

                "rank": rank
            })
            rank += 1

        # Get team metadata for response
        team_name = group_key
        manager_name = None
        if members:
            first_member = db.Public_Leaderboard.find_one(filter_query)
            if first_member:
                if group_type == "team":
                    team_name = group_key  # Could enhance with Teams collection lookup
                    manager_name = first_member.get("RM_Name")
                elif group_type == "manager":
                    team_name = f"{first_member.get('RM_Name', 'Unknown')}'s Team"
                    manager_name = first_member.get("RM_Name")

        return func.HttpResponse(
            json.dumps({
                "group_type": group_type,
                "group_key": group_key,
                "team_name": team_name,
                "manager_name": manager_name,
                "members": members
            }, default=json_serial),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error in get_team_view_members: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def get_all_breakdown(req):
    """
    GET /api/leaderboard/breakdown?month=YYYY-MM&group_key=MASTER_TEAM
    Returns granular breakdown data for active and inactive RMs.
    Supports virtual grouping (MASTER_TEAM) to return all RMs.
    """
    # RBAC Check (Admin/Superadmin only)
    user_email = rbac.get_user_email(req)
    if not rbac.is_admin(user_email):
         # Dev check
         is_dev = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or os.getenv("DEBUG_RBAC") == "1"
         if not is_dev:
              return func.HttpResponse(json.dumps({"error": "Forbidden"}), status_code=403, mimetype="application/json")

    month = req.params.get("month", datetime.utcnow().strftime("%Y-%m"))
    group_key = req.params.get("group_key", "MASTER_TEAM") # Default to Master

    db = get_db()

    # Query: If MASTER_TEAM, fetch ALL. Else filter by team/manager (future proofing)
    if group_key == "MASTER_TEAM":
        query = {"period_month": month}
    else:
        # Fallback to team filter if needed, though not requested yet
        query = {"period_month": month, "team_id": group_key}

    # Fetch all records, including inactive
    # Sort by total points desc
    # CRITICAL: We need inactive users to show up for 'MASTER_TEAM' view
    cursor = db.Public_Leaderboard.find(query).sort("total_points_public", -1)

    members = []

    for doc in cursor:
        # Determine Status
        status = "active" if doc.get("is_active") is not False else "inactive" # Default to active if missing

        # Calculate Metrics from requested JSON structure
        # Public_Leaderboard now has split points (after re-agg)

        mf_sip_pts = doc.get("mf_sip_points", 0) or 0
        mf_ls_pts = doc.get("mf_lumpsum_points", 0) or 0

        # Fallback: if re-agg hasn't finished or field missing, estimate from total MF
        mf_total = doc.get("mf_points", 0) or 0
        if mf_sip_pts == 0 and mf_ls_pts == 0 and mf_total > 0:
             # Just put it all in SIP (safe default if unknown) or leave as is
             pass

        # Breakdown Details Mapping
        # net_sip_contribution -> sip_net
        # fresh_premium_collected -> ins_fresh_premium
        # meetings_attended -> 0 (Placeholder)
        # conversions -> 0 (Placeholder, derived from Ref?)

        net_sip = doc.get("sip_net", 0) or 0
        fresh_prem = doc.get("ins_fresh_premium", 0) or 0

        # Float conversion safety
        try:
             total_pts = float(doc.get("total_points_public", 0))
             ins_pts = float(doc.get("ins_points", 0) or 0)
             ref_pts = float(doc.get("ref_points", 0) or 0)
        except:
             total_pts = 0
             ins_pts = 0
             ref_pts = 0

        # Fetch Leaderboard_Lumpsum data for meeting multiplier info
        ls_rec = db.Leaderboard_Lumpsum.find_one({"month": month, "employee_id": doc.get("employee_id")})
        if not ls_rec and doc.get("rm_name"):
            ls_rec = db.Leaderboard_Lumpsum.find_one({"month": month, "employee_name": doc.get("rm_name")})

        # Extract meeting multiplier data
        meetings_count = 0
        meetings_multiplier = 1.0
        base_incentive = 0.0
        final_incentive = 0.0

        if ls_rec:
            meetings_count = ls_rec.get("meetings_count", 0) or 0
            meetings_multiplier = ls_rec.get("meetings_multiplier", 1.0) or 1.0
            base_incentive = ls_rec.get("base_incentive", 0.0) or 0.0
            final_incentive = ls_rec.get("final_incentive", 0.0) or 0.0

        members.append({
            "employee_id": doc.get("employee_id"),
            "employee_name": doc.get("rm_name") or doc.get("RM_Name") or "Unknown",
            "status": status,
            "metrics": {
                "mf_lumpsum_points": mf_ls_pts,
                "mf_sip_points": mf_sip_pts,
                "insurance_points": ins_pts,
                "referral_points": ref_pts,
                "total_points": total_pts
            },
            "breakdown_details": {
                "net_sip_contribution": net_sip,
                "fresh_premium_collected": fresh_prem,
                "meetings_attended": meetings_count,  # Now showing actual meeting count
                "meetings_multiplier": meetings_multiplier,  # NEW: Meeting multiplier
                "base_incentive": base_incentive,  # NEW: Base incentive (before multiplier)
                "final_incentive": final_incentive,  # NEW: Final incentive (after multiplier)
                "conversions": 0 # Placeholder
            }
        })

    res = {
        "month": month,
        "group_key": group_key,
        "members": members
    }

# --- Helpers for On-the-Fly Bonus Projection ---
def _get_quarter_bounds_api(dt_val: datetime, fy_mode: str = "FY_APR") -> tuple[datetime, datetime, str]:
    """
    Return (start_date, end_date, label) for the quarter containing dt_val.
    fy_mode: "FY_APR" (India) or "FY_JAN" (Calendar).
    """
    m = dt_val.month
    y = dt_val.year
    from datetime import timedelta

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
            label = f"Q4 FY{y-1}-{str(y)[-2:]}"
    else:
        # Calendar Year
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

    q_end_clamped = datetime(q_end.year, q_end.month, q_end.day, 23, 59, 59)
    return q_start, q_end_clamped, label

def _get_fy_bounds_api(dt_val: datetime, fy_mode: str = "FY_APR") -> tuple[datetime, datetime, str]:
    m, y = dt_val.month, dt_val.year
    from datetime import timedelta
    if fy_mode == "FY_APR":
        if m >= 4:
            start = datetime(y, 4, 1)
            end = datetime(y + 1, 4, 1) - timedelta(days=1)
            label = f"FY {y}-{str(y+1)[-2:]}"
        else:
            start = datetime(y - 1, 4, 1)
            end = datetime(y, 4, 1) - timedelta(days=1)
            label = f"FY {y-1}-{str(y)[-2:]}"
    else:
        start = datetime(y, 1, 1)
        end = datetime(y + 1, 1, 1) - timedelta(days=1)
        label = f"CY {y}"

    end_clamped = datetime(end.year, end.month, end.day, 23, 59, 59)
    return start, end_clamped, label

def _select_np_slab_bonus_api(np_value: float, template: dict) -> float:
    try:
        v = float(np_value or 0.0)
    except:
        v = 0.0
    slabs = sorted(template.get("slabs", []), key=lambda x: float(x.get("min_np", 0.0)))
    bonus = 0.0
    for slab in slabs:
        try:
            threshold = float(slab.get("min_np", 0.0) or 0.0)
            b = float(slab.get("bonus_rupees", 0) or 0)
        except:
            continue
        if v >= threshold:
            bonus = b
    return float(bonus)

    return func.HttpResponse(json.dumps(res, default=json_serial), mimetype="application/json")
