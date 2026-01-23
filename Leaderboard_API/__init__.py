import logging
import azure.functions as func
import json
import os
import pymongo
import pymongo
from datetime import datetime, timezone
from ..utils import rbac

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
        db_name = os.getenv("PLI_DB_NAME", os.getenv("DB_NAME", "PLI_Leaderboard"))
        # Mask URI
        masked_uri = uri.split("@")[1] if "@" in uri else "Hidden"
        return func.HttpResponse(json.dumps({
            "db_name_in_use": db_name,
            "mongo_host_redacted": masked_uri,
            "collections_confirmed": ["Public_Leaderboard", "MF_SIP_Leaderboard", "Insurance_Policy_Scoring", "Config"]
        }), mimetype="application/json")

    return func.HttpResponse("Not Found", status_code=404)

def get_db():
    uri = os.getenv("MongoDb-Connection-String")
    client = pymongo.MongoClient(uri)
    db_name = os.getenv("PLI_DB_NAME", os.getenv("DB_NAME", "PLI_Leaderboard_v2"))
    return client[db_name]

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
    # Query Params
    try:
        month = req.params.get("month")
        if not month:
            # Default to current month
            month = datetime.utcnow().strftime("%Y-%m")

        view_mode = req.params.get("view") # 'MTD' (default) or 'YTD'
        db = get_db()

        leaderboard = []
        adjustments = []

        if view_mode == "YTD":
            # 1. Determine FY Start
            try:
                sy, sm = map(int, month.split('-'))
                # If Apr-Dec (4-12), start is Apr of same year.
                # If Jan-Mar (1-3), start is Apr of prev year.
                start_year = sy if sm >= 4 else sy - 1
                start_month_str = f"{start_year}-04"
            except:
                start_month_str = f"{datetime.now().year}-04" # Fallback

            logging.info(f"Fetching YTD Leaderboard: {start_month_str} to {month}")

            # 2. Aggregate Public Points
            pipeline = [
                {
                    "$match": {
                        "period_month": {"$gte": start_month_str, "$lte": month},
                        "is_active": {"$ne": False}
                    }
                },
                {
                    "$group": {
                        "_id": "$employee_id",
                        "total_points_public": {"$sum": {"$toDouble": "$total_points_public"}},
                        "ins_points": {"$sum": {"$toDouble": {"$ifNull": ["$ins_points", 0]}}},
                        "mf_points": {"$sum": {"$toDouble": {"$ifNull": ["$mf_points", 0]}}},
                        "ref_points": {"$sum": {"$toDouble": {"$ifNull": ["$ref_points", 0]}}},
                        "sip_gross": {"$sum": {"$toDouble": {"$ifNull": ["$sip_gross", 0]}}},
                        "sip_net": {"$sum": {"$toDouble": {"$ifNull": ["$sip_net", 0]}}},
                        "sip_cancel": {"$sum": {"$toDouble": {"$ifNull": ["$sip_cancel", 0]}}},
                        "sip_swp_reg": {"$sum": {"$toDouble": {"$ifNull": ["$sip_swp_reg", 0]}}},
                        "sip_swp_canc": {"$sum": {"$toDouble": {"$ifNull": ["$sip_swp_canc", 0]}}},
                        "ins_fresh_premium": {"$sum": {"$toDouble": {"$ifNull": ["$ins_fresh_premium", 0]}}},
                        "ins_renewal_premium": {"$sum": {"$toDouble": {"$ifNull": ["$ins_renewal_premium", 0]}}},
                        "avg_dtr": {"$avg": {"$toDouble": {"$ifNull": ["$avg_dtr", 0]}}},
                        "ins_policy_count": {"$sum": {"$toDouble": {"$ifNull": ["$ins_policy_count", 0]}}},
                        "lumpsum_gross_purchase": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_gross_purchase", 0]}}},
                        "lumpsum_redemption": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_redemption", 0]}}},
                        "lumpsum_switch_in": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_switch_in", 0]}}},
                        "lumpsum_switch_out": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_switch_out", 0]}}},
                        "lumpsum_cob_in": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_cob_in", 0]}}},
                        "lumpsum_cob_out": {"$sum": {"$toDouble": {"$ifNull": ["$lumpsum_cob_out", 0]}}},
                        "rm_name": {"$first": "$rm_name"},
                        "name": {"$first": "$name"},
                        "employee_id": {"$first": "$employee_id"} # Ensure we keep it
                    }
                }
            ]
            leaderboard = list(db.Public_Leaderboard.aggregate(pipeline))

            # 3. Fetch YTD Adjustments
            adjustments = list(db.Leaderboard_Adjustments.find({
                "month": {"$gte": start_month_str, "$lte": month},
                "status": "APPROVED"
            }))

        else:
            # MTD (Default)
            base_cursor = db.Public_Leaderboard.find({"period_month": month, "is_active": {"$ne": False}})
            leaderboard = list(base_cursor)

            # Fetch MTD Adjustments
            adjustments = list(db.Leaderboard_Adjustments.find({
                "month": month,
                "status": "APPROVED"
            }))

        # 3. Overlay (Common Logic)
        # Index adjustments by employee_id
        adj_map = {}
        for a in adjustments:
            eid = a.get("employee_id")
            if eid not in adj_map: adj_map[eid] = []
            adj_map[eid].append(a)

        # 4. Fetch Rupee Incentives (if MTD view)
        inc_map = {}
        if view_mode != "YTD":
            incentives = list(db.Rupee_Incentives.find({"period_month": month}))
            for inc in incentives:
                eid = inc.get("employee_id")
                if eid:
                    inc_map[eid] = inc

        final_list = []
        for row in leaderboard:
            eid = row.get("employee_id") or row.get("_id") # Handle aggregate output

            # Ensure safe float conversion
            try:
                pts = float(row.get("total_points_public", 0))
            except:
                pts = 0.0

            # Apply overlays
            applied_adj = []
            if eid in adj_map:
                for adj in adj_map[eid]:
                    try:
                        val = float(adj.get("value", 0))
                    except:
                        val = 0.0

                    if adj.get("adjustment_type") == "Points":
                        pts += val
                    applied_adj.append({
                        "id": str(adj.get("_id")),
                        "reason": adj.get("reason"),
                        "val": val,
                        "type": adj.get("adjustment_type")
                    })

            # Enrich row (non-destructive to DB)
            out = dict(row)
            out["total_points_final"] = pts
            out["adjustments"] = applied_adj
            out["employee_id"] = eid

            # Attach Incentive Data
            if eid in inc_map:
                out["rupee_incentive"] = inc_map[eid]
            else:
                out["rupee_incentive"] = {"total_incentive": 0}

            out.pop("_id", None)
            final_list.append(out)

        # Sort by final points desc
        final_list.sort(key=lambda x: x.get("total_points_final", 0), reverse=True)

        return func.HttpResponse(
            json.dumps(final_list, default=json_serial),
            mimetype="application/json"
        )

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
    return fetch_user_breakdown(req, employee_id)

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

    # 1. SIP
    sip = db.MF_SIP_Leaderboard.find_one({
        "$or": [{"period_month": month}, {"month": month}],
        "employee_id": eid
    })
    if not sip and name_fallback:
        sip = db.MF_SIP_Leaderboard.find_one({
            "$or": [{"period_month": month}, {"month": month}],
            "rm_name": name_fallback
        })

    # 2. Lumpsum
    lumpsum = db.Leaderboard_Lumpsum.find_one({"month": month, "employee_id": eid})
    if not lumpsum and name_fallback:
        lumpsum = db.Leaderboard_Lumpsum.find_one({"month": month, "employee_name": name_fallback})

    # 3. Insurance (Assume ID is present as it's more strictly typed, but add fallback if needed)
    ins_query = {"period_month": month, "employee_id": eid}
    ins_cursor = db.Insurance_Policy_Scoring.find(ins_query)
    insurance = list(ins_cursor)
    if not insurance and name_fallback:
        # Fallback for insurance if ID missing? Less likely but safe.
        insurance = list(db.Insurance_Policy_Scoring.find({"period_month": month, "employee_name": name_fallback}))

    # 4. Referrals
    ref_cursor = db.referralLeaderboard.find({"period_month": month, "employee_id": eid})
    ref = list(ref_cursor)
    if not ref and name_fallback:
         ref_cursor = db.referralLeaderboard.find({"period_month": month, "referrer_name": name_fallback})
         ref = list(ref_cursor)

    # 5. Rupee Incentives
    rupee_incentive = db.Rupee_Incentives.find_one({"period_month": month, "employee_id": eid})
    if not rupee_incentive and name_fallback:
        rupee_incentive = db.Rupee_Incentives.find_one({"period_month": month, "rm_name": name_fallback})

    # 6. Authoritative Summary (Public Leaderboard)
    summary_row = db.Public_Leaderboard.find_one({"employee_id": eid, "period_month": month})
    if not summary_row and name_fallback:
        summary_row = db.Public_Leaderboard.find_one({"rm_name": name_fallback, "period_month": month})

    res = {
        "employee_id": eid,
        "month": month,
        "sip": sip,
        "lumpsum": lumpsum,
        "insurance_policies": insurance,
        "referral": ref,
        "rupee_incentive": rupee_incentive,
        "summary": summary_row # Frontend can use this for consistent KPIs
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
                mongo_uri = os.getenv("MongoDb-Connection-String")
                client_v2 = pymongo.MongoClient(mongo_uri)
                db_v2 = client_v2[v2_db_name]
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

    # Sanitize NaN/Infinity values before JSON serialization
    res = sanitize_for_json(res)

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
                "meetings_attended": 0, # Placeholder
                "conversions": 0 # Placeholder
            }
        })

    res = {
        "month": month,
        "group_key": group_key,
        "members": members
    }

    return func.HttpResponse(json.dumps(res, default=json_serial), mimetype="application/json")
