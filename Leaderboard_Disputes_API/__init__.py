import logging
import azure.functions as func
import json
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from ..utils import rbac

def main(req: func.HttpRequest) -> func.HttpResponse:
    method = req.method
    action = req.route_params.get("action", "")

    # Router
    if method == "POST" and (action == "" or action == "/"):
        return create_dispute(req)
    elif method == "GET" and action == "me":
        return get_my_disputes(req)
    elif method == "GET" and (action == "" or action == "/"):
        return list_disputes_manager(req)
    elif method == "POST" and action == "update":
        return update_dispute(req)

    return func.HttpResponse("Not Found", status_code=404)

def get_db():
    uri = os.getenv("MONGODB_CONNECTION_STRING")
    client = pymongo.MongoClient(uri)
    db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")
    return client[db_name]

def create_dispute(req):
    # Auth: Any authenticated user
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    try:
        body = req.get_json()
    except:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Resolve Employee ID
    db = get_db()

    # Optional: Allow passing employee_id if Manager?
    # For now, strict: Self only unless logic mandates otherwise.
    # Plan says: "Team creates dispute".

    user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})
    if not user: return func.HttpResponse("User not linked", status_code=403)
    eid = user.get("id")

    required = ["month", "scope", "message"]
    if not all(k in body for k in required):
        return func.HttpResponse("Missing fields", status_code=400)

    doc = {
        "employee_id": eid,
        "month": body["month"],
        "scope": body["scope"], # ROW/SIP/LUMPSUM etc
        "source_refs": body.get("source_refs", []),
        "message": body["message"],
        "status": "OPEN",
        "created_by": email,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "audit": {
            "events": [
                {
                    "action": "CREATED",
                    "by": email,
                    "at": datetime.utcnow().isoformat(),
                    "msg": body["message"]
                }
            ]
        }
    }

    res = db.Leaderboard_Disputes.insert_one(doc)
    return func.HttpResponse(json.dumps({"id": str(res.inserted_id), "status": "OPEN"}), mimetype="application/json")

def get_my_disputes(req):
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    db = get_db()
    user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})
    if not user: return func.HttpResponse("User not linked", status_code=403)
    eid = user.get("id")

    cursor = db.Leaderboard_Disputes.find({"employee_id": eid}).sort("created_at", -1)
    return func.HttpResponse(json.dumps(list(cursor), default=str), mimetype="application/json")

def list_disputes_manager(req):
    email = rbac.get_user_email(req)
    if not rbac.is_manager(email):
        return func.HttpResponse("Forbidden", status_code=403)

    query = {}
    # Filters
    if "month" in req.params: query["month"] = req.params["month"]
    if "status" in req.params: query["status"] = req.params["status"]
    if "employee_id" in req.params: query["employee_id"] = req.params["employee_id"]

    db = get_db()
    cursor = db.Leaderboard_Disputes.find(query).sort("created_at", -1)
    return func.HttpResponse(json.dumps(list(cursor), default=str), mimetype="application/json")

def update_dispute(req):
    email = rbac.get_user_email(req)
    if not rbac.is_manager(email):
        return func.HttpResponse("Forbidden", status_code=403)

    try:
        body = req.get_json()
        did = body["id"]
        action = body["resolution"]["action"] # ADJUSTMENT_CREATED etc
        status = body["status"] # RESOLVED/REJECTED/ACK
    except:
        return func.HttpResponse("Invalid Payload", status_code=400)

    db = get_db()

    # Validation if Adjustment Linked
    if action == "ADJUSTMENT_CREATED":
        adj_id = body.get("resolution", {}).get("adjustment_id")
        if not adj_id:
             return func.HttpResponse("Must provide adjustment_id for this action", status_code=400)

    update = {
        "$set": {
            "status": status,
            "updated_at": datetime.utcnow(),
            "resolution": {
                "action": action,
                "notes": body.get("resolution", {}).get("notes", ""),
                "resolved_by": email,
                "resolved_at": datetime.utcnow(),
                "adjustment_id": body.get("resolution", {}).get("adjustment_id")
            }
        },
        "$push": {
            "audit.events": {
                "action": f"UPDATE_{status}",
                "by": email,
                "at": datetime.utcnow().isoformat(),
                "notes": body.get("resolution", {}).get("notes", "")
            }
        }
    }

    res = db.Leaderboard_Disputes.update_one({"_id": ObjectId(did)}, update)
    if res.matched_count == 0:
        return func.HttpResponse("Dispute not found", status_code=404)

    return func.HttpResponse(json.dumps({"id": did, "status": status}), mimetype="application/json")
