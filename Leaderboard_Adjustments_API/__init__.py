import logging
import azure.functions as func
import json
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from ..utils import rbac

def main(req: func.HttpRequest) -> func.HttpResponse:
    action = req.route_params.get("action", "")

    if action == "" or action == "/":
        return create_adjustment(req)
    elif action == "approve":
        return transition_adjustment(req, "APPROVED")
    elif action == "reject":
        return transition_adjustment(req, "REJECTED")
    elif action == "revoke":
        return transition_adjustment(req, "REVOKED")

    return func.HttpResponse("Not Found", status_code=404)

def get_db():
    uri = os.getenv("MONGODB_CONNECTION_STRING")
    client = pymongo.MongoClient(uri)
    db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")
    return client[db_name]

def create_adjustment(req):
    # Auth: Manager only
    email = rbac.get_user_email(req)
    if not rbac.is_manager(email):
        return func.HttpResponse("Forbidden: Managers only", status_code=403)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Validation
    required = ["employee_id", "month", "bucket", "adjustment_type", "value", "reason"]
    if not all(k in body for k in required):
        return func.HttpResponse("Missing required fields", status_code=400)

    doc = {
        "employee_id": body["employee_id"],
        "month": body["month"],
        "bucket": body["bucket"], # SIP/Lumpsum/etc
        "adjustment_type": body["adjustment_type"], # Points/Rupees
        "value": float(body["value"]),
        "reason": body["reason"],
        "status": "PROPOSED",
        "created_by": email,
        "created_at": datetime.utcnow(),
        "audit": {
            "events": [
                {
                    "action": "CREATED",
                    "by": email,
                    "at": datetime.utcnow().isoformat(),
                    "reason": body["reason"]
                }
            ]
        }
    }

    db = get_db()
    res = db.Leaderboard_Adjustments.insert_one(doc)

    return func.HttpResponse(
        json.dumps({"id": str(res.inserted_id), "status": "PROPOSED"}),
        status_code=201,
        mimetype="application/json"
    )

def transition_adjustment(req, target_status):
    # Auth: Admin only for approval/revoke? Or Manager?
    # Spec says: Approve/Reject/Revoke -> ADMIN
    email = rbac.get_user_email(req)
    if not rbac.is_admin(email):
        return func.HttpResponse("Forbidden: Admins only", status_code=403)

    try:
        body = req.get_json()
        adj_id = body.get("id")
        reason = body.get("reason", "No reason provided")
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if not adj_id:
        return func.HttpResponse("Missing 'id'", status_code=400)

    db = get_db()

    # Fetch existing
    try:
        oid = ObjectId(adj_id)
    except:
        return func.HttpResponse("Invalid ID format", status_code=400)

    curr = db.Leaderboard_Adjustments.find_one({"_id": oid})
    if not curr:
        return func.HttpResponse("Adjustment not found", status_code=404)

    # Validation logic
    current_status = curr.get("status")

    # State Machine checks
    allowed = False
    if target_status == "APPROVED" and current_status == "PROPOSED": allowed = True
    if target_status == "REJECTED" and current_status == "PROPOSED": allowed = True
    if target_status == "REVOKED" and current_status == "APPROVED": allowed = True

    if not allowed:
        return func.HttpResponse(f"Invalid transition from {current_status} to {target_status}", status_code=409)

    # Execute Update
    update = {
        "$set": {
            "status": target_status,
            f"{target_status.lower()}_by": email,
            f"{target_status.lower()}_at": datetime.utcnow()
        },
        "$push": {
            "audit.events": {
                "action": target_status,
                "by": email,
                "at": datetime.utcnow().isoformat(),
                "reason": reason
            }
        }
    }

    db.Leaderboard_Adjustments.update_one({"_id": oid}, update)

    return func.HttpResponse(
        json.dumps({"id": adj_id, "status": target_status}),
        status_code=200,
        mimetype="application/json"
    )
