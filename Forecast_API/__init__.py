import logging
import azure.functions as func
import json
import os
import pymongo
from bson import ObjectId
from datetime import datetime
from ..utils import rbac

def main(req: func.HttpRequest) -> func.HttpResponse:
    route = req.route_params.get("route", "")
    method = req.method

    if route == "events" and method == "POST":
        return upsert_event(req)
    elif route == "leaderboard" and method == "GET":
        return get_leaderboard(req)
    elif route == "me" and method == "GET":
        return get_me(req)

    return func.HttpResponse("Not Found", status_code=404)

def get_db():
    uri = os.getenv("MongoDb-Connection-String")
    client = pymongo.MongoClient(uri)
    db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")
    return client[db_name]

def upsert_event(req):
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    try:
        body = req.get_json()
    except:
        return func.HttpResponse("Invalid JSON", status_code=400)

    db = get_db()

    # Determine Employee ID
    # If Manager, allowed to set 'employee_id' field.
    # If Team, forced to Self.
    user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})
    if not user: return func.HttpResponse("User not linked", status_code=403)

    target_eid = user.get("id")
    if rbac.is_manager(email) and "employee_id" in body:
        target_eid = body["employee_id"]

    required = ["month", "product", "expected_amount", "stage", "probability", "expected_close_date"]
    if not all(k in body for k in required):
        return func.HttpResponse("Missing fields", status_code=400)

    # Upsert Logic (if ID provided update, else insert)
    eid = body.get("id")

    data = {
        "employee_id": target_eid,
        "month": body["month"],
        "product": body["product"], # SIP/LUMPSUM/INSURANCE
        "expected_amount": float(body["expected_amount"]),
        "stage": body["stage"],
        "probability": float(body["probability"]),
        "expected_close_date": datetime.fromisoformat(body["expected_close_date"].replace("Z", "")),
        "source_ref": body.get("source_ref"),
        "updated_at": datetime.utcnow()
    }

    if eid:
        # Update
        res = db.Forecast_Events.update_one(
            {"_id": ObjectId(eid)},
            {"$set": data}
        )
        final_id = eid
    else:
        data["created_at"] = datetime.utcnow()
        data["created_by"] = email
        res = db.Forecast_Events.insert_one(data)
        final_id = str(res.inserted_id)

    return func.HttpResponse(json.dumps({"id": final_id}), mimetype="application/json")

def get_leaderboard(req):
    # Auth?
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    month = req.params.get("month", datetime.utcnow().strftime("%Y-%m"))
    channel = req.params.get("channel", "BASE")

    db = get_db()
    cursor = db.Forecast_Leaderboard.find({"month": month, "channel": channel})
    return func.HttpResponse(json.dumps(list(cursor), default=str), mimetype="application/json")

def get_me(req):
    email = rbac.get_user_email(req)
    if not email: return func.HttpResponse("Unauthorized", status_code=401)

    db = get_db()
    user = db.Zoho_Users.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})
    if not user: return func.HttpResponse("User not linked", status_code=403)
    eid = user.get("id")

    month = req.params.get("month", datetime.utcnow().strftime("%Y-%m"))

    # Return all channels for this user
    cursor = db.Forecast_Leaderboard.find({"month": month, "employee_id": eid})
    return func.HttpResponse(json.dumps(list(cursor), default=str), mimetype="application/json")
