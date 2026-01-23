
import logging
import azure.functions as func
import json
import os
import pymongo

def get_user_email_inline(req) -> str | None:
    # 1. Try x-ms-client-principal-name (Azure App Service Auth)
    val = req.headers.get("x-ms-client-principal-name")
    if val: return val

    # 2. Try simple header for local debug / custom auth proxy
    val = req.headers.get("X-User-Email")
    if val: return val

    return None

def is_admin_inline(email: str) -> bool:
    if not email: return False
    email = email.lower()

    # 1. Check Env
    admins_raw = os.getenv("LEADERBOARD_ADMIN_EMAILS", "")
    admins = {x.strip().lower() for x in admins_raw.split(",") if x.strip()}
    if email in admins:
        return True

    # 2. Check DB
    try:
        uri = os.getenv("MONGODB_CONNECTION_STRING")
        if uri:
            client = pymongo.MongoClient(uri)
            db = client["PLI_Leaderboard"]
            user = db.Admin_Permissions.find_one({"email": email})
            if user:
                roles = user.get("roles", [])
                return "admin" in roles or "super_admin" in roles
    except Exception as e:
        logging.error(f"RBAC DB check failed: {e}")

    return False

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('WhoAmI processed a request (Inlined).')

    try:
        # 1. Identity
        email = get_user_email_inline(req)

        if not email:
            logging.info("WhoAmI: No identity found.")
            return func.HttpResponse(
                json.dumps({"error": "No identity found"}),
                status_code=401,
                mimetype="application/json"
            )

        # 2. Resolve Roles
        is_admin_user = is_admin_inline(email)
        logging.info(f"WhoAmI: User={email}, IsAdmin={is_admin_user}")

        # 3. Construct Response matches frontend UserContext interface
        response_data = {
            "email": email,
            "roles": [],
            "scopes": {
                "self": True,
                "public": True,
                "teams": [], # Populate real teams later if needed
                "is_admin": is_admin_user
            }
        }

        if is_admin_user:
            response_data["roles"].append("admin")

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"WhoAmI Critical Error: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
