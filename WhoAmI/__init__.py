import logging
import azure.functions as func
import json
import os
import pymongo
from ..utils.db_utils import get_db
from utils.auth_utils import get_email_from_jwt_cookie
from utils.http import respond, options_response

def get_user_email_inline(req) -> str | None:
    if req.method == "OPTIONS":
        return options_response()
    # 1. Try x-ms-client-principal-name (Azure App Service Auth)
    val = req.headers.get("x-ms-client-principal-name")
    if val: return val

    # 2. Try JWT from Cookie (Node/Express App Integration)
    val = get_email_from_jwt_cookie(req)
    if val: return val

    # 3. Try simple header for local debug / custom auth proxy
    val = req.headers.get("X-User-Email")
    if val: return val

    # 4. Fallback for local development
    # env = os.getenv("APP_ENV", "Production")
    # if env == "Development":
    #     return "vilakshan@niveshonline.com"

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
    # 2. Check DB
    try:
        db = get_db()
        user = db.Admin_Permissions.find_one({"email": email})
        if user:
            roles = user.get("roles", [])
            return "admin" in roles or "super_admin" in roles
    except Exception as e:
        logging.error(f"RBAC DB check failed: {e}")

    return False

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('WhoAmI processed a request (Inlined).')

    if req.method == "OPTIONS":
        return options_response()

    try:
        # 1. Identity
        email = get_user_email_inline(req)
        print(f"EMAIL DECODED FROM COOKIE:--> {email}")

        if not email:
            logging.info("WhoAmI: No identity found.")
            return func.HttpResponse(
                json.dumps({"error": "No identity found"}),
                status_code=401,
                mimetype="application/json",
                headers={
                    "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
                    "Access-Control-Allow-Credentials": "true",
                }
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
            mimetype="application/json",
            headers={
                "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization"
            }
        )
    except Exception as e:
        logging.error(f"WhoAmI Critical Error: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={
                "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
                "Access-Control-Allow-Credentials": "true",
            }
        )
