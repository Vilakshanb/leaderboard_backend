import os
import logging

def get_allowed_emails(env_var_name: str) -> set[str]:
    raw = os.getenv(env_var_name, "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}

import pymongo

def _get_db():
    try:
        uri = os.getenv("MongoDb-Connection-String")
        if not uri: return None
        client = pymongo.MongoClient(uri)
        return client["PLI_Leaderboard"]
    except:
        return None

def _check_db_role(email: str, role: str) -> bool:
    if not email: return False
    db = _get_db()
    if db is None:
        return False

    user = db.Admin_Permissions.find_one({"email": email})
    if not user: return False

    roles = user.get("roles", [])
    return role in roles

def is_manager(email: str) -> bool:
    if not email: return False
    email = email.lower()

    # Check Env
    managers = get_allowed_emails("LEADERBOARD_MANAGER_EMAILS")
    admins = get_allowed_emails("LEADERBOARD_ADMIN_EMAILS")
    if (email in managers) or (email in admins):
        return True

    # Check DB
    return _check_db_role(email, "admin") or _check_db_role(email, "manager") or _check_db_role(email, "super_admin")

def is_admin(email: str) -> bool:
    if not email: return False
    email = email.lower()

    # Check Env
    admins = get_allowed_emails("LEADERBOARD_ADMIN_EMAILS")
    if email in admins:
        return True

    # Check DB
    return _check_db_role(email, "admin") or _check_db_role(email, "super_admin")

def get_user_email(req) -> str | None:
    # 1. Try x-ms-client-principal-name (Azure App Service Auth) - ALWAYS honored
    val = req.headers.get("x-ms-client-principal-name")
    if val: return val

    # 2. Dev/Test-only: Allow X-User-Email (for E2E tests, local development)
    # CRITICAL: In Production, ignore X-User-Email to prevent spoofing
    is_dev_or_test = (
        os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") != "Production" or
        os.getenv("DEBUG_RBAC") == "1" or
        os.getenv("E2E_MODE") == "1"
    )

    if is_dev_or_test:
        val = req.headers.get("X-User-Email")
        if val: return val

    return None
