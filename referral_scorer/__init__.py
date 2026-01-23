def _zoho_get_with_retry(url, params, access_token):
    """GET Zoho API with one automatic retry on INVALID_TOKEN."""

    def _do_call(tok):
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        return requests.get(url, headers=headers, params=params)

    resp = _do_call(access_token)
    if resp.status_code != 200:
        try:
            j = resp.json()
        except Exception:
            j = {"message": resp.text}
        if j.get("code") == "INVALID_TOKEN":
            logging.warning("Zoho token invalid; refreshing and retrying once…")
            new_tok = get_access_token()
            resp = _do_call(new_tok)
    return resp


import requests
import logging
import pandas as pd
import ast
import pymongo
import azure.functions as func
from pymongo.errors import ConnectionFailure, BulkWriteError, ServerSelectionTimeoutError
import datetime

# --- Azure Key Vault imports ---
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# Let Azure Functions' worker manage handlers. Only set basicConfig if no handlers exist (local CLI / direct run).
_root_logger = logging.getLogger()
if not _root_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


# Zoho CRM configuration
# Global holders populated by load_secrets()
ZOHO_CLIENT_ID = None
ZOHO_CLIENT_SECRET = None
ZOHO_REFRESH_TOKEN = None
CONNECTIONSTRING = None
investment_cvid = "2969103000507884060"
insurance_cvid = "2969103000498919061"

KV_SECRET_ZOHO_CLIENT_ID = os.getenv("KV_SECRET_ZOHO_CLIENT_ID", "Zoho-client-id-vilakshan-account")
KV_SECRET_ZOHO_CLIENT_SECRET = os.getenv(
    "KV_SECRET_ZOHO_CLIENT_SECRET", "Zoho-client-secret-vilakshan-account"
)
KV_SECRET_ZOHO_REFRESH_TOKEN = os.getenv(
    "KV_SECRET_ZOHO_REFRESH_TOKEN", "Zoho-refresh-token-vilakshan-account"
)
KV_SECRET_MONGO_CONNSTRING = os.getenv("KV_SECRET_MONGO_CONNSTRING", "MongoDb-Connection-String")


# Effective RM-name skip set: aliases
SKIP_RM_ALIASES = {
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
}


def _skip_match(name: str) -> bool:
    """Returns True if the RM name should be skipped."""
    if not name:
        return False
    s = " ".join(str(name).lower().split())
    # Exact match against the set
    if s in SKIP_RM_ALIASES:
        return True
    return False


KEY_VAULT_URL = "https://milestonetsl1.vault.azure.net/"

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:  # ImportError or any runtime import issue
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE = {}


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


def connect_to_mongo(collection_name):
    try:
        conn = get_secret(KV_SECRET_MONGO_CONNSTRING)
        if not conn:
            logging.error(
                "Mongo connection string missing (secret %s). Skipping Mongo connection.",
                KV_SECRET_MONGO_CONNSTRING,
            )
            return None
        # Ensure the MongoDB connection string is securely managed and correctly formatted
        client = pymongo.MongoClient(conn, serverSelectionTimeoutMS=5000)

        # Attempt to retrieve the server information to verify the connection
        client.server_info()  # This will raise an exception if the connection fails

        db = client["PLI_Leaderboard"]  # Specify the database name
        logging.info(
            f"Successfully connected to MongoDB database: Milestone, Collection: {collection_name}"
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


def get_insurance_pli_records(access_token, cvid):
    url = "https://www.zohoapis.com/crm/v6/Insurance_Leads"
    params = {
        "cvid": cvid,
        "per_page": 200,
        "page": 1,
    }
    all_users = []
    required_columns = [
        "Lead_ID",
        "Conversion_Lost_Date",
        "Processing_User",
        "Reference_Owner",
        "Relevant_Premium_figure",
    ]
    while True:
        response = _zoho_get_with_retry(url, params, access_token)
        if response.status_code == 200:
            data = response.json()
            logging.info("Zoho header received")
            all_users.extend(data["data"])
            if not data["info"]["more_records"]:
                break
            params["page"] += 1
        else:
            try:
                err = response.json()
            except Exception:
                err = {"message": response.text}
            logging.error(f"Failed to fetch Zoho CRM users: {err}")
            # Fail gracefully: return empty DataFrame with required columns
            return pd.DataFrame(columns=required_columns)
    df_users = pd.DataFrame(all_users)
    df_users.rename(columns={"Name": "Insurance_Lead_Name"}, inplace=True)
    # Select only the required columns and make a copy to avoid SettingWithCopyWarning
    df_users = (
        df_users[required_columns].copy()
        if not df_users.empty
        else pd.DataFrame(columns=required_columns)
    )
    return df_users


def get_investment_pli_records(access_token, cvid):
    """
    Fetches PLI records from Zoho CRM and returns a DataFrame filtered to specific columns.
    Only the following columns are included in the returned DataFrame:
      - "Conversion_Date"
      - "Converting_user"
      - "Investor_Number"
      - "Is_Family_Head"
      - "Lead_UCC"
      - "Reference_Owner"
      - "Special_permission_to_FH_rule"
    """
    url = "https://www.zohoapis.com/crm/v6/Investment_leads"
    params = {
        "cvid": cvid,
        "per_page": 200,
        "page": 1,
    }
    all_users = []
    required_columns = [
        "Conversion_Date",
        "Converting_user",
        "Investor_Number",
        "Is_Family_Head",
        "Lead_UCC",
        "Reference_Owner",
        "Special_permission_to_FH_rule",
    ]
    while True:
        response = _zoho_get_with_retry(url, params, access_token)
        if response.status_code == 200:
            data = response.json()
            logging.info("Zoho header received")
            all_users.extend(data["data"])
            if not data["info"]["more_records"]:
                break
            params["page"] += 1
        else:
            try:
                err = response.json()
            except Exception:
                err = {"message": response.text}
            logging.error(f"Failed to fetch Zoho CRM users: {err}")
            # Fail gracefully: return empty DataFrame with required columns
            return pd.DataFrame(columns=required_columns)
    df_users = pd.DataFrame(all_users)
    # Only keep the specified columns
    filtered_columns = [col for col in required_columns if col in df_users.columns]
    filtered_df = (
        df_users[filtered_columns].copy()
        if not df_users.empty
        else pd.DataFrame(columns=required_columns)
    )
    return filtered_df


# --- Helper: fetch Zoho employee roster (for inactive 6‑month rule) ---
def fetch_active_employee_ids_from_mongo():
    """
    Return a *dict* of Zoho users keyed by user ID (string) with:
      {
        "active_flag": bool,
        "inactive_since": datetime or None
      }

    A user is considered active if any of these are true:
      • status/Status == "active" (case‑insensitive)
      • active / is_active / IsActive == True

    This roster is used to apply the 6‑month inactive rule for referral incentives.
    """
    try:
        conn = get_secret(KV_SECRET_MONGO_CONNSTRING)
        if not conn:
            logging.warning(
                "User roster unavailable: Mongo connection string secret %s missing. Treating all users as active for this run.",
                KV_SECRET_MONGO_CONNSTRING,
            )
            return None  # signal to bypass gating
        client = pymongo.MongoClient(conn, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client["PLI_Leaderboard"]
        col_name = os.getenv("ZOHO_USERS_COLL", "Zoho_Users")
        col = db[col_name]

        roster: dict[str, dict] = {}
        for doc in col.find(
            {},
            {
                "id": 1,
                "zoho_user_id": 1,
                "User_ID": 1,
                "status": 1,
                "Status": 1,
                "active": 1,
                "is_active": 1,
                "IsActive": 1,
                "inactive_since": 1,
            },
        ):
            # choose an id field
            uid = doc.get("id") or doc.get("zoho_user_id") or doc.get("User_ID")
            if uid is None:
                continue
            uid_str = str(uid)

            # TEMP: force this specific user to inactive (for testing / overrides)
            if uid_str == "2969103000000135011":
                doc["Status"] = "inactive"

            # determine active flag
            status = str(doc.get("status") or doc.get("Status") or "").strip().lower()
            active_flag = (
                status == "active"
                or bool(doc.get("active"))
                or bool(doc.get("is_active"))
                or bool(doc.get("IsActive"))
            )

            roster[uid_str] = {
                "active_flag": bool(active_flag),
                "inactive_since": doc.get("inactive_since"),
            }

        logging.info(
            f"[Mongo] Fetched {len(roster)} Zoho users from PLI_Leaderboard.{col_name} for referral gating."
        )
        return roster
    except Exception as e:
        logging.error(f"Failed to fetch user roster from MongoDB: {e}", exc_info=True)
        return None


# --- Helper: extract period_month and apply 6‑month inactive gating ---
def _extract_month_label_from_date(val) -> str | None:
    """Return 'YYYY-MM' for a given date-like value, or None if parsing fails."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return None
        return f"{dt.year:04d}-{dt.month:02d}"
    except Exception:
        return None


def _eligible_for_month(roster, employee_id, month_key: str | None) -> bool:
    """
    Apply 6‑month inactive rule for a given employee + period month.

    Rules:
      • If roster is None → no gating (return True).
      • If employee_id is None or not present in roster → treat as eligible.
      • If active_flag in roster is True → eligible.
      • If inactive_since is missing → eligible.
      • If inactive_since present and user is inactive:
          - Let diff = (period_year*12+period_month) - (inactive_year*12+inactive_month)
          - Eligible only when 0 <= diff < 6 (exit month + 5 months).
    """
    if roster is None:
        return True
    if employee_id is None:
        return False

    emp_key = str(employee_id)
    info = roster.get(emp_key)
    if not info:
        # No Zoho record – do not block
        return True

    # Active flag wins immediately
    if info.get("active_flag", False):
        return True

    inactive_since = info.get("inactive_since")
    if not inactive_since:
        # No inactive_since date – don't block
        return True

    if not month_key or "-" not in str(month_key):
        # Cannot compute month; fail open
        return True

    try:
        parts = str(month_key).split("-")
        py = int(parts[0])
        pm = int(parts[1])
        period_index = py * 12 + pm
    except Exception:
        return True

    try:
        iy = int(getattr(inactive_since, "year", 0))
        im = int(getattr(inactive_since, "month", 0))
        if iy <= 0 or im <= 0:
            return True
        inactive_index = iy * 12 + im
    except Exception:
        return True

    diff = period_index - inactive_index
    eligible = (diff >= 0) and (diff < 6)
    if not eligible:
        try:
            iso = (
                inactive_since.isoformat()
                if hasattr(inactive_since, "isoformat")
                else str(inactive_since)
            )
        except Exception:
            iso = str(inactive_since)
        logging.debug(
            "[InactiveGate-REF] Blocking employee_id=%s for month=%s (inactive_since=%s, diff=%s)",
            emp_key,
            month_key,
            iso,
            diff,
        )
    return bool(eligible)


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
    Return the number of days between *start* (typically the conversion date)
    and *end* (the **previous** policy‑end date).

    * If either date is missing, returns None.
    * The function always returns a positive (or zero) integer – conversions
      that happen after the previous end date will give 0.
    """
    if pd.isna(start) or pd.isna(end):
        return None
    delta = (pd.to_datetime(end) - pd.to_datetime(start)).days
    return max(delta, 0)


def classify_term(start_date, end_date):
    """
    Return the policy term in whole years, rounded **upwards**.
    • If either date is missing, default to 1.
    • Minimum value returned is 1.

    Examples
    --------
    start=2025-04-01, end=2026-03-31  → 1  (364 days → 1 yr)
    start=2025-04-01, end=2027-04-01  → 2  (732 days → 2 yrs)
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return 1

    delta_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
    years = (delta_days + 364) // 365  # “ceil” divide by 365
    return max(years, 1)


def compute_points(row):
    """
    Compute insurance lead scoring based on Reference_Owner and Processing_User.
    - If Reference_Owner is present and equals Processing_User.id → 100 points
    - If Reference_Owner is present but different from Processing_User.id → 50 to converter, 30 to referrer
    - Otherwise → 0 points
    Always attach lead_id, employee_id, employee_name, reference_id, reference_name, and points.
    """
    # Extract relevant fields
    processing_user = row.get("processing_user", {})
    if isinstance(processing_user, dict):
        employee_id = processing_user.get("id")
        employee_name = processing_user.get("name")
    else:
        employee_id = None
        employee_name = processing_user

    lead_id = row.get("lead_id")

    # Reference_Owner may be dict or string or None
    ref_owner = row.get("Reference_Owner", None)
    reference_id = None
    reference_name = None
    if isinstance(ref_owner, dict):
        reference_id = ref_owner.get("id")
        reference_name = ref_owner.get("name")
    elif isinstance(ref_owner, str):
        reference_id = ref_owner
        reference_name = None
    # If ref_owner is None, both reference_id and reference_name stay None

    points = 0
    referral_incentive = None
    # Rule 1: Reference_Owner present and equals Processing_User.id
    if reference_id and employee_id and str(reference_id) == str(employee_id):
        points = 100
    # Rule 2: Reference_Owner present but different from Processing_User.id
    elif reference_id and employee_id and str(reference_id) != str(employee_id):
        # 50 to converter, 30 to referrer
        points = 50
        # Emit a referral incentive record for the referrer
        referral_incentive = {
            "lead_id": lead_id,
            "employee_id": reference_id,
            "employee_name": reference_name,
            "points": 30,
            "justification": f"Referral {lead_id} insurance",
            "referral_type": "insurance",
        }
    else:
        points = 0

    row["lead_id"] = lead_id
    row["employee_id"] = employee_id
    row["employee_name"] = employee_name
    row["reference_id"] = reference_id
    row["reference_name"] = reference_name
    row["points"] = points
    row["total_points"] = points
    # Attach referral incentive record for main logic to collect
    row["_referral_incentive"] = referral_incentive
    return row


# def process_and_upsert(df_raw, mongo_collection):
#     # 1. Pre‑process date fields (handle whichever variants are present)
#     date_cols = [
#         "Conversion/Lost Date", "Conversion_Lost_Date",
#         "Policy Start Date", "Policy_Start_Date1",
#         "Policy End Date", "Policy_End_Date",
#         "Eldest Member Age", "Eldest_Member_Age"
#     ]
#     for col in date_cols:
#         if col in df_raw.columns:
#             df_raw[col] = pd.to_datetime(df_raw[col], errors="coerce")

#     # 2. Rename columns – support both space‑separated and underscore variants
#     df = df_raw.rename(columns={
#         # premium
#         "Premium B/f GST": "this_year_premium",
#         "Premium_B/f_GST": "this_year_premium",
#         "Premium_B_f_GST": "this_year_premium",
#         "Last Year Premium": "last_year_premium",
#         "Last_Year_Premium": "last_year_premium",
#         # renewal notice
#         "Renewal Notice Premium": "renewal_notice_premium",
#         "Renewal_Notice_Premium": "renewal_notice_premium",
#         # eldest member DOB
#         "Eldest Member Age": "eldest_member_dob",
#         "Eldest_Member_Age": "eldest_member_dob",
#         # basic identity
#         "Insurance Lead Name": "client_name",
#         "Insurance_Lead_Name": "client_name",
#         "Policy Number": "policy_number",
#         "Policy_Number": "policy_number",
#         # dates
#         "Conversion/Lost Date": "conversion_date",
#         "Conversion_Lost_Date": "conversion_date",
#         "Policy Start Date": "policy_start",
#         "Policy_Start_Date1": "policy_start",
#         "Policy End Date": "policy_end",
#         "Policy_End_Date": "policy_end",
#         # misc
#         "Lead_ID": "lead_id",
#         "Insurance_Type": "policy_type",
#         "Conversion_Status": "conversion_status",
#         "Processing_User": "processing_user",
#         "Processing User": "processing_user",
#     })

#     # ensure processing_user column exists for employee extraction
#     if "processing_user" not in df.columns:
#         df["processing_user"] = None

#     # ---- derive explicit employee name / id columns from processing_user ----
#     df["employee_name"] = df["processing_user"].apply(
#         lambda v: v.get("name") if isinstance(v, dict) else v
#     )
#     df["employee_id"] = df["processing_user"].apply(
#         lambda v: v.get("id") if isinstance(v, dict) else None
#     )

#     # 3. Ensure required columns exist even if missing in source
#     required_cols = {
#         "this_year_premium": 0,
#         "last_year_premium": 0,
#         "renewal_notice_premium": 0,
#         "policy_type": None,
#         "policy_start": pd.NaT,
#         "policy_end": pd.NaT,
#         "conversion_date": pd.NaT
#     }
#     for col, default in required_cols.items():
#         if col not in df.columns:
#             df[col] = default

#     # --- Ensure numeric columns are proper floats ---
#     numeric_cols = ["this_year_premium", "last_year_premium", "renewal_notice_premium"]
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

#     # --- deductible flag (dropdown text ➜ boolean) ---
#     # Accept any variant of the column name (case / underscore / spacing)
#     deductible_col = None
#     for col in df.columns:
#         if col.strip().lower() == "deductible_in_policy":
#             deductible_col = col
#             break

#     if deductible_col:
#         df["deductible_added"] = (
#             df[deductible_col]
#             .fillna("")
#             .astype(str)
#             .str.strip()
#             .str.lower()
#             .str.startswith("yes")
#         )
#     else:
#         df["deductible_added"] = False

#     df["premium_delta"] = df["this_year_premium"].fillna(0) - df["last_year_premium"].fillna(0)
#     # Add upsell tracking columns
#     # Mark as upsell only if last‑year premium is available (>0) to avoid false positives
#     df["is_upsell"] = (
#         (df["premium_delta"] > 0)
#         & (df["policy_type"] == "Renewal")
#         & (df["last_year_premium"] > 0)
#     )
#     df["upsell_amount"] = df["premium_delta"].where(df["is_upsell"], 0)

#     # --- derive “previous policy end” (one day before current start) ---
#     df["prev_policy_end"] = df["policy_start"] - pd.Timedelta(days=1)

#     # recalc days_to_renewal using previous cycle's end date
#     df["days_to_renewal"] = df.apply(
#         lambda r: days_before_due(r["conversion_date"], r["prev_policy_end"]),
#         axis=1
#     )

#     df["term_years"] = df.apply(
#         lambda r: classify_term(r["policy_start"], r["policy_end"]), axis=1
#     )


#     # 3. Calculate points
#     df = df.apply(compute_points, axis=1)

#     # --- explode the `points` dict into separate columns for detailed view ---
#     point_cols_df = pd.DataFrame(df["points"].tolist()).fillna(0)
#     # Ensure all six keys exist
#     for col in ["base", "upsell", "early_renew", "term_bonus", "deductible_bonus", "slab_bonus"]:
#         if col not in point_cols_df.columns:
#             point_cols_df[col] = 0
#     point_cols_df.rename(columns={
#         "base": "base_points",
#         "upsell": "upsell_points",
#         "early_renew": "early_renew_points",
#         "term_bonus": "term_bonus_points",
#         "deductible_bonus": "deductible_bonus_points",
#         "slab_bonus": "slab_bonus_points",
#     }, inplace=True)

#     # Drop any previously-created breakout columns to avoid duplication on rerun
#     df.drop(columns=[c for c in ["base_points", "upsell_points",
#                                  "early_renew_points", "term_bonus_points", "deductible_bonus_points"]
#                      if c in df.columns],
#             inplace=True, errors="ignore")

#     # Concatenate the breakout columns back into the main DataFrame (index will align)
#     df = pd.concat([df, point_cols_df], axis=1)

#     # --- Weight‑factor calculation ---
#     def _weight(row):
#         # --- referral‑fee penalty ---
#         ref_fee = row.get("referral_fee")
#         prem    = row.get("this_year_premium", 0) or 0
#         if pd.isna(ref_fee):
#             ref_factor = 1
#         elif ref_fee <= 250:
#             ref_factor = 1
#         else:
#             pct = ref_fee / prem if prem else 0
#             if pct < 0.05:
#                 ref_factor = 0.6
#             elif pct < 0.08:
#                 ref_factor = 0.4
#             else:
#                 ref_factor = 0

#         # --- associate penalty ---
#         assoc_present = not pd.isna(row.get("Associate_Payout")) or not pd.isna(row.get("Associate_id"))
#         policy_type_lower = (row.get("policy_type") or "").lower()
#         # --- base weight for Motor/Term ---
#         base_wt = 0.5 if ("motor" in policy_type_lower or "term" in policy_type_lower) else 1
#         # 5a. Fire/Home/Burglary ⇒ 0.5 base weight
#         if any(k in policy_type_lower for k in ["fire", "burglary", "home"]):
#             base_wt = min(base_wt, 0.5)
#         # 5b. In‑house portability (lead has last‑year premium) ⇒ 0.5
#         pol_status = (row.get("conversion_status") or "").lower()
#         if "portability" in pol_status and row.get("last_year_premium", 0) > 0:
#             base_wt = min(base_wt, 0.5)
#         # --- special rule for Motor‑Private: 30% weight if associate present ---
#         if "motor" in policy_type_lower and "private" in policy_type_lower:
#             if assoc_present:
#                 return min(base_wt, ref_factor, 0.3)
#             # no associate on Motor‑Private → fall through (full weight)

#         # Updated associate logic for Health/Misc, Term, etc.
#         if not assoc_present:
#             assoc_factor = 1
#         else:
#             # Health / Misc. renewals 0.2, others (fresh/port or term) 0.5
#             if "renewal" in pol_status:
#                 assoc_factor = 0.2
#             else:
#                 assoc_factor = 0.5

#         # 5d. Term single‑premium ⇒ weight × 0.2
#         ppt_val = (
#             row.get("Premium_Paying_Term")
#             or row.get("Premium Paying Term")
#         )
#         if "term" in policy_type_lower and isinstance(ppt_val, str) and "single" in ppt_val.lower():
#             return min(base_wt, ref_factor, assoc_factor) * 0.2

#         return min(base_wt, ref_factor, assoc_factor)

#     def _age_penalty(dob_val, wt):
#         dob = pd.to_datetime(dob_val, errors="coerce")

#         # If DOB is missing (NaT) → default to 50 % weight
#         if dob is pd.NaT:
#             return wt * 0.5

#         age = (pd.Timestamp("today") - dob).days // 365

#         # Age strictly greater than 60 ⇒ 50 % weight
#         return wt * 0.5 if age > 60 else wt

#     df["weight_factor"] = df.apply(_weight, axis=1)
#     df["weight_factor"] = df.apply(
#         lambda r: _age_penalty(r.get("eldest_member_dob"), r["weight_factor"]),
#         axis=1
#     )
#     df["total_points"] = (df["total_points"] * df["weight_factor"]).round(2)

#     logging.info(f"Upserted {0} records to MongoDB.")
#     return df[
#         [
#             "lead_id",
#             "policy_number",
#             "conversion_status",
#             "total_points",
#             "base_points",
#             "upsell_points",
#             "early_renew_points",
#             "term_bonus_points",
#             "deductible_bonus_points",
#             "slab_bonus_points",
#             "weight_factor",
#             "premium_delta",
#             "days_to_renewal",
#             "term_years",
#             "deductible_in_policy",
#             "deductible_added",
#             "this_year_premium",
#             "last_year_premium",
#             "policy_start",
#             "policy_end",
#             "conversion_date",
#             "policy_type",
#             "processing_user",
#             "employee_name",
#             "employee_id",
#         ]
#     ]


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

    now = datetime.datetime.utcnow()

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


def main(mytimer: func.TimerRequest) -> None:
    logging.info("[Referral] Function start")
    access_token = get_access_token()
    logging.info("[Referral] Got access token")
    active_ids = fetch_active_employee_ids_from_mongo()

    # Fetch Zoho records
    df_insurance = get_insurance_pli_records(access_token, insurance_cvid)
    df_investment = get_investment_pli_records(access_token, investment_cvid)

    logging.info(f"Insurance rows fetched: {len(df_insurance)}")
    logging.info(f"Investment rows fetched: {len(df_investment)}")

    # Drop duplicates
    df_insurance.drop_duplicates(subset=["Lead_ID"], inplace=True)
    df_investment.drop_duplicates(subset=["Lead_UCC"], inplace=True)

    # Connect to MongoDB
    referral_leaderboard = connect_to_mongo("referralLeaderboard")
    if referral_leaderboard is None:
        logging.error("Mongo connection failed: referralLeaderboard handle is None. Exiting run.")
        return
    now = datetime.datetime.utcnow()

    # --- Insurance Incentives ---
    for _, row in df_insurance.iterrows():
        lead_id = row.get("Lead_ID")
        ref_owner = row.get("Reference_Owner", {})
        proc_user = row.get("Processing_User", {})

        ref_id = ref_owner.get("id") if isinstance(ref_owner, dict) else ref_owner
        ref_name = ref_owner.get("name") if isinstance(ref_owner, dict) else None
        conv_id = proc_user.get("id") if isinstance(proc_user, dict) else None
        conv_name = proc_user.get("name") if isinstance(proc_user, dict) else proc_user

        # Derive period_month from Conversion_Lost_Date (insurance conversion month)
        period_month = _extract_month_label_from_date(row.get("Conversion_Lost_Date"))

        # Enforce 6‑month inactive gating for converter and presence of lead_id
        if not lead_id or not conv_id or not _eligible_for_month(active_ids, conv_id, period_month):
            continue

        # Skip blacklisted RMs
        if _skip_match(conv_name):
            logging.info("Skipping referral for blacklisted RM (Insurance): %s", conv_name)
            continue

        # Scenario A: Converter is Referrer – give 100 pts to converter
        if ref_id == conv_id:
            # Award 100 points to the converter for insurance referral
            referral_leaderboard.update_one(
                {"lead_id": lead_id, "employee_id": conv_id, "referral_type": "insurance"},
                {
                    "$set": {
                        "employee_id": conv_id,
                        "employee_name": conv_name,
                        "points": 100,
                        "lead_id": lead_id,
                        "referral_type": "insurance",
                        "period_month": period_month,
                        "justification": f"Referral {lead_id} insurance",
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
        # Scenario B: Reference exists and is different – 50 pts to converter, 30 pts to referrer
        elif ref_id and conv_id:
            # Award 50 points to the converter for insurance referral
            referral_leaderboard.update_one(
                {"lead_id": lead_id, "employee_id": conv_id, "referral_type": "insurance"},
                {
                    "$set": {
                        "employee_id": conv_id,
                        "employee_name": conv_name,
                        "points": 50,
                        "lead_id": lead_id,
                        "referral_type": "insurance",
                        "period_month": period_month,
                        "justification": f"Referral {lead_id} insurance",
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
            # Award 30 points to the referrer for insurance referral, only if referrer is eligible for this month
            if ref_id and _eligible_for_month(active_ids, ref_id, period_month):
                referral_leaderboard.update_one(
                    {"lead_id": lead_id, "employee_id": ref_id, "referral_type": "insurance"},
                    {
                        "$set": {
                            "employee_id": ref_id,
                            "employee_name": ref_name,
                            "points": 30,
                            "lead_id": lead_id,
                            "referral_type": "insurance",
                            "period_month": period_month,
                            "justification": f"Referral {lead_id} insurance",
                            "updated_at": now,
                        }
                    },
                    upsert=True,
                )

    # --- Investment Incentives ---
    for _, row in df_investment.iterrows():
        ucc = row.get("Lead_UCC")
        ref_owner = row.get("Reference_Owner", {})
        conv_user = row.get("Converting_user", {})

        ref_id = ref_owner.get("id") if isinstance(ref_owner, dict) else ref_owner
        ref_name = ref_owner.get("name") if isinstance(ref_owner, dict) else None
        conv_id = conv_user.get("id") if isinstance(conv_user, dict) else None
        conv_name = conv_user.get("name") if isinstance(conv_user, dict) else conv_user

        # Derive period_month from Conversion_Date (investment conversion month)
        period_month = _extract_month_label_from_date(row.get("Conversion_Date"))

        # --- Extract family head and special permission flags ---
        is_family_head = str(row.get("Is_Family_Head", "")).strip().lower() == "yes"
        special_permission = (
            str(row.get("Special_permission_to_FH_rule", "")).strip().lower() == "yes"
        )

        # Enforce 6‑month inactive gating for converter and presence of ucc
        if not ucc or not conv_id or not _eligible_for_month(active_ids, conv_id, period_month):
            continue

        # Skip blacklisted RMs
        if _skip_match(conv_name):
            logging.info("Skipping referral for blacklisted RM (Investment): %s", conv_name)
            continue

        # Scenario A: Converter is Referrer – award 200 pts to converter
        if ref_id and conv_id and ref_id == conv_id:
            points = 200
            if not is_family_head and not special_permission:
                points = int(points * 0.3)
            referral_leaderboard.update_one(
                {"lead_id": ucc, "employee_id": conv_id, "referral_type": "investment"},
                {
                    "$set": {
                        "employee_id": conv_id,
                        "employee_name": conv_name,
                        "points": points,
                        "lead_id": ucc,
                        "referral_type": "investment",
                        "period_month": period_month,
                        "justification": f"Referral {ucc} investment",
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
        # Scenario B: Reference exists and is different – award 50 pts to referrer
        elif ref_id and ref_id != conv_id:
            points = 50
            if not is_family_head and not special_permission:
                points = int(points * 0.3)
            # Only award to referrer if referrer is eligible for this month
            if ref_id and _eligible_for_month(active_ids, ref_id, period_month):
                referral_leaderboard.update_one(
                    {"lead_id": ucc, "employee_id": ref_id, "referral_type": "investment"},
                    {
                        "$set": {
                            "employee_id": ref_id,
                            "employee_name": ref_name,
                            "points": points,
                            "lead_id": ucc,
                            "referral_type": "investment",
                            "period_month": period_month,
                            "justification": f"Referral {ucc} investment",
                            "updated_at": now,
                        }
                    },
                    upsert=True,
                )
        # Scenario C: No reference exists – award 50 pts to converter
        else:
            points = 50
            if not is_family_head and not special_permission:
                points = int(points * 0.3)
            referral_leaderboard.update_one(
                {"lead_id": ucc, "employee_id": conv_id, "referral_type": "investment"},
                {
                    "$set": {
                        "employee_id": conv_id,
                        "employee_name": conv_name,
                        "points": points,
                        "lead_id": ucc,
                        "referral_type": "investment",
                        "period_month": period_month,
                        "justification": f"Referral {ucc} investment",
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
    logging.info("[Referral] Function done")


if __name__ == "__main__":
    main(None)
