

# import azure.functions as func
import logging
import os
import time
import json
import zipfile
from io import BytesIO, StringIO
from datetime import datetime, timedelta

import certifi
import pandas as pd
import pymongo
import requests
from playwright.sync_api import sync_playwright
from pymongo.database import Database as MongoDatabase
from pymongo.mongo_client import MongoClient as MongoClientType
from urllib.parse import quote

import argparse
import re
import hashlib

from dateutil.relativedelta import relativedelta

# --- Load environment variables from project .env (one level up) if available ---
try:
    from dotenv import load_dotenv  # type: ignore

    _DOTENV_OK = load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except Exception:
    _DOTENV_OK = False

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:  # ImportError or any runtime import issue
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE = {}

# Mongo client/db cache and single-warning flag
_MONGO_CLIENT: MongoClientType | None = None
_MONGO_DB: MongoDatabase | None = None
_MONGO_WARNED_MISSING: bool = False

# --- Logging setup ---
logger = logging.getLogger("investwell")
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
logger.propagate = True  # let Azure/scheduler handlers pick it up
# --- Debug overrides (manual, for local debugging only) ---
# Set to a string like "2025-04-01" to force a from-date without CLI args
DEBUG_FROM_DATE = "2025-04-01"  # e.g., "2025-04-01"
# Optionally cap the to-date (defaults to yesterday if None)
DEBUG_TO_DATE = None  # e.g., "2025-04-30"

# --- Windowing config ---
WINDOW_DAYS = 25  # inclusive window size (e.g., 25 => 01->25, 25->19)
OVERLAP_DAYS = 1  # days overlapped between adjacent windows

# --- Secrets helper: env first, then Azure Key Vault if configured ---
KEY_VAULT_URL = "https://milestonetsl1.vault.azure.net/"


def get_secret(name: str, default: str | None = None) -> str | None:
    """Return secret value from environment if present; otherwise fetch from Azure Key Vault.
    Falls back to `default` if neither source is available. Values are cached per-process.
    Supports KV names that disallow underscores by trying hyphenated variants.
    """
    # 1) Env precedence (easy local override for dev/testing)
    if name in os.environ and os.environ[name]:
        return os.environ[name]

    # Back-compat alias: if code asks for the KV key but env only provides legacy name
    if name == "MONGODB_CONNECTION_STRING":
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
            logger.warning(
                "Secrets: '%s' not found in Key Vault (tried: %s). Using default if provided.",
                name,
                ", ".join(lookup_names),
            )
            return default
        except Exception as e:
            # Don't crash the pipeline if KV is unreachable; rely on default
            logger.warning("Secrets: failed to fetch '%s' from Key Vault: %s", name, e)
            return default

    # 4) Fallback
    return default


# --- Mongo helper (uses Azure KV key: MongoDb-Connection-String) ---
from typing import Tuple, Optional


# --- Mongo index helpers ---
def ensure_unique_index(collection, field: str):
    # `_id` always has an implicit unique index in MongoDB; adding `unique=True` is invalid
    if field == "_id":
        return
    try:
        # Idempotent: same spec returns existing index; different spec may raise which we log and continue
        collection.create_index([(field, 1)], name=f"uniq_{field}", unique=True)
    except Exception as e:
        # Non-fatal; index may already exist or cannot be created due to existing dupes
        logger.warning("Index: could not ensure unique index on '%s': %s", field, e)
def get_mongo_client() -> Tuple[Optional[MongoClientType], Optional[MongoDatabase]]:
    global _MONGO_CLIENT, _MONGO_DB, _MONGO_WARNED_MISSING

    # Serve from cache if already initialized
    if _MONGO_CLIENT is not None and _MONGO_DB is not None:
        return _MONGO_CLIENT, _MONGO_DB

    mongo_uri = get_secret("MONGODB_CONNECTION_STRING")
    if not mongo_uri:
        if not _MONGO_WARNED_MISSING:
            logger.error(
                "Mongo: connection string not configured (env or Key Vault 'MongoDb-Connection-String')."
            )
            _MONGO_WARNED_MISSING = True
        return None, None

    try:
        client = pymongo.MongoClient(
            mongo_uri,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=20000,
            connectTimeoutMS=20000,
        )
        # cache
        _MONGO_CLIENT = client
        _MONGO_DB = client["iwell"]
        return _MONGO_CLIENT, _MONGO_DB
    except Exception as e:
        if not _MONGO_WARNED_MISSING:
            logger.exception("Mongo: connection failed: %s", e)
            _MONGO_WARNED_MISSING = True
        return None, None


def build_txn_url(filters: dict, is_purchase: bool, is_sell: bool) -> str:
    encoded_filters = quote(json.dumps([filters]))
    return (
        f"https://mnivesh.investwell.app/api/broker/txn/downloadTxnStatement?"
        f"filters={encoded_filters}"
        f"&clubTxns=true"
        f"&view-state=c7dde67a3b2054223db0f4c98f3be02e2ed87c844e6de58309ba74c45c6fb2605278ef9efe7ffa41feb51f850ac0fe39"
        f"&isPurchase={'true' if is_purchase else 'false'}"
        f"&isSell={'true' if is_sell else 'false'}"
        f"&isDivPayout=false"
    )


monthly_summary = {}


def read_response_to_df(response):
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        print("Server returned JSON instead of Excel. Likely auth or request error.")
        print("JSON Response:", response.json())
        return None
    try:
        # Excel files (xlsx) start with PK, common for binary Office files
        if response.content[:2] == b"PK" or "application/vnd.openxmlformats" in content_type:
            try:
                df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
            except zipfile.BadZipFile:
                print("Failed to read Excel file: File is not a zip file")
                print("Not a valid XLSX. Attempting CSV fallback...")
                try:
                    df = pd.read_csv(BytesIO(response.content), encoding="utf-8")
                except Exception as e:
                    print("CSV fallback also failed:", str(e))
                    df = pd.DataFrame()
            return df
        else:
            df = pd.read_csv(StringIO(response.content.decode("utf-8")))
            return df
    except Exception as e:
        print(f"Failed to parse file: {e}")
        print("Response Text Snippet:")
        print(response.content[:500])
        return None


#
# --- Robust CSV decoding helper ---
def _read_csv_bytes(data: bytes) -> pd.DataFrame:
    import io

    # 1) Let pandas sniff the delimiter
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(data), encoding=enc, sep=None, engine="python")
        except Exception:
            continue
    # 2) Try a few explicit delimiters
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        for delim in (",", ";", "|", "\t"):
            try:
                return pd.read_csv(io.BytesIO(data), encoding=enc, sep=delim)
            except Exception:
                continue
    # 3) As a last resort, decode with replacement and re-parse
    try:
        text = data.decode("utf-8", errors="replace")
        return pd.read_csv(io.StringIO(text), sep=None, engine="python")
    except Exception:
        return pd.DataFrame()


# --- Robust Excel decoding helper (XLSX & legacy XLS) ---
def _read_excel_bytes(data: bytes) -> pd.DataFrame:
    """Try multiple engines and magic byte heuristics to parse Excel bytes.
    Supports XLSX (PK zip) and legacy XLS (OLE/CFBF). Falls back to empty DF.
    """
    bio = BytesIO(data)

    # Heuristics
    head = data[:8]
    is_zip = head[:2] == b"PK"  # XLSX/OOXML
    is_ole = head.startswith(b"\xd0\xcf\x11\xe0")  # legacy XLS OLE/CFBF

    # Try calamine if available (handles xlsx/xls)
    try:
        bio.seek(0)
        return pd.read_excel(bio, engine="calamine")  # type: ignore[arg-type]
    except Exception:
        pass

    # Try openpyxl for XLSX
    if is_zip:
        try:
            bio.seek(0)
            return pd.read_excel(bio, engine="openpyxl")
        except zipfile.BadZipFile:
            pass
        except Exception:
            pass

    # Try xlrd for legacy XLS
    if is_ole:
        try:
            import importlib.util as _il

            if _il.find_spec("xlrd") is not None:
                bio.seek(0)
                return pd.read_excel(bio, engine="xlrd")
        except Exception:
            pass

    # Let pandas auto-pick as a last resort
    try:
        bio.seek(0)
        return pd.read_excel(bio)
    except Exception:
        return pd.DataFrame()


# INVESTWELL API Configuration
INVESTWELL_API_URL = get_secret("INVESTWELL-API-URL")
INVESTWELL_AUTH_NAME = get_secret("INVESTWELL-AUTHNAME")
INVESTWELL_AUTH_PASSWORD = get_secret("INVESTWELL-PASSWORD")


def get_investwell_token():
    url = f"{INVESTWELL_API_URL}/auth/getAuthorizationToken"
    payload = json.dumps({"authName": INVESTWELL_AUTH_NAME, "password": INVESTWELL_AUTH_PASSWORD})
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get("result", {}).get("token")


def get_sso_token(token, username):
    url = f"{INVESTWELL_API_URL}/auth/getAuthenticationKey"
    payload = json.dumps({"token": token, "username": username})
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=payload)
    sso_token = response.json().get("result", {}).get("SSOToken")
    return f"https://mnivesh.investwell.app/app/#/login?SSOToken={sso_token}"


def save_cookies_from_sso(SSO_URL):
    browser = None
    context = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--disable-logging", "--log-level=3"])
            context = browser.new_context()
            page = context.new_page()

            print("Opening SSO login URL...")
            page.goto(SSO_URL, timeout=30000)
            # Wait until network is idle or up to 15s, whichever comes first
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                # Proceed even if network never fully idles; SSO often streams
                pass

            cookies = context.cookies()
            print("Cookies captured in memory")
            return cookies
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"SSO cookie capture failed: {e}")
        return []
    finally:
        try:
            if context is not None:
                context.close()
        except Exception:
            pass
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass


def download_individual_txn_file(url, txn_label, cookies):
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"))

    response = session.get(url, timeout=60)
    # Robust JSON detection: treat as JSON if content-type says json OR payload starts with '{'
    ct = response.headers.get("Content-Type", "").lower()
    head = response.content[:200].lstrip()
    # Treat as JSON if content-type says json OR payload starts with '{'
    if "json" in ct or (head[:1] == b"{" and b"message" in head):
        try:
            payload = response.json()
            msg = payload.get("message", "JSON error")
        except Exception:
            msg = head.decode("utf-8", errors="ignore")
        print(f"{txn_label}: server replied JSON: {msg}")
        return pd.DataFrame()

    # print(f" [{txn_label}] Content-Type: {response.headers.get('Content-Type', '')}")
    # print(f" [{txn_label}] First 20 bytes: {response.content[:20]}")

    if not response.ok:
        print(f"Failed to download {txn_label} transactions")
        return pd.DataFrame()

    try:
        df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        df.columns = df.columns.str.strip()
        df["TXN SOURCE"] = txn_label

        # Clean and normalize
        if "TXN AMOUNT" in df.columns:
            df["TXN AMOUNT"] = pd.to_numeric(df["TXN AMOUNT"], errors="coerce")

        if "RELATIONSHIP  MANAGER" in df.columns:
            df["Main RM"] = df["RELATIONSHIP  MANAGER"].apply(
                lambda x: None if pd.isna(x) else x.split(" AND ")[-1].strip()
            )

        # No raw-transaction persistence; caller will handle cleaning and per-type upserts
        return df
    except Exception as e:
        print(
            f"Failed to parse {txn_label} Excel: {e}\n Raw response starts with: {response.content[:200]}"
        )
        return pd.DataFrame()


def download_COB_data_file(from_date_str, to_date_str, cookies, cob_type="TICOB"):
    print(f" [COB-{cob_type}] Fetching COB data...")

    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"))

    url = (
        "https://mnivesh.investwell.app/api/broker/reports/downloadChangeOfBrokerReportXLS"
        f"?filters=[{{%22fromDate%22:%22{from_date_str}%22,%22toDate%22:%22{to_date_str}%22}}]"
        "&orderBy=processDate&orderByDesc=true"
        f"&changeType={cob_type}"
    )

    # --- Fetch with light retries (handles transient JSON error payloads) ---
    def _fetch_with_retries(sess, req_url, attempts=3, backoff=0.75):
        last_resp = None
        for i in range(1, attempts + 1):
            try:
                resp = sess.get(req_url, timeout=60)
                last_resp = resp
            except Exception:
                resp = None
                last_resp = None
            if resp is None:
                # Backoff before next attempt if request failed entirely
                if i < attempts:
                    time.sleep(backoff * i)
                continue
            ct = resp.headers.get("Content-Type", "").lower()
            # If we clearly got an Excel payload (or bytes start with PK), return immediately
            if (b"PK" == resp.content[:2]) or ("openxml" in ct) or ("excel" in ct):
                return resp
            # If JSON with error, retry (some windows flake for TOCOB)
            if "json" in ct:
                try:
                    payload = resp.json()
                    msg = str(payload.get("message", "")).lower()
                    # Hard no-data messages shouldn’t likely change; break early
                    if "no data" in msg:
                        return resp
                except Exception:
                    pass
            # Backoff before next attempt
            if i < attempts:
                time.sleep(backoff * i)
        return last_resp

    response = _fetch_with_retries(session, url)
    if response is None:
        print(f"COB-{cob_type}: request failed after retries; no response received")
        return pd.DataFrame()
    content_type = response.headers.get("Content-Type", "")
    dispo = response.headers.get("Content-Disposition", "")
    data = response.content

    # Debug headers + filename hint
    filename_hint = None
    if "filename=" in dispo:
        try:
            filename_hint = dispo.split("filename=")[-1].strip('"')
        except Exception:
            filename_hint = None
    print(f"content_type:{content_type}{(', '+dispo) if dispo else ''}")
    print(f"[DEBUG] bytes={len(data)} head={data[:8]!r} filename={filename_hint}")

    # If server signals an error payload
    if content_type.lower().startswith("application/json"):
        try:
            payload = json.loads(data.decode("utf-8", errors="ignore"))
        except Exception:
            payload = {"message": data[:200].decode("utf-8", errors="ignore")}
        msg = str(payload.get("message", "")).strip()
        # Normalize common no-data/error messages from IW
        no_data_markers = ("no data", "something is not good", "no records")
        if any(tok in msg.lower() for tok in no_data_markers):
            print(f"COB-{cob_type}: server replied no-data/soft-error: {msg}")
            return pd.DataFrame()
        print(f"Warning: COB-{cob_type} returned JSON instead of Excel. Message:\n{msg}")
        return pd.DataFrame()

    try:
        # Prefer Excel decoding first; only fall back to CSV if absolutely needed
        df = _read_excel_bytes(data)
        if df is None or getattr(df, "empty", False):
            df = _read_csv_bytes(data)

        if df is None or getattr(df, "empty", True):
            print(f"[INFO] COB {cob_type} DataFrame is empty after parsing attempts.")
            return pd.DataFrame()

        # --- Robust header detection & normalization (relaxed) ---
        df_raw = df.copy()

        # If we accidentally parsed as a single unnamed column, try splitting it as CSV once more
        if df_raw.shape[1] == 1 and df_raw.columns.tolist() in ([0], [None], ["Unnamed: 0"]):
            import io

            df_raw = pd.read_csv(io.BytesIO(data), sep=None, engine="python")

        def _norm(s: object) -> str:
            return re.sub(r"[^A-Z0-9 ]+", "", str(s).upper()).strip()

        expected_any = [
            "CLIENT",  # CLIENT NAME / INVESTOR NAME
            "SCHEME",  # SCHEME NAME
            "FOLIO",  # FOLIO NO
            "RELATIONSHIP",  # RELATIONSHIP  MANAGER
            "TRANSFER",  # TRANSFER IN/OUT/PROCESS DATE
            "PROCESS",
        ]
        header_row_idx = None
        best_hits = -1
        scan_rows = min(25, len(df_raw))
        for i in range(scan_rows):
            row_vals = [_norm(v) for v in df_raw.iloc[i].tolist()]
            hits = sum(any(token in cell for token in expected_any) for cell in row_vals)
            if hits > best_hits:
                best_hits = hits
                header_row_idx = i

        if best_hits > 0 and header_row_idx is not None:
            new_cols = [str(v).strip() for v in df_raw.iloc[header_row_idx].tolist()]
            df_norm = df_raw.iloc[header_row_idx + 1 :].reset_index(drop=True)
            df_norm.columns = new_cols
        else:
            first_non_empty = next(
                (
                    i
                    for i in range(scan_rows)
                    if any(str(x).strip() for x in df_raw.iloc[i].tolist())
                ),
                None,
            )
            if first_non_empty is not None:
                new_cols = [str(v).strip() for v in df_raw.iloc[first_non_empty].tolist()]
                df_norm = df_raw.iloc[first_non_empty + 1 :].reset_index(drop=True)
                df_norm.columns = new_cols
            else:
                df_norm = df_raw.copy()

        # Upper-case/strip column names to simplify matching
        df_norm.columns = [str(c).strip().upper() for c in df_norm.columns]

        # --- Column picking with tolerant candidates (relaxed) ---
        def pick_col(candidates: list[str]) -> str | None:
            norm_cols = {c: re.sub(r"\s+", " ", c).strip() for c in df_norm.columns}
            for cand in candidates:
                if cand in df_norm.columns:
                    return cand
            for c in df_norm.columns:
                for cand in candidates:
                    if cand in c or cand in norm_cols[c]:
                        return c
            return None

        date_candidates = [
            ("TICOB", ["TRANSFER IN DATE", "IN DATE", "TRANSFER DATE", "PROCESS DATE"]),
            ("TOCOB", ["TRANSFER OUT DATE", "OUT DATE", "TRANSFER DATE", "PROCESS DATE"]),
        ]
        date_list = next((dl for tag, dl in date_candidates if tag == cob_type), ["TRANSFER DATE"])
        date_col = pick_col([d.upper() for d in date_list])
        client_col = pick_col(["CLIENT NAME", "INVESTOR NAME", "CLIENTNAME"])
        scheme_col = pick_col(["SCHEME NAME", "SCHEMENAME", "SCHEME"])
        folio_col = pick_col(["FOLIO NO", "FOLIO", "FOLIO NUMBER"])
        rm_col = pick_col(
            ["RELATIONSHIP  MANAGER", "RELATIONSHIP MANAGER", "RELATIONSHIP MANAGER "]
        )
        amount_col = pick_col(["AMOUNT", "AMOUNT (₹)", "AMT", "TOTAL AMOUNT"])

        cols_required = {
            "DATE": date_col,
            "CLIENT NAME": client_col,
            "SCHEME NAME": scheme_col,
            "FOLIO NO": folio_col,
            "RELATIONSHIP  MANAGER": rm_col,
            "AMOUNT": amount_col,
        }

        keep_map = {k: v for k, v in cols_required.items() if v is not None}
        if keep_map:
            df = df_norm[list(keep_map.values())].copy()
            df.columns = list(keep_map.keys())
        else:
            df = df_norm.copy()

        for k in cols_required.keys():
            if k not in df.columns:
                df[k] = None

        if cols_required["DATE"] is not None:
            dcol = "DATE"
            df[dcol] = df[dcol].astype(str)
            df = df[
                ~df[dcol]
                .str.upper()
                .isin(
                    [
                        "",
                        "NONE",
                        "NAN",
                        "TOTAL",
                        "TRANSFER IN DATE",
                        "TRANSFER OUT DATE",
                        "TRANSFER DATE",
                    ]
                )
            ].copy()

        from pymongo import UpdateOne

        client, db = get_mongo_client()
        if db is None:
            return pd.DataFrame()
        collection = db["ChangeofBroker"]

        operations = []
        for _, row in df.iterrows():
            prefix = "TICOB_" if cob_type == "TICOB" else "TOCOB_"
            base_unique = (
                f"{prefix}{row.get('DATE', '')}_{'IN' if cob_type=='TICOB' else 'OUT'}_"
                f"{row.get('CLIENT NAME', '')}_{row.get('FOLIO NO', '')}_{row.get('SCHEME NAME', '')}"
            )
            required_vals = [
                row.get("DATE"),
                row.get("CLIENT NAME"),
                row.get("FOLIO NO"),
                row.get("SCHEME NAME"),
            ]
            needs_fallback = any(
                v in (None, "") or (isinstance(v, float) and pd.isna(v)) for v in required_vals
            )
            if needs_fallback:
                try:
                    raw_map = {
                        k: (None if (isinstance(v, float) and pd.isna(v)) else v)
                        for k, v in row.to_dict().items()
                    }
                    raw_str = json.dumps(raw_map, default=str, ensure_ascii=False)
                except Exception:
                    raw_str = str(row.to_dict())
                digest = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:10]
                unique_id = f"{base_unique}__{digest}"
            else:
                unique_id = base_unique

            rm_raw = row.get("RELATIONSHIP  MANAGER")
            if isinstance(rm_raw, str) and " AND " in rm_raw:
                main_rm = rm_raw.split(" AND ")[-1].strip()
            elif isinstance(rm_raw, str):
                main_rm = rm_raw.strip()
            else:
                main_rm = None

            amount_val = row.get("AMOUNT")
            doc = row.to_dict()
            if "AMOUNT" in doc:
                del doc["AMOUNT"]
            doc.update(
                {
                    "_id": unique_id,
                    "Amount": amount_val,
                    "COB TYPE": cob_type,
                    "MAIN RM": main_rm,
                    "Direction": "COB IN" if cob_type == "TICOB" else "COB OUT",
                    "TRANSFER DATE": row.get("DATE"),
                }
            )
            if " RELATIONSHIP  MANAGER" in doc:
                del doc[" RELATIONSHIP  MANAGER"]

            operations.append(UpdateOne({"_id": unique_id}, {"$set": doc}, upsert=True))

        if operations:
            try:
                res = collection.bulk_write(operations, ordered=False)
                # Derive granular counts
                total_ops = len(operations)
                upserts = (
                    len(getattr(res, "upserted_ids", {}))
                    if hasattr(res, "upserted_ids")
                    else getattr(res, "upserted_count", 0)
                )
                updated = getattr(res, "modified_count", 0) or 0
                matched = getattr(res, "matched_count", 0) or 0
                unchanged = max(matched - updated, 0)
                print(
                    f"COB-{cob_type}: bulk upsert done | ops={total_ops} | upserts={upserts} | updated={updated} | matched_unchanged={unchanged}"
                )
            except Exception as e:
                print(f"COB-{cob_type}: bulk upsert failed: {e}")

        return df
    except Exception as e:
        print(f"Failed to parse COB {cob_type}: {e}")
        return pd.DataFrame()


# --- Helpers: month starts within a date range ---
from typing import Iterable


def _month_starts_in_range(start_date: datetime, end_date: datetime) -> list[str]:
    """Return list of YYYY-MM-01 strings for every month intersecting [start_date, end_date]."""
    cur = start_date.replace(day=1)
    out: list[str] = []
    while cur <= end_date:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += relativedelta(months=1)
    return out


# --- AUM monthly fetch helper (explicit month list) ---
def download_AUM_for_months(month_starts, cookies):
    t0 = time.perf_counter()
    logger.info(
        "AUM(months): start | months=%s | cookies=%s", month_starts, len(cookies) if cookies else 0
    )

    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"))

    client, db = get_mongo_client()
    if db is None:
        logger.error("AUM: Mongo not configured; aborting")
        return
    collection = db["AUM_Report"]

    for toDate in month_starts:
        filters = [{"arnid": "", "category": "", "toDate": toDate}]
        filters_encoded = quote(json.dumps(filters))
        download_url = (
            f"https://mnivesh.investwell.app/api/broker/AUMReport/downloadAUMReportForMutualFundsCSV?"
            f"filters={filters_encoded}&groupBy=8&orderBy=name&orderByDesc=false"
            "&view-state=c7dde67a3b2054223db0f4c98f3be02ec88cfbdf0effb28bc03c309c61eeda28c158739ce37956875ee4d3110ed8dc8b"
        )
        response = session.get(download_url, timeout=60)
        logger.info(
            "AUM[%s]: status=%s content-type=%s bytes=%s",
            toDate,
            response.status_code,
            response.headers.get("Content-Type", ""),
            len(response.content),
        )
        if not response.ok:
            logger.error(
                "AUM[%s]: fetch failed | status=%s | snippet=%s",
                toDate,
                response.status_code,
                response.text[:500],
            )
            continue

        df = read_response_to_df(response)
        if df is None or getattr(df, "empty", True):
            logger.warning("AUM[%s]: empty/None dataframe from parser; skipping", toDate)
            continue

        # Skip header rows then set proper headers
        df = df.iloc[2:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df.columns = df.columns.str.strip()

        # Ensure TOTAL is numeric
        if "TOTAL" in df.columns:
            df["TOTAL"] = pd.to_numeric(df["TOTAL"], errors="coerce")
        else:
            logger.warning("AUM[%s]: 'TOTAL' column missing after parse; skipping", toDate)
            continue

        # Select and normalize
        if "RELATIONSHIP MANAGER" not in df.columns:
            logger.warning(
                "AUM[%s]: 'RELATIONSHIP MANAGER' missing; columns=%s", toDate, list(df.columns)
            )
            continue

        df = df[["RELATIONSHIP MANAGER", "TOTAL"]]
        df["MAIN_RM"] = df["RELATIONSHIP MANAGER"].apply(
            lambda x: (
                x.split(" AND ")[-1].strip()
                if isinstance(x, str) and " AND " in x
                else (x.strip() if isinstance(x, str) else x)
            )
        )
        df = df[~df["MAIN_RM"].isin(["", "Total", "TOTAL"])].copy()

        # Parse PRIMARY / SECONDARY and enumerate members for audit
        def _split_rm(val: object) -> tuple[str | None, str | None, list[str]]:
            if isinstance(val, str):
                parts = [p.strip() for p in val.split(" AND ") if p.strip()]
                if len(parts) == 0:
                    return None, None, []
                if len(parts) == 1:
                    return parts[0], parts[0], parts
                return parts[0], parts[-1], parts
            return None, None, []

        primaries: list[str | None] = []
        secondaries: list[str | None] = []
        members_list: list[list[str]] = []
        for v in df["RELATIONSHIP MANAGER"].tolist():
            p, s, m = _split_rm(v)
            primaries.append(p)
            secondaries.append(s)
            members_list.append(m)
        df["PRIMARY_RM"] = primaries
        df["SECONDARY_RM"] = secondaries
        df["MEMBERS_LIST"] = members_list

        # Build docs
        grouped = df.groupby("MAIN_RM", dropna=False)
        # Remove existing AUM entries for the same (MAIN_RM, month) before upsert
        deletions = 0
        for main_rm, _ in grouped:
            res = collection.delete_many({"Month": toDate, "MAIN RM": main_rm})
            deletions += getattr(res, "deleted_count", 0)
        logger.debug("AUM[%s]: pre-upsert deletes=%s", toDate, deletions)

        ops = []
        for main_rm, g in grouped:
            total_amount = float(pd.to_numeric(g["TOTAL"], errors="coerce").sum())
            breakdown_df = (
                g.groupby("RELATIONSHIP MANAGER", dropna=False)["TOTAL"].sum().reset_index()
            )
            breakdown_records = [
                {
                    "RELATIONSHIP_MANAGER": (
                        str(r["RELATIONSHIP MANAGER"])
                        if pd.notna(r["RELATIONSHIP MANAGER"])
                        else ""
                    ),
                    "AUM": float(r["TOTAL"]),
                }
                for _, r in breakdown_df.iterrows()
            ]
            doc_id = f"{toDate}_{main_rm}"
            doc = {
                "_id": doc_id,
                "MAIN RM": main_rm,
                "Amount": total_amount,
                "Month": toDate,
                "SOURCE_BREAKDOWN": json.dumps(breakdown_records),
            }
            ops.append(pymongo.UpdateOne({"_id": doc_id}, {"$set": doc}, upsert=True))

        if ops:
            try:
                bulk_res = collection.bulk_write(ops)
                logger.info(
                    "AUM[%s]: bulk upsert done | ops=%s matched=%s modified=%s upserted=%s",
                    toDate,
                    len(ops),
                    getattr(bulk_res, "matched_count", None),
                    getattr(bulk_res, "modified_count", None),
                    getattr(bulk_res, "upserted_count", None),
                )
            except Exception as e:
                logger.exception("AUM[%s]: bulk upsert failed: %s", toDate, e)

        # --- Additional role-based aggregation (PRIMARY and SECONDARY) ---
        role_collection = db["AUM_Report_ByRole"]
        # Clean existing for this month to avoid duplicates on re-run
        role_collection.delete_many({"Month": toDate})

        def _agg_role(df_local: pd.DataFrame, role_field: str, role_name: str):
            # Explicit DF argument avoids optional-access warnings in static analysis
            if role_field not in df_local.columns:
                return
            grp = df_local.groupby(role_field, dropna=False)
            role_ops = []
            for rm_name, g in grp:
                if rm_name is None or str(rm_name).strip() == "":
                    continue
                total_amount = float(pd.to_numeric(g["TOTAL"], errors="coerce").sum())
                # Breakdown by the raw RM string for audit parity
                breakdown_df = (
                    g.groupby("RELATIONSHIP MANAGER", dropna=False)["TOTAL"].sum().reset_index()
                )
                breakdown_records = [
                    {
                        "RELATIONSHIP_MANAGER": (
                            str(r["RELATIONSHIP MANAGER"])
                            if pd.notna(r["RELATIONSHIP MANAGER"])
                            else ""
                        ),
                        "AUM": float(r["TOTAL"]),
                    }
                    for _, r in breakdown_df.iterrows()
                ]
                # Unique member names observed under this grouping
                members: list[str] = sorted({m for sub in g["MEMBERS_LIST"].tolist() for m in sub})
                doc_id = f"{toDate}_{role_name}_{rm_name}"
                doc = {
                    "_id": doc_id,
                    "ROLE": role_name,
                    "RM": rm_name,
                    "Amount": total_amount,
                    "Month": toDate,
                    "SOURCE_BREAKDOWN": json.dumps(breakdown_records),
                    "MEMBERS": members,
                }
                role_ops.append(pymongo.UpdateOne({"_id": doc_id}, {"$set": doc}, upsert=True))
            if role_ops:
                try:
                    role_collection.bulk_write(role_ops)
                except Exception:
                    logger.exception("AUM[%s]: role=%s bulk upsert failed", toDate, role_name)

        _agg_role(df, "PRIMARY_RM", "PRIMARY")
        _agg_role(df, "SECONDARY_RM", "SECONDARY")

    logger.info("AUM(months): done | elapsed=%.3fs", time.perf_counter() - t0)


# --- Pipeline Logic Wrapped for Reuse ---
def upsert_mongodb(df, collection_name, index_field):
    """
    Upserts each record from the DataFrame into the given MongoDB collection.
    Returns the count of inserted documents (not updated).
    """
    client, db = get_mongo_client()
    if db is None:
        return 0
    collection = db[collection_name]
    ensure_unique_index(collection, index_field)
    inserted_count = 0
    for record in df.to_dict(orient="records"):
        result = collection.update_one(
            {index_field: record[index_field]}, {"$set": record}, upsert=True
        )
        if result.upserted_id:
            inserted_count += 1
    return inserted_count


def generate_unique_hash(df):
    # Always operate on a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Prefer amount columns in this order to avoid empty-string collisions
    preferred_amount_cols = [
        "TOTAL AMOUNT",
        "TXN AMOUNT",
        "AMOUNT",
    ]

    def _pick_amount(row):
        for col in preferred_amount_cols:
            if col in row and pd.notna(row[col]):
                return row[col]
        return ""  # last resort

    def _mk(row):
        return (
            f"{row.get('TRANSACTION DATE', '')}_"
            f"{row.get('FOLIO NO', '')}_"
            f"{row.get('SCHEME NAME', '')}_"
            f"{row.get('TXN TYPE', '')}_"
            f"{_pick_amount(row)}"
        )

    df.loc[:, "unique_hash"] = df.apply(_mk, axis=1)
    return df


def clean_txn_data(df, label):
    # print(f"[DEBUG] Columns before cleaning {label}: {df.columns.tolist()}")
    # Drop unwanted columns if they exist
    cols_to_drop = [
        "UNITS",
        "NAV",
        "STT",
        "TDS",
        "STAMP DUTY",
        "ARN NO",
        "TXN DESCRIPTION",
        "TXN SOURCE",
        "SIP REG DATE",
        "REMARKS",
        "EQUITY CODE",
        "APP CODE",
        "SB CODE",
        "BACK  OFFICE 2",
        "BACK  OFFICE 2 CODE",
        "TEAM  MANAGER",
        "TEAM  MANAGER CODE",
        "TEAM  LEADER",
        "TEAM  LEADER CODE",
        "RELATIONSHIP  MANAGER CODE",
        "SUB  BROKER CODE",
        "SERVICE  R M",
        "SERVICE  R M CODE",
    ]
    df.drop(columns=cols_to_drop, errors="ignore", inplace=True)
    # print(f"[DEBUG] Columns after cleaning {label}: {df.columns.tolist()}")
    # Retain only desired columns
    desired_cols = [
        "TRANSACTION DATE",
        "SCHEME NAME",
        "CATEGORY",
        "SUB CATEGORY",
        "FOLIO NO",
        "APPLICANT",
        "FAMILY HEAD",
        "PAN",
        "TXN TYPE",
        "EUIN",
        "TOTAL AMOUNT",
        "FUND NAME",
        "SOURCE SCHEME NAME",
        "TARGET SCHEME NAME",
        "IWELL CODE",
        "RELATIONSHIP  MANAGER",
        "SUB  BROKER",
        "Main RM",
    ]
    df = df.loc[:, [col for col in desired_cols if col in df.columns]].copy()
    # Always generate unique_hash after all transformations
    df = generate_unique_hash(df)
    return df


from typing import List, Tuple


def _iter_overlapping_windows(
    from_date_str: str,
    to_date_str: str,
    window_days: int = WINDOW_DAYS,
    overlap_days: int = OVERLAP_DAYS,
) -> List[Tuple[str, str]]:
    """Yield [start, end] date windows (inclusive) of size `window_days` with `overlap_days` overlap. Next window starts at previous_end for overlap=1."""
    start = datetime.strptime(from_date_str, "%Y-%m-%d")
    end = datetime.strptime(to_date_str, "%Y-%m-%d")
    windows: List[Tuple[str, str]] = []
    cur_start = start
    while cur_start <= end:
        cur_end = min(cur_start + timedelta(days=window_days - 1), end)
        windows.append((cur_start.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d")))
        # If we already covered the overall end, stop to avoid a redundant tail window
        if cur_end >= end:
            break
        # Advance with desired overlap: for overlap=1, next window starts AT previous end
        next_start = cur_end - timedelta(days=overlap_days - 1)
        if next_start <= cur_start:
            # Safety guard (shouldn’t trigger if window_days > overlap_days)
            break
        cur_start = next_start
    return windows


#
# --- Transactions-only runner for a date window ---
def run_transactions_window(from_date_str: str, to_date_str: str, cookies=None):
    print(f"Fetching TRANSACTIONS: {from_date_str} to {to_date_str}")

    if not cookies:
        raise RuntimeError("run_transactions_window requires cookies; pass them from main().")

    filters_common = {
        "fundid": "",
        "arnid": "9c825d3f701c63846a0e5d0d068d5543",
        "rta": "",
        "objectiveid": "",
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    urls = {
        "Purchase": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "SWI"],
                "sellTxnTypeNotIn": ["NRS", "STO", "SWO", "SWP"],
            },
            is_purchase=True,
            is_sell=False,
        ),
        "Switch In": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "DIR", "BON", "NRP"],
                "sellTxnTypeNotIn": ["NRS", "STO", "SWO", "SWP"],
            },
            is_purchase=True,
            is_sell=False,
        ),
        "Redemption": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "NRP", "SWI"],
                "sellTxnTypeNotIn": ["STO", "SWP", "SWO"],
            },
            is_purchase=False,
            is_sell=True,
        ),
        "Switch Out": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "NRP", "SWI"],
                "sellTxnTypeNotIn": ["SWP", "NRS"],
            },
            is_purchase=False,
            is_sell=True,
        ),
    }

    df_purchase = download_individual_txn_file(urls["Purchase"], "Purchase", cookies)
    if df_purchase is not None and not df_purchase.empty:
        df_purchase = clean_txn_data(df_purchase, "Purchase")
        inserted_count = upsert_mongodb(df_purchase, "purchase_txn", "unique_hash")
        print(f"Purchase transactions inserted: {inserted_count}")

    df_switch_in = download_individual_txn_file(urls["Switch In"], "Switch In", cookies)
    if df_switch_in is not None and not df_switch_in.empty:
        df_switch_in = clean_txn_data(df_switch_in, "Switch In")
        inserted_count = upsert_mongodb(df_switch_in, "switchin_txn", "unique_hash")
        print(f"Switch In transactions inserted: {inserted_count}")

    df_redemption = download_individual_txn_file(urls["Redemption"], "Redemption", cookies)
    if df_redemption is not None and not df_redemption.empty:
        df_redemption = clean_txn_data(df_redemption, "Redemption")
        inserted_count = upsert_mongodb(df_redemption, "redemption_txn", "unique_hash")
        print(f"Redemption transactions inserted: {inserted_count}")

    df_switch_out = download_individual_txn_file(urls["Switch Out"], "Switch Out", cookies)
    if df_switch_out is not None and not df_switch_out.empty:
        df_switch_out = clean_txn_data(df_switch_out, "Switch Out")
        inserted_count = upsert_mongodb(df_switch_out, "switchout_txn", "unique_hash")
        print(f"Switch Out transactions inserted: {inserted_count}")


def run_pipeline(from_date_str: str, to_date_str: str, cookies=None):
    print(f"Fetching data: {from_date_str} to {to_date_str}")

    # One-time hint if Mongo isn’t configured
    if get_secret("MONGODB_CONNECTION_STRING") in (None, ""):
        print(
            "Mongo not configured. Set ENV 'MongoDb-Connection-String' or store it in Azure Key Vault.\n"
            "   Example (env): export MongoDb-Connection-String='mongodb+srv://user:pass@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority&appName=Milestone'\n"
            "   Key Vault secret name: 'MongoDb-Connection-String'"
        )

    if not cookies:
        raise RuntimeError("run_pipeline requires cookies; fetch once in main() and pass them in.")

    filters_common = {
        "fundid": "",
        "arnid": "9c825d3f701c63846a0e5d0d068d5543",
        "rta": "",
        "objectiveid": "",
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    urls = {
        "Purchase": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "SWI"],
                "sellTxnTypeNotIn": ["NRS", "STO", "SWO", "SWP"],
            },
            is_purchase=True,
            is_sell=False,
        ),
        "Switch In": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "NRP"],
                "sellTxnTypeNotIn": ["NRS", "STO", "SWO", "SWP"],
            },
            is_purchase=True,
            is_sell=False,
        ),
        "Redemption": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "NRP", "SWI"],
                "sellTxnTypeNotIn": ["STO", "SWP", "SWO"],
            },
            is_purchase=False,
            is_sell=True,
        ),
        "Switch Out": build_txn_url(
            {
                **filters_common,
                "purchaseTxnTypeNotIn": ["SIP", "STI", "DIR", "BON", "NRP", "SWI"],
                "sellTxnTypeNotIn": ["STO", "SWP", "NRS"],
            },
            is_purchase=False,
            is_sell=True,
        ),
    }

    # Download and upsert each transaction type, with logging
    df_purchase = download_individual_txn_file(urls["Purchase"], "Purchase", cookies)
    if df_purchase is not None and not df_purchase.empty:
        df_purchase = clean_txn_data(df_purchase, "Purchase")
        inserted_count = upsert_mongodb(df_purchase, "purchase_txn", "unique_hash")
        print(f"Purchase transactions inserted: {inserted_count}")

    df_switch_in = download_individual_txn_file(urls["Switch In"], "Switch In", cookies)
    if df_switch_in is not None and not df_switch_in.empty:
        df_switch_in = clean_txn_data(df_switch_in, "Switch In")
        inserted_count = upsert_mongodb(df_switch_in, "switchin_txn", "unique_hash")
        print(f"Switch In transactions inserted: {inserted_count}")

    df_redemption = download_individual_txn_file(urls["Redemption"], "Redemption", cookies)
    if df_redemption is not None and not df_redemption.empty:
        df_redemption = clean_txn_data(df_redemption, "Redemption")
        inserted_count = upsert_mongodb(df_redemption, "redemption_txn", "unique_hash")
        print(f"Redemption transactions inserted: {inserted_count}")

    df_switch_out = download_individual_txn_file(urls["Switch Out"], "Switch Out", cookies)
    if df_switch_out is not None and not df_switch_out.empty:
        df_switch_out = clean_txn_data(df_switch_out, "Switch Out")
        inserted_count = upsert_mongodb(df_switch_out, "switchout_txn", "unique_hash")
        print(f"Switch Out transactions inserted: {inserted_count}")

    df_in = download_COB_data_file(from_date_str, to_date_str, cookies, cob_type="TICOB")
    if df_in is None or df_in.empty:
        print("[INFO] COB In DataFrame is empty.")

    df_out = download_COB_data_file(from_date_str, to_date_str, cookies, cob_type="TOCOB")
    if df_out is None or df_out.empty:
        print("[INFO] COB Out DataFrame is empty.")

    # download_AUM_report(from_date_str, to_date_str, cookies)

    # Call user hierarchy pipeline at the end of main
    # download_and_process_user_hierarchy(cookies)


# --- Azure Function Timer Trigger Entry Point ---
def main():
    logger.info("Azure Function triggered via timer")

    # Parse optional CLI arg: --from-date YYYY-MM-DD
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--from-date", dest="from_date", default=None)
    args, _ = parser.parse_known_args()

    to_date = datetime.today() - timedelta(days=1)

    # Apply manual debug overrides if present
    DEBUG_FROM_DATE = "2025-04-01"  # set to None to disable
    DEBUG_TO_DATE = None  # set to None to disable
    DEBUG_FROM_DATE = None  # set to None to disable

    if args.from_date is None and DEBUG_FROM_DATE:
        args.from_date = DEBUG_FROM_DATE
        print(f"[DEBUG] Using DEBUG_FROM_DATE override: {args.from_date}")
    if DEBUG_TO_DATE:
        try:
            td_override = datetime.strptime(DEBUG_TO_DATE, "%Y-%m-%d")
            # Never go beyond yesterday
            to_date = min(to_date, td_override)
        except ValueError:
            raise SystemExit("DEBUG_TO_DATE must be YYYY-MM-DD")

    if args.from_date:
        try:
            fd = datetime.strptime(args.from_date, "%Y-%m-%d")
        except ValueError:
            raise SystemExit("--from-date must be in YYYY-MM-DD format")
        from_date = fd
    else:
        from_date = to_date - timedelta(days=5)

    from_date_str = from_date.strftime("%Y-%m-%d")
    to_date_str = to_date.strftime("%Y-%m-%d")

    print(f"to_date:{to_date_str}")
    print(f"from_date:{from_date_str}")

    # Single-shot full-period run (COB & Transactions). IW COB supports large ranges.
    token = get_investwell_token()
    sso_url = get_sso_token(token, "mnivesh_admin")
    cookies = save_cookies_from_sso(sso_url)

    # --- Transactions: 25d windows with 1d overlap ---
    windows = _iter_overlapping_windows(from_date_str, to_date_str, WINDOW_DAYS, OVERLAP_DAYS)
    print(f"Planned windows:\n({WINDOW_DAYS}d windows, {OVERLAP_DAYS}d overlap)")
    for i, (wf, wt) in enumerate(windows, 1):
        _wf = datetime.strptime(wf, "%Y-%m-%d")
        _wt = datetime.strptime(wt, "%Y-%m-%d")
        _span = (_wt - _wf).days + 1
        print(f"  [{i}] {wf} -> {wt} ({_span}d, {OVERLAP_DAYS}d overlap)")

    for win_from, win_to in windows:
        _wf = datetime.strptime(win_from, "%Y-%m-%d")
        _wt = datetime.strptime(win_to, "%Y-%m-%d")
        _span = (_wt - _wf).days + 1
        print(f"Window: {win_from} -> {win_to} ({_span}d, {OVERLAP_DAYS}d overlap)")
        run_transactions_window(win_from, win_to, cookies=cookies)

    # --- COB: full period in a single shot ---
    df_in = download_COB_data_file(from_date_str, to_date_str, cookies, cob_type="TICOB")
    if df_in is None or df_in.empty:
        print("[INFO] COB In DataFrame is empty.")
    df_out = download_COB_data_file(from_date_str, to_date_str, cookies, cob_type="TOCOB")
    if df_out is None or df_out.empty:
        print("[INFO] COB Out DataFrame is empty.")

    # AUM snapshots
    if args.from_date:
        month_starts = _month_starts_in_range(from_date, to_date)
        if month_starts:
            download_AUM_for_months(month_starts, cookies)
    else:
        day_now = to_date.day
        if 2 <= day_now <= 5:
            this_month_start = to_date.replace(day=1).strftime("%Y-%m-%d")
            months = [this_month_start]
            download_AUM_for_months(months, cookies)
        else:
            logger.debug(
                "AUM: skipped (day=%s not in 2..5); from=%s to=%s",
                day_now,
                from_date_str,
                to_date_str,
            )
    return 0

if __name__ == "__main__":
    main()
