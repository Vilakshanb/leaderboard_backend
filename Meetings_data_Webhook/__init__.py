import os
import json
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Dict, Tuple, cast
from urllib.parse import parse_qs, urlparse

import azure.functions as func
from pymongo import MongoClient, errors
import logging

from dotenv import load_dotenv, find_dotenv

# --- Azure Key Vault (guarded import) ---
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:
    DefaultAzureCredential = None  # type: ignore
    SecretClient = None  # type: ignore

# Simple in-process cache for secrets
_SECRET_CACHE: dict[str, str] = {}

KEY_VAULT_URL = os.getenv("KEY_VAULT_URL", "https://milestonetsl1.vault.azure.net/")


logger = logging.getLogger("meetings-webhook")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def get_secret(name: str, default: str | None = None) -> str | None:
    """
    Fetch secret with **Key Vault priority**:
      1) In-process cache
      2) Azure Key Vault (tries underscore and hyphenated name)
      3) Environment variables (including legacy aliases)
      4) Fallback default

    Caches successful lookups by the original `name`.
    """
    # 0) Cache (fast path)
    if name in _SECRET_CACHE:
        return _SECRET_CACHE[name]

    # 1) Azure Key Vault first (if SDK and vault URL are available)
    if KEY_VAULT_URL and SecretClient and DefaultAzureCredential:
        lookup_names = [name]
        if "_" in name:
            lookup_names.append(name.replace("_", "-"))
        try:
            cred = DefaultAzureCredential()
            client = SecretClient(vault_url=KEY_VAULT_URL, credential=cred)
            for _nm in lookup_names:
                try:
                    secret = client.get_secret(_nm)
                    val = getattr(secret, "value", None)
                    if isinstance(val, str) and val:
                        _SECRET_CACHE[name] = val
                        return val
                except Exception:
                    # Try next candidate name
                    continue
        except Exception as e:
            logging.warning("Secrets: Key Vault lookup failed for '%s': %s", name, e)

    # 2) Environment variables (fallback after KV)
    env_val = os.getenv(name)
    if env_val:
        _SECRET_CACHE[name] = env_val
        return env_val

    # Back-compat aliases if caller asked for the canonical KV key but env has legacy names
    if name == "MongoDb-Connection-String":
        legacy = os.getenv("MONGO_CONN") or os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
        if legacy:
            _SECRET_CACHE[name] = legacy
            return legacy

    # 3) Final fallback
    return default


mongo_uri = get_secret("MongoDb-Connection-String")
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)


def _log_mongo_target(uri: str) -> None:
    try:
        u = urlparse(uri)
        host = u.hostname or ""
        masked_host = (host.split(".")[0] + ".***") if host else ""
        appname = ""
        if u.query:
            for part in u.query.split("&"):
                if part.lower().startswith("appname="):
                    appname = part.split("=", 1)[1]
                    break
        logger.info("Mongo target host=%s appName=%s scheme=%s", masked_host, appname, u.scheme)
    except Exception:
        pass


db = client["iwell"]
coll = db["Investor_Meetings_Data"]
try:
    coll.create_index("ID", unique=True)
except Exception:
    pass


REQUIRED = ["ID", "Location", "Type", "owner", "investor", "Date"]

# Accept common alternate casings / names from CRMs
KEY_ALIASES = {
    "id": "ID",
    "Id": "ID",
    "Investor": "investor",
    "Owner": "owner",
    "type": "Type",
    "location": "Location",
    "Investors": "investor",
    "Owners": "owner",
    "date": "Date",
    "Time": "Date",
    "from": "Date",
    "From": "Date",
}


def _compute_source(meeting_id: Any) -> str:
    s = str(meeting_id or "").strip().upper()
    if s.startswith("MPF"):
        return "Investment Lead"
    if s.startswith("MPR"):
        return "Portfolio Review"
    return "Unknown"


def _normalize_keys(d: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    # Bring through direct matches first
    for k in REQUIRED:
        if k in d and d[k] is not None:
            out[k] = d[k]
    # Then map aliases
    for src, dest in KEY_ALIASES.items():
        if dest not in out and src in d and d[src] is not None:
            out[dest] = d[src]
    return out


def _payload_from_request(req: func.HttpRequest) -> Optional[Dict[str, Any]]:
    """
    Consolidate data from Query Params, Form Body, and JSON Body.
    Priority: JSON > Form > Params (but ignore empty/null values in higher priority sources).
    """

    # 1. Start with Query Params
    combined = {}
    if req.params:
        combined.update(_normalize_keys(req.params))

    # Helper to merge if value is non-empty
    def _merge_non_empty(source_data):
        norm = _normalize_keys(source_data)
        for k, v in norm.items():
            # If value is truthy, or at least not an empty string if existing is present
            if v:
                combined[k] = v
            elif k not in combined:
                 # If we don't have it yet, take the empty value (it might be all we have)
                combined[k] = v

    # 2. Form Data
    try:
        if getattr(req, "form", None):
             _merge_non_empty(req.form)
        else:
             # Fallback parsing for x-www-form-urlencoded if req.form is empty/missing
             raw = req.get_body()
             if raw and not req.headers.get("content-type", "").lower().startswith("application/json"):
                 text = raw.decode("utf-8", errors="ignore")
                 parsed = {
                     k: (v[0] if isinstance(v, list) and v else v)
                     for k, v in parse_qs(text, keep_blank_values=True).items()
                 }
                 if parsed:
                     _merge_non_empty(parsed)
    except Exception:
        pass

    # 3. JSON Body
    try:
        # Check standard JSON
        if req.headers.get("content-type", "").lower().startswith("application/json"):
            try:
                data = req.get_json()
                if isinstance(data, dict):
                    _merge_non_empty(data)
            except ValueError:
                pass
        else:
             # Last ditch: try parsing body as JSON regardless of header
             raw = req.get_body()
             if raw:
                 try:
                     data = json.loads(raw)
                     if isinstance(data, dict):
                         _merge_non_empty(data)
                 except ValueError:
                     pass
    except Exception:
        pass

    return combined if combined else None


def _validate(doc: Optional[Mapping[str, Any]]) -> Tuple[bool, Optional[str]]:
    if not doc:
        return False, "Empty or invalid payload."

    def _is_missing(v: Any) -> bool:
        if v is None:
            return True
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return True
            if s.startswith("${") and s.endswith("}"):  # Deluge-style unresolved placeholder
                return True
        return False

    missing = [k for k in REQUIRED if _is_missing(doc.get(k))]
    if missing:
        return False, f"Missing required field(s): {', '.join(missing)}"
    # enforce strings (coerce non-strings)
    bad_types = [k for k in REQUIRED if not isinstance(doc.get(k), str)]
    if bad_types:
        return False, f"Field(s) must be string: {', '.join(bad_types)}"
    return True, None


def main(req: func.HttpRequest) -> func.HttpResponse:
    # CORS/preflight (harmless for server→server)
    if req.method == "OPTIONS":
        return func.HttpResponse(
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            },
        )

    if req.method not in ("GET", "POST"):
        return func.HttpResponse("Only GET/POST supported.", status_code=405)

    try:
        safe_headers = {
            k: v
            for k, v in req.headers.items()
            if k.lower() in ("content-type", "user-agent", "host")
        }
        logger.info(
            "Incoming %s %s | params=%s | headers=%s",
            req.method,
            req.url,
            dict(req.params or {}),
            safe_headers,
        )
    except Exception:
        pass

    # Utility modes for connectivity/debug
    mode = (req.params.get("mode") or "").lower() if req.params else ""
    if mode == "ping":
        try:
            pong = client.admin.command("ping")
            return func.HttpResponse(
                json.dumps({"ok": True, "mongo_ping": pong}),
                mimetype="application/json",
                status_code=200,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"ok": False, "error": f"mongo ping failed: {e}"}),
                mimetype="application/json",
                status_code=500,
                headers={"Access-Control-Allow-Origin": "*"},
            )
    if mode == "get":
        mid = req.params.get("ID") if req.params else None
        if not mid:
            return func.HttpResponse(
                json.dumps({"ok": False, "error": "ID is required for mode=get"}),
                mimetype="application/json",
                status_code=400,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        try:
            doc_found = coll.find_one({"ID": mid}, {"_id": 0})
            return func.HttpResponse(
                json.dumps({"ok": True, "doc": doc_found}),
                mimetype="application/json",
                status_code=200,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"ok": False, "error": f"mongo find failed: {e}"}),
                mimetype="application/json",
                status_code=500,
                headers={"Access-Control-Allow-Origin": "*"},
            )

    data = _payload_from_request(req)
    ok, err = _validate(data)
    if not ok:
        debug_payload = {
            "error": err,
            "received": {
                "params": dict(req.params or {}),
                "note": "Include ?ID=...&Location=...&Type=...&owner=...&investor=... for GET; or send JSON for POST.",
            },
        }
        try:
            # Attempt to include a small raw body preview (first 200 bytes) for debugging
            raw = req.get_body()
            if raw:
                debug_payload["received"]["raw_body_snippet"] = raw[:200].decode(errors="ignore")
        except Exception:
            pass
        return func.HttpResponse(
            json.dumps(debug_payload),
            status_code=400,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"},
        )

    assert data is not None  # for type checkers; guarded by validation above

    data_map = cast(Mapping[str, Any], data)
    # Strip quotes explicitly if Zoho sends them wrapped in extra quotes
    doc = {
        k: str(data_map.get(k)).strip('"').strip("'")
        for k in REQUIRED
    }

    # Derive Period (YYYY-MM) from Date
    # Assumes Date is in YYYY-MM-DD format or similar ISO start
    date_val = doc.get("Date", "").strip()
    if len(date_val) >= 7:
        doc["Period"] = date_val[:7]
    else:
        doc["Period"] = ""

    # Derive 'source' from ID prefix
    doc["source"] = _compute_source(doc.get("ID"))
    doc["created_at"] = datetime.now(timezone.utc).isoformat()

    try:
        result = coll.insert_one(doc)
    except errors.DuplicateKeyError:
        return func.HttpResponse("A document with this ID already exists.", status_code=409)
    except Exception as e:
        return func.HttpResponse(f"Insertion error: {e}", status_code=500)

    return func.HttpResponse(
        json.dumps({"ok": True, "inserted_id": str(result.inserted_id)}),
        status_code=201,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )
