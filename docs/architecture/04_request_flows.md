# Request Flows

## 4. Request Flow Scenarios

### Flow A: Standard Request with Host Token

**Scenario**: User requests `GET /api/leads?status=new`

```
┌─────────┐                                                        ┌──────────────┐
│ Browser │                                                        │ Azure Func   │
└────┬────┘                                                        └──────┬───────┘
     │                                                                      │
     │ GET /api/leads?status=new                                          │
     │ Authorization: Bearer <JWT>                                         │
     ├────────────────────────────────────────────────────────────────────>│
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 1. VALIDATE JWT                      │     │
     │                        │  - Fetch JWKS from cache/Key Vault   │     │
     │                        │  - Verify signature                  │     │
     │                        │  - Check iss, aud, exp               │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 2. RESOLVE TENANT                    │     │
     │                        │  - Extract tenant_id from token      │     │
     │                        │  - Lookup tenant in DB (active?)     │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 3. GET EFFECTIVE RBAC                │     │
     │                        │  - Check Redis: rbac:{tid}:{uid}     │     │
     │                        │  - If MISS → call host              │     │
     │<───────────────────────┤      GET /rbac/effective?            │     │
     │                        │        tenant_id={tid}&user_id={sub} │     │
     │ {permissions:          ├───────────────────────────────>│          │
     │  [LEADS_READ,...],     │                                │          │
     │  ttl_seconds: 180}     │<───────────────────────────────┘          │
     │                        │  - Filter by allowlist               │     │
     │                        │  - Cache with TTL                   │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 4. AUTHORIZE                         │     │
     │                        │  - Check permissions.includes        │     │
     │                        │    ('LEADS_READ')                   │     │
     │                        │  - If denied → 403 + audit          │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 5. EXECUTE QUERY                     │     │
     │                        │  - SELECT * FROM leads               │     │
     │                        │    WHERE tenant_id = $1              │     │
     │                        │      AND status = $2                 │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │                        ┌─────────────────────────────────────┐     │
     │                        │ 6. AUDIT LOG                         │     │
     │                        │  - event_type: LEADS_QUERIED         │     │
     │                        │  - result: success                   │     │
     │                        │  - hash chain updated                │     │
     │                        └─────────────────────────────────────┘     │
     │                                                                      │
     │ 200 OK                                                              │
     │ {data: [...leads...]}                                              │
     │<────────────────────────────────────────────────────────────────────┤
```

**Steps**:
1. **JWT Validation**: Fetch JWKS URL from Key Vault, verify signature, claims (iss, aud, exp)
2. **Tenant Resolution**: Parse `tenant_id` from token claims; lookup in `tenants` table (status=active)
3. **RBAC Fetch**:
   - Check Redis key `rbac:{tenant_id}:{user_id}`
   - If MISS → Call `GET {HOST_RBAC_API}/rbac/effective?tenant_id={tid}&user_id={sub}`
   - Filter returned permissions against `module_permissions` allowlist
   - Cache with `SETEX rbac:{tid}:{uid} <ttl> <permissions_json>`
4. **Authorization**: Verify `permissions.includes('LEADS_READ')`; if missing → 403 + audit
5. **DB Query**: Execute `SELECT * FROM leads WHERE tenant_id = $1 AND status = $2`
6. **Audit**: Append event `{LEADS_QUERIED, success, tid, uid, resource: leads, ip, timestamp}`
7. **Response**: Return 200 with data

---

### Flow B: RBAC Cache Purge Webhook

**Scenario**: Host notifies module that user's permissions changed

```
┌──────────────┐                                    ┌────────────────┐
│ Host System  │                                    │ Azure Function │
└──────┬───────┘                                    └────────┬───────┘
       │                                                     │
       │ POST /webhooks/rbac-changed                        │
       │ X-Webhook-Signature: sha256=<HMAC>                 │
       │ {tenant_id: "uuid", user_id: "sub123"}            │
       ├─────────────────────────────────────────────────>  │
       │                                                     │
       │                   ┌──────────────────────────────┐ │
       │                   │ 1. VERIFY WEBHOOK SIGNATURE  │ │
       │                   │  - Compute HMAC-SHA256       │ │
       │                   │    (secret from Key Vault)   │ │
       │                   │  - Compare with header       │ │
       │                   │  - If mismatch → 401         │ │
       │                   └──────────────────────────────┘ │
       │                                                     │
       │                   ┌──────────────────────────────┐ │
       │                   │ 2. PURGE CACHE               │ │
       │                   │  - DEL rbac:{tid}:{uid}      │ │
       │                   └──────────────────────────────┘ │
       │                                                     │
       │                   ┌──────────────────────────────┐ │
       │                   │ 3. AUDIT EVENT               │ │
       │                   │  - RBAC_CACHE_PURGED         │ │
       │                   │  - tenant_id, user_id        │ │
       │                   └──────────────────────────────┘ │
       │                                                     │
       │ 200 OK {purged: true}                              │
       │ <───────────────────────────────────────────────── │
```

**Steps**:
1. **Webhook Signature Verification**:
   ```python
   secret = get_secret("WEBHOOK_HMAC_SECRET")  # from Key Vault
   expected = hmac.new(secret, body_bytes, sha256).hexdigest()
   received = request.headers["X-Webhook-Signature"].replace("sha256=", "")
   if not hmac.compare_digest(expected, received):
       return 401
   ```
2. **Parse Payload**: Extract `tenant_id`, `user_id` (optional: all users if `user_id` is null)
3. **Cache Purge**: `redis.delete(f"rbac:{tenant_id}:{user_id}")`
4. **Audit**: Log `RBAC_CACHE_PURGED` event with tenant/user context
5. **Response**: `200 OK {"purged": true}`

---

### Flow C: Email Send via Host Gateway

**Scenario**: Module needs to send "Lead Assignment" email

```
┌──────────────┐                                   ┌─────────────────┐
│ Azure Func   │                                   │ Host Email GW   │
└──────┬───────┘                                   └────────┬────────┘
       │                                                     │
       │ POST /email/send                                   │
       │ Authorization: Bearer <API_KEY>                    │
       │ {                                                  │
       │   tenant_id: "uuid",                              │
       │   template: "lead_assigned",                      │
       │   to: "rm@example.com",                           │
       │   vars: {lead_name, assigned_by},                 │
       │   category: "transactional",                      │
       │   idempotency_key: "lead-123-assign-20241228"    │
       │ }                                                  │
       ├──────────────────────────────────────────────────> │
       │                                                     │
       │                        ┌─────────────────────────┐ │
       │                        │ Host selects provider   │ │
       │                        │ - Check tenant SMTP     │ │
       │                        │ - Fallback: ZeptoMail   │ │
       │                        │ - Send email             │ │
       │                        └─────────────────────────┘ │
       │                                                     │
       │ 200 OK                                             │
       │ {message_id: "msg_abc123", status: "queued"}      │
       │ <──────────────────────────────────────────────── │
       │                                                     │
       │ ┌────────────────────────────────────────┐        │
       │ │ AUDIT EVENT                             │        │
       │ │ - EMAIL_SEND_REQUESTED                 │        │
       │ │ - message_id: msg_abc123               │        │
       │ │ - to: rm@example.com (hashed)          │        │
       │ │ - template: lead_assigned              │        │
       │ └────────────────────────────────────────┘        │
```

**Steps**:
1. **Trigger**: Business logic determines email needed (e.g., lead assigned)
2. **Build Payload**:
   ```json
   {
     "tenant_id": "<from-token>",
     "template": "lead_assigned",
     "to": "recipient@example.com",
     "vars": {"lead_name": "Acme Corp", "assigned_by": "John"},
     "category": "transactional",
     "idempotency_key": "unique-per-action"
   }
   ```
3. **Call Host Gateway**: `POST {HOST_EMAIL_API}/email/send` with API key from Key Vault
4. **Handle Response**:
   - Success (200): Extract `message_id`, log audit event
   - Failure (4xx/5xx): Log error audit event, optional retry via outbox
5. **Audit**: `EMAIL_SEND_REQUESTED, message_id, template, to (hashed), result`

**Idempotency**: Host should dedupe using `idempotency_key` (prevent duplicate sends on retries)

---

### Flow D: Step-Up Required

**Scenario**: User attempts sensitive action without strong authentication

```
┌─────────┐                                              ┌────────────┐
│ Browser │                                              │Azure Func  │
└────┬────┘                                              └─────┬──────┘
     │                                                          │
     │ DELETE /api/leads/123                                   │
     │ Authorization: Bearer <JWT>                             │
     ├─────────────────────────────────────────────────────────>│
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 1. JWT VALID, RBAC FETCHED           │   │
     │              │  - permissions: [LEADS_DELETE]       │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 2. CHECK STEP-UP REQUIREMENT         │   │
     │              │  - module_permissions: requires_step_up=TRUE │
     │              │  - Check token claim 'amr'           │   │
     │              │    or RBAC response 'assurance.mfa'  │   │
     │              │  - Result: NOT STRONG                │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 3. DENY + AUDIT                      │   │
     │              │  - event: STEP_UP_REQUIRED           │   │
     │              │  - result: denied                    │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │ 403 Forbidden                                           │
     │ {                                                       │
     │   error: "STEP_UP_REQUIRED",                           │
     │   message: "Strong auth required for this action",     │
     │   retry_after_mfa: true                                │
     │ }                                                       │
     │<────────────────────────────────────────────────────────┤
     │                                                          │
     │ ┌─────────────────────────────────────────┐            │
     │ │ Frontend: Redirect to host MFA flow      │            │
     │ │ Host re-authenticates user with MFA      │            │
     │ │ Returns new JWT with amr: ["mfa", "pwd"] │            │
     │ └─────────────────────────────────────────┘            │
     │                                                          │
     │ DELETE /api/leads/123 (retry)                           │
     │ Authorization: Bearer <NEW_JWT_WITH_MFA>                │
     ├─────────────────────────────────────────────────────────>│
     │                                                          │
     │ 200 OK (deletion succeeds)                              │
     │<────────────────────────────────────────────────────────┤
```

**Steps**:
1. **Permission Check**: User has `LEADS_DELETE` permission
2. **Step-Up Check**:
   ```python
   permission = "LEADS_DELETE"
   requires_step_up = db.query("SELECT requires_step_up FROM module_permissions WHERE permission_name = ?", permission)

   if requires_step_up:
       assurance = token.claims.get("amr") or rbac_response.get("assurance", {}).get("level")
       is_strong = "mfa" in assurance or assurance == "high"
       if not is_strong:
           audit("STEP_UP_REQUIRED", result="denied")
           return 403, {"error": "STEP_UP_REQUIRED", "retry_after_mfa": True}
   ```
3. **Frontend Handling**: Redirect to host's MFA challenge flow
4. **Retry**: Submit with new token containing MFA assertion

**Assurance Sources** (priority order):
- Token claim `amr` (authentication methods reference): `["mfa", "pwd"]`
- Token claim `acr` (authentication context reference): `"urn:example:policy:strong"`
- RBAC response `assurance.level`: `"high"` or `assurance.mfa: true`

---

### Flow E: Staging Platform Admin Override

**Scenario**: Staging environment grants super admin to email in env var

```
┌─────────┐                                              ┌────────────┐
│ Browser │                                              │Azure Func  │
└────┬────┘                                              └─────┬──────┘
     │                                                          │
     │ GET /api/audit (admin-only endpoint)                    │
     │ Authorization: Bearer <JWT>                             │
     │ (email: vilakshan@niveshonline.com)                     │
     ├─────────────────────────────────────────────────────────>│
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 1. JWT VALID                         │   │
     │              │  - sub: user123                      │   │
     │              │  - email: vilakshan@niveshonline.com │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 2. PLATFORM SUPER ADMIN CHECK        │   │
     │              │  Priority 1: Token claim check       │   │
     │              │    - platform_super_admin: (none)    │   │
     │              │  Priority 2: Staging env allowlist   │   │
     │              │    - ENV: ENVIRONMENT=staging ✓      │   │
     │              │    - STAGING_PLATFORM_ADMIN_EMAILS:  │   │
     │              │      "vilakshan@niveshonline.com"   │   │
     │              │    - token.email MATCHES ✓           │   │
     │              │  → GRANT SUPER ADMIN                 │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 3. AUDIT (HIGH SEVERITY)             │   │
     │              │  - STAGING_ADMIN_OVERRIDE_USED       │   │
     │              │  - severity: critical                │   │
     │              │  - actor_email: vilakshan@...        │   │
     │              │  - action: AUDIT_READ                │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │              ┌──────────────────────────────────────┐   │
     │              │ 4. EXECUTE ADMIN ACTION              │   │
     │              │  - SELECT * FROM audit_events        │   │
     │              │    (no tenant restriction)           │   │
     │              └──────────────────────────────────────┘   │
     │                                                          │
     │ 200 OK {audit_events: [...]}                            │
     │<────────────────────────────────────────────────────────┤
```

**Steps**:
1. **JWT Validation**: Extract `sub`, `email`, `tenant_id`
2. **Platform Super Admin Check**:
   ```python
   def is_platform_super_admin(token, env_config):
       # Priority 1: Token claim (production-ready)
       if token.claims.get("platform_super_admin") == True:
           return True, "token_claim"

       if token.claims.get("groups") and "Leaderboard_SuperAdmins" in token.claims["groups"]:
           return True, "token_group"

       # Priority 2: Staging env allowlist (ONLY if ENVIRONMENT=staging)
       if env_config["ENVIRONMENT"] == "staging":
           allowed_emails = env_config.get("STAGING_PLATFORM_ADMIN_EMAILS", "").split(",")
           if token.claims.get("email") in allowed_emails:
               return True, "staging_override"

       # Priority 3: Deny
       return False, None
   ```
3. **Audit Critical Event**: If `staging_override` used → emit high-severity audit
4. **Execute**: Perform admin action (bypass tenant scoping if needed)

**Environment Config**:
```bash
# Production
ENVIRONMENT=production
# STAGING_PLATFORM_ADMIN_EMAILS is IGNORED in production

# Staging
ENVIRONMENT=staging
STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com
```

**Security Notes**:
- **NEVER expose override UI**: No checkbox/button to grant super admin
- **NEVER store in DB**: No `is_super_admin` column in database
- **Production safeguard**: `STAGING_PLATFORM_ADMIN_EMAILS` must be ignored unless `ENVIRONMENT=staging`
- **Audit trail**: All super admin actions logged with `severity=critical`

