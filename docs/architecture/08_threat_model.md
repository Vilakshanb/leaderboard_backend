# Threat Model

## 8. Top 12 Security Threats & Mitigations

### Threat 1: Tenant Data Breakout

**Description**: Attacker from Tenant A accesses data from Tenant B

**Attack Vectors**:
- Manipulated `tenant_id` in request body/params
- SQL injection bypassing `WHERE tenant_id = ?`
- Cached RBAC from wrong tenant
- JWT claim tampering (changing `tenant_id` in token)

**Mitigations**:
1. **Never trust frontend**: Derive `tenant_id` ONLY from server-validated JWT claim
2. **Parameterized queries**: Use ORM with automatic `WHERE tenant_id = $1` injection
3. **Row-Level Security**: Optional PostgreSQL RLS policies as defense-in-depth
4. **Cache key scoping**: Include `tenant_id` in Redis keys (`rbac:{tid}:{uid}`)
5. **JWT signature verification**: Prevent token tampering via JWKS validation
6. **Audit all cross-tenant access**: Log when platform super admin accesses multiple tenants

**Residual Risk**: LOW (with all mitigations)

---

### Threat 2: JWT Token Forgery

**Description**: Attacker creates fake JWT to impersonate users

**Attack Vectors**:
- Weak signing algorithm (HS256 with leaked secret)
- Missing signature verification
- Expired/invalid tokens accepted
- JWKS endpoint spoofing (SSRF)

**Mitigations**:
1. **RS256 only**: Require asymmetric signing (host private key, module public key)
2. **JWKS validation**: Fetch public keys from trusted host JWKS endpoint
3. **Claim validation**: Verify `iss`, `aud`, `exp`, `iat`, `tenant_id`
4. **JWKS URL pinning**: Store JWKS URI in Key Vault (prevent SSRF)
5. **Token expiry**: Reject tokens with `exp` > 4 hours from `iat`
6. **JTI tracking** (optional): Store `jti` claim in Redis to prevent replay attacks

**Residual Risk**: LOW (RS256 + JWKS)

---

### Threat 3: Replay Attacks

**Description**: Attacker intercepts valid JWT and reuses it before expiration

**Attack Vectors**:
- Stolen token from network sniffing (if HTTPS compromised)
- Token logged in browser console/storage
- Token leaked in URL params

**Mitigations**:
1. **Short token lifetime**: 15-60 min recommended, 4hr max
2. **HTTPS only**: Enforce TLS 1.2+ for all traffic
3. **JTI deduplication** (optional): Store `jti` in Redis with TTL = token lifetime
4. **IP binding** (optional): Log IP in audit, alert on geo-anomalies
5. **Refresh token flow**: Use short-lived access tokens + long-lived refresh tokens
6. **Logout/revocation**: Host provides token revocation endpoint

**Residual Risk**: MEDIUM (without JTI), LOW (with JTI)

---

### Threat 4: Stale/Cached Permissions Abuse

**Description**: User's permissions revoked in host but module still uses cached RBAC

**Attack Vectors**:
- Host revokes `LEADS_DELETE` but Redis cache still has it (TTL not expired)
- User performs privileged actions during cache TTL window
- Webhook delivery failure (cache never purged)

**Mitigations**:
1. **Short cache TTL**: 60-300s (balance performance vs. freshness)
2. **Host TTL control**: Respect `ttl_seconds` from RBAC response
3. **Webhook purge**: Host sends `POST /webhooks/rbac-changed` on permission changes
4. **Fail-closed**: If RBAC fetch fails, deny all write/sensitive actions
5. **Stale cache rejection**: Don't use cached RBAC older than 10 min for sensitive actions
6. **Audit stale permission use**: Log when cached RBAC is >5 min old

**Residual Risk**: MEDIUM (inherent trade-off between performance and freshness)

---

### Threat 5: Host RBAC API Compromise

**Description**: Attacker gains access to host RBAC API and grants themselves admin permissions

**Attack Vectors**:
- Leaked `HOST_RBAC_API_KEY`
- Host RBAC API vulnerability (SQL injection, authz bypass)
- Man-in-the-middle attack on RBAC API calls

**Mitigations**:
1. **Mutual TLS**: Use client certificates for host API calls (recommended)
2. **API key rotation**: Rotate `HOST_RBAC_API_KEY` every 90 days in Key Vault
3. **IP allowlisting**: Host restricts RBAC API to module's Azure IP range
4. **Host security audits**: Regular penetration testing of host RBAC system
5. **Audit RBAC responses**: Log all permissions fetched from host
6. **Anomaly detection**: Alert if user suddenly gets 10+ new permissions

**Residual Risk**: MEDIUM (depends on host security posture)

---

### Threat 6: Webhook Spoofing (RBAC Purge)

**Description**: Attacker sends fake `POST /webhooks/rbac-changed` to purge legitimate user's cache

**Attack Vectors**:
- Missing HMAC signature verification
- Weak/leaked `WEBHOOK_HMAC_SECRET`
- Replay of valid webhook requests

**Mitigations**:
1. **HMAC-SHA256 signature**: Verify `X-Webhook-Signature` header
2. **Constant-time comparison**: Use `hmac.compare_digest()` to prevent timing attacks
3. **Secret rotation**: Rotate `WEBHOOK_HMAC_SECRET` every 90 days
4. **Timestamp validation**: Reject webhooks older than 5 minutes
5. **IP allowlisting**: Only accept webhooks from host's IP range
6. **Audit all purges**: Log every cache purge with `severity=warning`

**Residual Risk**: LOW (with HMAC + timestamp check)

---

### Threat 7: Email Abuse / Spam

**Description**: Attacker uses module to send spam emails via host gateway

**Attack Vectors**:
- Compromised user account with email-sending permissions
- Missing rate limits on email endpoint
- Template injection (if host doesn't sanitize `vars`)
- Bulk email exports triggering thousands of sends

**Mitigations**:
1. **Host-side rate limiting**: Host enforces max emails per tenant/user/hour
2. **Module rate limiting**: Limit email requests to 100/hour per user
3. **Idempotency keys**: Prevent duplicate sends on retries
4. **Audit all email requests**: Log `to`, `template`, `category` (hash PII)
5. **Template allowlist**: Module only uses pre-approved templates
6. **Host sanitization**: Host sanitizes `vars` to prevent injection
7. **Category enforcement**: `transactional` emails only (no marketing)

**Residual Risk**: LOW (host controls delivery)

---

### Threat 8: Audit Log Tampering

**Description**: Attacker modifies or deletes audit events to hide malicious activity

**Attack Vectors**:
- Direct DB access (compromised credentials)
- SQL injection bypassing append-only constraints
- Breaking hash chain by modifying `prev_event_hash`
- Deleting recent events

**Mitigations**:
1. **Hash chaining**: Each event's `current_event_hash` includes `prev_event_hash`
2. **Integrity verification**: Periodically run `verify_audit_chain()` function
3. **Append-only table**: Revoke `UPDATE`/`DELETE` permissions on `audit_events`
4. **DB credentials**: Store in Key Vault with minimal privileges (INSERT only)
5. **Audit the auditors**: Log all queries to `audit_events` table
6. **Offsite backup**: Stream audit logs to immutable storage (Azure Blob Archive)
7. **Monitor hash breaks**: Alert if `verify_audit_chain()` returns any broken links

**Residual Risk**: LOW (with hash chaining + append-only + backups)

---

### Threat 9: SQL Injection

**Description**: Attacker injects SQL via input fields to bypass tenant scoping or exfiltrate data

**Attack Vectors**:
- Unsanitized query params (e.g., `status`, `assigned_to`)
- String concatenation in SQL queries
- ORM misuse (raw queries)

**Mitigations**:
1. **Parameterized queries**: Use `?` placeholders, never string concatenation
2. **ORM with type safety**: TypeScript/Python ORM with compile-time checks
3. **Input validation**: Whitelist allowed values for enums (e.g., `status`)
4. **No dynamic table/column names**: Never use user input in `SELECT ... FROM ${table}`
5. **Least privilege**: DB user has no `DROP`, `ALTER`, `GRANT` permissions
6. **Web Application Firewall**: Azure Front Door with SQL injection rules

**Residual Risk**: VERY LOW (with parameterized queries)

---

### Threat 10: Server-Side Request Forgery (SSRF)

**Description**: Attacker tricks module into making requests to internal/arbitrary URLs

**Attack Vectors**:
- User-controlled URL in export callbacks
- Fetching JWKS from attacker-controlled URL
- Webhook delivery to internal services

**Mitigations**:
1. **URL allowlisting**: Only fetch JWKS from pre-configured host URL (Key Vault)
2. **No user-controlled URLs**: Never call URLs from request body/params
3. **Network segmentation**: Module has no access to internal Azure services
4. **Disable redirects**: HTTP client should not follow 3xx redirects
5. **Timeout limits**: 5s timeout on all external requests
6. **IP allowlisting**: Only call known host IPs (stored in Key Vault)

**Residual Risk**: VERY LOW (no user-controlled URLs)

---

### Threat 11: Data Exfiltration via Exports

**Description**: Attacker exports large datasets to steal sensitive information

**Attack Vectors**:
- Compromised account with `EXPORTS_EXECUTE` permission
- Missing step-up enforcement (no MFA required)
- No audit trail of exports
- Exports accessible forever (no expiry)

**Mitigations**:
1. **Step-up required**: Enforce MFA for `EXPORTS_EXECUTE` permission
2. **Rate limiting**: Max 5 exports per user per hour
3. **Audit all exports**: Log filter criteria, row count, download IP
4. **Expiry on download URLs**: Signed URLs expire after 1 hour
5. **Watermarking** (optional): Include user email in exported data
6. **DLP scanning**: Host scans exports for PII/sensitive data (optional)
7. **Export approval flow** (optional): Require manager approval for large exports

**Residual Risk**: MEDIUM (authorized users can still export)

---

### Threat 12: Privilege Escalation (Platform Super Admin)

**Description**: Attacker gains platform super admin privileges via misconfig

**Attack Vectors**:
- `STAGING_PLATFORM_ADMIN_EMAILS` applied in production
- UI button to "make me super admin" (insecure implementation)
- DB column `is_super_admin=true` set via SQL injection
- Token claim `platform_super_admin` added by compromised host

**Mitigations**:
1. **No UI for super admin**: Never expose button/checkbox to grant super admin
2. **No DB storage**: Never store super admin status in `users` table
3. **Three-priority check**:
   - Priority 1: Token claim `platform_super_admin` (prod-ready)
   - Priority 2: `STAGING_PLATFORM_ADMIN_EMAILS` (ONLY if `ENVIRONMENT=staging`)
   - Priority 3: DENY
4. **Environment gating**: `if env != "staging": ignore STAGING_PLATFORM_ADMIN_EMAILS`
5. **High-severity audit**: Log all super admin actions with `severity=critical`
6. **Alert on staging override**: Send Slack/email alert when staging override used
7. **Production safeguard**: CI/CD pipeline fails if `STAGING_PLATFORM_ADMIN_EMAILS` set in prod

**Residual Risk**: LOW (with environment gating + auditing)

---

## Summary: Risk Ratings

| Threat | Likelihood | Impact | Residual Risk | Priority |
|--------|-----------|--------|---------------|----------|
| 1. Tenant Breakout | Low | Critical | LOW | P0 |
| 2. JWT Forgery | Low | Critical | LOW | P0 |
| 3. Replay Attacks | Medium | High | MEDIUM | P1 |
| 4. Stale Permissions | Medium | Medium | MEDIUM | P1 |
| 5. Host RBAC Compromise | Low | Critical | MEDIUM | P0 |
| 6. Webhook Spoofing | Low | Medium | LOW | P2 |
| 7. Email Abuse | Medium | Low | LOW | P2 |
| 8. Audit Tampering | Low | High | LOW | P1 |
| 9. SQL Injection | Low | Critical | VERY LOW | P0 |
| 10. SSRF | Low | High | VERY LOW | P1 |
| 11. Data Exfiltration | Medium | High | MEDIUM | P1 |
| 12. Privilege Escalation | Low | Critical | LOW | P0 |

**Priority Legend**:
- **P0**: Must implement before production launch
- **P1**: Implement within first 3 months
- **P2**: Implement within first 6 months

