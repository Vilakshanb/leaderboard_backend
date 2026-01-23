# Host Contract Specification

## 7. Host Integration Contract

This document defines the required interfaces between the host application and the multi-tenant module.

---

## A. JWT Token Claims

The host MUST issue JWT tokens with the following claims:

### Required Claims

| Claim | Type | Description | Example |
|-------|------|-------------|---------|
| `iss` | string | Token issuer (host authority) | `"https://accounts.zoho.com"` |
| `aud` | string | Audience (module identifier) | `"leaderboard-module"` |
| `sub` | string | Subject (unique user ID from host) | `"user_123456"` |
| `email` | string | User email address | `"john@example.com"` |
| `tenant_id` | string (UUID) | Tenant identifier (MUST match module DB) | `"550e8400-e29b-41d4-a716-446655440000"` |
| `exp` | number | Expiration timestamp (Unix epoch) | `1703737675` |
| `iat` | number | Issued at timestamp (Unix epoch) | `1703734075` |

### Optional Claims (Recommended)

| Claim | Type | Description | Example |
|-------|------|-------------|---------|
| `amr` | string[] | Authentication methods reference (for step-up) | `["mfa", "pwd"]` |
| `acr` | string | Authentication context class reference | `"urn:example:policy:strong"` |
| `jti` | string | JWT ID (for replay prevention) | `"jwt_abc123xyz"` |
| `groups` | string[] | User groups (for platform super admin) | `["Leaderboard_SuperAdmins"]` |
| `platform_super_admin` | boolean | Platform super admin flag (production-ready) | `true` |

### Example Token Payload

```json
{
  "iss": "https://accounts.zoho.com",
  "aud": "leaderboard-module",
  "sub": "user_123456",
  "email": "john@example.com",
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "exp": 1703737675,
  "iat": 1703734075,
  "amr": ["mfa", "pwd"],
  "acr": "2",
  "jti": "jwt_abc123xyz"
}
```

### Token Signature

- **Algorithm**: `RS256` (RSA with SHA-256)
- **Key Distribution**: Host MUST expose JWKS endpoint at `{iss}/.well-known/jwks.json`
- **Key Rotation**: Module caches JWKS for 1 hour, supports key rotation via `kid` header

### Token Lifetime

- **Recommended**: 15-60 minutes
- **Maximum**: 4 hours (module rejects longer-lived tokens for security)

---

## B. RBAC API Endpoint

The host MUST implement an endpoint for the module to fetch effective permissions.

### Endpoint

```
GET {HOST_RBAC_API_URL}/rbac/effective
```

### Authentication

- **Method**: Bearer token (API key)
- **Header**: `Authorization: Bearer <HOST_RBAC_API_KEY>`
- Module stores API key in Azure Key Vault

### Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tenant_id` | string (UUID) | Yes | Tenant identifier |
| `user_id` | string | Yes | User identifier (from JWT `sub`) |

### Example Request

```http
GET /rbac/effective?tenant_id=550e8400-e29b-41d4-a716-446655440000&user_id=user_123456
Authorization: Bearer sk_rbac_xyz...
```

### Response Schema

```json
{
  "permissions": [
    "LEADS_READ",
    "LEADS_WRITE",
    "EXPORTS_EXECUTE"
  ],
  "roles": [
    "Sales Manager"
  ],
  "assurance": {
    "level": "high",
    "mfa": true
  },
  "ttl_seconds": 180,
  "version": "v1"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `permissions` | string[] | Yes | List of permission strings |
| `roles` | string[] | No | User's roles (informational, not used for authz) |
| `assurance` | object | No | Authentication assurance info |
| `assurance.level` | string | No | `"low"`, `"medium"`, `"high"` |
| `assurance.mfa` | boolean | No | Whether user authenticated with MFA |
| `ttl_seconds` | number | No | Cache TTL (default 180, range 60-300) |
| `version` | string | No | API version for compatibility |

### Error Responses

#### User Not Found
```json
{
  "error": "USER_NOT_FOUND",
  "message": "User does not exist in tenant"
}
```
**Status**: `404 Not Found`

#### Tenant Not Found
```json
{
  "error": "TENANT_NOT_FOUND",
  "message": "Tenant does not exist"
}
```
**Status**: `404 Not Found`

#### Rate Limit Exceeded
```json
{
  "error": "RATE_LIMIT_EXCEEDED",
  "message": "Too many RBAC requests",
  "retry_after": 60
}
```
**Status**: `429 Too Many Requests`

### Performance SLA

- **P95 Latency**: <100ms
- **Availability**: 99.9%
- **Rate Limits**: 100 requests/sec per module

---

## C. Webhook: RBAC Cache Purge

The host SHOULD send webhooks when user permissions change to invalidate module's cache.

### Endpoint (Module Implements)

```
POST {MODULE_URL}/webhooks/rbac-changed
```

### Authentication

- **Method**: HMAC-SHA256 signature
- **Header**: `X-Webhook-Signature: sha256=<hex_digest>`

### Signature Calculation

```python
import hmac
import hashlib

secret = "shared_webhook_secret"  # from Key Vault
body = request.body  # raw bytes
expected_signature = hmac.new(
    secret.encode(),
    body,
    hashlib.sha256
).hexdigest()

header_value = f"sha256={expected_signature}"
```

### Request Payload

```json
{
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user_123456",
  "timestamp": "2024-12-28T01:46:55Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant_id` | string (UUID) | Yes | Tenant identifier |
| `user_id` | string | No | User ID (if null, purge all users for tenant) |
| `timestamp` | string (ISO 8601) | Yes | Event timestamp |

### Response

```json
{
  "purged": true,
  "cache_keys_deleted": 1
}
```
**Status**: `200 OK`

### Error Responses

#### Invalid Signature
```json
{
  "error": "INVALID_SIGNATURE",
  "message": "Webhook signature verification failed"
}
```
**Status**: `401 Unauthorized`

---

## D. Email Gateway

The host MUST implement an email sending endpoint for the module to delegate email delivery.

### Endpoint

```
POST {HOST_EMAIL_API_URL}/email/send
```

### Authentication

- **Method**: Bearer token (API key)
- **Header**: `Authorization: Bearer <HOST_EMAIL_API_KEY>`

### Request Payload

```json
{
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "template": "lead_assigned",
  "to": "recipient@example.com",
  "cc": [],
  "bcc": [],
  "vars": {
    "lead_name": "Acme Corp",
    "assigned_by": "John Doe",
    "action_url": "https://app.example.com/leads/123"
  },
  "category": "transactional",
  "idempotency_key": "lead-123-assign-20241228-123456"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant_id` | string (UUID) | Yes | Tenant identifier (for SMTP selection) |
| `template` | string | Yes | Email template identifier |
| `to` | string | Yes | Recipient email address |
| `cc` | string[] | No | CC recipients |
| `bcc` | string[] | No | BCC recipients |
| `vars` | object | Yes | Template variable substitutions |
| `category` | string | No | `"transactional"` or `"marketing"` |
| `idempotency_key` | string | Yes | Unique key for deduplication |

### Response

```json
{
  "message_id": "msg_abc123xyz",
  "status": "queued",
  "provider": "zeptomail"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `message_id` | string | Unique message identifier (for tracking) |
| `status` | string | `"queued"`, `"sent"`, `"failed"` |
| `provider` | string | Email provider used (`"zeptomail"`, `"tenant_smtp"`, etc.) |

**Status**: `200 OK`

### Error Responses

#### Invalid Template
```json
{
  "error": "TEMPLATE_NOT_FOUND",
  "message": "Email template 'lead_assigned' does not exist"
}
```
**Status**: `404 Not Found`

#### Duplicate Request
```json
{
  "error": "DUPLICATE_REQUEST",
  "message": "Idempotency key already processed",
  "original_message_id": "msg_abc123xyz"
}
```
**Status**: `409 Conflict`

#### Rate Limit
```json
{
  "error": "RATE_LIMIT_EXCEEDED",
  "message": "Email sending rate limit exceeded for tenant",
  "retry_after": 300
}
```
**Status**: `429 Too Many Requests`

### Idempotency

- Host MUST deduplicate requests using `idempotency_key`
- If duplicate detected within 24 hours, return original `message_id` with `409 Conflict`
- Module retries failed requests with same `idempotency_key`

### Template Management

- Host owns all email templates (HTML, subject, from address)
- Module only provides `template` identifier and `vars`
- Template variables sanitized by host to prevent injection

---

## E. Module Configuration (Shared Secrets)

The following secrets MUST be shared between host and module:

| Secret | Storage Location | Purpose | Rotation Interval |
|--------|------------------|---------|-------------------|
| `HOST_JWKS_URI` | Key Vault | JWT signature verification | N/A (URL) |
| `JWT_ISSUER` | Key Vault | Expected `iss` claim | N/A (config) |
| `JWT_AUDIENCE` | Key Vault | Expected `aud` claim | N/A (config) |
| `HOST_RBAC_API_URL` | Key Vault | RBAC endpoint base URL | N/A (URL) |
| `HOST_RBAC_API_KEY` | Key Vault | RBAC API authentication | 90 days |
| `HOST_EMAIL_API_URL` | Key Vault | Email gateway base URL | N/A (URL) |
| `HOST_EMAIL_API_KEY` | Key Vault | Email API authentication | 90 days |
| `WEBHOOK_HMAC_SECRET` | Key Vault | Webhook signature verification | 90 days |

---

## F. Host Responsibilities

1. **Authentication UI**: Login, MFA, password reset
2. **RBAC Management**: Define roles, assign permissions to users
3. **Email Delivery**: Template rendering, SMTP/provider selection, delivery tracking
4. **Webhook Delivery**: Notify module of permission changes
5. **Token Issuance**: Issue short-lived JWTs with required claims
6. **API SLAs**: Maintain 99.9% uptime for RBAC and Email APIs

---

## G. Module Responsibilities

1. **Token Validation**: Verify JWT signature and claims
2. **Tenant Isolation**: Enforce tenant scoping on all queries
3. **Permission Enforcement**: Check RBAC before privileged actions
4. **Audit Logging**: Maintain tamper-evident audit trail
5. **Email Requests**: Delegate all email sending to host
6. **Webhook Handling**: Accept and verify RBAC cache purge webhooks

---

## H. Integration Checklist

### Host Setup
- [ ] Implement RBAC API endpoint (`/rbac/effective`)
- [ ] Implement Email Gateway endpoint (`/email/send`)
- [ ] Configure JWT signing with RS256
- [ ] Expose JWKS endpoint (`.well-known/jwks.json`)
- [ ] Generate and share API keys (RBAC, Email)
- [ ] Generate and share webhook HMAC secret
- [ ] Configure webhook to send RBAC change events
- [ ] Define email templates (lead_assigned, etc.)

### Module Setup
- [ ] Store host secrets in Azure Key Vault
- [ ] Implement JWT validation with JWKS
- [ ] Implement RBAC fetch with caching (Redis)
- [ ] Implement webhook endpoint with HMAC verification
- [ ] Implement email sending via host gateway
- [ ] Configure tenant data in `tenants` table
- [ ] Seed `module_permissions` allowlist

### Testing
- [ ] Validate JWT with valid/invalid signatures
- [ ] Test RBAC fetch and caching
- [ ] Test RBAC cache purge webhook
- [ ] Test email sending with idempotency
- [ ] Test permission enforcement (read, write, delete)
- [ ] Test step-up requirement for sensitive actions
- [ ] Test platform super admin (staging override)
- [ ] Test audit log hash chain integrity

