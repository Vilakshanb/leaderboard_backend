# API Specification

## 5. API Endpoints

### Health & System

#### `GET /health`
**Purpose**: Health check endpoint (no auth required)

**Request**: None

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-12-28T01:46:55Z",
  "version": "1.0.0",
  "environment": "staging"
}
```

**Status Codes**: `200 OK`, `503 Service Unavailable`

---

### Webhooks

#### `POST /webhooks/rbac-changed`
**Purpose**: Host notifies module of RBAC changes (purge cache)

**Authentication**: HMAC signature verification (`X-Webhook-Signature`)

**Request**:
```json
{
  "tenant_id": "uuid",
  "user_id": "sub123",  // optional: null = purge all users for tenant
  "timestamp": "2024-12-28T01:46:55Z"
}
```

**Response**:
```json
{
  "purged": true,
  "cache_keys_deleted": 1
}
```

**Status Codes**:
- `200 OK`: Cache purged
- `401 Unauthorized`: Invalid HMAC signature
- `400 Bad Request`: Invalid payload

**Security**: Verify HMAC-SHA256 signature
```
Expected = HMAC-SHA256(webhook_secret, request_body)
Received = headers["X-Webhook-Signature"].replace("sha256=", "")
if Expected != Received: return 401
```

---

### Leads (Example CRUD)

#### `GET /api/leads`
**Purpose**: List leads for current tenant

**Authentication**: JWT required

**Permissions**: `LEADS_READ`

**Query Parameters**:
- `status` (optional): `new`, `contacted`, `qualified`, `converted`, `lost`
- `assigned_to` (optional): Filter by user_id
- `limit` (default: 50, max: 200)
- `offset` (default: 0)

**Request**: None (query params only)

**Response**:
```json
{
  "data": [
    {
      "lead_id": "uuid",
      "name": "Acme Corp",
      "email": "contact@acme.com",
      "phone": "+1234567890",
      "status": "new",
      "assigned_to": "user123",
      "source": "website",
      "created_at": "2024-12-28T01:00:00Z",
      "updated_at": "2024-12-28T01:00:00Z"
    }
  ],
  "pagination": {
    "limit": 50,
    "offset": 0,
    "total": 120
  }
}
```

**Status Codes**:
- `200 OK`
- `401 Unauthorized`: Invalid/missing JWT
- `403 Forbidden`: Missing `LEADS_READ` permission
- `500 Internal Server Error`

---

#### `POST /api/leads`
**Purpose**: Create new lead

**Authentication**: JWT required

**Permissions**: `LEADS_WRITE`

**Request**:
```json
{
  "name": "Acme Corp",
  "email": "contact@acme.com",
  "phone": "+1234567890",
  "status": "new",
  "source": "website",
  "custom_fields": {
    "industry": "Finance",
    "annual_revenue": 5000000
  }
}
```

**Response**:
```json
{
  "lead_id": "uuid",
  "name": "Acme Corp",
  "tenant_id": "uuid",  // derived from token
  "created_at": "2024-12-28T01:46:55Z",
  "created_by": "user123"
}
```

**Status Codes**:
- `201 Created`
- `400 Bad Request`: Validation error
- `403 Forbidden`: Missing `LEADS_WRITE` permission

---

#### `DELETE /api/leads/{lead_id}`
**Purpose**: Delete lead (sensitive action)

**Authentication**: JWT required

**Permissions**: `LEADS_DELETE` (requires step-up)

**Request**: None

**Response**:
```json
{
  "deleted": true,
  "lead_id": "uuid"
}
```

**Status Codes**:
- `200 OK`
- `403 Forbidden`:
  - Missing `LEADS_DELETE` permission
  - **OR** `STEP_UP_REQUIRED` (user needs MFA)
- `404 Not Found`: Lead not found or belongs to different tenant

**Step-Up Response** (if MFA required):
```json
{
  "error": "STEP_UP_REQUIRED",
  "message": "Strong authentication required for this action",
  "retry_after_mfa": true
}
```

---

### Exports (Sensitive)

#### `POST /api/exports/leads`
**Purpose**: Export leads data (sensitive, requires step-up)

**Authentication**: JWT required

**Permissions**: `EXPORTS_EXECUTE` (requires step-up)

**Request**:
```json
{
  "format": "csv",  // csv, xlsx, json
  "filters": {
    "status": "qualified",
    "created_after": "2024-01-01"
  },
  "columns": ["name", "email", "phone", "status"]
}
```

**Response**:
```json
{
  "export_id": "uuid",
  "status": "processing",
  "download_url": null,  // populated when complete
  "expires_at": null,
  "created_at": "2024-12-28T01:46:55Z"
}
```

**Status Codes**:
- `202 Accepted`: Export queued
- `403 Forbidden`: Missing permission or step-up required
- `400 Bad Request`: Invalid format/filters

**Security Notes**:
- Exports are rate-limited (max 5 per hour per user)
- Download URLs expire after 1 hour
- Audit event logged with data lineage

---

### Audit (Admin Only)

#### `GET /api/audit`
**Purpose**: Query audit log (admin or platform super admin)

**Authentication**: JWT required

**Permissions**: `AUDIT_READ` OR Platform Super Admin

**Query Parameters**:
- `tenant_id` (optional, super admin only): Filter by tenant
- `event_type` (optional): Filter by event type
- `actor_user_id` (optional): Filter by actor
- `start_date`, `end_date` (optional): Date range
- `limit` (default: 100, max: 1000)
- `offset` (default: 0)

**Request**: None

**Response**:
```json
{
  "data": [
    {
      "event_id": 12345,
      "tenant_id": "uuid",
      "event_type": "LEADS_DELETED",
      "severity": "warning",
      "actor_user_id": "user123",
      "actor_email": "john@example.com",
      "resource_type": "lead",
      "resource_id": "lead-uuid",
      "action": "delete",
      "result": "success",
      "ip_address": "203.0.113.45",
      "created_at": "2024-12-28T01:46:55Z",
      "event_payload": {
        "lead_name": "Acme Corp"
      }
    }
  ],
  "pagination": {
    "limit": 100,
    "offset": 0,
    "total": 5432
  }
}
```

**Status Codes**:
- `200 OK`
- `403 Forbidden`: Missing `AUDIT_READ` permission
- `400 Bad Request`: Invalid filters

**Tenant Scoping**:
- Regular users: Can only query own tenant's audit log
- Platform Super Admin: Can query across tenants (if `tenant_id` param provided)

---

### Error Response Format

All error responses follow this schema:
```json
{
  "error": "ERROR_CODE",
  "message": "Human-readable error message",
  "details": {
    "field": "validation error details"
  },
  "request_id": "trace-id-12345"
}
```

**Common Error Codes**:
- `INVALID_TOKEN`: JWT validation failed
- `TOKEN_EXPIRED`: JWT exp claim in past
- `PERMISSION_DENIED`: User lacks required permission
- `STEP_UP_REQUIRED`: Action requires MFA/strong auth
- `TENANT_SUSPENDED`: Tenant account suspended
- `RATE_LIMIT_EXCEEDED`: Too many requests
- `INVALID_REQUEST`: Validation error
- `RESOURCE_NOT_FOUND`: Entity not found or not in tenant
- `RBAC_FETCH_FAILED`: Host RBAC API unreachable (fail-closed)

---

### Rate Limiting

**Per-User Limits** (sliding window):
- Standard endpoints: 100 req/min
- Export endpoints: 5 req/hour
- Webhook endpoints: 1000 req/min (host-to-module)

**Headers**:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 87
X-RateLimit-Reset: 1703737675  // Unix timestamp
```

**Exceeded Response** (`429 Too Many Requests`):
```json
{
  "error": "RATE_LIMIT_EXCEEDED",
  "message": "Rate limit exceeded",
  "retry_after": 45  // seconds
}
```

