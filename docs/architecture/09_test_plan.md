# Test Plan

## 9. Testing Strategy

### Test Pyramid

```
         ┌─────────────┐
         │   Manual    │  User Acceptance Testing
         │   Testing   │
         ├─────────────┤
         │    E2E      │  Full flow testing
         │   Tests     │  (Browser + API)
         ├─────────────┤
         │ Integration │  API + DB + Cache + Host
         │   Tests     │
         ├─────────────┤
         │    Unit     │  Middleware functions
         │   Tests     │  (validateJwt, RBAC, etc.)
         └─────────────┘
```

---

## A. Unit Tests (Middleware Functions)

### Test Suite: JWT Validation

**File**: `tests/unit/middleware/validateJwt.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Valid JWT with all claims | Valid RS256 token | Decoded payload | P0 |
| Expired token | `exp` in past | `AuthError: TOKEN_EXPIRED` | P0 |
| Invalid signature | Tampered token | `AuthError: INVALID_TOKEN` | P0 |
| Missing `tenant_id` claim | Token without `tenant_id` | `AuthError: INVALID_TOKEN` | P0 |
| Wrong issuer | `iss` != expected | `AuthError: INVALID_TOKEN` | P0 |
| Wrong audience | `aud` != expected | `AuthError: INVALID_TOKEN` | P0 |
| Missing Authorization header | No header | `AuthError: INVALID_TOKEN` | P0 |
| Malformed JWT | `"not.a.jwt"` | `AuthError: INVALID_TOKEN` | P0 |

**Run Command**:
```bash
npm test -- validateJwt.test.ts
# or
pytest tests/unit/middleware/test_validate_jwt.py
```

---

### Test Suite: Tenant Resolution

**File**: `tests/unit/middleware/resolveTenant.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Valid active tenant | `tenant_id` exists, status=active | Tenant object | P0 |
| Tenant not found | Non-existent `tenant_id` | `AuthError: TENANT_NOT_FOUND` | P0 |
| Suspended tenant | `status=suspended` | `AuthError: TENANT_SUSPENDED` | P0 |
| Archived tenant | `status=archived` | `AuthError: TENANT_SUSPENDED` | P0 |

---

### Test Suite: RBAC Fetch & Caching

**File**: `tests/unit/middleware/getEffectivePermissions.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Cache HIT | Redis has cached permissions | Permissions from cache (no API call) | P0 |
| Cache MISS | Redis empty | API call → permissions cached | P0 |
| Host API success | API returns `{permissions: [...]}` | Filtered by allowlist, cached | P0 |
| Host API timeout | API takes >5s | `AuthError: RBAC_FETCH_FAILED` | P0 |
| Host API 500 error | API returns 500 | `AuthError: RBAC_FETCH_FAILED` | P0 |
| Unknown permissions filtered | API returns `["UNKNOWN_PERM"]` | Empty array (filtered out) | P1 |
| Custom TTL respected | API returns `ttl_seconds: 120` | Cache expires after 120s | P1 |

---

### Test Suite: Permission Enforcement

**File**: `tests/unit/middleware/requirePermission.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Has required permission | `["LEADS_READ"]`, require `LEADS_READ` | No error | P0 |
| Missing required permission | `["LEADS_READ"]`, require `LEADS_DELETE` | `AuthError: PERMISSION_DENIED` | P0 |
| requireAnyPermission - HAS one | `["A", "B"]`, require any of `["B", "C"]` | No error | P1 |
| requireAnyPermission - MISSING all | `["A"]`, require any of `["B", "C"]` | `AuthError: PERMISSION_DENIED` | P1 |

---

### Test Suite: Step-Up Enforcement

**File**: `tests/unit/middleware/requireStepUp.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Permission doesn't require step-up | `LEADS_READ` | No error | P0 |
| Permission requires step-up, has MFA | `LEADS_DELETE`, `amr: ["mfa"]` | No error | P0 |
| Permission requires step-up, NO MFA | `LEADS_DELETE`, `amr: ["pwd"]` | `AuthError: STEP_UP_REQUIRED` | P0 |
| High assurance from RBAC response | `assurance.level: "high"` | No error | P1 |

---

### Test Suite: Platform Super Admin

**File**: `tests/unit/middleware/isPlatformSuperAdmin.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| Token claim = true | `platform_super_admin: true` | `{isSuperAdmin: true, source: "token_claim"}` | P0 |
| Token group match | `groups: ["Leaderboard_SuperAdmins"]` | `{isSuperAdmin: true, source: "token_group"}` | P0 |
| Staging env + allowed email | `ENVIRONMENT=staging`, email in allowlist | `{isSuperAdmin: true, source: "staging_override"}` | P0 |
| Production + allowed email (ignored) | `ENVIRONMENT=production`, email in allowlist | `{isSuperAdmin: false}` | P0 |
| No match | No claim, no group, no env match | `{isSuperAdmin: false}` | P0 |

---

### Test Suite: Audit Hash Chaining

**File**: `tests/unit/audit/auditAppend.test.ts`

| Test Case | Input | Expected Output | Priority |
|-----------|-------|----------------|----------|
| First event (no prev) | Empty audit log | `prev_event_hash: null` | P0 |
| Second event (chain prev) | 1 existing event | `prev_event_hash: <prev_current_hash>` | P0 |
| Hash collision detection | Duplicate event JSON | Different `current_event_hash` (includes timestamp) | P1 |
| Verify chain integrity | 100 events | `verify_audit_chain()` returns no breaks | P0 |

**Run Command**:
```bash
npm test -- audit.test.ts
```

---

## B. Integration Tests (API + DB + Cache)

### Test Suite: Leads API

**File**: `tests/integration/api/leads.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| GET /api/leads (authorized) | Valid JWT with `LEADS_READ` | 200, list of leads for tenant | P0 |
| GET /api/leads (unauthorized) | JWT missing `LEADS_READ` | 403, `PERMISSION_DENIED` | P0 |
| GET /api/leads (wrong tenant) | JWT for tenant A | Only tenant A's leads returned | P0 |
| POST /api/leads (create) | Valid JWT with `LEADS_WRITE` | 201, lead created with correct `tenant_id` | P0 |
| DELETE /api/leads/{id} (no MFA) | JWT without MFA, `LEADS_DELETE` permission | 403, `STEP_UP_REQUIRED` | P0 |
| DELETE /api/leads/{id} (with MFA) | JWT with `amr: ["mfa"]` | 200, lead deleted, audit event logged | P0 |
| GET /api/leads (rate limit) | 101 requests in 1 minute | 429 on request 101 | P1 |

**Setup**:
1. Seed DB with 2 tenants, 10 leads each
2. Mock host RBAC API to return predefined permissions
3. Use in-memory Redis for cache

**Run Command**:
```bash
npm run test:integration -- leads.test.ts
# or
pytest tests/integration/test_leads_api.py
```

---

### Test Suite: Exports API

**File**: `tests/integration/api/exports.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| POST /api/exports/leads (authorized) | Valid JWT with `EXPORTS_EXECUTE` + MFA | 202, export queued | P0 |
| POST /api/exports/leads (no MFA) | JWT without MFA | 403, `STEP_UP_REQUIRED` | P0 |
| POST /api/exports/leads (rate limit) | 6th export in 1 hour | 429, `RATE_LIMIT_EXCEEDED` | P1 |
| Export audit trail | Run export | Audit event with `EXPORT_EXECUTED`, filters, row count | P0 |

---

### Test Suite: Audit API

**File**: `tests/integration/api/audit.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| GET /api/audit (regular user) | JWT with `AUDIT_READ` for tenant A | 200, only tenant A's events | P0 |
| GET /api/audit (super admin) | Platform super admin, query tenant B | 200, tenant B's events | P0 |
| GET /api/audit (unauthorized) | JWT without `AUDIT_READ` | 403, `PERMISSION_DENIED` | P0 |
| Audit hash chain integrity | Fetch 1000 events | `verify_audit_chain()` passes | P0 |

---

## C. Host Integration Tests

### Test Suite: RBAC API Integration

**File**: `tests/integration/host/rbac.test.ts`

**Prerequisites**: Mock host RBAC API server running on localhost:9000

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| Fetch permissions (200 OK) | Host returns `{permissions: [...]}` | Module caches permissions | P0 |
| Host returns 404 | User not found | `AuthError: RBAC_FETCH_FAILED`, audit event | P0 |
| Host returns 500 | Internal server error | `AuthError: RBAC_FETCH_FAILED`, failsafe to deny writes | P0 |
| Host timeout (>5s) | Simulate slow response | `AuthError: RBAC_FETCH_FAILED` | P0 |
| Custom TTL honored | Host returns `ttl_seconds: 60` | Redis cache expires after 60s | P1 |

**Run Command**:
```bash
# Start mock host server
npm run start:mock-host
# Run tests
npm run test:integration -- rbac.test.ts
```

---

### Test Suite: Webhook Integration

**File**: `tests/integration/host/webhook.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| Valid HMAC signature | Correct `X-Webhook-Signature` | 200, cache purged | P0 |
| Invalid HMAC signature | Wrong signature | 401, `INVALID_SIGNATURE` | P0 |
| Missing signature header | No `X-Webhook-Signature` | 401 | P0 |
| Purge single user | `{user_id: "123"}` | Only `rbac:{tid}:123` deleted | P0 |
| Purge all users for tenant | `{user_id: null}` | All `rbac:{tid}:*` deleted | P1 |
| Audit webhook received | Valid webhook | Audit event `RBAC_CACHE_PURGED` | P1 |

---

### Test Suite: Email Gateway Integration

**File**: `tests/integration/host/email.test.ts`

**Prerequisites**: Mock host email API server

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| Send email (success) | Valid request | 200, `message_id` returned, audit logged | P0 |
| Idempotency check | Same `idempotency_key` twice | 2nd request returns same `message_id`, 409 Conflict | P0 |
| Template not found | Invalid `template` | 404, `TEMPLATE_NOT_FOUND` | P1 |
| Host rate limit | Exceed host rate limit | 429, `RATE_LIMIT_EXCEEDED` | P1 |

---

## D. Security Tests

### Test Suite: Tenant Isolation

**File**: `tests/security/tenantIsolation.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| Cross-tenant read attempt | Tenant A JWT, query tenant B lead ID | 404 (lead not found in tenant) | P0 |
| Cross-tenant write attempt | Tenant A JWT, update tenant B lead | 404 or 403 | P0 |
| Manipulated `tenant_id` in body | POST lead with different `tenant_id` in JSON | Ignored, uses `tenant_id` from token | P0 |
| SQL injection attempt | `status='; DROP TABLE leads; --` | No SQL executed, query returns empty | P0 |

---

### Test Suite: JWT Security

**File**: `tests/security/jwtValidation.test.ts`

| Test Case | Description | Expected Result | Priority |
|-----------|-------------|----------------|----------|
| HS256 token (downgrade attack) | Token signed with HS256 | Rejected (RS256 required) | P0 |
| None algorithm attack | `alg: "none"` | Rejected | P0 |
| Expired token | `exp` 1 hour ago | 401, `TOKEN_EXPIRED` | P0 |
| Token from wrong issuer | `iss: "attacker.com"` | 401, `INVALID_TOKEN` | P0 |
| Token for wrong audience | `aud: "other-app"` | 401, `INVALID_TOKEN` | P0 |

---

## E. Performance Tests

### Test Suite: RBAC Caching Performance

**File**: `tests/performance/rbacCaching.test.ts`

| Test Case | Metric | Target | Priority |
|-----------|--------|--------|----------|
| Cache HIT latency | P95 | <10ms | P1 |
| Cache MISS latency | P95 | <200ms (includes host API call) | P1 |
| Concurrent requests (100 users) | Throughput | >500 req/s | P1 |
| Cache eviction rate | % | <5% (most requests hit cache) | P1 |

**Run Command**:
```bash
npm run test:load -- --users 100 --duration 60s
```

---

### Test Suite: Database Query Performance

**File**: `tests/performance/dbQueries.test.ts`

| Test Case | Metric | Target | Priority |
|-----------|--------|--------|----------|
| Leads query (1M rows, 100 tenants) | P95 | <100ms | P1 |
| Audit append (concurrent inserts) | Throughput | >1000 inserts/s | P1 |
| Index effectiveness | Query plan | Uses `idx_leads_tenant` | P0 |

---

## F. End-to-End Tests

### Test Suite: Full User Flow

**File**: `tests/e2e/leadManagement.spec.ts`

**Tool**: Playwright or Cypress

| Test Case | Steps | Expected Result | Priority |
|-----------|-------|----------------|----------|
| Create lead (full flow) | 1. User logs in (host)<br/>2. JWT issued<br/>3. Click "New Lead"<br/>4. Submit form<br/>5. Verify lead in DB | Lead created, audit logged | P1 |
| Delete lead with MFA | 1. Attempt delete<br/>2. Get 403 STEP_UP_REQUIRED<br/>3. Re-auth with MFA<br/>4. Retry delete<br/>5. Verify in DB | Lead deleted after MFA | P1 |
| Export leads | 1. Request export<br/>2. Wait for completion<br/>3. Download CSV<br/>4. Verify content | CSV contains correct leads, audit logged | P1 |

**Run Command**:
```bash
npm run test:e2e
```

---

## G. Manual Testing Checklist

### Platform Super Admin (Staging Override)

**Prerequisites**: Staging environment deployed

- [ ] Set `ENVIRONMENT=staging`
- [ ] Set `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com`
- [ ] Login as `vilakshan@niveshonline.com` (via host)
- [ ] Access `/api/audit` endpoint
- [ ] Verify: 200 OK (super admin access granted)
- [ ] Verify audit log: Event `STAGING_ADMIN_OVERRIDE_USED` with `severity=critical`
- [ ] Change `ENVIRONMENT=production`
- [ ] Retry `/api/audit`
- [ ] Verify: 403 Forbidden (staging override ignored in production)

---

## H. CI/CD Integration

### Test Execution in Pipeline

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: npm install
      - run: npm test

  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
      redis:
        image: redis:7
    steps:
      - uses: actions/checkout@v3
      - run: npm run test:integration

  security-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: npm run test:security
      - run: npm audit --audit-level=moderate
```

---

## I. Test Coverage Goals

| Category | Target Coverage | Priority |
|----------|----------------|----------|
| Middleware functions | 95% | P0 |
| API endpoints | 90% | P0 |
| Business logic | 85% | P1 |
| Error handling | 80% | P1 |

**Measure Command**:
```bash
npm run test:coverage
```

