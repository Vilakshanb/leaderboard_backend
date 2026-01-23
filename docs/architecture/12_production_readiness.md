# Production Readiness Checklist

**PLI_Leaderboard Multi-Tenant Module**
**Last Updated**: December 2024
**Version**: 2.0 (Multi-Tenant Architecture + Project-Specific)

---

## 🚨 CRITICAL SECURITY ISSUES

### 1. Authentication Configuration (BLOCKER)
- [ ] **CRITICAL**: [Leaderboard_API/function.json](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/Leaderboard_API/function.json) has `"authLevel": "anonymous"`
  - **Risk**: PUBLIC API access without authentication
  - **MUST FIX**:
    - **Option A** (Multi-Tenant): Change to `"authLevel": "anonymous"` + implement JWT validation middleware (validates host-issued tokens)
    - **Option B** (Legacy): Revert to `"authLevel": "function"` (requires function key)
  - **Recommended**: Option A (multi-tenant JWT validation per blueprint)
  - **Current State**: Changed for local dev, BLOCKS production deployment

### 2. Platform Super Admin Configuration
- [ ] **CRITICAL - STAGING**: Verify `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com` set in staging environment
  - **Risk**: Cannot test admin features in staging
  - **Verification**: `az functionapp config appsettings list --name func-leaderboard-staging`

- [ ] **CRITICAL - PRODUCTION**: Verify `STAGING_PLATFORM_ADMIN_EMAILS` NOT SET in production
  - **Risk**: Unauthorized platform admin access
  - **Verification**: `az functionapp config appsettings list --name func-leaderboard-prod`
  - **CI/CD Check**: Pipeline should FAIL if this variable exists in production config

### 3. Host JWT Configuration
- [ ] **NEW - Multi-Tenant**: Host JWKS URI configured in Key Vault
  - Secret: `HOST_JWKS_URI` (e.g., `https://accounts.zoho.com/.well-known/jwks.json`)
  - Verification: Fetch JWKS endpoint returns valid public keys

- [ ] **NEW - Multi-Tenant**: JWT validation parameters configured:
  - `JWT_ISSUER` (expected `iss` claim)
  - `JWT_AUDIENCE` (expected `aud` claim, e.g., `leaderboard-module`)

---

## 🔐 SECURITY & RBAC

### Multi-Tenant Security (NEW)

- [ ] **Tenant Isolation Verified**:
  - [ ] Cross-tenant read test: Tenant A cannot access Tenant B data ✅
  - [ ] Cross-tenant write test: Tenant A cannot modify Tenant B data ✅
  - [ ] `tenant_id` injection test: Request body `tenant_id` ignored, token claim used ✅
  - [ ] SQL injection test: Malicious query params rejected ✅

- [ ] **Host RBAC Integration**:
  - [ ] Host RBAC API endpoint accessible: `GET /rbac/effective?tenant_id={tid}&user_id={uid}`
  - [ ] RBAC response schema validated (permissions array required)
  - [ ] Unknown permissions filtered by module allowlist
  - [ ] RBAC caching working (Redis key: `rbac:{tenant_id}:{user_id}`)
  - [ ] Cache TTL respected (default 180s, or from host `ttl_seconds`)

- [ ] **Webhook Security**:
  - [ ] RBAC purge webhook endpoint: `POST /webhooks/rbac-changed`
  - [ ] HMAC-SHA256 signature verification working
  - [ ] Invalid signature returns 401
  - [ ] Cache purge verified (Redis key deleted)

- [ ] **Email Delegation**:
  - [ ] Host email gateway configured: `POST /email/send`
  - [ ] Idempotency key support verified
  - [ ] Module has NO SMTP credentials stored
  - [ ] Email audit events logged with `message_id`

### Legacy RBAC (Current Implementation)

- [ ] **Hardcoded Admin**: [utils/rbac.py](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/utils/rbac.py) `vilakshan@niveshonline.com`
  - **Status**: ✅ Safe - checks `AZURE_FUNCTIONS_ENVIRONMENT != "Production"`
  - **Migration Path**: Replace with multi-tenant platform super admin logic

- [ ] **Verify RBAC Roles**:
  - [ ] Admin users can access Settings/Scoring pages
  - [ ] Non-admin users blocked from admin pages (403 Forbidden)
  - [ ] Test with 3 user types: Admin, Manager, Regular RM

### Secrets Management

- [ ] **Key Vault Secrets** (Multi-Tenant):
  - [ ] `HOST_JWKS_URI`
  - [ ] `JWT_ISSUER`
  - [ ] `JWT_AUDIENCE`
  - [ ] `HOST_RBAC_API_KEY`
  - [ ] `HOST_EMAIL_API_KEY`
  - [ ] `WEBHOOK_HMAC_SECRET`

- [ ] **Key Vault Secrets** (Existing):
  - [ ] `MongoDb-Connection-String` ✅
  - [ ] Zoho OAuth secrets ✅
  - [ ] Database connection string for PostgreSQL (if migrating)

- [ ] **NO hardcoded secrets**: All use `get_secret()` pattern ✅

### Step-Up Enforcement (NEW)

- [ ] **Sensitive Actions Require MFA**:
  - [ ] `LEADS_DELETE` requires `amr: ["mfa"]` in token
  - [ ] `EXPORTS_EXECUTE` requires step-up
  - [ ] Returns `403 STEP_UP_REQUIRED` if MFA missing
  - [ ] Frontend redirects to host MFA flow

---

## 🔍 DATA INTEGRITY VERIFICATION

### Database Migration (Multi-Tenant)

- [ ] **PostgreSQL Schema Deployed** (if migrating from MongoDB):
  - [ ] `tenants` table created ✅
  - [ ] `audit_events` table with hash chaining ✅
  - [ ] `module_permissions` table seeded ✅
  - [ ] `tenant_id` column added to all domain tables
  - [ ] Indexes on `(tenant_id, ...)` for all queries

- [ ] **Data Backfill** (if migrating):
  - [ ] Existing MongoDB data migrated to PostgreSQL
  - [ ] All rows have valid `tenant_id`
  - [ ] Row count reconciliation: Before = After
  - [ ] `tenant_id` set to NOT NULL after backfill

### MongoDB Database Separation (Current)

- [ ] **Verify NO references to legacy database**:
  - [ ] [Insurance_Scorer/__init__.py](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/Insurance_Scorer/__init__.py) line 363: uses `PLI_DB_NAME` env var ✅
  - [ ] [referral_scorer/__init__.py](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/referral_scorer/__init__.py) line 175: uses `PLI_DB_NAME` env var ✅
  - [ ] All scorers respect `PLI_DB_NAME=PLI_Leaderboard_v2`

### Data Completeness (Apr-Dec 2025)

- [ ] **SIP Data**: `MF_SIP_Leaderboard` has records for all months
- [ ] **Lumpsum Data**: `Leaderboard_Lumpsum` has records for all months
- [ ] **Insurance Data**: `Insurance_Policy_Scoring` has records
- [ ] **Referral Data**: `referralLeaderboard` has records
- [ ] **Public Leaderboard**: Aggregated data for all active RMs
- [ ] **Rupee Incentives**: Calculations verified for all months

### Configuration Integrity

- [ ] **Config Documents in V2**:
  - [ ] `config/Leaderboard_Lumpsum` ✅ (synced)
  - [ ] `config/Leaderboard_SIP_Config` (verify exists)
  - [ ] `config/Leaderboard_Insurance` ✅ (synced, content verified)
  - [ ] `config/Leaderboard_Referral` (verify exists)

### User Data

- [ ] `Zoho_Users` collection: All 24 users present ✅
- [ ] `is_active` status correctly set for all users
- [ ] `Admin_Permissions` collection exists with roles
- [ ] **NEW - Multi-Tenant**: `tenants` table has at least 1 active tenant

---

## 🧪 FUNCTIONAL TESTING

### API Endpoints

**Legacy Endpoints** (MongoDB-based):
- [ ] `GET /api/leaderboard` - Main leaderboard (MTD/YTD views)
- [ ] `GET /api/leaderboard/user/{id}/breakdown` - User breakdown ✅ (works in dev)
- [ ] `GET /api/leaderboard/team-view` - Team aggregation
- [ ] `GET /api/leaderboard/me` - Current user info (`X-User-Email` header)
- [ ] `GET /api/settings/scoring/*` - Config endpoints (RBAC protected)
- [ ] `POST /api/settings/scoring/*` - Config updates (RBAC protected)

**NEW Multi-Tenant Endpoints**:
- [ ] `GET /health` - Health check (no auth)
- [ ] `POST /webhooks/rbac-changed` - RBAC cache purge (HMAC auth)
- [ ] `GET /api/leads` - List leads (tenant-scoped, requires `LEADS_READ`)
- [ ] `POST /api/leads` - Create lead (requires `LEADS_WRITE`)
- [ ] `DELETE /api/leads/{id}` - Delete lead (requires `LEADS_DELETE` + MFA)
- [ ] `POST /api/exports/leads` - Export data (requires `EXPORTS_EXECUTE` + MFA)
- [ ] `GET /api/audit` - Query audit log (admin or super admin)

### Authentication Flows (NEW)

- [ ] **Valid JWT**: Request with valid host token succeeds
- [ ] **Expired JWT**: Returns 401 `TOKEN_EXPIRED`
- [ ] **Invalid Signature**: Returns 401 `INVALID_TOKEN`
- [ ] **Missing tenant_id**: Returns 401 `INVALID_TOKEN`
- [ ] **Suspended Tenant**: Returns 403 `TENANT_SUSPENDED`

### Frontend User Flows (Existing)

- [ ] Landing page loads leaderboard data
- [ ] Breakdown page displays SIP, Lumpsum, Insurance, Referral components
- [ ] Team view shows aggregated metrics
- [ ] Admin users can access Settings pages
- [ ] Non-admin users blocked from admin pages (UI + API)

### Edge Cases

- [ ] Inactive RM data visibility (historical data shown)
- [ ] Missing data handling (null/zero values, no UI crash)
- [ ] Invalid employee_id returns proper error (404, not 500)
- [ ] Future month requests return empty/zero data gracefully
- [ ] **NEW**: RBAC host API timeout → uses cached permissions (read-only) OR fails closed

---

## ⚡ PERFORMANCE & SCALABILITY

### Database Indexes (MongoDB)

- [ ] **Verify indexes on `Public_Leaderboard`**:
  - `period_month`, `employee_id`
  - `period_month`, `team_id`
  - `period_month`, `is_active`
- [ ] Indexes on source collections (SIP, Lumpsum, Insurance, Referral)

### Database Indexes (PostgreSQL - if migrated)

- [ ] `idx_leads_tenant` on `leads(tenant_id)`
- [ ] `idx_leads_tenant_status` on `leads(tenant_id, status)`
- [ ] `idx_audit_tenant_created` on `audit_events(tenant_id, created_at DESC)`
- [ ] All indexes per [03_data_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/03_data_model.md)

### Query Performance

**Legacy (MongoDB)**:
- [ ] Leaderboard query (full dataset): <2s
- [ ] Breakdown query (single user): <1s
- [ ] Team-view aggregation: <3s

**NEW (Multi-Tenant)**:
- [ ] RBAC cache HIT latency: P95 <10ms
- [ ] RBAC cache MISS latency: P95 <200ms (includes host API call)
- [ ] Leads query (1M rows, 100 tenants): P95 <100ms
- [ ] Audit append: P95 <50ms
- [ ] All queries use indexes (verify with `EXPLAIN`)

### Caching Performance (NEW)

- [ ] **Redis Cache Metrics**:
  - Cache hit rate: >95% for RBAC
  - Memory usage: <1GB (staging), <5GB (production)
  - Eviction policy: `allkeys-lru`
  - Connection pooling verified

---

## 📡 OPENTELEMETRY OBSERVABILITY

### OTel Stack Integration

- [ ] **OpenTelemetry SDK Installed**:
  - Python: `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp`
  - Node.js: `@opentelemetry/sdk-node`, `@opentelemetry/auto-instrumentations-node`
  - Verification: Check `requirements.txt` or `package.json`

- [ ] **OTel Setup Module Created**:
  - Python: [otel_setup.py](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/utils/otel_setup.py) (tracer + meter initialization)
  - Node.js: `otel-setup.js` (NodeSDK configuration)
  - Imports in main application file BEFORE other imports

- [ ] **Service Instrumentation**:
  - Resource attributes configured:
    - `service.name` (e.g., `pli-leaderboard-api`)
    - `deployment.environment` (dev/staging/production/onprem)
    - `service.version` (optional but recommended)
  - Auto-instrumentation enabled (Flask/Express/etc.)
  - Custom spans for business logic (scorers, RBAC calls, exports)

### KeyGate API Key Management

- [ ] **API Key Obtained from KeyGate Admin**:
  - Navigate to `https://keygate.dev.mnivesh.com`
  - Create key with appropriate environment (dev/staging/production)
  - Format: `kg_v1.abc123...` (copy immediately, shown once)

- [ ] **API Key Stored Securely**:
  - **Staging**: Stored in Azure Key Vault secret `OTEL_API_KEY`
  - **Production**: Separate API key stored in Key Vault
  - **NEVER** commit to version control
  - Verification: `az keyvault secret show --name OTEL_API_KEY --vault-name kv-leaderboard-prod`

- [ ] **API Key Rotation Schedule**:
  - Keys rotated every 90 days
  - Old key kept active for 7-day grace period during rotation
  - Documented procedure for emergency key revocation

### OTel Environment Variables

- [ ] **Required Variables Set**:
  ```bash
  OTEL_SERVICE_NAME=pli-leaderboard-api
  DEPLOYMENT_ENVIRONMENT=production  # or dev, staging, onprem
  OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.dev.mnivesh.com
  OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
  OTEL_EXPORTER_OTLP_HEADERS=X-API-Key=kg_v1.YOUR_KEY_HERE
  ```
  - Verification: Check Azure Function App Configuration

- [ ] **Environment-Specific Configuration**:
  - Staging: `DEPLOYMENT_ENVIRONMENT=staging`, staging API key
  - Production: `DEPLOYMENT_ENVIRONMENT=production`, production API key
  - Development: `DEPLOYMENT_ENVIRONMENT=dev`, dev API key

### OTel Connection Tests

- [ ] **OTLP Endpoint Reachability**:
  ```bash
  curl -v https://otel.dev.mnivesh.com/healthz
  # Should return: 200 OK
  ```

- [ ] **Authentication Test**:
  ```bash
  curl -H "X-API-Key: kg_v1.YOUR_KEY_HERE" \
       https://otel.dev.mnivesh.com/v1/traces
  # Should return: 200 or 405 (not 401)
  ```

- [ ] **Application Startup Logs**:
  - Check for `OpenTelemetry initialized for pli-leaderboard-api`
  - No `401 Unauthorized` errors
  - No connection timeout errors

### Telemetry Data Validation

- [ ] **Generate Test Traffic**:
  ```bash
  # Hit various endpoints to generate spans
  curl https://YOUR_APP/api/leaderboard
  curl https://YOUR_APP/api/leaderboard/user/123/breakdown
  curl https://YOUR_APP/health
  ```

- [ ] **Verify Data in OpenSearch Dashboards**:
  - Navigate to `https://obs.dev.mnivesh.com`
  - Authenticate via Entra ID SSO
  - Go to **Discover** → Filter by:
    ```
    service.name: "pli-leaderboard-api" AND deployment.environment: "production"
    ```
  - Verify traces appear within 1-2 minutes
  - Check index patterns exist: `logs-otel-*`, `traces-otel-*`, `metrics-otel-*`

- [ ] **Verify Key Spans**:
  - RBAC fetch spans (if multi-tenant)
  - Database query spans (MongoDB or PostgreSQL)
  - HTTP request spans (Express/Flask auto-instrumentation)
  - Scorer execution spans (Insurance, Lumpsum, SIP, Referral)

### Sensitive Data Redaction (OTel Collector)

- [ ] **Auto-Redaction Verified**:
  - `email` attributes → `[REDACTED_EMAIL]`
  - `phone` attributes → `[REDACTED_PHONE]`
  - `authorization` headers → Deleted
  - `cookie` headers → Deleted
  - Test by searching for PII in OpenSearch (should not appear)

- [ ] **Application-Level Sanitization**:
  - No SSN, credit card, or password data in span attributes
  - User input sanitized before adding to spans
  - Error messages scrubbed of sensitive data

### OTel Best Practices Compliance

- [ ] **Span Naming** (low cardinality):
  - ✅ Good: `GET /api/leaderboard`, `database.query.users`, `scorer.insurance.execute`
  - ❌ Bad: `GET /api/users/12345`, `query`, `/api/*`

- [ ] **Sampling Strategy**:
  - OTel Collector configured with 5% probabilistic sampling (default)
  - Critical transactions force-sampled in application:
    ```python
    span.set_attribute("force.sample", True)
    ```
  - Health check endpoints excluded from tracing

- [ ] **Error Recording**:
  - Exceptions recorded in spans with `span.record_exception(e)`
  - Span status set to ERROR on failure
  - Stack traces included (sanitized)

- [ ] **Batch Processing**:
  - BatchSpanProcessor configured (default):
    - `max_export_batch_size=1000`
    - `schedule_delay_millis=10000`
  - Prevents performance impact on application

### OTel Performance Targets

- [ ] **Tracing Overhead**:
  - P95 latency increase: <5% compared to non-instrumented baseline
  - Memory overhead: <50MB per application instance
  - CPU overhead: <2% average

- [ ] **Data Volume**:
  - Traces: ~100KB/day per active user (with 5% sampling)
  - Logs: ~1MB/day per application instance
  - Metrics: ~50KB/day per service
  - Total: <5GB/month for 100 users (staging), <50GB/month (production)

---

## 📊 MONITORING & LOGGING

### Application Insights (Legacy)

- [ ] Application Insights configured
- [ ] Logging for scorer runs (Insurance, Referral)
- [ ] Error logging with stack traces
- [ ] **NEW**: Custom metrics tracked:
  - RBAC cache hit rate
  - RBAC fetch latency
  - Permission denied count
  - Step-up required count
  - Audit events per hour

### Dashboards

- [ ] **Operational Dashboard** (Application Insights):
  - Request rate, error rate, P95 latency
  - Function execution count
  - Database query performance

- [ ] **Security Dashboard** (Application Insights):
  - Failed JWT validations per hour
  - Permission denied events
  - Step-up required events
  - Platform super admin usage (critical alert)
  - RBAC API health (latency, error rate)

- [ ] **OpenSearch Observability Dashboard** (NEW):
  - Navigate to `https://obs.dev.mnivesh.com`
  - Create saved searches for:
    - All production logs: `deployment.environment: "production"`
    - Error logs: `service.name: "pli-leaderboard-api" AND severity: "ERROR"`
    - Slow traces: `span.attributes.duration_ms: > 1000`
  - Create visualizations:
    - Request rate by endpoint (last 24h)
    - Error rate by service (last 7d)
    - P95 latency by endpoint (last 1h)
    - Top 10 slowest database queries
  - Save dashboard as "PLI Leaderboard - Production Monitoring"


### Alerts Configured

**Application Insights Alerts**:

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| API 500 errors | >5 in 10 min | High | Slack + investigate |
| Scorer failures | Any scorer execution error | High | Slack + investigate |
| Database connection issues | Connection pool exhausted OR timeout | Critical | Page on-call |

**Multi-Tenant Security Alerts**:

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| RBAC fetch failures | >1% in 5 min window | Critical | Page on-call |
| High latency | P95 >500ms for 10 min | High | Slack notification |
| JWT validation failures | >10/min | High | Slack notification |
| Audit hash chain break | Any break detected | Critical | Page on-call + email security team |
| Cache hit rate low | <90% for 15 min | Medium | Investigate host RBAC changes |
| Host RBAC API down | 100% errors for 2 min | Critical | Page on-call |
| Staging super admin used | Any usage | Info | Slack notification (expected) |
| Production super admin env var | Detected at startup | Critical | Block deployment |

**OpenTelemetry / OpenSearch Alerts** (NEW):

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| OTel export failures | >5 failed exports in 10 min | High | Investigate KeyGate API key |
| No telemetry data | Zero spans received for 15 min | Critical | Check OTLP connection |
| High trace volume | >10K spans/min (potential DDoS) | Medium | Review sampling rate |
| Scorer execution errors | Any span with status=ERROR in scorer operations | High | Slack + investigate |
| Database slow queries | Query duration >5s | Medium | Optimize query or add index |
| OpenSearch disk space low | <20% free space | Critical | Archive old indices |
| KeyGate API key expiring | <7 days until expiration | High | Rotate API key |

**Alert Delivery Channels**:
- [ ] Slack: `#pli-leaderboard-alerts` channel configured
- [ ] Email: On-call team email group configured
- [ ] PagerDuty: Critical alerts route to on-call rotation

**Verification**: Send test alerts to confirm delivery


---

## 🔐 AUDIT & COMPLIANCE

### Audit Log (NEW - Multi-Tenant)

- [ ] **Hash Chaining Verified**:
  - `verify_audit_chain()` returns no breaks
  - Tamper test: Manual hash modification detected
  - Daily verification scheduled (Azure Function timer trigger at 2 AM)

- [ ] **Append-Only Enforcement**:
  - Database user lacks UPDATE/DELETE on `audit_events`
  - Attempt to modify audit event fails with permission error

- [ ] **Retention Policy**:
  - Staging: 30 days
  - Production: 7 years
  - Archive to Azure Blob Storage (cold tier) after 90 days

- [ ] **Event Coverage**:
  - Login validated, RBAC fetched/failed, permission denied
  - Exports, deletes, settings changes
  - Staging admin override (critical severity)
  - Webhook received, email sent

### GDPR Compliance (NEW)

- [ ] Data subject rights API (user data export)
- [ ] User data deletion procedure documented
- [ ] PII hashed in audit logs (email addresses)
- [ ] Encryption at rest (PostgreSQL, Redis)

---

## 🚀 DEPLOYMENT PREREQUISITES

### Pre-Deployment Checklist (CRITICAL)

**Legacy/Current**:
1. [ ] **BLOCKER**: Fix `authLevel` in [Leaderboard_API/function.json](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/Leaderboard_API/function.json)
2. [ ] Run full data verification script (all collections have data)
3. [ ] Verify all environment variables in Azure portal
4. [ ] Test on staging environment first
5. [ ] Prepare rollback plan (backup MongoDB access)

**NEW - Multi-Tenant**:
6. [ ] Production secrets rotated (NEW API keys, different from staging)
7. [ ] `ENVIRONMENT=production` verified
8. [ ] `STAGING_PLATFORM_ADMIN_EMAILS` NOT SET in production
9. [ ] Host RBAC API production endpoint tested
10. [ ] Host Email Gateway production endpoint tested
11. [ ] Load testing passed (100 users, <300ms P95)
12. [ ] Backup and restore tested
13. [ ] Rollback procedure tested on staging

### Environment Variables

**Existing**:
- [ ] `AZURE_FUNCTIONS_ENVIRONMENT=Production`
- [ ] `PLI_DB_NAME=PLI_Leaderboard_v2`
- [ ] `LEADERBOARD_ADMIN_EMAILS` (comma-separated)
- [ ] `MongoDb-Connection-String` (from Key Vault)

**Multi-Tenant**:
- [ ] `ENVIRONMENT=staging|production`
- [ ] `STAGING_PLATFORM_ADMIN_EMAILS` (ONLY in staging)
- [ ] `APPLICATIONINSIGHTS_CONNECTION_STRING`
- [ ] Database connection string (if PostgreSQL)
- [ ] Redis connection string

**OpenTelemetry** (NEW):
- [ ] `OTEL_SERVICE_NAME=pli-leaderboard-api`
- [ ] `DEPLOYMENT_ENVIRONMENT=production` (or dev/staging/onprem)
- [ ] `OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.dev.mnivesh.com`
- [ ] `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
- [ ] `OTEL_EXPORTER_OTLP_HEADERS=X-API-Key=kg_v1.YOUR_KEY_HERE` (from Key Vault `OTEL_API_KEY`)
- [ ] Verification: Check Azure Function App Configuration

### Deployment Order

1. Deploy Azure Functions backend
2. Deploy frontend static site
3. Run smoke tests on production URLs:
   - [ ] `/health` returns 200 OK
   - [ ] Authenticated request with valid JWT succeeds
   - [ ] Invalid JWT returns 401
4. Monitor logs for 24 hours

---

## 📝 KNOWN ISSUES TO MONITOR

### Legacy Issues
1. **Kawal Singh**: `is_active: False` (not in Zoho_Users) - acceptable
2. **Missing SIP/Lumpsum data**: Some employees in Dec 2025 have only Insurance/Referral points
3. **Routing previously broken**: Fixed in dev, verify works in production ✅

### Multi-Tenant Migration Risks
4. **Host RBAC API dependency**: Module fails closed if host is down
5. **RBAC cache staleness**: Permissions may be cached up to 5 minutes (acceptable trade-off)
6. **First-time deployment**: No tenant data initially (onboard pilot tenant first)

---

## 🎯 GO/NO-GO CHECKLIST

### Staging Go-Live (Multi-Tenant)

**Infrastructure**:
- [ ] Azure Function App deployed (Consumption plan)
- [ ] PostgreSQL database deployed (if migrating) OR MongoDB v2 verified
- [ ] Redis cache deployed and accessible
- [ ] Key Vault with all secrets

**Security**:
- [ ] JWT validation working (host token accepted)
- [ ] RBAC fetch from host API working
- [ ] `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com` set
- [ ] Webhook HMAC signature verified

**Testing**:
- [ ] Unit tests: 100% passing
- [ ] Integration tests: 100% passing
- [ ] Security tests: 100% passing (tenant isolation, JWT, RBAC)

**Data**:
- [ ] 1 pilot tenant onboarded
- [ ] Tenant data verified (if legacy data migrated)
- [ ] Audit log hash chain verified

**Monitoring**:
- [ ] Application Insights dashboards operational
- [ ] Alerts configured and tested
- [ ] **OpenTelemetry Integration** (NEW):
  - [ ] OTel SDK installed and configured
  - [ ] KeyGate API key obtained and stored in Key Vault
  - [ ] OTLP endpoint reachable (`https://otel.dev.mnivesh.com/healthz`)
  - [ ] Test traffic generates spans in OpenSearch Dashboards
  - [ ] OpenSearch dashboard created ("PLI Leaderboard - Production Monitoring")

### Production Go-Live

**All Staging Criteria** + **Production-Specific**:

**Critical Security**:
- [ ] `STAGING_PLATFORM_ADMIN_EMAILS` NOT SET (verified)
- [ ] `ENVIRONMENT=production` (verified)
- [ ] Production secrets rotated (different from staging)
- [ ] Authentication level correctly set (see Critical Issue #1)
- [ ] **Production OpenTelemetry API key** stored in Key Vault (different from staging)

**Host Integration (NEW)**:
- [ ] Host RBAC API production endpoint tested
- [ ] Host Email Gateway production endpoint tested
- [ ] Webhook delivery from production host verified
- [ ] Host SLA documented (99.9% for RBAC API)

**Performance**:
- [ ] Load testing passed (100 concurrent users)
- [ ] P95 latency <300ms
- [ ] Database query plan optimized
- [ ] Cache hit rate >95%

**Operational**:
- [ ] On-call rotation configured
- [ ] Incident response runbook documented
- [ ] Rollback procedure tested
- [ ] Backup restore verified

**Compliance**:
- [ ] Audit log retention policy configured (7 years)
- [ ] GDPR procedures documented
- [ ] Security review completed

**Business**:
- [ ] 1 production tenant ready to onboard
- [ ] User communication sent (migration notice)
- [ ] Executive sign-off obtained

---

## 📅 POST-PRODUCTION MONITORING

### First 24 Hours
- [ ] Monitor error rate (<0.1%)
- [ ] Monitor P95 latency (<200ms for cached paths)
- [ ] Verify RBAC cache hit rate (>95%)
- [ ] Check audit log hash chain integrity
- [ ] Verify no cross-tenant data leaks (run audit query)
- [ ] Review all critical alerts (should be zero)
- [ ] Monitor legacy endpoints (if still running in parallel)
- [ ] **OpenTelemetry Monitoring**:
  - [ ] Verify telemetry data flowing to OpenSearch
  - [ ] Check OTel export success rate (>99%)
  - [ ] Verify no 401 authentication errors from OTLP endpoint
  - [ ] Review trace sampling rate (should be ~5%)
  - [ ] Confirm no PII leakage in spans (check redaction)

### First Week
- [ ] Collect feedback from pilot tenant users
- [ ] Review all audit events for anomalies
- [ ] Analyze cost vs. budget (<$500/mo target)
- [ ] Optimize slow queries (if any)
- [ ] Review RBAC cache TTL effectiveness
- [ ] Schedule retrospective

### First Month
- [ ] Onboard 5-10 additional tenants (per migration plan)
- [ ] Review host API SLA compliance
- [ ] Tune cache configuration based on metrics
- [ ] Plan migration timeline for remaining tenants
- [ ] Review security posture (zero P0/P1 findings)

---

## 🎯 NEXT STEPS (Priority Order)

### P0 - Critical (Must Fix Before Any Deployment)

1. **Fix Authentication**: [Leaderboard_API/function.json](file:///Users/vilakshanbhutani/Desktop/Azure%20Function/PLI_Leaderboard/Leaderboard_API/function.json) `authLevel`
   - **Decision**: Multi-tenant JWT OR function key?
   - **Owner**: Tech Lead
   - **Target**: Before staging deploy

2. **Implement JWT Validation Middleware** (if multi-tenant):
   - Validate RS256 signature via JWKS
   - Extract `tenant_id` from token
   - **Owner**: Backend Team
   - **Target**: Before staging deploy

3. **Configure Staging Super Admin**:
   - Set `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com` in staging
   - Verify NOT SET in production
   - **Owner**: DevOps
   - **Target**: Before staging deploy

### P1 - High Priority (Should Fix Before Production)

4. **Implement RBAC Integration**:
   - Connect to host RBAC API
   - Implement Redis caching
   - **Owner**: Backend Team
   - **Target**: Sprint 1 (2 weeks)

5. **Data Verification**:
   - Verify all config documents in V2
   - Run comprehensive data check (Apr-Dec 2025)
   - **Owner**: Data Team
   - **Target**: Before pilot tenant onboarding

6. **Audit Log Implementation**:
   - Implement hash-chained audit_events table
   - Daily integrity check
   - **Owner**: Backend Team
   - **Target**: Sprint 1

7. **Load Testing**:
   - 100 concurrent users
   - Verify P95 latency target
   - **Owner**: QA Team
   - **Target**: Before production deploy

### P2 - Medium Priority (Nice to Have)

8. **Health Checks**: `/health` endpoint with DB connectivity
9. **E2E Tests**: Automated tests for critical flows
10. **Error Monitoring**: Advanced Application Insights alerts
11. **API Documentation**: OpenAPI/Swagger spec

---

## ✅ SIGN-OFF

### Staging Approval

| Role | Name | Date | Status |
|------|------|------|--------|
| Tech Lead | | | ⏳ Pending |
| Security Lead | | | ⏳ Pending |
| DevOps Lead | | | ⏳ Pending |

### Production Approval

| Role | Name | Date | Status |
|------|------|------|--------|
| Tech Lead | | | ⏳ Pending |
| Security Lead | | | ⏳ Pending |
| Product Owner | | | ⏳ Pending |
| Engineering Manager | | | ⏳ Pending |

---

**Current Status**: 🔴 **BLOCKED ON CRITICAL ISSUE #1**

**Blocker**: Authentication configuration must be fixed before ANY deployment.

**Related Documents**:
- [Multi-Tenant Blueprint README](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/README.md)
- [Migration Plan](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/10_migration_plan.md)
- [Threat Model](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/08_threat_model.md)
