# MVP Scope Definition

## 11. Minimal Viable Product (MVP) vs. Future Enhancements

### MVP Philosophy

**Goal**: Ship a secure, functional multi-tenant module in 6-8 weeks that satisfies all hard requirements with minimal ongoing cost.

**Principle**: Security and tenant isolation are NON-NEGOTIABLE. Polish and advanced features can wait.

---

## MVP Must-Haves (First Build)

### 1. Tenant Isolation ✅

**Features**:
- `tenant_id` column in all domain tables
- Server-side enforcement: `WHERE tenant_id = $1` on every query
- JWT claim-based tenant resolution (never trust client)
- PostgreSQL indexes on `(tenant_id, ...)`

**Why MVP**: Core security requirement, absolute blocker

**Effort**: 1 week (database schema + middleware)

---

### 2. JWT Authentication ✅

**Features**:
- RS256 signature verification via JWKS
- Claim validation: `iss`, `aud`, `sub`, `email`, `tenant_id`, `exp`
- Integration with host's JWKS endpoint
- Reject tokens with `exp > 4 hours`

**Why MVP**: No authentication = no access control

**Effort**: 3 days (middleware + tests)

---

### 3. RBAC Fetch & Caching ✅

**Features**:
- Call host API `GET /rbac/effective?tenant_id={tid}&user_id={uid}`
- Cache in Redis with TTL (default 180s, respect host `ttl_seconds`)
- Filter permissions against module allowlist
- **Graceful degradation**: If host RBAC down, use stale cache (<10 min) for read-only actions, deny all writes

**Why MVP**: Authorization is non-negotiable; caching is critical for performance/cost

**Effort**: 1 week (RBAC client + Redis integration + tests)

---

### 4. Permission Enforcement ✅

**Features**:
- `module_permissions` table with allowlist
- Middleware: `requirePermission(permission)`
- Middleware: `requireAnyPermission([...])`
- Ignore unknown permissions from host

**Why MVP**: Prevents unauthorized actions

**Effort**: 2 days (middleware + DB seed)

---

### 5. Platform Super Admin ✅

**Features**:
- Three-priority check:
  1. Token claim `platform_super_admin=true` OR group `Leaderboard_SuperAdmins`
  2. Staging env override: `STAGING_PLATFORM_ADMIN_EMAILS` (ONLY if `ENVIRONMENT=staging`)
  3. Deny
- **Critical**: Staging override emits high-severity audit event
- **Never** expose UI to grant super admin

**Why MVP**: Required for vilakshan@niveshonline.com to test in staging

**Effort**: 1 day (middleware + env var check)

---

### 6. Audit Log (Hash-Chained) ✅

**Features**:
- `audit_events` table with `prev_event_hash` → `current_event_hash` linkage
- Append-only (no UPDATE/DELETE permissions)
- Log: login validated, RBAC fetched/failed, permission denied, exports, sensitive actions
- Daily cron to verify hash chain integrity

**Why MVP**: Compliance-ready from day 1, tamper detection

**Effort**: 1 week (table schema + hash chaining logic + verification function)

---

### 7. Email Delegation to Host ✅

**Features**:
- `POST {HOST_EMAIL_API}/email/send` with `{tenant_id, template, to, vars, idempotency_key}`
- Store API key in Key Vault
- Audit event: `EMAIL_SEND_REQUESTED` with `message_id`
- **No SMTP credentials** stored in module

**Why MVP**: Hard requirement (module doesn't manage email)

**Effort**: 3 days (API client + audit integration)

---

### 8. Webhook: RBAC Cache Purge ✅

**Features**:
- `POST /webhooks/rbac-changed` endpoint
- HMAC-SHA256 signature verification
- Purge Redis key: `rbac:{tenant_id}:{user_id}`
- Audit event: `RBAC_CACHE_PURGED`

**Why MVP**: Prevents stale permissions after host RBAC changes

**Effort**: 2 days (endpoint + HMAC verification)

---

### 9. Step-Up / Assurance ✅

**Features**:
- Check `module_permissions.requires_step_up` for sensitive actions
- Verify token claims `amr` (authentication methods reference) includes `"mfa"`
- Alternative: Check RBAC response `assurance.mfa = true` or `assurance.level = "high"`
- Return `403 STEP_UP_REQUIRED` if not strong

**Why MVP**: Required for EXPORTS_EXECUTE, LEADS_DELETE, etc.

**Effort**: 2 days (middleware + tests)

---

### 10. Basic CRUD API (Leads Example) ✅

**Features**:
- `GET /api/leads` (with tenant scoping)
- `POST /api/leads` (with audit)
- `DELETE /api/leads/{id}` (with step-up check)
- Demonstrates full auth/authz flow

**Why MVP**: End-to-end validation of architecture

**Effort**: 3 days (endpoints + tests)

---

### 11. Health & System Endpoints ✅

**Features**:
- `GET /health` (unauthenticated, returns 200 OK)
- `GET /api/audit` (admin-only, demonstrates cross-tenant query for super admin)

**Why MVP**: Operational readiness

**Effort**: 1 day

---

### 12. Secrets Management ✅

**Features**:
- All secrets in Azure Key Vault:
  - `HOST_JWKS_URI`
  - `HOST_RBAC_API_KEY`
  - `HOST_EMAIL_API_KEY`
  - `WEBHOOK_HMAC_SECRET`
  - Database connection string
- Never hard-code secrets

**Why MVP**: Security best practice, compliance requirement

**Effort**: 2 days (Key Vault integration)

---

### 13. Staging Environment Config ✅

**Features**:
- `ENVIRONMENT=staging` env var
- `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com`
- Separate Azure resources (Function App, DB, Redis)

**Why MVP**: Required for testing before production launch

**Effort**: 1 day

---

## MVP Total Effort

**Estimated**: 6-8 weeks with 2 full-time developers

| Component | Effort | Priority |
|-----------|--------|----------|
| Database schema + tenant isolation | 1 week | P0 |
| JWT validation + middleware | 3 days | P0 |
| RBAC fetch + Redis caching | 1 week | P0 |
| Permission enforcement | 2 days | P0 |
| Platform super admin | 1 day | P0 |
| Audit log (hash-chained) | 1 week | P0 |
| Email delegation | 3 days | P0 |
| Webhook (RBAC purge) | 2 days | P0 |
| Step-up enforcement | 2 days | P0 |
| Basic CRUD API | 3 days | P1 |
| Health endpoints | 1 day | P2 |
| Key Vault integration | 2 days | P0 |
| Testing (unit + integration) | 1 week | P0 |
| Staging deployment | 1 day | P0 |

---

## Deferred Features (Post-MVP)

### 1. JTI Replay Prevention ⏸️

**Description**: Store JWT IDs in Redis to prevent token replay attacks

**Why Deferred**: Short token lifetime (15-60 min) limits replay window; Redis storage cost for every token

**Effort**: 2 days

**Timeline**: Month 2-3 (if penetration test flags as critical)

---

### 2. SAML / Advanced SSO ⏸️

**Description**: Support SAML 2.0, OIDC for host authentication

**Why Deferred**: Host owns authentication; module only validates JWTs

**Effort**: 2 weeks

**Timeline**: Only if host doesn't issue JWTs (unlikely)

---

### 3. SCIM Provisioning ⏸️

**Description**: Auto-sync users/groups from host via SCIM 2.0

**Why Deferred**: Module doesn't store user data; host manages users

**Effort**: 1 week

**Timeline**: Never (unless requirements change)

---

### 4. Advanced Analytics Dashboard ⏸️

**Description**: BI dashboard for tenant admins (user activity, exports, audit trends)

**Why Deferred**: Not a security/functionality blocker; can use external tools (Metabase, Redash)

**Effort**: 2 weeks

**Timeline**: Month 4-6 (if customer demand)

---

### 5. UI Polish & Frontend Framework ⏸️

**Description**: React/Vue frontend with SPA architecture, design system

**Why Deferred**: Focus on backend security first; basic HTML/htmx sufficient for MVP

**Effort**: 3 weeks

**Timeline**: Month 3-6 (if embedded in host UI, may not be needed)

---

### 6. Multi-Region Deployment ⏸️

**Description**: Deploy to multiple Azure regions (US, EU, APAC) for low latency

**Why Deferred**: Single region sufficient for <100 tenants; complexity vs. benefit

**Effort**: 2 weeks

**Timeline**: Post-100 tenants (scalability trigger)

---

### 7. Row-Level Security (RLS) Enforcement ⏸️

**Description**: PostgreSQL RLS policies as defense-in-depth

**Why Deferred**: Application-layer tenant scoping is sufficient; RLS adds complexity

**Effort**: 3 days

**Timeline**: Month 2-3 (optional hardening)

---

### 8. IP Allowlisting / Geofencing ⏸️

**Description**: Restrict access by IP ranges or geographic location

**Why Deferred**: Not a common requirement for SaaS; adds operational burden

**Effort**: 1 week

**Timeline**: Only if enterprise customer demands it

---

### 9. Advanced Audit Log Search ⏸️

**Description**: Full-text search, filters, export to CSV, anomaly detection

**Why Deferred**: Basic SQL queries sufficient for MVP; invest in UI later

**Effort**: 2 weeks

**Timeline**: Month 4-6 (if audit log volume is high)

---

### 10. Email Templates (Module-Owned) ⏸️

**Description**: Module manages its own email templates (HTML, subject lines)

**Why Deferred**: Host owns all templates (hard requirement)

**Effort**: N/A

**Timeline**: Never (violates architecture)

---

### 11. Automated Compliance Reports ⏸️

**Description**: Generate SOC 2, GDPR, HIPAA compliance reports

**Why Deferred**: Audit log is compliance-ready; automated reports are polish

**Effort**: 2 weeks

**Timeline**: Month 6-12 (if pursuing certifications)

---

### 12. GraphQL API ⏸️

**Description**: GraphQL endpoint as alternative to REST

**Why Deferred**: REST is sufficient; GraphQL adds complexity

**Effort**: 1 week

**Timeline**: Only if customer requests it

---

### 13. Webhooks (Module → Host) ⏸️

**Description**: Module sends webhooks to host on key events (lead created, export completed)

**Why Deferred**: Not a hard requirement; can add if host needs real-time notifications

**Effort**: 3 days

**Timeline**: Month 3-4 (if host integrates with other systems)

---

### 14. Tenant Self-Service Portal ⏸️

**Description**: UI for tenant admins to manage users, settings, billing

**Why Deferred**: Host owns RBAC management; module only enforces permissions

**Effort**: 2 weeks

**Timeline**: Only if module expands beyond embedded use case

---

### 15. Cost Optimization Tooling ⏸️

**Description**: Dashboards for tracking Azure spend per tenant, recommendations

**Why Deferred**: Manual monitoring sufficient for <10 tenants

**Effort**: 1 week

**Timeline**: Post-50 tenants (cost becomes significant)

---

## MVP vs. Full Feature Comparison

| Feature | MVP | Post-MVP | Timeline |
|---------|-----|----------|----------|
| Tenant isolation | ✅ | ✅ | Day 1 |
| JWT authentication | ✅ | ✅ | Day 1 |
| RBAC fetch + caching | ✅ | ✅ | Day 1 |
| Permission enforcement | ✅ | ✅ | Day 1 |
| Platform super admin | ✅ | ✅ | Day 1 |
| Audit log (hash-chained) | ✅ | ✅ | Day 1 |
| Email delegation | ✅ | ✅ | Day 1 |
| Webhook (RBAC purge) | ✅ | ✅ | Day 1 |
| Step-up enforcement | ✅ | ✅ | Day 1 |
| JTI replay prevention | ❌ | ✅ | Month 2-3 |
| Row-level security (RLS) | ❌ | ✅ (optional) | Month 2-3 |
| Advanced audit search | ❌ | ✅ | Month 4-6 |
| Multi-region | ❌ | ✅ | Post-100 tenants |
| GraphQL API | ❌ | ✅ (optional) | On demand |
| Compliance reports | ❌ | ✅ | Month 6-12 |

---

## MVP Success Criteria

### Functional
- [ ] 1 pilot tenant successfully onboarded
- [ ] All CRUD operations work with tenant scoping
- [ ] Exports require step-up (MFA)
- [ ] Staging admin override works for vilakshan@niveshonline.com
- [ ] Audit log hash chain verifies with zero breaks

### Security
- [ ] No cross-tenant data leakage (tested with 2 tenants)
- [ ] JWT validation rejects invalid/expired tokens
- [ ] RBAC cache purge webhook verified with HMAC
- [ ] Platform super admin only works in staging (not production)

### Performance
- [ ] P95 latency <200ms for cached RBAC paths
- [ ] RBAC cache hit rate >90%
- [ ] Database queries use `tenant_id` indexes (query plan verified)

### Operational
- [ ] Zero downtime deployment achieved
- [ ] Rollback procedure tested in staging (<15 min)
- [ ] All secrets in Key Vault (no hard-coded credentials)
- [ ] Application Insights dashboards show key metrics

### Cost
- [ ] Monthly Azure spend <$200 for staging (1-5 tenants)
- [ ] Projected production cost <$500 for 100 tenants

---

## Decision Log

| Decision | Rationale | Date | Owner |
|----------|-----------|------|-------|
| Use Azure Functions (Consumption plan) | Pay-per-execution for low cost | TBD | Tech Lead |
| PostgreSQL over CosmosDB | ACID transactions, lower cost for <100 tenants | TBD | Tech Lead |
| Redis caching (not in-memory) | Shared cache across function instances | TBD | Tech Lead |
| Defer JTI replay prevention | Short token TTL mitigates risk; can add later | TBD | Security Lead |
| Defer RLS policies | App-layer scoping is sufficient; RLS is defense-in-depth | TBD | Tech Lead |
| MVP: Leads CRUD only | Demonstrates full flow; other entities can follow same pattern | TBD | Product |

---

## Launch Checklist

### Pre-Launch (Staging)
- [ ] All MVP features implemented
- [ ] Unit tests >95% coverage
- [ ] Integration tests passing
- [ ] Security tests (tenant isolation, JWT validation) passing
- [ ] Staging environment deployed
- [ ] vilakshan@niveshonline.com can access as super admin
- [ ] 1 pilot tenant onboarded and tested

### Launch (Production)
- [ ] Production environment deployed
- [ ] Secrets rotated (new API keys for prod)
- [ ] `ENVIRONMENT=production` (staging override disabled)
- [ ] Monitoring dashboards active
- [ ] On-call rotation configured
- [ ] Rollback procedure documented
- [ ] First production tenant onboarded

### Post-Launch (Week 1)
- [ ] Monitor error rates (<0.1%)
- [ ] Monitor P95 latency (<200ms)
- [ ] Monitor RBAC cache hit rate (>90%)
- [ ] Run audit chain verification
- [ ] Collect feedback from pilot tenant

