# Migration Plan

## 10. Migration Strategy: Internal App → Embedded Module

### Current State Analysis

**Assumptions about "internal-only" app:**
- Authentication: Custom login (username/password or basic OAuth)
- Authorization: Hard-coded roles (admin, manager, user) or simple DB flags
- Email: Direct SMTP integration (credentials in config)
- Single tenant: No `tenant_id` concept
- Session handling: Cookie-based sessions stored in DB/memory

**Target State:**
- Authentication: Host-issued JWT tokens only
- Authorization: Dynamic RBAC fetched from host API
- Email: Delegated to host email gateway
- Multi-tenant: Every entity scoped by `tenant_id`
- Stateless: Token-per-request, no sessions

---

## Migration Phases

### Phase 0: Pre-Migration Preparation (2-3 weeks)

**Objective**: Prepare infrastructure and contracts without touching production app

#### Tasks

1. **Host Contract Finalization**
   - [ ] Finalize JWT claims schema with host team
   - [ ] Host implements RBAC API endpoint (`/rbac/effective`)
   - [ ] Host implements Email Gateway endpoint (`/email/send`)
   - [ ] Host exposes JWKS endpoint for JWT verification
   - [ ] Exchange secrets (API keys, HMAC secret) via secure channel

2. **Azure Infrastructure Setup**
   - [ ] Provision Azure Function App (Consumption plan)
   - [ ] Provision Azure Database for PostgreSQL (Flexible Server)
   - [ ] Provision Azure Cache for Redis (Basic tier for staging, Standard for prod)
   - [ ] Provision Key Vault for secrets
   - [ ] Configure Virtual Network (VNet) integration for security
   - [ ] Set up Application Insights for observability

3. **Database Schema Migration**
   - [ ] Add `tenant_id` column to all existing tables (nullable initially)
   - [ ] Create `tenants` table
   - [ ] Create `audit_events` table with hash chain
   - [ ] Create `module_permissions` table and seed allowlist
   - [ ] Create indexes on `tenant_id` columns
   - [ ] Set up automated backups and point-in-time recovery

4. **Development Environment**
   - [ ] Deploy staging environment (separate Azure resources)
   - [ ] Configure `ENVIRONMENT=staging` env var
   - [ ] Set `STAGING_PLATFORM_ADMIN_EMAILS=vilakshan@niveshonline.com`
   - [ ] Run unit tests and integration tests

**Deliverables**:
- Staging environment fully functional
- Host contract documented and signed off
- Database schema ready (with nullable `tenant_id`)

**Risks**:
- Host team delays RBAC API implementation → **Mitigation**: Start with mock host API for testing
- Database migration causes performance issues → **Mitigation**: Test on production replica first

---

### Phase 1: Pilot Rollout (1-2 weeks)

**Objective**: Migrate 1 pilot tenant (internal team) to new module

#### Tasks

1. **Tenant Creation**
   - [ ] Create first tenant in `tenants` table:
     ```sql
     INSERT INTO tenants (tenant_id, tenant_slug, display_name, status)
     VALUES ('550e8400-e29b-41d4-a716-446655440000', 'nivesh-internal', 'Nivesh Internal Team', 'active');
     ```
   - [ ] Backfill `tenant_id` for existing data:
     ```sql
     UPDATE leads SET tenant_id = '550e8400-e29b-41d4-a716-446655440000' WHERE tenant_id IS NULL;
     UPDATE ... -- repeat for all tables
     ```
   - [ ] Make `tenant_id` NOT NULL after backfill

2. **Dual-Mode Operation**
   - [ ] Deploy module to staging with feature flag `LEGACY_AUTH_ENABLED=true`
   - [ ] Implement "dual auth" middleware:
     ```typescript
     if (req.headers.authorization?.startsWith("Bearer ")) {
       // New: Host JWT validation
       token = validateJwt(req);
     } else if (req.cookies.session_id) {
       // Legacy: Session cookie validation
       token = validateLegacySession(req);
     } else {
       throw new AuthError("UNAUTHORIZED");
     }
     ```
   - [ ] Support both RBAC fetch from host AND legacy DB roles during transition

3. **Pilot User Testing**
   - [ ] Onboard 5 internal users to host application
   - [ ] Host issues JWTs with `tenant_id: 550e8400-...`
   - [ ] Users test full CRUD flows (leads, exports, audit)
   - [ ] Verify audit logs capture all actions
   - [ ] Test step-up enforcement (delete action requires MFA)

4. **Email Cutover**
   - [ ] Configure outbox table for email requests
   - [ ] Redirect all email sends to host gateway
   - [ ] Disable legacy SMTP configuration (keep as backup)

**Deliverables**:
- 1 pilot tenant successfully migrated
- Dual-mode authentication working
- Email delegation functional

**Rollback Plan**:
- Keep legacy auth enabled (`LEGACY_AUTH_ENABLED=true`)
- If host RBAC fails, fall back to DB-based roles for pilot tenant
- Retain legacy SMTP config for 30 days

**Metrics**:
- Zero data loss (compare row counts pre/post migration)
- <5% increase in P95 latency (due to RBAC API calls)
- 100% email delivery success rate

---

### Phase 2: Multi-Tenant Expansion (2-4 weeks)

**Objective**: Onboard 5-10 additional tenants (external customers)

#### Tasks

1. **Tenant Onboarding Workflow**
   - [ ] Create tenant provisioning API (admin-only):
     ```http
     POST /admin/tenants
     {
       "tenant_slug": "acme-corp",
       "display_name": "Acme Corporation",
       "tier": "standard"
     }
     ```
   - [ ] Host team adds tenant to their system (matching `tenant_id`)
   - [ ] Host team configures RBAC for new tenant's users

2. **RBAC Caching Optimization**
   - [ ] Monitor Redis cache hit rate (target >95%)
   - [ ] Tune cache TTL based on host RBAC change frequency
   - [ ] Implement webhook listener for cache invalidation

3. **Performance Testing**
   - [ ] Load test with 100 concurrent users across 10 tenants
   - [ ] Verify tenant isolation (no cross-tenant data leakage)
   - [ ] Test RBAC API resilience (simulate host downtime)

4. **Audit & Compliance**
   - [ ] Run `verify_audit_chain()` daily via scheduled function
   - [ ] Set up alerts for audit hash chain breaks
   - [ ] Configure 7-year retention policy for audit logs

**Deliverables**:
- 10 tenants onboarded
- RBAC caching at >95% hit rate
- Audit trail verified tamper-proof

**Risks**:
- Host RBAC API cannot handle load → **Mitigation**: Increase cache TTL to 300s
- Tenant data accidentally mixed → **Mitigation**: Run daily tenant isolation audit queries

---

### Phase 3: Legacy Auth Deprecation (1-2 weeks)

**Objective**: Remove legacy authentication entirely

#### Tasks

1. **Verify Migration Completeness**
   - [ ] All tenants using host JWT authentication (check metrics)
   - [ ] No legacy session cookies in use (check logs)
   - [ ] All emails sent via host gateway (zero direct SMTP)

2. **Remove Legacy Code**
   - [ ] Delete legacy auth middleware
   - [ ] Remove session table from database
   - [ ] Remove SMTP config from Key Vault
   - [ ] Set `LEGACY_AUTH_ENABLED=false` (eventually remove flag)

3. **Final Cutover**
   - [ ] Deploy to production with legacy auth disabled
   - [ ] Monitor error rates for 48 hours
   - [ ] Verify all users can still authenticate

**Deliverables**:
- Legacy auth code removed
- 100% of authentication via host JWT

**Rollback Plan**:
- Keep legacy auth code in Git history (don't delete branch)
- Can redeploy previous version within 15 minutes if critical issues

---

### Phase 4: Production Hardening (Ongoing)

**Objective**: Optimize and secure for production SaaS usage

#### Tasks

1. **Security Enhancements**
   - [ ] Enable JTI deduplication for replay prevention
   - [ ] Implement IP-based rate limiting (WAF)
   - [ ] Add DDoS protection (Azure Front Door)
   - [ ] Conduct penetration testing (external firm)

2. **Cost Optimization**
   - [ ] Analyze Azure Function execution metrics
   - [ ] Right-size PostgreSQL instance (vertical scaling)
   - [ ] Optimize Redis cache usage (eviction policies)
   - [ ] Set up budget alerts (Azure Cost Management)

3. **Observability**
   - [ ] Create Application Insights dashboards
   - [ ] Set up alerts for RBAC fetch failures, high latency, errors
   - [ ] Integrate with PagerDuty/Slack for on-call
   - [ ] Implement distributed tracing (OpenTelemetry)

4. **Compliance Readiness**
   - [ ] Document data retention policies (GDPR, SOC 2)
   - [ ] Implement tenant data export (for GDPR requests)
   - [ ] Set up audit log archival to cold storage
   - [ ] Prepare incident response runbook

**Deliverables**:
- Production-grade security posture
- Cost <$500/month for 100 tenants
- 99.9% uptime SLA

---

## Rollback Strategy

### Immediate Rollback (<1 hour)

If critical issues arise during migration:

1. **Azure Function rollback**:
   ```bash
   az functionapp deployment source config-zip \
     --resource-group rg-leaderboard \
     --name func-leaderboard \
     --src previous-deployment.zip
   ```

2. **Database rollback** (if schema changes made):
   - Restore from automated backup (point-in-time recovery)
   - Revert `tenant_id` columns to nullable
   - Restore legacy session table

3. **Re-enable legacy auth**:
   - Set `LEGACY_AUTH_ENABLED=true`
   - Restore SMTP secrets in Key Vault

### Gradual Rollback (tenant-by-tenant)

If specific tenants experience issues:

1. Move tenant back to legacy system
2. Update `tenants.status = 'migrated_back'`
3. Route tenant's traffic to old application URL
4. Investigate issues offline

---

## Data Migration Scripts

### Script 1: Backfill `tenant_id`

```sql
-- Create default tenant for existing data
INSERT INTO tenants (tenant_id, tenant_slug, display_name, status)
VALUES ('00000000-0000-0000-0000-000000000001', 'legacy-import', 'Legacy Data', 'active');

-- Backfill tenant_id for all tables
UPDATE leads SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE exports SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
-- ... repeat for all domain tables

-- Make tenant_id required after backfill
ALTER TABLE leads ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE exports ALTER COLUMN tenant_id SET NOT NULL;
```

### Script 2: Migrate Email Queue

```sql
-- Move existing email queue to outbox pattern
INSERT INTO outbox_events (tenant_id, event_type, payload, status)
SELECT
  '00000000-0000-0000-0000-000000000001',
  'EMAIL_SEND_REQUEST',
  jsonb_build_object('to', recipient, 'template', template_id, 'vars', variables),
  CASE
    WHEN sent_at IS NOT NULL THEN 'completed'
    WHEN failed_at IS NOT NULL THEN 'failed'
    ELSE 'pending'
  END
FROM legacy_email_queue;
```

---

## Timeline Summary

| Phase | Duration | Dependencies | Go/No-Go Criteria |
|-------|----------|--------------|-------------------|
| 0. Preparation | 2-3 weeks | Host team bandwidth | Host RBAC API functional, staging deployed |
| 1. Pilot | 1-2 weeks | Phase 0 complete | 5 pilot users successful, zero data loss |
| 2. Expansion | 2-4 weeks | Phase 1 successful | 10 tenants onboarded, <5% latency increase |
| 3. Deprecation | 1-2 weeks | Phase 2 stable | 100% JWT auth, zero legacy sessions |
| 4. Hardening | Ongoing | Phase 3 complete | Pentest passed, cost <$500/mo |

**Total Duration**: 6-11 weeks (1.5-3 months aggressive timeline)

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Host RBAC API downtime | Medium | High | Fail-closed policy, cached RBAC for reads |
| Data loss during migration | Low | Critical | Test on replica, automated backups |
| Performance degradation | Medium | Medium | Load testing, caching, connection pooling |
| Tenant data leakage | Low | Critical | RLS policies, daily isolation audits |
| Cost overruns | Medium | Low | Budget alerts, consumption plan auto-scaling |

---

## Success Criteria

### Technical
- [ ] Zero data loss (verified via row count reconciliation)
- [ ] <5% latency increase compared to legacy system
- [ ] 99.9% uptime during migration period
- [ ] All audit events hash chain verified

### Business
- [ ] 100% of pilot users successfully migrated
- [ ] Zero escalations from pilot tenants
- [ ] Monthly cost <$500 for first 100 tenants
- [ ] Security audit passed (no P0/P1 findings)

### Operational
- [ ] On-call runbook documented
- [ ] Rollback procedure tested and <1 hour execution time
- [ ] Team trained on new architecture
- [ ] Monitoring dashboards operational

