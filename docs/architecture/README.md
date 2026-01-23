# Multi-Tenant Azure Functions Module - Security Blueprint

**Production-Ready Architecture for SaaS Multi-Tenant Module**

**Author**: Principal Security Architect + Azure Functions Lead
**Date**: December 2024
**Version**: 1.0
**Target**: Embedded module for MFD (Mutual Fund Distributor) tenants

---

## Executive Summary

This blueprint provides a complete, implementable architecture for a **multi-tenant module** running on Azure Functions that embeds inside a host application (Zoho, Entra, Okta, etc.). The module acts as a **resource server**, delegating authentication, RBAC, and email delivery to the host while maintaining strict tenant isolation and security.

### Key Characteristics

- **Stateless Resource Server**: Validates host-issued JWT tokens
- **RBAC Consumer**: Fetches effective permissions from host API with intelligent caching
- **Email Delegator**: Sends all emails via host gateway (zero SMTP credentials stored)
- **Tenant-Scoped**: Every database query isolated by `tenant_id` from token claims
- **Audit-First**: Append-only, hash-chained audit log for compliance
- **Fail-Closed**: Denies privileged actions when host RBAC unavailable
- **Cost-Optimized**: Consumption plan + aggressive caching for <$500/mo at 100 tenants

### Hard Requirements Satisfied

✅ Tenant isolation (server-side enforcement)
✅ JWT authentication (RS256 + JWKS validation)
✅ RBAC Option B (fetch from host API + cache)
✅ Platform super admin (staging-only env override)
✅ Step-up authentication for sensitive actions
✅ Email delegation to host gateway
✅ Hash-chained audit log (tamper-evident)
✅ Fail-closed on RBAC failures
✅ Webhook for RBAC cache purge
✅ Security controls (rate limiting, HMAC webhooks, SQL injection protection)

---

## Document Index

### 1. System Overview
**File**: [01_system_overview.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/01_system_overview.md)

**Contents**:
- Purpose and key characteristics
- Trust model
- Technology stack (Azure Functions, PostgreSQL, Redis, Key Vault)
- Cost optimization strategy
- Non-functional requirements (latency, availability, scalability)

---

### 2. Architecture & Trust Boundaries
**File**: [02_architecture.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/02_architecture.md)

**Contents**:
- Component diagram (Mermaid) showing host, module, database, cache, secrets
- Trust boundaries (External → Module, Module → Host APIs, Module → DB, Host → Webhooks)
- Component responsibilities
- Data flow security (JWT validation → RBAC fetch → permission check → DB query → audit)

---

### 3. Data Model (PostgreSQL DDL)
**File**: [03_data_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/03_data_model.md)

**Contents**:
- `tenants` table
- `leads` table (example domain entity with tenant scoping)
- `audit_events` table (hash-chained for tamper detection)
- `outbox_events` table (optional transactional email queue)
- `module_permissions` table (allowlist)
- Indexes, constraints, RLS policies
- Hash chain verification function

---

### 4. Request Flows
**File**: [04_request_flows.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/04_request_flows.md)

**Contents**:
- **Flow A**: Standard request with host token (JWT validation → tenant resolution → RBAC fetch → authorization → DB query → audit)
- **Flow B**: RBAC cache purge webhook (HMAC verification → cache invalidation → audit)
- **Flow C**: Email send via host gateway (idempotency + audit)
- **Flow D**: Step-up required (MFA enforcement for sensitive actions)
- **Flow E**: Staging platform admin override (env-based super admin access)

Each flow includes step-by-step sequence diagrams and logic branches.

---

### 5. API Specification
**File**: [05_api_specification.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/05_api_specification.md)

**Contents**:
- Health endpoint (`GET /health`)
- Webhook endpoint (`POST /webhooks/rbac-changed`)
- Leads CRUD endpoints (example: `GET /api/leads`, `POST /api/leads`, `DELETE /api/leads/{id}`)
- Exports endpoint (`POST /api/exports/leads` with step-up)
- Audit endpoint (`GET /api/audit` for admin/super admin)
- Error response format (standardized error codes)
- Rate limiting policies

---

### 6. Middleware & Pipeline Pseudocode (Azure Functions)
**File**: [06_middleware_pseudocode.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/06_middleware_pseudocode.md)

**Contents**:
- **TypeScript implementation** (Node.js recommended for Azure Functions)
- **Python implementation** (alternative)
- Core middleware functions:
  1. `validateJwt()` - RS256 signature verification via JWKS
  2. `resolveTenant()` - Lookup tenant in DB, check status
  3. `getEffectivePermissions()` - Fetch RBAC from host API with Redis caching
  4. `isPlatformSuperAdmin()` - Three-priority check (token claim → staging env → deny)
  5. `requirePermission()` - Permission enforcement
  6. `requireStepUp()` - MFA/assurance check for sensitive actions
  7. `auditAppend()` - Hash-chained audit logging

All functions include error handling, retry logic, and audit integration.

---

### 7. Host Contract Specification
**File**: [07_host_contract.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/07_host_contract.md)

**Contents**:

**A. JWT Token Claims**
- Required: `iss`, `aud`, `sub`, `email`, `tenant_id`, `exp`, `iat`
- Optional: `amr`, `acr`, `jti`, `groups`, `platform_super_admin`
- Signature: RS256 with JWKS endpoint

**B. RBAC API Endpoint**
- `GET /rbac/effective?tenant_id={tid}&user_id={uid}`
- Response schema: `{permissions: [], roles: [], assurance: {}, ttl_seconds: 180}`

**C. Webhook: RBAC Cache Purge**
- `POST /webhooks/rbac-changed` (module implements)
- HMAC-SHA256 signature verification

**D. Email Gateway**
- `POST /email/send` with `{tenant_id, template, to, vars, idempotency_key}`
- Response: `{message_id, status, provider}`

**E. Shared Secrets** (Key Vault)
- `HOST_JWKS_URI`, `HOST_RBAC_API_KEY`, `HOST_EMAIL_API_KEY`, `WEBHOOK_HMAC_SECRET`

**F-H. Responsibilities & Integration Checklist**

---

### 8. Threat Model
**File**: [08_threat_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/08_threat_model.md)

**Contents**: Top 12 security threats with detailed mitigations

1. **Tenant Data Breakout** - SQL injection, JWT tampering
2. **JWT Token Forgery** - Weak algorithms, missing validation
3. **Replay Attacks** - Stolen tokens reused
4. **Stale Permissions** - Cached RBAC after revocation
5. **Host RBAC Compromise** - Leaked API keys, MITM
6. **Webhook Spoofing** - Fake cache purge requests
7. **Email Abuse** - Spam via host gateway
8. **Audit Log Tampering** - Direct DB modification
9. **SQL Injection** - Unsanitized inputs
10. **SSRF** - User-controlled URLs
11. **Data Exfiltration via Exports** - Unrestricted exports
12. **Privilege Escalation** - Staging override in production

Each threat includes likelihood, impact, residual risk rating, and priority (P0-P2).

---

### 9. Test Plan
**File**: [09_test_plan.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/09_test_plan.md)

**Contents**:

**A. Unit Tests** (middleware functions)
- JWT validation, tenant resolution, RBAC caching, permission enforcement, step-up, platform super admin, audit hash chaining

**B. Integration Tests** (API + DB + Cache)
- Leads API, Exports API, Audit API

**C. Host Integration Tests**
- RBAC API, Webhook, Email Gateway

**D. Security Tests**
- Tenant isolation, JWT security (downgrade attacks, expiration)

**E. Performance Tests**
- RBAC caching latency, database query performance, load testing

**F. End-to-End Tests**
- Full user flows (create lead, delete with MFA, export data)

**G. Manual Testing Checklist**
- Platform super admin staging override

**H-I. CI/CD Integration & Coverage Goals** (95% for middleware, 90% for APIs)

---

### 10. Migration Plan
**File**: [10_migration_plan.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/10_migration_plan.md)

**Contents**:

**Phase 0: Pre-Migration Preparation** (2-3 weeks)
- Host contract finalization, Azure infrastructure setup, database schema migration, dev environment

**Phase 1: Pilot Rollout** (1-2 weeks)
- 1 pilot tenant, dual-mode operation (legacy + JWT auth), email cutover

**Phase 2: Multi-Tenant Expansion** (2-4 weeks)
- Onboard 5-10 tenants, RBAC caching optimization, performance testing, audit verification

**Phase 3: Legacy Auth Deprecation** (1-2 weeks)
- Remove legacy code, final cutover to JWT-only

**Phase 4: Production Hardening** (Ongoing)
- Security enhancements (JTI, DDoS protection, pentesting), cost optimization, observability, compliance

**Rollback Strategy** (immediate <1 hr, gradual tenant-by-tenant)

**Timeline**: 6-11 weeks total (1.5-3 months)

---

### 11. MVP Definition
**File**: [11_mvp_definition.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/11_mvp_definition.md)

**Contents**:

**MVP Must-Haves** (First Build - 6-8 weeks):
1. Tenant isolation ✅
2. JWT authentication ✅
3. RBAC fetch & caching ✅
4. Permission enforcement ✅
5. Platform super admin ✅
6. Audit log (hash-chained) ✅
7. Email delegation to host ✅
8. Webhook: RBAC cache purge ✅
9. Step-up / assurance ✅
10. Basic CRUD API (Leads) ✅
11. Health & system endpoints ✅
12. Secrets management (Key Vault) ✅
13. Staging environment config ✅

**Deferred Features** (Post-MVP):
- JTI replay prevention (Month 2-3)
- SAML / Advanced SSO (if needed)
- SCIM provisioning (never - host manages users)
- Advanced analytics dashboard (Month 4-6)
- UI polish (Month 3-6)
- Multi-region deployment (Post-100 tenants)
- Row-level security (optional hardening)
- IP allowlisting / geofencing (on demand)
- Audit log search UI (Month 4-6)
- Compliance reports (Month 6-12)
- GraphQL API (on demand)
- Module → Host webhooks (Month 3-4)
- Tenant self-service portal (if needed)
- Cost optimization tooling (Post-50 tenants)

**MVP Success Criteria**: Functional, security, performance, operational, and cost targets defined

---

### 12. Production Readiness Checklist
**File**: [12_production_readiness.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/12_production_readiness.md)

**Contents**:

Comprehensive production readiness checklist combining multi-tenant architecture requirements with existing PLI_Leaderboard project validations:

**Critical Security Issues**:
- Authentication configuration (blocker: fix `authLevel` in function.json)
- Platform super admin configuration (staging vs. production)
- Host JWT configuration and validation

**OpenTelemetry Observability** (NEW):
- OTel SDK installation and setup (Python/Node.js)
- KeyGate API key management and rotation (90-day schedule)
- OTLP endpoint connectivity tests
- Telemetry data validation in OpenSearch Dashboards
- Sensitive data redaction (email, phone, auth headers)
- Best practices compliance (span naming, sampling, error recording)
- Performance targets (<5% latency overhead, <50MB memory)

**Security & RBAC**:
- Multi-tenant security (tenant isolation, RBAC integration, webhooks, email delegation)
- Legacy RBAC compatibility
- Secrets management (Key Vault)
- Step-up enforcement for sensitive actions

**Data Integrity**:
- Database migration checklist (PostgreSQL multi-tenant OR MongoDB v2)
- Data completeness verification (Apr-Dec 2025 leaderboard data)
- Configuration integrity
- User data validation

**Testing**:
- API endpoints (legacy + new multi-tenant)
- Authentication flows (JWT validation)
- Frontend user flows
- Edge cases and error handling

**Performance**:
- Database indexes (MongoDB + PostgreSQL)
- Query performance targets
- RBAC caching performance (>95% hit rate)

**Monitoring**:
- Application Insights dashboards (operational + security)
- Alerts configuration (25+ alerts)

**Audit & Compliance**:
- Hash-chained audit log verification
- GDPR compliance procedures

**Deployment**:
- Pre-deployment checklist (critical blockers)
- Environment variables (existing + new multi-tenant)
- Deployment order and smoke tests

**Go/No-Go Checklists**:
- Staging approval criteria
- Production approval criteria

**Post-Production**: 24-hour, 1-week, 1-month monitoring plans

**Priority Order**: P0 (critical blockers), P1 (high priority), P2 (nice to have)

---

## Quick Start Guide

### For Developers

1. **Read this first**: [01_system_overview.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/01_system_overview.md) + [02_architecture.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/02_architecture.md)
2. **Database schema**: [03_data_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/03_data_model.md)
3. **Implementation**: [06_middleware_pseudocode.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/06_middleware_pseudocode.md) → Start coding middleware
4. **Testing**: [09_test_plan.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/09_test_plan.md) → Write unit tests as you go
5. **MVP scope**: [11_mvp_definition.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/11_mvp_definition.md) → Know what to build first vs. defer

### For Security Reviewers

1. **Threat model**: [08_threat_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/08_threat_model.md) → Top 12 threats + mitigations
2. **Request flows**: [04_request_flows.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/04_request_flows.md) → Flow D (step-up), Flow E (staging admin override)
3. **Tenant isolation**: [03_data_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/03_data_model.md) → Database scoping + RLS
4. **Audit**: [03_data_model.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/03_data_model.md) → Hash-chained audit_events table

### For Product/Project Managers

1. **MVP scope**: [11_mvp_definition.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/11_mvp_definition.md) → What's in/out of first release
2. **Migration plan**: [10_migration_plan.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/10_migration_plan.md) → Timeline: 6-11 weeks total
3. **Host integration**: [07_host_contract.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/07_host_contract.md) → What host team must deliver
4. **API spec**: [05_api_specification.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/05_api_specification.md) → External API surface

### For Host Integration Team

1. **Host contract**: [07_host_contract.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/07_host_contract.md) → What you must implement (RBAC API, Email Gateway, Webhooks)
2. **Request flows**: [04_request_flows.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/04_request_flows.md) → Flow A (RBAC fetch), Flow B (webhook), Flow C (email)
3. **JWT claims**: [07_host_contract.md](file:///Users/vilakshanbhutani/.gemini/antigravity/brain/e697cc0a-14a9-41dc-a790-a05093d09aee/07_host_contract.md) Section A → Required token claims

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Azure Functions (Consumption plan)** | Pay-per-execution, auto-scaling, <$500/mo for 100 tenants |
| **PostgreSQL over CosmosDB** | ACID transactions, lower cost for <100 tenants, better multi-tenancy support |
| **Redis caching (60-300s TTL)** | Minimize host RBAC API calls, shared cache across function instances |
| **Hash-chained audit log** | Tamper-evident compliance without blockchain overhead |
| **Fail-closed on RBAC failures** | Deny all writes if host down, allow stale cached reads (<10 min) |
| **Email delegation to host** | Zero SMTP credentials in module, host controls provider selection |
| **Platform super admin via env var ONLY** | Never expose UI, never store in DB, staging-only override |
| **Step-up enforcement** | MFA required for EXPORTS, DELETE actions via token `amr` claim or RBAC `assurance` |

---

## Non-Functional Requirements

| Requirement | Target | Measurement |
|-------------|--------|-------------|
| **Latency (P95)** | <200ms | RBAC cached paths |
| **Availability** | 99.9% | Dependent on host RBAC SLA |
| **Scalability** | 100+ tenants, 10K+ users | Tested with load testing |
| **Cost** | <$500/mo at 100 tenants | Azure Cost Management |
| **Audit Retention** | 7 years | Compliance-ready (GDPR, SOC 2) |
| **Cache Hit Rate** | >95% | Redis metrics |
| **Test Coverage** | 95% (middleware), 90% (APIs) | Jest/Pytest |

---

## Compliance & Security

✅ **Tenant Isolation**: Server-side enforcement, never trust client
✅ **Audit Trail**: Hash-chained, append-only, tamper-evident
✅ **Secrets Management**: Azure Key Vault, 90-day rotation
✅ **Encryption**: TLS 1.2+ in transit, at-rest encryption for PostgreSQL + Redis
✅ **Rate Limiting**: Per-user and per-tenant limits
✅ **DDoS Protection**: Azure Front Door (Phase 4)
✅ **Penetration Testing**: Scheduled for Phase 4
✅ **GDPR-Ready**: Tenant data export, audit log retention policies

---

## Implementation Timeline

| Milestone | Duration | Deliverables |
|-----------|----------|--------------|
| **Setup** | Week 1-2 | Azure infra, DB schema, Key Vault, staging env |
| **Core Middleware** | Week 3-4 | JWT validation, RBAC fetch, caching, permission enforcement |
| **Audit & Security** | Week 5 | Hash-chained audit log, step-up enforcement, platform super admin |
| **API Development** | Week 6 | Leads CRUD, exports, webhooks, email delegation |
| **Testing** | Week 7 | Unit tests, integration tests, security tests |
| **Pilot Deployment** | Week 8 | 1 tenant onboarded, staging validation |
| **Production Launch** | Week 9+ | Multi-tenant rollout per migration plan |

---

## Support & Maintenance

### Operational Runbooks
- **Host RBAC API Down**: Use cached RBAC (<10 min), deny writes, alert on-call
- **Audit Hash Chain Break**: Run `verify_audit_chain()`, investigate tampering, alert security team
- **Webhook Signature Failure**: Verify `WEBHOOK_HMAC_SECRET`, check host IP allowlist
- **High Latency**: Check Redis cache hit rate, monitor host RBAC API latency

### Monitoring Alerts
- RBAC fetch failures >1% (5 min window)
- P95 latency >500ms
- Audit hash chain break detected
- Cache hit rate <90%
- Error rate >0.5%
- Cost exceeds $600/mo

---

## Appendices

### A. Glossary
- **RBAC**: Role-Based Access Control
- **JWKS**: JSON Web Key Set (public keys for JWT verification)
- **RS256**: RSA Signature with SHA-256
- **TTL**: Time To Live (cache expiration)
- **RLS**: Row-Level Security (PostgreSQL feature)
- **HMAC**: Hash-based Message Authentication Code
- **Step-Up**: Re-authentication with MFA for sensitive actions

### B. References
- Azure Functions Best Practices: [docs.microsoft.com/azure/azure-functions](https://docs.microsoft.com/azure/azure-functions)
- JWT Best Practices: [RFC 8725](https://datatracker.ietf.org/doc/html/rfc8725)
- PostgreSQL Row-Level Security: [postgresql.org/docs/current/ddl-rowsecurity.html](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

---

## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-12-28 | Initial comprehensive blueprint | Principal Security Architect |

---

## Contact

For questions or clarifications on this blueprint:
- **Technical Lead**: [Your Name]
- **Security Review**: [Security Team]
- **Product Owner**: [Product Lead]

---

**Status**: ✅ **READY FOR IMPLEMENTATION**

All hard requirements satisfied. Blueprint is production-ready and implementable by development team.
