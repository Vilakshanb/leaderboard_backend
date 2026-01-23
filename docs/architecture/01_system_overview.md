# Multi-Tenant Azure Functions Module - System Overview

## 1. System Overview

### Purpose
Embeddable multi-tenant module running on Azure Functions that delegates authentication, RBAC, and email to a host application while maintaining strict tenant isolation and security.

### Key Characteristics
- **Stateless Resource Server**: No local authentication; validates host-issued JWT tokens
- **RBAC Consumer**: Fetches effective permissions from host API with intelligent caching
- **Email Delegator**: Sends all emails via host gateway (no SMTP credentials stored)
- **Tenant-Scoped**: Every operation isolated by `tenant_id` from token claims
- **Audit-First**: Append-only, hash-chained audit log for compliance
- **Fail-Closed**: Denies privileged actions when host RBAC unavailable

### Trust Model
- **HOST is authority** for: identity, permissions, email delivery
- **MODULE is authority** for: business logic, data storage, audit trail
- **FRONTEND is untrusted**: Never accept `tenant_id` from client; derive from server-validated token only

### Technology Stack
- **Compute**: Azure Functions (HTTP-triggered, Node.js/Python recommended)
- **Database**: PostgreSQL (tenant-scoped queries)
- **Cache**: Redis (primary) or in-memory (local dev only)
- **Secrets**: Azure Key Vault (JWKS URI, host API credentials, webhook secrets)
- **Observability**: Application Insights or equivalent

### Cost Optimization Strategy
- Consumption plan for Azure Functions (pay-per-execution)
- Aggressive RBAC caching (60-300s TTL) to minimize host API calls
- Postgres connection pooling
- Redis for shared caching across function instances

### Non-Functional Requirements
- **Latency**: <200ms p95 for cached RBAC paths
- **Availability**: 99.9% (dependent on host RBAC availability)
- **Scalability**: Handle 100+ tenants, 10K+ users
- **Audit Retention**: 7 years (compliance-ready)

