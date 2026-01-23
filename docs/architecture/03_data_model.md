# Data Model (PostgreSQL DDL)

## 3. Database Schema

### Core Tables

```sql
-- ============================================
-- TENANTS TABLE
-- ============================================
CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_slug VARCHAR(100) UNIQUE NOT NULL, -- e.g., 'nivesh', 'acme'
    display_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- active, suspended, archived
    tier VARCHAR(50) DEFAULT 'standard', -- standard, premium, enterprise
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb -- flexible config
);

CREATE INDEX idx_tenants_slug ON tenants(tenant_slug);
CREATE INDEX idx_tenants_status ON tenants(status) WHERE status = 'active';

-- ============================================
-- SAMPLE DOMAIN TABLE (Example: Leads)
-- ============================================
CREATE TABLE leads (
    lead_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,

    -- Business fields
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    status VARCHAR(50) DEFAULT 'new', -- new, contacted, qualified, converted, lost
    assigned_to VARCHAR(255), -- user_id from host
    source VARCHAR(100),

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255), -- user_id from token
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(255),

    -- Flexible data
    custom_fields JSONB DEFAULT '{}'::jsonb
);

-- Tenant isolation: EVERY query must filter by tenant_id
CREATE INDEX idx_leads_tenant ON leads(tenant_id);
CREATE INDEX idx_leads_tenant_status ON leads(tenant_id, status);
CREATE INDEX idx_leads_tenant_assigned ON leads(tenant_id, assigned_to);

-- Optional: Row-Level Security (Defense in depth)
ALTER TABLE leads ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_policy ON leads
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- Usage: Before each request, SET app.current_tenant_id = '<validated-tenant-id>';


-- ============================================
-- AUDIT EVENTS TABLE (Hash-Chained)
-- ============================================
CREATE TABLE audit_events (
    event_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,

    -- Event metadata
    event_type VARCHAR(100) NOT NULL, -- LOGIN_VALIDATED, RBAC_FETCHED, PERMISSION_DENIED, EXPORT_EXECUTED, etc.
    severity VARCHAR(20) NOT NULL DEFAULT 'info', -- debug, info, warning, error, critical
    actor_user_id VARCHAR(255), -- from token 'sub'
    actor_email VARCHAR(255), -- from token 'email'

    -- Action context
    resource_type VARCHAR(100), -- 'lead', 'export', 'webhook', etc.
    resource_id VARCHAR(255),
    action VARCHAR(100), -- 'create', 'update', 'delete', 'export', 'permission_check'

    -- Request context
    ip_address INET,
    user_agent TEXT,
    request_id VARCHAR(100), -- trace ID

    -- Result
    result VARCHAR(20) NOT NULL, -- success, failure, denied
    http_status_code INT,
    error_code VARCHAR(100),
    error_message TEXT,

    -- Payload (sanitized, no PII if avoidable)
    event_payload JSONB DEFAULT'{}'::jsonb,

    -- Hash chain for tamper detection
    prev_event_hash VARCHAR(64), -- SHA-256 of previous event
    current_event_hash VARCHAR(64) NOT NULL, -- SHA-256 of this event

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_tenant_created ON audit_events(tenant_id, created_at DESC);
CREATE INDEX idx_audit_type ON audit_events(event_type);
CREATE INDEX idx_audit_actor ON audit_events(actor_user_id, created_at DESC);
CREATE INDEX idx_audit_resource ON audit_events(resource_type, resource_id);
CREATE INDEX idx_audit_severity ON audit_events(severity) WHERE severity IN ('error', 'critical');

-- Hash chain integrity check function
CREATE OR REPLACE FUNCTION verify_audit_chain(p_tenant_id UUID, p_limit INT DEFAULT 1000)
RETURNS TABLE(broken_at BIGINT, expected_hash VARCHAR, actual_hash VARCHAR) AS $$
BEGIN
    RETURN QUERY
    WITH events AS (
        SELECT event_id, prev_event_hash, current_event_hash,
               LAG(current_event_hash) OVER (ORDER BY event_id) AS expected_prev_hash
        FROM audit_events
        WHERE tenant_id = p_tenant_id
        ORDER BY event_id DESC
        LIMIT p_limit
    )
    SELECT event_id, expected_prev_hash, prev_event_hash
    FROM events
    WHERE prev_event_hash IS DISTINCT FROM expected_prev_hash;
END;
$$ LANGUAGE plpgsql;


-- ============================================
-- OUTBOX TABLE (Optional: Transactional Email Queueing)
-- ============================================
CREATE TABLE outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,

    -- Event type
    event_type VARCHAR(100) NOT NULL, -- EMAIL_SEND_REQUEST, WEBHOOK_DELIVERY, etc.

    -- Payload
    payload JSONB NOT NULL, -- {to, template, vars, category, idempotency_key}

    -- Processing state
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ,

    -- Result
    result JSONB, -- {message_id, status_code, error}
    completed_at TIMESTAMPTZ,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_pending ON outbox_events(status, next_retry_at)
    WHERE status IN ('pending', 'processing');
CREATE INDEX idx_outbox_tenant_created ON outbox_events(tenant_id, created_at DESC);


-- ============================================
-- PERMISSION ALLOWLIST (Module's Permission Dictionary)
-- ============================================
CREATE TABLE module_permissions (
    permission_name VARCHAR(100) PRIMARY KEY,
    description TEXT,
    category VARCHAR(50), -- leads, exports, settings, admin
    requires_step_up BOOLEAN DEFAULT FALSE, -- true for sensitive actions
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed module permissions (allowlist)
INSERT INTO module_permissions (permission_name, description, category, requires_step_up) VALUES
    ('LEADS_READ', 'View leads', 'leads', FALSE),
    ('LEADS_WRITE', 'Create/edit leads', 'leads', FALSE),
    ('LEADS_DELETE', 'Delete leads', 'leads', TRUE),
    ('EXPORTS_EXECUTE', 'Run data exports', 'exports', TRUE),
    ('SETTINGS_READ', 'View module settings', 'settings', FALSE),
    ('SETTINGS_WRITE', 'Modify module settings', 'settings', TRUE),
    ('AUDIT_READ', 'View audit logs', 'admin', FALSE),
    ('WEBHOOKS_MANAGE', 'Configure webhooks', 'admin', TRUE);


-- ============================================
-- FUNCTIONS & TRIGGERS
-- ============================================

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_tenants_updated_at BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_leads_updated_at BEFORE UPDATE ON leads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

### Key Design Decisions

1. **Tenant Isolation**: Every tenant-scoped table has `tenant_id` as first indexed column
2. **Audit Hash Chain**: `prev_event_hash` → `current_event_hash` linkage prevents tampering
3. **No User Table**: User data lives in host; we only store `user_id` references
4. **No Roles Table**: RBAC is fully delegated to host; we only validate permissions
5. **Outbox Pattern**: Enables transactional email requests (optional for high reliability)
6. **RLS Opt-in**: Row-Level Security as defense-in-depth (can be enforced at app layer instead)
7. **JSONB Flexibility**: `custom_fields` and `metadata` columns for tenant-specific extensions

