# Middleware & Pipeline Pseudocode (Azure Functions)

## 6. Middleware Pipeline for Azure Functions

### Overview

Azure Functions HTTP triggers use a middleware-like pattern. Below is pseudocode for Node.js (TypeScript) and Python implementations.

---

## TypeScript Implementation

### Main Handler Pattern

```typescript
// function_app.ts
import { app, HttpRequest, HttpResponseInit, InvocationContext } from "@azure/functions";
import { validateJwt, resolveTenant, getEffectivePermissions,
         isPlatformSuperAdmin, requirePermission, requireStepUp,
         auditAppend } from "./middleware";

app.http("getLeads", {
    methods: ["GET"],
    authLevel: "anonymous",
    route: "leads",
    handler: async (req: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> => {
        try {
            // 1. Validate JWT
            const token = await validateJwt(req, context);

            // 2. Resolve Tenant
            const tenant = await resolveTenant(token, context);

            // 3. Get Effective Permissions
            const permissions = await getEffectivePermissions(token, tenant.tenant_id, context);

            // 4. Authorize
            requirePermission(permissions, "LEADS_READ");

            // 5. Business Logic
            const leads = await getLeads(tenant.tenant_id, req.query);

            // 6. Audit
            await auditAppend({
                tenantId: tenant.tenant_id,
                eventType: "LEADS_QUERIED",
                actorUserId: token.sub,
                actorEmail: token.email,
                resourceType: "lead",
                action: "read",
                result: "success",
                ip: req.headers.get("x-forwarded-for") || "unknown",
                payload: { count: leads.length }
            });

            return {
                status: 200,
                jsonBody: { data: leads }
            };

        } catch (error) {
            return handleError(error, context);
        }
    }
});
```

---

### Middleware Functions

#### 1. validateJwt()

```typescript
import * as jwt from "jsonwebtoken";
import * as jwksClient from "jwks-rsa";
import { getSecret } from "./keyVault";

interface JwtPayload {
    iss: string;
    aud: string;
    sub: string;
    email: string;
    tenant_id: string;
    exp: number;
    iat: number;
    amr?: string[];
    groups?: string[];
    platform_super_admin?: boolean;
}

let jwksClientInstance: jwksClient.JwksClient | null = null;

async function getJwksClient(): Promise<jwksClient.JwksClient> {
    if (!jwksClientInstance) {
        const jwksUri = await getSecret("HOST_JWKS_URI");  // e.g., https://zoho.com/.well-known/jwks.json
        jwksClientInstance = jwksClient({
            jwksUri,
            cache: true,
            cacheMaxAge: 3600000,  // 1 hour
            rateLimit: true
        });
    }
    return jwksClientInstance;
}

export async function validateJwt(req: HttpRequest, context: InvocationContext): Promise<JwtPayload> {
    const authHeader = req.headers.get("authorization");

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
        throw new AuthError("INVALID_TOKEN", "Missing or invalid Authorization header");
    }

    const token = authHeader.substring(7);

    // Decode without verification first to get 'kid'
    const decoded = jwt.decode(token, { complete: true });
    if (!decoded || typeof decoded === "string") {
        throw new AuthError("INVALID_TOKEN", "Malformed JWT");
    }

    // Fetch signing key
    const client = await getJwksClient();
    const key = await client.getSigningKey(decoded.header.kid);
    const publicKey = key.getPublicKey();

    // Verify signature and claims
    const expectedAudience = await getSecret("JWT_AUDIENCE");  // e.g., "leaderboard-module"
    const expectedIssuer = await getSecret("JWT_ISSUER");      // e.g., "https://accounts.zoho.com"

    const payload = jwt.verify(token, publicKey, {
        audience: expectedAudience,
        issuer: expectedIssuer,
        algorithms: ["RS256"]
    }) as JwtPayload;

    // Additional validation
    if (!payload.tenant_id) {
        throw new AuthError("INVALID_TOKEN", "Missing tenant_id claim");
    }

    context.log(`JWT validated for user ${payload.sub}, tenant ${payload.tenant_id}`);

    return payload;
}
```

---

#### 2. resolveTenant()

```typescript
import { sql } from "./database";

interface Tenant {
    tenant_id: string;
    tenant_slug: string;
    display_name: string;
    status: string;
    tier: string;
}

export async function resolveTenant(token: JwtPayload, context: InvocationContext): Promise<Tenant> {
    const tenantId = token.tenant_id;

    const result = await sql<Tenant>`
        SELECT tenant_id, tenant_slug, display_name, status, tier
        FROM tenants
        WHERE tenant_id = ${tenantId}
    `;

    if (result.length === 0) {
        throw new AuthError("TENANT_NOT_FOUND", "Tenant does not exist");
    }

    const tenant = result[0];

    if (tenant.status !== "active") {
        throw new AuthError("TENANT_SUSPENDED", `Tenant status: ${tenant.status}`);
    }

    context.log(`Tenant resolved: ${tenant.tenant_slug} (${tenant.tenant_id})`);

    return tenant;
}
```

---

#### 3. getEffectivePermissions()

```typescript
import { getRedisClient } from "./cache";
import { getSecret } from "./keyVault";
import axios from "axios";

interface RBACResponse {
    permissions: string[];
    roles?: string[];
    assurance?: {
        level?: string;
        mfa?: boolean;
    };
    ttl_seconds?: number;
    version?: string;
}

export async function getEffectivePermissions(
    token: JwtPayload,
    tenantId: string,
    context: InvocationContext
): Promise<string[]> {
    const userId = token.sub;
    const cacheKey = `rbac:${tenantId}:${userId}`;

    // 1. Check cache
    const redis = await getRedisClient();
    const cached = await redis.get(cacheKey);

    if (cached) {
        context.log(`RBAC cache HIT for ${userId}`);
        return JSON.parse(cached);
    }

    context.log(`RBAC cache MISS for ${userId}, fetching from host...`);

    // 2. Fetch from host RBAC API
    const hostApiUrl = await getSecret("HOST_RBAC_API_URL");
    const hostApiKey = await getSecret("HOST_RBAC_API_KEY");

    let rbacResponse: RBACResponse;

    try {
        const response = await axios.get(`${hostApiUrl}/rbac/effective`, {
            params: { tenant_id: tenantId, user_id: userId },
            headers: { "Authorization": `Bearer ${hostApiKey}` },
            timeout: 5000  // 5s timeout
        });

        rbacResponse = response.data;

    } catch (error) {
        context.error(`RBAC fetch failed: ${error.message}`);

        // Fail-closed: deny privileged actions, optionally allow read-only with stale cache
        await auditAppend({
            tenantId,
            eventType: "RBAC_FETCH_FAILED",
            severity: "error",
            actorUserId: userId,
            result: "failure",
            payload: { error: error.message }
        });

        throw new AuthError("RBAC_FETCH_FAILED", "Unable to fetch permissions from host");
    }

    // 3. Filter by module allowlist
    const allowedPermissions = await getAllowedPermissions();
    const effectivePermissions = rbacResponse.permissions.filter(p =>
        allowedPermissions.has(p)
    );

    // 4. Cache with TTL
    const ttl = rbacResponse.ttl_seconds || 180;  // default 3 minutes
    await redis.setex(cacheKey, ttl, JSON.stringify(effectivePermissions));

    context.log(`RBAC fetched and cached: ${effectivePermissions.length} permissions`);

    // 5. Audit
    await auditAppend({
        tenantId,
        eventType: "RBAC_FETCHED",
        severity: "info",
        actorUserId: userId,
        actorEmail: token.email,
        result: "success",
        payload: {
            permissions_count: effectivePermissions.length,
            cached_ttl: ttl
        }
    });

    return effectivePermissions;
}

// Cache allowlist in memory
let allowlistCache: Set<string> | null = null;

async function getAllowedPermissions(): Promise<Set<string>> {
    if (!allowlistCache) {
        const result = await sql<{ permission_name: string }>`
            SELECT permission_name FROM module_permissions
        `;
        allowlistCache = new Set(result.map(r => r.permission_name));
    }
    return allowlistCache;
}
```

---

#### 4. isPlatformSuperAdmin()

```typescript
export function isPlatformSuperAdmin(token: JwtPayload): { isSuperAdmin: boolean; source: string | null } {
    // Priority 1: Token claim
    if (token.platform_super_admin === true) {
        return { isSuperAdmin: true, source: "token_claim" };
    }

    // Priority 1b: Token group
    if (token.groups && token.groups.includes("Leaderboard_SuperAdmins")) {
        return { isSuperAdmin: true, source: "token_group" };
    }

    // Priority 2: Staging environment allowlist (ONLY if ENVIRONMENT=staging)
    const environment = process.env.ENVIRONMENT;
    if (environment === "staging") {
        const allowedEmails = (process.env.STAGING_PLATFORM_ADMIN_EMAILS || "").split(",");
        if (token.email && allowedEmails.includes(token.email)) {
            return { isSuperAdmin: true, source: "staging_override" };
        }
    }

    // Priority 3: Deny
    return { isSuperAdmin: false, source: null };
}
```

---

#### 5. requirePermission()

```typescript
export function requirePermission(permissions: string[], required: string): void {
    if (!permissions.includes(required)) {
        throw new AuthError("PERMISSION_DENIED", `Missing permission: ${required}`);
    }
}

export function requireAnyPermission(permissions: string[], required: string[]): void {
    const hasAny = required.some(r => permissions.includes(r));
    if (!hasAny) {
        throw new AuthError("PERMISSION_DENIED", `Missing any of: ${required.join(", ")}`);
    }
}
```

---

#### 6. requireStepUp()

```typescript
export async function requireStepUp(
    permission: string,
    token: JwtPayload,
    rbacResponse?: RBACResponse
): Promise<void> {
    // Check if permission requires step-up
    const result = await sql<{ requires_step_up: boolean }>`
        SELECT requires_step_up
        FROM module_permissions
        WHERE permission_name = ${permission}
    `;

    if (result.length === 0 || !result[0].requires_step_up) {
        return;  // No step-up required
    }

    // Check assurance level
    let isStrong = false;

    // Source 1: Token 'amr' claim (authentication methods reference)
    if (token.amr && (token.amr.includes("mfa") || token.amr.includes("otp"))) {
        isStrong = true;
    }

    // Source 2: Token 'acr' claim (authentication context reference)
    if (!isStrong && token.acr) {
        const strongContexts = ["urn:example:policy:strong", "2"];  // adjust per host
        if (strongContexts.includes(token.acr)) {
            isStrong = true;
        }
    }

    // Source 3: RBAC response assurance
    if (!isStrong && rbacResponse?.assurance) {
        if (rbacResponse.assurance.mfa === true || rbacResponse.assurance.level === "high") {
            isStrong = true;
        }
    }

    if (!isStrong) {
        throw new AuthError("STEP_UP_REQUIRED", "Strong authentication required for this action");
    }
}
```

---

#### 7. auditAppend()

```typescript
import { createHash } from "crypto";

interface AuditEvent {
    tenantId: string;
    eventType: string;
    severity?: string;
    actorUserId?: string;
    actorEmail?: string;
    resourceType?: string;
    resourceId?: string;
    action?: string;
    result: string;
    httpStatusCode?: number;
    errorCode?: string;
    errorMessage?: string;
    ip?: string;
    userAgent?: string;
    requestId?: string;
    payload?: any;
}

export async function auditAppend(event: AuditEvent): Promise<void> {
    // 1. Get previous event hash for chain
    const prevResult = await sql<{ current_event_hash: string }>`
        SELECT current_event_hash
        FROM audit_events
        WHERE tenant_id = ${event.tenantId}
        ORDER BY event_id DESC
        LIMIT 1
    `;

    const prevHash = prevResult.length > 0 ? prevResult[0].current_event_hash : null;

    // 2. Compute current hash
    const eventJson = JSON.stringify({
        tenant_id: event.tenantId,
        event_type: event.eventType,
        actor_user_id: event.actorUserId,
        timestamp: new Date().toISOString(),
        result: event.result,
        payload: event.payload
    });

    const currentHash = createHash("sha256")
        .update((prevHash || "") + eventJson)
        .digest("hex");

    // 3. Insert audit event
    await sql`
        INSERT INTO audit_events (
            tenant_id, event_type, severity, actor_user_id, actor_email,
            resource_type, resource_id, action, result, http_status_code,
            error_code, error_message, ip_address, user_agent, request_id,
            event_payload, prev_event_hash, current_event_hash
        ) VALUES (
            ${event.tenantId}, ${event.eventType}, ${event.severity || "info"},
            ${event.actorUserId}, ${event.actorEmail}, ${event.resourceType},
            ${event.resourceId}, ${event.action}, ${event.result}, ${event.httpStatusCode},
            ${event.errorCode}, ${event.errorMessage}, ${event.ip}, ${event.userAgent},
            ${event.requestId}, ${JSON.stringify(event.payload || {})}, ${prevHash}, ${currentHash}
        )
    `;
}
```

---

## Python Implementation (Alternative)

```python
# function_app.py
import azure.functions as func
import jwt
from middleware import (
    validate_jwt, resolve_tenant, get_effective_permissions,
    is_platform_super_admin, require_permission, audit_append
)

app = func.FunctionApp()

@app.route(route="leads", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_leads(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # 1. Validate JWT
        token = validate_jwt(req)

        # 2. Resolve Tenant
        tenant = resolve_tenant(token)

        # 3. Get Effective Permissions
        permissions = get_effective_permissions(token, tenant["tenant_id"])

        # 4. Authorize
        require_permission(permissions, "LEADS_READ")

        # 5. Business Logic
        leads = fetch_leads(tenant["tenant_id"], req.params)

        # 6. Audit
        audit_append({
            "tenant_id": tenant["tenant_id"],
            "event_type": "LEADS_QUERIED",
            "actor_user_id": token["sub"],
            "result": "success"
        })

        return func.HttpResponse(
            json.dumps({"data": leads}),
            status_code=200,
            mimetype="application/json"
        )

    except AuthError as e:
        return handle_auth_error(e)
```

---

### Error Handling

```typescript
class AuthError extends Error {
    constructor(public code: string, message: string) {
        super(message);
        this.name = "AuthError";
    }
}

function handleError(error: any, context: InvocationContext): HttpResponseInit {
    if (error instanceof AuthError) {
        const statusMap = {
            "INVALID_TOKEN": 401,
            "TOKEN_EXPIRED": 401,
            "PERMISSION_DENIED": 403,
            "STEP_UP_REQUIRED": 403,
            "TENANT_SUSPENDED": 403,
            "RBAC_FETCH_FAILED": 503
        };

        return {
            status: statusMap[error.code] || 500,
            jsonBody: {
                error: error.code,
                message: error.message
            }
        };
    }

    context.error(`Unhandled error: ${error}`);

    return {
        status: 500,
        jsonBody: {
            error: "INTERNAL_ERROR",
            message: "An unexpected error occurred"
        }
    };
}
```

---

### Key Design Patterns

1. **Middleware Chain**: Each function is composable and testable
2. **Fail-Closed**: RBAC fetch failures deny all privileged actions
3. **Caching**: Redis for RBAC, in-memory for allowlist
4. **Hash Chaining**: Audit events are tamper-evident
5. **Tenant Scoping**: Every DB query includes `WHERE tenant_id = $1`
6. **Platform Super Admin**: Three-priority check (token → staging env → deny)
7. **Step-Up**: Check token `amr`/`acr` claims or RBAC assurance

