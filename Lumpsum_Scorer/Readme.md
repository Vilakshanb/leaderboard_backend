# Lumpsum Scorer Module

## Purpose & Overview

This module computes and audits incentive payouts for Relationship Managers (RMs) based on multiple performance metrics including Net Purchase (NP), meetings conducted, and applicable bonus slabs. It integrates data ingestion, aggregation, scoring logic, and audit trails to ensure transparent and reproducible incentive calculations.

## Architecture Notes for AI Agents

The module is organized into key functional units:

- **Data Ingestion:** Loads raw RM performance data from various sources.
- **Aggregation & Scoring:** Applies business rules to aggregate NP, meetings, and other metrics, then computes incentive scores using predefined slabs and weights.
- **Audit Layer:** Persists computed results and metadata in MongoDB collections for traceability.
- **Mongo Configuration Loader:** Dynamically loads incentive configuration, slab definitions, and rules from MongoDB, ensuring a single source of truth.

## AI Planning Context

For extending or retraining incentive models, AI agents should focus on the following variables and functions:

- `rate_slabs`: Defines NP-based incentive slabs and payout rates.
- `meeting_slabs`: Defines meeting-related bonus slabs and multipliers.
- `category_rules`: Business rules for RM categorization and eligibility.
- `periodic_bonus_templates`: Templates for periodic or ad-hoc bonus calculations.
- Core functions like `run_net_purchase` and `_apply_meeting_multiplier` encapsulate the scoring logic and are primary extension points.

## Configuration Philosophy

MongoDB acts as the single source of truth for all incentive configurations. The system supports:

- **Auto-bootstrap Defaults:** Default slab and rule configurations are auto-loaded if missing.
- **Schema Versioning:** Configuration documents include schema versions to enable backward compatibility and smooth upgrades.

## Upgrade Hooks

To introduce new incentive components, weights, or penalty rules, developers and AI agents can extend or override:

- `run_net_purchase`: Core function for net purchase incentive computation.
- `_apply_meeting_multiplier`: Applies meeting-based multipliers to incentives.
- Additional hooks for penalty application or bonus adjustments can be inserted in the scoring pipeline.

## Extensibility Roadmap

Future AI-driven enhancements may include:

- **Predictive Target Bonuses:** Use predictive models to set personalized bonus targets.
- **Anomaly Detection:** Detect and flag irregular incentive patterns or data inconsistencies.
- **Personalized RM Nudges:** Recommend personalized actions or goals to RMs based on performance trends.
- **Data-Driven Slab Tuning:** Automatically optimize slab thresholds and payout rates using historical data and reinforcement learning.

## Audit Layer

Incentive computation results are stored in two MongoDB collections:

- `Leaderboard_Lumpsum`: Contains the final incentive payouts per RM.
- `Lumpsum_audit`: Stores detailed audit trails including input data, computed scores, and metadata.

Each audit document includes a `config_hash` representing the exact configuration snapshot used, ensuring reproducibility and traceability of incentive calculations.

## Schema Overview

### Mongo Config Example

```json
{
  "schema_version": "1.0",
  "rate_slabs": [
    { "min_np": 0, "max_np": 100000, "rate": 0.01 },
    { "min_np": 100001, "max_np": 500000, "rate": 0.015 }
  ],
  "meeting_slabs": [
    { "min_meetings": 0, "max_meetings": 5, "multiplier": 1.0 },
    { "min_meetings": 6, "max_meetings": 10, "multiplier": 1.1 }
  ],
  "category_rules": {
    "silver": { "min_np": 0, "max_np": 100000 },
    "gold": { "min_np": 100001, "max_np": 500000 }
  },
  "periodic_bonus_templates": [{ "period": "Q1", "bonus_rate": 0.02 }]
}
```

### Audit Schema Example

```json
{
  "rm_id": "RM12345",
  "incentive_period": "2024-Q1",
  "computed_np": 350000,
  "computed_meetings": 8,
  "final_payout": 5250,
  "config_hash": "abc123def456",
  "timestamp": "2024-06-01T12:00:00Z",
  "audit_details": {
    "np_breakdown": {...},
    "meeting_multiplier": 1.1,
    "bonus_applied": 0.02
  }
}
```

## AI Integration Notes

For future Model-Centric Programming (MCP) or autonomous orchestration:

- Implement model hooks to observe the impact of incentive changes on RM behavior.
- Track fairness and bias metrics across RM categories and demographics.
- Enable feedback loops where AI agents can propose and validate new incentive rules or slabs.
- Integrate anomaly detection models to flag suspicious incentive payouts automatically.

This README serves as a comprehensive guide for both human developers and AI agents working with the Lumpsum Scorer module to understand, extend, and audit incentive computations effectively.

# Lumpsum Scorer — Incentives Engine (Human + AI Handbook)

> **What this is:** A reproducible, Mongo‑driven incentives engine that computes monthly payouts for Relationship Managers (RMs) based on **Net Purchase (NP)**, **meetings**, and **periodic bonus slabs** (QTD/YTD).
> **Who this is for:** Engineers, data folks, and AI agents that will extend, audit, or orchestrate this module.

---

## 1) High‑Level Overview

**Inputs (read):**

- `iwell.purchase_txn`, `iwell.redemption_txn`, `iwell.switchin_txn`, `iwell.switchout_txn`
- `iwell.ChangeofBroker` (COB: TICOB/TOCOB)
- `iwell.AUM_Report` (AUM at month start)
- `iwell.Investor_Meetings_Data` (fallbacks: `investor_meetings_data`, `investor_meeting_data`)
- `PLI_Leaderboard.Zoho_Users` (active users + aliases)

**Outputs (write):**

- `PLI_Leaderboard.Leaderboard_Lumpsum` — compact monthly row per RM (unique by `(employee_id, month)`)
- `PLI_Leaderboard.Lumpsum_audit` — verbose audit per RM/window (reproducible)

**Control plane (read/write):**

- `PLI_Leaderboard.Config` — **single source of truth** for runtime options, slabs, rules (auto‑bootstraps defaults)

**Core steps each run:**

1. Load **effective config** from Mongo (create defaults if missing).
2. Ingest + normalize TXNs; morph STI/STO → Switch; parse dates; normalize categories/subcategories.
3. Aggregate by RM; compute **NP = Additions − Subtractions** (with weights and debt‑bonus logic).
4. Apply **meeting multiplier** + **growth penalty** (vs AUM start).
5. (Optionally) compute **QTD/YTD bonuses**; apply if period end and enabled.
6. Write **Leaderboard** + **Audit** documents, stamped with `config_hash` and schema version.
7. Enforce **inactive employee policy** (purge/freeze/**mark last N months ineligible**).

---

## 2) Runtime Flow (for AI planners)

```
run_net_purchase()
 ├─ _init_runtime_config()                 # Mongo config load/merge; default bootstrap if missing
 ├─ _build_effective_config_snapshot()     # → config_hash (MD5) stamped into every row
 ├─ _fetch_txn_window()                    # union + addFields + dateFromString + filters
 ├─ _aggregate_by_rm()                     # Purchase/Redemption/Switch/COB + Debt% calc
 ├─ _apply_debt_bonus_if_applicable()
 ├─ _apply_meeting_multiplier()
 ├─ _apply_growth_penalties()
 ├─ _apply_periodic_bonuses_qtd_ytd()
 ├─ _inactive_policy_enforcement()         # purge | freeze | ineligible (mark last N months)
 ├─ _normalize_ls_record() / _normalize_np_record()
 └─ write Leaderboard_Lumpsum & Lumpsum_audit
```

> **Golden rule:** all computations should be reproducible from inputs + the `config_hash` snapshot.

---

## 3) Configuration Philosophy

- **Mongo‑first**: environment variables are deprecated for business logic. Secrets (e.g., Mongo URI) still via ENV/Key Vault.
- **Auto‑bootstrap**: if the config doc is absent, one is created with versioned defaults (safe, conservative).
- **Versioning**: both code and config carry schema versions. Increment when changing fields/logic shape.

---

## 4) Config Document (Mongo)

**Location**

- DB: `PLI_Leaderboard`
- Collection: `Config`
- `_id`: `"Leaderboard_Lumpsum"` (overridable via code, but fixed by convention)

**Canonical shape (JSONC)**

```jsonc
{
  "_id": "Leaderboard_Lumpsum",
  "schema_name": "Leaderboard_Lumpsum",
  "schema_version": "2025-09-27.r1", // matches code's SCHEMA_VERSION
  "status": "active",

  "options": {
    "range_mode": "last5", // "last5" or "fy"
    "fy_mode": "FY_APR", // "FY_APR" (Apr–Mar) or "CAL" (Jan–Dec)

    "periodic_bonus_enable": false, // enable quarter/year bonus calc
    "periodic_bonus_apply": true, // add bonus into final_incentive if true

    "audit_mode": "compact", // "compact" trims heavy audit payloads; "full" keeps all

    // Inactive employees control
    "inactivity_grace_months": 6, // act only if inactive for >= this many months
    "inactive_action": "ineligible", // "purge" | "freeze" | "ineligible"
    "inactive_ineligibility_months": 6 // for "ineligible": mark last N months as ineligible
  },

  // NP incentive slabs by growth band (pct of AUM start) → rate
  "rate_slabs": [
    { "min_pct": 0.0, "max_pct": 0.25, "rate": 0.0006, "label": "0–<0.25%" },
    { "min_pct": 0.25, "max_pct": 0.5, "rate": 0.0009, "label": "0.25–<0.5%" },
    { "min_pct": 0.5, "max_pct": 0.75, "rate": 0.00115, "label": "0.5–<0.75%" },
    { "min_pct": 0.75, "max_pct": 1.25, "rate": 0.00135, "label": "0.75–<1.25%" },
    { "min_pct": 1.25, "max_pct": 1.5, "rate": 0.00145, "label": "1.25–<1.5%" },
    { "min_pct": 1.5, "max_pct": 2.0, "rate": 0.00148, "label": "1.5–<2%" },
    { "min_pct": 2.0, "max_pct": null, "rate": 0.0015, "label": "≥2%" }
  ],

  // Meetings → payout multiplier
  "meeting_slabs": [
    { "max_count": 5, "mult": 1.0, "label": "0–5" },
    { "max_count": 11, "mult": 1.05, "label": "6–11" },
    { "max_count": 17, "mult": 1.075, "label": "12–17" },
    { "max_count": null, "mult": 1.1, "label": "18+" }
  ],

  // Category rules — **Mongo‑configurable** blacklist w/ behavior knobs
  "category_rules": {
    "blacklisted_categories": [
      // tokens (case‑insensitive); matched per match_mode/scope
      "LIQUID",
      "OVERNIGHT",
      "LOW DURATION",
      "MONEY MARKET",
      "ULTRA SHORT"
    ],
    "match_mode": "substring", // "substring" (default) or "exact"
    "scope": ["SUB CATEGORY"], // or ["CATEGORY","SUB CATEGORY"]
    "zero_weight_purchase": true, // 0% weight for purchase in blacklisted buckets
    "zero_weight_switch_in": true, // 0% weight for switch‑in to blacklisted categories
    "exclude_from_debt_bonus": true // exclude these from debt% bonus numerator/denominator
    // Note: Redemptions FROM blacklisted categories are also ignored for NP (hard‑coded fairness rule).
  },

  // Periodic bonus slabs — default 4 levels, all zeros until business sets real values
  "qtr_bonus_template": {
    "slabs": [
      { "min_np": 0, "bonus_rupees": 0, "label": "Qtr L0" },
      { "min_np": 1000000, "bonus_rupees": 0, "label": "Qtr L1" },
      { "min_np": 2500000, "bonus_rupees": 0, "label": "Qtr L2" },
      { "min_np": 5000000, "bonus_rupees": 0, "label": "Qtr L3" }
    ]
  },
  "annual_bonus_template": {
    "slabs": [
      { "min_np": 0, "bonus_rupees": 0, "label": "Ann L0" },
      { "min_np": 3000000, "bonus_rupees": 0, "label": "Ann L1" },
      { "min_np": 7500000, "bonus_rupees": 0, "label": "Ann L2" },
      { "min_np": 12000000, "bonus_rupees": 0, "label": "Ann L3" }
    ]
  },

  "meta": {
    "module": "Lumpsum_Scorer",
    "createdBy": "system",
    "createdAt": "2025-11-13T00:00:00Z",
    "config_revision": 1 // bump on manual edits
  }
}
```

**Notes**

- If `category_rules` is absent, the engine falls back to built‑in defaults (same blacklist; substring on SUB CATEGORY).
- `options.audit_mode="compact"` trims `Audit.*.ByCategory` to top‑3 non‑zero entries but keeps `ByType` intact.

---

## 5) Incentive Computation Details

### 5.1 Additions & Subtractions

- **Additions**
  - Purchase (100% for normal categories; 0% if the target scheme is blacklisted)
  - Switch In (120% for normal categories; 0% if the _destination_ scheme is blacklisted)
  - **Note (fairness rule):** redemptions _from_ blacklisted categories do **not** count as subtractions for NP.
  - COB In (TICOB) at +50%
  - **Debt Purchase Bonus**: +20% of **Purchase** _if_ (Debt Purchase % of total purchase) **&lt; 75%**
    - If `category_rules.exclude_from_debt_bonus=true`, blacklisted buckets are excluded from the debt% check.
- **Subtractions**
  - Redemption (100% for normal categories; 0% if the _source_ scheme is blacklisted)
  - Switch Out (120%)
  - COB Out (TOCOB) at −120%

> **COB sample:**
>
> ```
> TICOB_amt = sum(ChangeofBroker where COB TYPE == "TICOB")
> TOCOB_amt = sum(ChangeofBroker where COB TYPE == "TOCOB")
> Additions += 0.5 * TICOB_amt
> Subtractions += 1.2 * TOCOB_amt
> ```

### 5.2 Net Purchase (NP)

```
NP = (Purchase * w_pur) + (SwitchIn * 1.2) + (DebtBonus) + (TICOB * 0.5)
     − ((Redemption * 1.0) + (SwitchOut * 1.2) + (TOCOB * 1.2))
```

- `w_pur = 0` for blacklisted (liquid/overnight/…); else `1.0`.

### 5.3 Growth % vs AUM start (for slab rate)

```
growth_pct = NP / AUM_start_of_month
rate = lookup(rate_slabs, growth_pct)     // inclusive of lower bound; open upper bound (null)
base_rupees = NP * rate
```

- `AUM_start_of_month` is fetched from `iwell.AUM_Report` (`Amount` where `_id` follows the `{YYYY-MM-01}_{RM_NAME}` convention).
- If the AUM row is missing or `AUM_start_of_month` ≤ 0, the engine:
  - Forces `growth_pct = 0` for slab lookup (no positive or negative growth band is triggered purely due to missing AUM).
  - Stores `AUM (Start of Month)` as `0` in the leaderboard document.
  - Computes `monthly_trail_used` from a safe surrogate (e.g. `np_val`) or `0`, depending on configuration.
  - Ensures negative growth penalties are never computed off an unknown/zero AUM.

  This behavior fixes the historical bug where some legacy rows showed `AUM (Start of Month) = 0` for live RMs even when a valid AUM snapshot existed in `AUM_Report`.

### 5.4 Meeting Multiplier

Lookup `meeting_slabs` by meeting count, multiply **only non‑negative** base payouts.

### 5.5 Negative Growth Penalties (illustrative defaults)

- ≤ −1.0%: **−5000 points**
- (−1.0%, −0.5%]: **−2500 points**
- (−0.5%, 0%]: **0 points**

These penalties are applied in **points space**, independent of the rupee trail calculation. The actual monthly trail in rupees is still:

```text
monthly_trail_used ≈ AUM_start_of_month * annual_trail_rate / 12
```

(with `annual_trail_rate` typically around `0.8` for equity, but fully driven by code/Mongo config). Negative growth penalties only depend on the `growth_pct` band and are no longer expressed as a percentage of trail in rupees.

### 5.6 Periodic Bonuses (QTD/YTD)

- Controlled by `options.periodic_bonus_enable` and `options.periodic_bonus_apply`.
- Slab on **QTD NP** (quarter) and **YTD NP** (financial year).
- Bonus added to `final_incentive` only if `period end` and `apply=true`; otherwise only reported in meta.

---

## 6) Inactive Employees Policy & Data Freezing

**Design goals:**

- Keep **all historical rows** for KPIs and analytics.
- Avoid accidental back-dated changes to payout logic.
- Treat inactive RMs fairly while still allowing eligibility overrides.

### 6.1 No TTLs — history is permanent

There are **no TTL indexes** on leaderboard collections:

- `Leaderboard_Lumpsum`
- `Lumpsum_audit`

All rows are retained indefinitely. We never physically expire documents for KPI correctness and long-term analytics.

### 6.2 Record lifecycle: Mutable → Semi-Frozen → Frozen

Each monthly leaderboard row can be in one of three conceptual states:

1. **Mutable**

   - Employee is **Active**.
   - Record is **younger than 6 months** (age measured from the record’s incentive month).
   - All business fields can be updated by the engine.

2. **Semi-Frozen**

   - Employee becomes **Inactive** (resigned/terminated) while the record is still **younger than 6 months**.
   - Kicks in only **after the calendar month is complete** (effective from the 1st of the next month).
   - Only **eligibility-related fields** may change; core numeric fields (NP, AUM, payout, bonuses) are treated as read-only.

3. **Frozen**
   - Record age is **≥ 6 months**, measured from its incentive month, and the **following calendar month has started**.
   - Applies to both active and inactive employees.
   - The row becomes fully **read-only**: no further updates to business fields are allowed.

In practice, the engine enforces this lifecycle via guards in the write path (and optionally at the DB layer via update rules / triggers).

### 6.3 Date semantics (month boundaries)

- Age is computed from the **incentive month** (e.g., `2025-01` for Jan 2025 rows).
- A row moves to **Frozen** at **00:00 on the 1st of the month** after it crosses the 6‑month mark.
- Semi-Frozen status for an inactive employee also activates from the **1st of the next month** after the inactivity date.

All comparisons are anchored to the deployment timezone (e.g., `Asia/Kolkata` for Indian FY usage).

### 6.4 Inactive employees and eligibility overrides

When a user is no longer present in the **active Zoho users** list:

- New leaderboard rows are **not** generated for future months for that employee.
- Existing rows in the last **6 months** are treated as **Semi-Frozen**:
  - `final_incentive` may be forced to zero via an **eligibility flag**.
  - Only eligibility / compliance flags are mutable (for example, to handle clawbacks, recoveries, or post-exit disputes).
- After 6 months, all their historical rows naturally transition into **Frozen** and become fully read-only.

A typical eligibility override payload for an inactive RM may look like:

```jsonc
{
  "eligible": false,
  "eligibility_meta": {
    "reason": "inactive_rm",
    "applied_on": "YYYY-MM-DD",
    "window_months": 6
  },
  "final_incentive": 0
}
```

This design keeps the **numbers** stable for KPIs while still allowing HR/Compliance to flip simple eligibility switches without touching the underlying audited calculations.

---

## 7) Audit, Provenance & Reproducibility

Each document (leaderboard & audit) stamps:

- `SchemaVersion` (code schema)
- `config_hash` (MD5 of compact effective config snapshot)
- `config_schema_version` (from config doc)
- `config_meta` (short, human‑readable subset of options)

**Audit compaction**

- `audit_mode="compact"` keeps `ByType` totals intact and truncates `ByCategory` to **top‑3 non‑zero**.
- Switch to `"full"` when deep debugging.

---

## 8) Indexes, Locks, Logging

- **Indexes**
  - `Leaderboard_Lumpsum`: unique `(employee_id, month)`; secondary index on `month` for quick purges
- **Locking**
  - Mongo‑backed TTL lock (`Job_Locks`) to avoid concurrent runs
- **Logging**
  - INFO‑level summary; prints **effective config hash** at start
  - Profiles: `noisy` | `summary` | `minimal` (ENV; non‑business knob)

---

## 9) Environment vs Mongo (Policy)

- **Mongo is authoritative** for slabs, rules, options.
- **ENV is for secrets & runtime plumbing** only (Mongo URI, Key Vault URL, log profile).
- First run will **create** a default config if missing.

---

## 10) Configuration Reference (quick glance)

| Key                                       | Where  | Type | Meaning                                      |
| ----------------------------------------- | ------ | ---- | -------------------------------------------- |
| `options.range_mode`                      | Config | enum | `"last5"` or `"fy"` windowing                |
| `options.fy_mode`                         | Config | enum | `"FY_APR"` or `"CAL"`                        |
| `options.audit_mode`                      | Config | enum | `"compact"` \| `"full"`                      |
| `options.periodic_bonus_enable`           | Config | bool | Enable QTD/YTD bonus logic                   |
| `options.periodic_bonus_apply`            | Config | bool | Actually add bonus to payout                 |
| `options.inactivity_grace_months`         | Config | int  | Months of inactivity before action           |
| `options.inactive_action`                 | Config | enum | `"purge"` \| `"freeze"` \| `"ineligible"`    |
| `options.inactive_ineligibility_months`   | Config | int  | Last N months to mark ineligible             |
| `rate_slabs[]`                            | Config | list | Growth% → rupee rate                         |
| `meeting_slabs[]`                         | Config | list | Meeting count → multiplier                   |
| `category_rules.blacklisted_categories[]` | Config | list | Tokens to match (case‑insensitive)           |
| `category_rules.match_mode`               | Config | enum | `"substring"` \| `"exact"`                   |
| `category_rules.scope[]`                  | Config | list | `"CATEGORY"`, `"SUB CATEGORY"`               |
| `category_rules.zero_weight_purchase`     | Config | bool | 0% weight on purchase if blacklisted         |
| `category_rules.zero_weight_switch_in`    | Config | bool | 0% weight on switch‑in if blacklisted        |
| `category_rules.exclude_from_debt_bonus`  | Config | bool | Exclude blacklisted buckets from debt% check |
| `qtr_bonus_template.slabs[]`              | Config | list | QTD bonus by NP                              |
| `annual_bonus_template.slabs[]`           | Config | list | YTD bonus by NP                              |

---

## 11) Minimal Examples

### 11.1 Mark Liquid categories as zero‑weight for purchase + exclude from debt%

```js
db.Config.updateOne(
  { _id: 'Leaderboard_Lumpsum' },
  {
    $set: {
      'category_rules.blacklisted_categories': ['LIQUID', 'OVERNIGHT'],
      'category_rules.zero_weight_purchase': true,
      'category_rules.exclude_from_debt_bonus': true,
    },
  }
);
```

### 11.2 Flip audit to full + enable periodic bonuses

```js
db.Config.updateOne(
  { _id: 'Leaderboard_Lumpsum' },
  {
    $set: {
      'options.audit_mode': 'full',
      'options.periodic_bonus_enable': true,
      'options.periodic_bonus_apply': true,
    },
  }
);
```

### 11.3 Inactive policy: mark last 6 months ineligible after 6‑month grace

```js
db.Config.updateOne(
  { _id: 'Leaderboard_Lumpsum' },
  {
    $set: {
      'options.inactive_action': 'ineligible',
      'options.inactivity_grace_months': 6,
      'options.inactive_ineligibility_months': 6,
    },
  }
);
```

---

## 12) Extensibility Roadmap (ideas for AI/Engineers)

- **Data‑driven slab tuning**: fit `rate_slabs` with Bayesian or quantile‑based updates.
- **Fairness guardrails**: monitor payout distribution by RM cohort; alert on drift.
- **Predictive targets**: Q‑learning or bandits to personalize quarterly targets.
- **Anomaly detection**: isolation forests on NP deltas, COB spikes, meeting outliers.
- **What‑if simulator**: run counterfactual configs and produce impact deltas before applying.

---

## 13) Troubleshooting

- Unique index conflicts → clean legacy null/empty `employee_id` rows and re‑run.
- “No active users” → inactive policy becomes a no‑op (we don’t guess).
- Slow runs → ensure indexes on `month`, `employee_id` and date filters on txn collections.
- Config not found → first run creates defaults; check logs for `[Config] Effective config hash:`.

---

## 14) Changelog (stub)

- 2025‑11‑13: Added **ineligible** inactive policy; Mongo‑configurable **category_rules**; compacted audits; `config_hash` stamping.
