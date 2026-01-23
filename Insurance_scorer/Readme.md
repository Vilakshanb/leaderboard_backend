# Insurance Scorer — Incentives Engine (Human + AI Handbook)

Azure Function • MongoDB • Golden Architecture

This document is the authoritative reference for how Insurance Scorer works end‑to‑end — schemas, config, date rules, attribution rules, points logic, leaderboard behavior, and deployment notes.

## 1. Purpose & Scope

Insurance Scorer computes monthly insurance performance for Relationship Managers (RMs) and converts it into points suitable for the main Leaderboard engine.

It covers:

- Health Insurance (fresh, renewal, portability, upsell)
- Motor Insurance
- Term Insurance
- SME / Corporate Insurance

The scorer:

- Aggregates policy‑level data
- Normalizes RM identity using RM master + aliases
- Classifies policies into canonical categories
- Applies category‑wise slabs to convert production into points
- Applies optional meeting multipliers
- Writes results to `PLI_Leaderboard.Insurance_Leaderboard`

## 2. Input Collections (MongoDB)

From `iwell` DB:

- `insurance_master` — raw policy data
- `user_master` — RM identity + alias mapping
- `Investor_Meetings_Data` — optional meeting multiplier source

## 3. Output Collections

- `Insurance_Leaderboard` (primary)
- Optional audit collection if audit_mode = full

Each document is keyed by month + employee_id.

## 4. Runtime Flow

```
run_insurance_scorer()
 ├─ ensure schema registry: Schemas/Insurance_Schema
 ├─ ensure runtime config: config/Leaderboard_Insurance
 ├─ build effective config snapshot → config_hash
 ├─ resolve windows (month / last5 / fy)
 ├─ for each window:
 │    ├─ load policies
 │    ├─ normalize RM
 │    ├─ classify into categories
 │    ├─ aggregate premium + counts
 │    ├─ apply slab points
 │    ├─ apply meeting multiplier
 │    └─ upsert Insurance_Leaderboard
 └─ exit
```

## 5. Runtime Config (Mongo) — `config/Leaderboard_Insurance`

Automatically created if missing.

Structure:

```
{
  "options": {
    "range_mode": "month",
    "fy_mode": "FY_APR",
    "audit_mode": "compact",
    "use_rm_owner_field": true,
    "fallback_to_alias": true,
    "include_cancelled": false,
    "include_lapsed": false
  },

  "category_map": {
    "health_keywords": [...],
    "motor_keywords": [...],
    "term_keywords": [...],
    "sme_keywords": [...]
  },

  "slabs": {
    "health": {
      "fresh_points": 400,
      "renewal_points": 200,
      "portability_points": 250,
      "upsell_pct_bonus": 0.05
    },
    "motor": {
      "fresh_points": 200,
      "renewal_points": 120
    },
    "term": {
      "fresh_points": 600
    },
    "sme": {
      "fresh_points": 500,
      "renewal_points": 250
    }
  },

  "premium_scaling": {
    "enable": false,
    "health_points_per_lakh": 40,
    "motor_points_per_lakh": 20,
    "term_points_per_lakh": 50,
    "sme_points_per_lakh": 45
  },

  "meeting_slabs": [
    { "max_count": 5,  "mult": 1.0,  "label": "0–5" },
    { "max_count": 10, "mult": 1.05, "label": "6–10" },
    { "max_count": 15, "mult": 1.075, "label": "11–15" },
    { "max_count": null, "mult": 1.1, "label": "16+" }
  ]
}
```

Every run stamps:

- `config_hash` (MD5 of effective config)
- `config_schema_version`

## 6. Schema Registry — `Schemas/Insurance_Schema`

Automatically created if missing.

Defines expected fields:

```
month, employee_id, employee_name, employee_alias,
GrossPremium, NetPremium,
CategoryBreakup.{Health,Motor,Term,SME,Other},
CountBreakup.{...},
PointsBreakup.{...},
TotalPoints,
meetings_count, meetings_slab, meetings_multiplier,
Audit {...},
config_hash, config_schema_version,
module, createdAt, updatedAt
```

The scorer enforces and writes documents according to this schema.

## 7. Policy Classification

Policy text fields are matched (case‑insensitive) with keyword lists from `category_map`.
Unmatched → category = "Other".

## 8. Scoring Logic (Business Layer)

- Health fresh → slabs.health.fresh_points
- Health renewal → slabs.health.renewal_points
- Portability → portability_points
- Upsell → renewal_points × (1 + upsell_pct_bonus)

Motor, Term, SME follow their slabs.
Optional `premium_scaling` adds extra points per lakh of premium.

`TotalPoints_before_meetings = Σ category points`.

If `meeting_slabs` enabled:

```
TotalPoints = TotalPoints_before_meetings × meetings_multiplier
```

## 9. Windowing

Month boundaries:

- WindowStart = YYYY‑MM‑01
- WindowEnd = next month YYYY‑(MM+1)‑01

Range determined by:

- options.range_mode (month / last5 / fy)
- options.fy_mode

## 10. Insurance_Leaderboard Document Example

```
{
  "_id": "2025-04_sagar_maini",
  "Metric": "Insurance",
  "month": "2025-04",

  "employee_id": "...",
  "employee_name": "SAGAR MAINI",
  "employee_alias": "sagar maini",

  "GrossPremium": 1250000,
  "NetPremium": 1180000,

  "CategoryBreakup": {
    "Health": 900000,
    "Motor": 250000,
    "Term": 100000,
    "SME": 0,
    "Other": 0
  },

  "CountBreakup": {
    "Health": 27,
    "Motor": 9,
    "Term": 3,
    "SME": 0,
    "Other": 0
  },

  "PointsBreakup": {
    "Health": 10800,
    "Motor": 1800,
    "Term": 1800,
    "SME": 0,
    "Other": 0
  },

  "TotalPoints": 14400,
  "meetings_count": 11,
  "meetings_slab": "11–15",
  "meetings_multiplier": 1.075,

  "Audit": {
    "WindowStart": "2025-04-01",
    "WindowEnd": "2025-04-30",
    "HasActivity": true,
    "ZeroTransactionWindow": false
  },

  "config_hash": "...",
  "config_schema_version": "2025-11-15.r1",
  "module": "Insurance_Scorer",
  "createdAt": "...",
  "updatedAt": "..."
}
```

## 11. Reproducibility Contract

Insurance scorer follows the Golden Architecture:

```
Inputs + Config + Code = Reproducible Output
```

`config_hash` makes every run traceable and auditable.

## 12. Deployment Notes

- Runs as Azure Function (manual or CRON)
- Mongo lock key: `insurance-scorer`
- Idempotent → safe to rerun windows
- All updates via `upsert`
