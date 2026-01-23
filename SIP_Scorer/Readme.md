# SIP Scorer — Single Source of Truth

Azure Function • MongoDB • Golden Architecture

This document is the **authoritative reference** for how SIP Scorer works end‑to‑end — schemas, logic, date rules, attribution rules, reconciliation rules, fraction rules, leaderboard modes, and deployment notes.

---

## 1. Purpose

SIP Scorer computes monthly SIP performance for RMs using **Ops Exec Date**, **Reconciliation**, and **Amount logic** (full + fractions).
It powers:

- Monthly SIP Leaderboard
- Incentive Calculation Input
- RM dashboards
- MIS consistency (Ops view vs AMC view vs Incentive view)

---

## 2. Data Sources (MongoDB Collections)

### **`internal.transactions`**

The raw transaction ledger used for scorer logic.

### **`PLI_Leaderboard.MF_SIP_Leaderboard`**

Final aggregated monthly leaderboard stored after scoring.

---

## 3. Transaction Schema (with & without fractions)

### **A. No Fractions**

```
{
  _id,
  panNumber,
  investorName,
  category: "systematic",
  transactionType: "SIP",
  transactionFor: "Registration" | "Cancellation",
  amount,
  relationshipManager,
  serviceManager,
  validations: [
    {
      status: "PENDING" | "APPROVED",
      validatedAt,
      validatedBy
    }
  ],
  reconciliation: {
    reconcileStatus: "RECONCILED" | "RECONCILED_WITH_MINOR" | ...
  }
}
```

### **B. With Fractions**

```
{
  _id,
  hasFractions: true,
  transactionFractions: [
    {
      fractionAmount,
      transactionDate,     // old SIP date, not used
      orderId,
      status,
      approvalStatus,
      validations: [
        {
          status: "APPROVED",
          validatedAt,
          validatedBy
        }
      ],
      reconciliation: {
        reconcileStatus
      }
    }
  ],
  relationshipManager,
  serviceManager,
  reconciliation
}
```

---

## 4. Core Scoring Logic

The Scoring Engine processes **two major branches**:

---

## 4.1. Branch 1 — Transactions WITHOUT Fractions

### **Ops Exec Date (No‑fraction logic)**

> **Latest APPROVED validation timestamp**

```
execDate = max(validations where status="APPROVED").validatedAt
```

### **Month filter**

```
start <= execDate < end
```

### **Amount logic**

```
If Registration → +amount
If Cancellation → -amount
```

### **Reconciliation rule**

Transaction counted only if:

```
reconciliation.reconcileStatus ∈ ["RECONCILED", "RECONCILED_WITH_MINOR"]
```

### **Attribution rule**

```
If relationshipManager exists → use RM
Else → use serviceManager
```

---

## 4.2. Branch 2 — Transactions WITH Fractions

Fractions behave like **independent mini-transactions**.

### **Exec Date (Fraction logic)**

Use **fraction-level** validation timestamps:

```
execDate_fraction = max(fraction.validations where status="APPROVED").validatedAt
```

### **Month filter**

```
start <= execDate_fraction < end
```

### **Reconciliation**

Each fraction must satisfy:

```
fraction.reconciliation.reconcileStatus ∈ OK_RECON
OR parent.reconciliation.reconcileStatus ∈ OK_RECON
```

### **Amount logic**

```
Registration → +fractionAmount
Cancellation → -fractionAmount
```

---

## 5. Unified Summary of SIP Rules

### ✅ **Count only APPROVED** validations

### ✅ **Use latest APPROVED timestamp as exec date**

### ✅ **Reconciliation required (OK only)**

### ✅ **Each fraction evaluated individually**

### ✅ **RM attribution takes priority**

### ✅ **Cancellation subtracts amount**

### ❌ No use of transactionPreference

### ❌ No use of sipSwpStpDate

### ❌ No use of original 1990 dates

---

## 6. Leaderboard Modes (Month Windows)

The scorer supports three modes:

### **1) Default Month Window**

```
start = first day of month (00:00 UTC)
end   = first day of next month
```

### **2) last5 Mode**

> If today ≤ 5th → merge previous month validation tail.

### **3) last10 Mode**

> Same as last5 but threshold is 10.

Logic:

```
if today <= N (5 or 10):
    include previous month (last N days spillover)
else:
    only current month window
```

---

## 7. Output Schema (Final Leaderboard)

Stored into:
**`PLI_Leaderboard.MF_SIP_Leaderboard`**

Example:

```
{
  month: "2025-08",
  rm_name: "Sagar Maini",

  gross_sip: 840900,
  cancel_sip: 391000,
  net_sip: 449900,
  avg_sip: 5005.35,

  "SIP Points": 6478.56,
  "Lumpsum Net": -2760751.11,
  "Total Points": 6478.56,

  Tier: "T1"
}
```

---

## 8. Deployment Notes (Azure)

### **Trigger**

Runs daily with CRON schedule:

```
0 3 * * *
```

### **Runtime**

- Python 3.10
- MongoDB Atlas connection
- Azure Function Consumption or Premium

### **Logging**

Outputs:

- Total transactions processed
- RM-level summary
- Error validation counters
- Debug logs for fractions

---

## 9. Known Edge Cases & Their Decisions

### **Case: Fraction has no APPROVED validation in window**

→ **Ignore the fraction** (execDate_fraction = null)

### **Case: 1990 SIP dates**

→ Ignored. Only validations matter.

### **Case: Parent APPROVED but fraction invalid**

→ Fraction excluded.

### **Case: RM empty but SM exists**

→ Attribute to SM.

### **Case: Multi-validation transactions**

→ Latest APPROVED determines exec date.

---

## 10. Versioning

Version: **v1.0 — Final Scoring Logic**
Status: **Production Ready**

---

## 11. SIP Scorer Is Now Ready to Ship 🚀

All validation rules, fraction rules, and reconciliation rules have been verified.
Aggregations match MIS expectations.
last5 + last10 modes validated.

This README is now your **single source of truth** for reviewers, developers, QA, and future auditors.
