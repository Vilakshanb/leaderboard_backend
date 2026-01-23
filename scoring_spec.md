# PLI Leaderboard Scoring Specification
**Version:** 1.0.0
**Status:** Authoritative
**Source of Truth:** Codebase (`SIP_Scorer`, `Lumpsum_Scorer`, `Insurance_scorer`, `Leaderboard`)

---

## 1. Metrics & Scoring Weights

The leaderboard aggregates points from three primary buckets: **Mutual Funds (SIP + Lumpsum)**, **Insurance**, and **Referrals**.

### 1.1 Mutual Funds (Bucket: `MF`)

**Total MF Points** = `SIP Points` + `Lumpsum Points`

#### A. SIP Scoring
*   **Metric:** Net SIP (in INR).
*   **Formula:** `Net SIP` * `SIP_POINTS_COEFF`.
*   **Coefficient:** **2.88 Points per ₹1 Net SIP**.
*   **Net SIP Definition:**
    *   Typically `SIP Only` (excluding SWP), unless configured otherwise.
    *   **Time Horizon:** 24 months (default).
*   **Tiers (for Payout Factors):**
    *   Based on **Total Points** (SIP + Lumpsum + Insurance + Referrals? *Correction: Tiers are usually calculated on Total Points but applied to AUM/SIP payouts. Code in `SIP_Scorer` suggests Tiers T1-T6 based on Total Points thresholds.*)
    *   **Thresholds:**
        *   **T6**: ≥ 60,000 pts
        *   **T5**: ≥ 40,000 pts
        *   **T4**: ≥ 25,000 pts
        *   **T3**: ≥ 15,000 pts
        *   **T2**: ≥ 8,000 pts
        *   **T1**: ≥ 2,000 pts
        *   **T0**: < 2,000 pts

#### B. Lumpsum Scoring
*   **Metric:** Net Lumpsum Purchase (in INR).
*   **Formula:** `Net Lumpsum` * `LUMPSUM_POINTS_COEFF`.
*   **Coefficient:** **0.1 Points per ₹100 Net Lumpsum** (Code: `0.001` per ₹1, i.e., 1 point per ₹1000).
    *   *Correction/Verification*: `LUMPSUM_POINTS_COEFF_DEFAULT = 0.001` (1 pt per 1000 INR). `SIP_POINTS_COEFF = 0.0288` (2.88 pts per 100 INR).
    *   **Standardized**:
        *   SIP: **0.0288 pts / INR**
        *   Lumpsum: **0.001 pts / INR**
*   **Gate / Penalty:**
    *   **Lumpsum Gate:** If `Net Lumpsum` < **-3%** of AUM (Start) **OR** `Net Lumpsum` (absolute negativity) < **₹50,000**, then **Lumpsum Points = 0** (or blocked).
    *   **Negative Growth Penalty (Growth Slab v1):**
        *   **Band 1 (Growth ≤ -1.0%)**: Penalty = Min(₹5,000, 0.5% of Monthly Trail).
        *   **Band 2 (-1.0% < Growth ≤ -0.5%)**: Penalty = ₹2,500.

### 1.2 Insurance (Bucket: `INS`)

*   **Metric:** Fresh Premium (pre-GST).
*   **Formula:** `Fresh Premium` * `INS_POINTS_COEFF`.
*   **Coefficient:** **1 Point per ₹1 Fresh Premium** (Implicit 1:1).
*   **Scope:** Only "Fresh" premiums count. Renewals excluded from leaderboard points.

### 1.3 Referrals (Bucket: `REF`)

*   **Metric:** Verified Referral Events.
*   **Points:**
    *   **Insurance Referral:** **30 Points** per converted lead (when Referrer ≠ Converter).
    *   **Converter:** Gets **50 Points**.
    *   **Self-Sourced:** Gets **100 Points** (Usually credited as Insurance Points).

---

## 2. Leader & Periodic Bonuses

### 2.1 Leader Bonuses (Manual overrides/regex)
*   **Insurance Leader:** "Sumit C" (or matched by ID). Boosts Insurance Slab.
*   **Mutual Fund Leader:** "Sagar M" (or matched by ID). Boosts MF Tier.

### 2.2 Periodic Bonuses (Lumpsum Scorer)
*   **Quarterly Bonus:**
    *   **Condition:** Min positive months (default 2) in quarter.
    *   **Template:** Slabs based on Net Purchase (NP).
*   **Annual Bonus:**
    *   **Condition:** Min positive months (default 6) in FY.
    *   **Template:** Slabs based on Net Purchase (NP).

---

## 3. Time Windows & Eligibility

### 3.1 Time Definition
*   **Time Zone:** UTC (Hardware/System), IST (Business Logic/Dates).
*   **FY Definition:**
    *   **FY_APR:** April 1st to March 31st (Default).
    *   **CAL:** Jan 1st to Dec 31st (Optional).

### 3.2 Eligibility & Period Lock
*   **Active Status:** Must be "Active" in `Zoho_Users` for Public Leaderboard visibility.
*   **Inactive Window:**
    *   Inactive employees eligible for **6 months** post-exit (`inactive_since`).
    *   **Logic:** Eligible if `0 <= (CurrentMonth - InactiveMonth) < 6`.
*   **Locking:**
    *   Historical months can be re-run; audit logs track calculation timestamps.

---

## 4. Audit Rules

*   **Collection:** `Public_Leaderboard` (and internal `Rupee_Incentives`).
*   **Audit Field:** `audit` object.
    *   **`buckets`**: Points breakdown (MF, INS, REF).
    *   **`sources`**: Traceability to source collections.
    *   **`inactive_block`**: Timestamp and reason if payout is blocked by inactive rules.
