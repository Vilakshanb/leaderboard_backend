#!/usr/bin/env python3
"""
Reset and seed PLI_Leaderboard_v2 with SOURCE DATA fixtures for E2E scoring tests.

This script:
1. Drops all collections in PLI_Leaderboard_v2.
2. Seeds Zoho_Users (employee master).
3. Seeds SOURCE transaction collections (purchase_txn, redemption_txn, etc.) for Lumpsum.
4. Seeds SOURCE SIP/SWP transactions for SIP scorer.
5. Does NOT seed output collections (Leaderboard_Lumpsum, etc.) - the scorers must generate these.

Usage:
    python tools/reset_seed_v2.py
"""

import os
import sys
from datetime import datetime, timedelta
from pymongo import MongoClient

# Safety check
DB_NAME = "PLI_Leaderboard_v2"
def seed_data():
    MONGO_URI = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")

    if not MONGO_URI:
        print("ERROR: MongoDb-Connection-String env var not set")
        sys.exit(1)

    print(f"Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print(f"Target database: {DB_NAME}")

    # Only verify interactions if running as script, or pass a flag.
    # For now, we trust the caller (like the test runner) to handle safety or accept force mode.
    # Check env var for confirmation or interactive mode
    if os.getenv("CONFIRM_DROP") != "yes":
        print(f"WARNING: This will DROP all collections in {DB_NAME}")
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    # Drop all collections
    print(f"\nDropping all collections in {DB_NAME}...")
    for collection_name in db.list_collection_names():
        db[collection_name].drop()
        print(f"  Dropped: {collection_name}")

    # Explicitly drop known collections to be safe against list_collection_names lagging
    db.Zoho_Users.drop()
    db.AUM_Report.drop()
    db.purchase_txn.drop()
    db.redemption_txn.drop()
    db.transactions.drop()
    db.Admin_Permissions.drop()
    db.config.drop()
    db.Leaderboard_Lumpsum.drop()
    db.MF_SIP_Leaderboard.drop()
    db.Public_Leaderboard.drop()

    print("\nSeeding fixture data...")

    # ============================================================================
    # 1. Zoho_Users (Employee master data)
    # ============================================================================
    print("\n[1/7] Seeding Zoho_Users...")
    zoho_users = [
        {
            "_id": "test_emp_001",
            "Full Name": "Test Employee One",
            "email": "test1@example.com",
            "employee_id": "EMP001",
            "id": "EMP001",
            "Active": "active",
            "status": "active"
        },
        {
            "_id": "test_emp_002",
            "Full Name": "Test Employee Two",
            "email": "test2@example.com",
            "employee_id": "EMP002",
            "id": "EMP002",
            "Active": "active",
            "status": "active"
        },
        {
            "_id": "test_emp_003",
            "Full Name": "Test Employee Inactive",
            "email": "test3@example.com",
            "employee_id": "EMP003",
            "id": "EMP003",
            "Active": "inactive",
            "status": "inactive",
            "inactive_since": datetime(2025, 10, 1) # Inactive before our test month
        },
        {
            "_id": "test_emp_gate",
            "Full Name": "Test Gate Edge Case",
            "email": "testgate@example.com",
            "employee_id": "EMPGATE",
            "id": "EMPGATE",
            "Active": "active",
            "status": "active"
        },
        {
            "_id": "test_emp_zero",
            "Full Name": "Test Zero Data",
            "email": "testzero@example.com",
            "employee_id": "EMPZERO",
            "id": "EMPZERO",
            "Active": "active",
            "status": "active"
        }
    ]
    db.Zoho_Users.insert_many(zoho_users)
    print(f"  Inserted {len(zoho_users)} users")

    # Test Month: November 2025
    test_month_start = "2025-11-01" # For AUM
    test_month_dt = datetime(2025, 11, 1)

    # ============================================================================
    # 2. AUM_Report (Source for Lumpsum AUM)
    # ============================================================================
    print("\n[2/7] Seeding AUM_Report...")
    # Lumpsum Scorer expects: "MAIN RM" (uppercase), "Month" (YYYY-MM-DD), "Amount"
    aum_records = [
        # Employee 1: 6 Crore AUM (Base for NP calc)
        {"MAIN RM": "TEST EMPLOYEE ONE", "Month": test_month_start, "Amount": 60000000},
        # Employee 2: 10 Crore
        {"MAIN RM": "TEST EMPLOYEE TWO", "Month": test_month_start, "Amount": 100000000},
        # Inactive guy
        {"MAIN RM": "TEST EMPLOYEE INACTIVE", "Month": test_month_start, "Amount": 40000},
        # Gate guy: 50 Lakh AUM (Boundary for gate?)
        {"MAIN RM": "TEST GATE EDGE CASE", "Month": test_month_start, "Amount": 5000000},
        # Zero guy
        {"MAIN RM": "TEST ZERO DATA", "Month": test_month_start, "Amount": 0},
    ]
    db.AUM_Report.insert_many(aum_records)
    print(f"  Inserted {len(aum_records)} AUM records")

    # ============================================================================
    # 3. purchase_txn (Lumpsum Additions)
    # ============================================================================
    print("\n[3/7] Seeding purchase_txn...")
    # Needs: "RM Name", "Amount", "Trxn Date"
    # Lumpsum Scorer normalizes RM Name
    purchase_txns = [
        # Employee 1: +1.5 Crore Purchase -> Total NP should be huge
        {
            "RM Name": "TEST EMPLOYEE ONE",
            "Amount": 15000000,
            "Trxn Date": datetime(2025, 11, 15)
        },
        # Employee 2: Small purchase
        {
            "RM Name": "TEST EMPLOYEE TWO",
            "Amount": 10000,
            "Trxn Date": datetime(2025, 11, 10)
        }
    ]
    db.purchase_txn.insert_many(purchase_txns)
    print(f"  Inserted {len(purchase_txns)} purchase records")

    # ============================================================================
    # 4. redemption_txn (Lumpsum Subtractions)
    # ============================================================================
    print("\n[4/7] Seeding redemption_txn...")
    redemption_txns = [
        # Employee Inactive: Big redemption
        {
            "RM Name": "TEST EMPLOYEE INACTIVE",
            "Amount": 10000000,
            "Trxn Date": datetime(2025, 11, 5)
        },
        # Gate guy: Redemption > Purchase (Net Negative)
        {
            "RM Name": "TEST GATE EDGE CASE",
            "Amount": 500000,
            "Trxn Date": datetime(2025, 11, 20)
        }
    ]
    db.redemption_txn.insert_many(redemption_txns)
    print(f"  Inserted {len(redemption_txns)} redemption records")

    # ... switch, cob (leaving empty for now to keep simple)

    # ============================================================================
    # 5. transactions (SIP Scorer Source)
    # ============================================================================
    print("\n[5/7] Seeding transactions (for SIP Scorer)...")
    # SIP Scorer BuildTxnDF queries 'transactions' collection
    # Needs: 'category': 'systematic', 'transactionType': 'Link', 'transactionFor': 'Purchase'
    # 'reconciliation.reconcileStatus': 'RECONCILED'
    # 'validations': [{'validatedAt': ..., 'status': 'approved'}]
    # 'relationshipManager': "Name"

    sip_txns = [
         # Employee 1: 1.5 Lakh SIP Registration
        {
            "category": "systematic",
            "transactionType": "SIP",
            "transactionFor": "Registration",
            "relationshipManager": "Test Employee One", # Matches Zoho Name
            "amount": 150000,
            "hasFractions": False,
            "reconciliation": {"reconcileStatus": "RECONCILED"},
            "validations": [
                {
                    "validatedAt": datetime(2025, 11, 10),
                    "status": "APPROVED" # Case insensitive check in scorer? usually upper
                }
            ],
            "createdAt": datetime(2025, 11, 10)
        },
        # Employee 1: 20k SIP Cancellation
        {
            "category": "systematic",
            "transactionType": "SIP",
            "transactionFor": "Cancellation",
            "relationshipManager": "Test Employee One",
            "amount": 20000,
            "hasFractions": False,
            "reconciliation": {"reconcileStatus": "RECONCILED"},
            "validations": [
                {
                    "validatedAt": datetime(2025, 11, 12),
                    "status": "APPROVED"
                }
            ],
            "createdAt": datetime(2025, 11, 12)
        },
        # Gate Guy: 80k SIP Reg
        {
            "category": "systematic",
            "transactionType": "SIP",
            "transactionFor": "Registration",
            "relationshipManager": "Test Gate Edge Case",
            "amount": 80000,
            "hasFractions": False,
            "reconciliation": {"reconcileStatus": "RECONCILED"},
            "validations": [
                {
                    "validatedAt": datetime(2025, 11, 15),
                    "status": "APPROVED"
                }
            ],
            "createdAt": datetime(2025, 11, 15)
        }
    ]
    db.transactions.insert_many(sip_txns)
    print(f"  Inserted {len(sip_txns)} SIP transaction records")

    # ============================================================================
    # 6. Admin_Permissions (Minimal)
    # ============================================================================
    print("\n[6/7] Seeding Admin_Permissions...")
    db.Admin_Permissions.insert_one({"email": "admin@example.com", "role": "admin"})
    print(f"  Inserted 1 admin permission records")

    print("\n======================================================================")
    print("✓ Source Data Seed Complete!")
    print("======================================================================")
    print(f"Database: {DB_NAME}")
    print("Test Month: 2025-11")
    print("Ready for Scoring Runner.")


if __name__ == "__main__":
    seed_data()
