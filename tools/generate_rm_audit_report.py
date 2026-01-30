
import pymongo
import datetime

def generate_audit_report():
    client = pymongo.MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))
    db = client["PLI_Leaderboard_v2"]

    # 1. Identify Inactive RMs
    inactive_query = {
        "$or": [
            {"status": {"$ne": "Active"}},
            {"Status": {"$ne": "Active"}},
            {"active": False},
            {"is_active": False},
            {"IsActive": False}
        ]
    }
    inactive_users = list(db.Zoho_Users.find(inactive_query))

    audit_logs = []
    report_rows = []

    print(f"Found {len(inactive_users)} inactive/suspended users in Zoho_Users.")

    for user in inactive_users:
        name = user.get("Full Name") or user.get("Name") or "Unknown"
        emp_id = user.get("id") or user.get("employee_id")
        status = user.get("status") or user.get("Status") or ("Inactive" if not user.get("active") else "Unknown")
        inactive_since = user.get("inactive_since")

        # Create Deactivation Log
        if inactive_since:
            log_entry = {
                "employee_id": str(emp_id),
                "employee_name": name,
                "action": "System_Deactivation",
                "effective_date": inactive_since,
                "reason": "Lifecycle Management (Inferred)",
                "status": "inactive",
                "timestamp": datetime.datetime.now()
            }
            audit_logs.append(log_entry)
            report_rows.append([name, "Deactivation", inactive_since, "Inactive"])

        # Check Visibility in Public_Leaderboard (Visibility Restoration)
        # We check a recent month where they might be inactive but visible (e.g. May for Kawal)
        pb_count = db.Public_Leaderboard.count_documents({"rm_name": name})

        if pb_count > 0:
            log_entry = {
                "employee_id": str(emp_id),
                "employee_name": name,
                "action": "Data_Visibility_Restored",
                "effective_date": datetime.datetime.now(),
                "reason": "Historical Rebuild Fix",
                "status": "visible_historic",
                "timestamp": datetime.datetime.now()
            }
            audit_logs.append(log_entry)
            report_rows.append([name, "Visibility Restored", "FY25-26", f"Visible ({pb_count} months)"])

    # 2. Persist to RM_Audit_Logs
    if audit_logs:
        db.RM_Audit_Logs.drop() # Reset for this "First Report" generation
        db.RM_Audit_Logs.insert_many(audit_logs)
        print(f"Persisted {len(audit_logs)} entries to RM_Audit_Logs.")


    # 3. Print Report
    print("\n=== RM Audit Log Report (Generated) ===\n")
    print(f"{'Employee':<25} | {'Action':<25} | {'Effective Date':<20} | {'Current State':<25}")
    print("-" * 105)
    for row in report_rows:
        emp, action, date_val, state = row
        date_str = str(date_val) if date_val else "N/A"
        print(f"{emp:<25} | {action:<25} | {date_str:<20} | {state:<25}")

if __name__ == "__main__":
    generate_audit_report()
