import os
import pymongo
import json
from bson import json_util

uri = os.getenv("MONGODB_CONNECTION_STRING")
client = pymongo.MongoClient(uri)
db = client["PLI_Leaderboard"]

eid = "2969103000000183019" # Sagar Maini
month = "2025-12"

print(f"--- Checking for EID: {eid} Month: {month} ---")

print("\n[MF_SIP_Leaderboard]")
sip = db.MF_SIP_Leaderboard.find_one({"period_month": month, "employee_id": eid})
if sip:
    print(json.dumps(sip, default=json_util.default, indent=2))
else:
    print("No SIP Record found.")

print("\n[Leaderboard_Lumpsum]")
ls = db.Leaderboard_Lumpsum.find_one({"month": month, "employee_id": eid})
if ls:
    print(json.dumps(ls, default=json_util.default, indent=2))
else:
    print("No Lumpsum Record found.")
