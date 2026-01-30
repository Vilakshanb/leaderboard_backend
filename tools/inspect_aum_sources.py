
import os
import sys
import pymongo
from datetime import datetime

# Setup Env
# os.environ["MONGODB_CONNECTION_STRING"] = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/PLI_Leaderboard?retryWrites=true&w=majority"
# os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"

def get_db():
    uri = os.getenv("MONGODB_CONNECTION_STRING")
    client = pymongo.MongoClient(uri)
    db_name = os.environ["PLI_DB_NAME"]
    return client[db_name]

if __name__ == "__main__":
    # Note: SIP uses 'rm_name', Lumpsum uses 'employee_name'/'employee_alias'
    # Arpit Gupta is a good candidate.
    # inspect_user("Arpit Gupta", month="2025-04") # Original call, commented out for new logic

    db = get_db() # Get the database connection
    name = "Arpit Gupta" # Example name
    month = "2025-04" # Example month

    # 2. Leaderboard_Lumpsum
    lump = db.Leaderboard_Lumpsum.find_one({"employee_name": name, "month": month})
    if not lump:
        lump = db.Leaderboard_Lumpsum.find_one({"employee_alias": name, "month": month})
    if lump:
        print(f"Lumpsum Collection AUM (AUM (Start of Month)): {lump.get('AUM (Start of Month)')}")
    else:
        print(f"No Lumpsum data found for {name} in {month}")

    # 1. MF_SIP_Leaderboard (Matches by rm_name usually)
    sip = db.MF_SIP_Leaderboard.find_one({"rm_name": {"$regex": "Arpit", "$options": "i"}, "period_month": month})
    if sip:
        print(f"SIP Collection: {sip['rm_name']}")
        print(f"SIP Collection AUM (aum_start): {sip.get('aum_start')}")
    else:
        print(f"No SIP data found for {name} in {month}")
