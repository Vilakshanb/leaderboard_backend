
import pymongo
import os
import pprint

def debug_kawal():
    client = pymongo.MongoClient("mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority")
    db = client["PLI_Leaderboard_v2"]

    print("--- Public_Leaderboard (May 2025) ---")
    # Fuzzy search for Kawal
    query = {
        "period_month": "2025-05",
        "rm_name": {"$regex": "Kawal", "$options": "i"}
    }
    cursor = db.Public_Leaderboard.find(query)
    found_bs = False
    for doc in cursor:
        found_bs = True
        print(f"RM: {doc.get('rm_name')}")
        print(f"Month: {doc.get('period_month')}")
        print(f"Is Active: {doc.get('is_active')}")
        print(f"Total Points: {doc.get('total_points_public')}")
        print("---")

    if not found_bs:
        print("No Public_Leaderboard records found for Kawal in May 2025")

    print("\n--- Zoho_Users (Kawal) ---")
    z_query = {
        "$or": [
            {"Full Name": {"$regex": "Kawal", "$options": "i"}},
            {"Name": {"$regex": "Kawal", "$options": "i"}},
            {"Email": {"$regex": "kawal", "$options": "i"}}
        ]
    }
    z_cursor = db.Zoho_Users.find(z_query)
    found_zoho = False
    for doc in z_cursor:
        found_zoho = True
        pprint.pprint(doc)

    if not found_zoho:
        print("No Zoho User found for Kawal")

if __name__ == "__main__":
    debug_kawal()
