
# import os
import pymongo

def check_may_data():
    try:
        # client = pymongo.MongoClient(os.getenv("MongoDb-Connection-String"))
        client = pymongo.MongoClient("mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority")
        db = client["PLI_Leaderboard_v2"]

        print(f"Connected to {db.name}")

        # Check Public_Leaderboard for 2025-05
        count = db.Public_Leaderboard.count_documents({"month": "2025-05"})
        print(f"Public_Leaderboard count for 2025-05: {count}")

        if count > 0:
            cursor = db.Public_Leaderboard.find({"month": "2025-05"}).limit(5)
            print("Sample data (first 5):")
            for i, doc in enumerate(cursor):
                print(f"Total: {doc.get('total_points_final')}")
        else:
            print("No data found for 2025-05 in Public_Leaderboard")

        # Check Leaderboard_Lumpsum
        l_count = db.Leaderboard_Lumpsum.count_documents({"month": "2025-05"})
        print(f"Leaderboard_Lumpsum count for 2025-05: {l_count}")

        # Check SIP
        s_count = db.MF_SIP_Leaderboard.count_documents({"month": "2025-05"})
        print(f"MF_SIP_Leaderboard count for 2025-05: {s_count}")

        # Check Zoho_Users count
        u_count = db.Zoho_Users.count_documents({})
        print(f"Zoho_Users count: {u_count}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_may_data()
