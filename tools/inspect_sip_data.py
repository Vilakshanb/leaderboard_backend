
import pymongo
import os

client = pymongo.MongoClient("mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority")
db = client["PLI_Leaderboard_v2"]

print("Connected to PLI_Leaderboard_v2")

cursor = db.MF_SIP_Leaderboard.find().limit(2)
for doc in cursor:
    print(f"ID: {doc.get('_id')}")
    print(f"Month (period_month): {doc.get('period_month')}")
    print(f"Month (month): {doc.get('month')}")
    print(f"Module: {doc.get('module')}")
    print(f"Keys: {list(doc.keys())}")
    print("---")
