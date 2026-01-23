import os
import pymongo
import datetime
import json
from bson import json_util

uri = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/PLI_Leaderboard?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
db = client["PLI_Leaderboard"]

months = ["2025-12"]

print("\n--- Breakdown Test (Dec) ---")
target = db.Public_Leaderboard.find_one({'period_month': '2025-12'})
if target:
    print(json.dumps(target, default=json_util.default, indent=2))
else:
    print("No target found for 2025-12")

target = db.Zoho_Users.find_one({'id': '2969103000154276001'})
if target:
    print('Found Email:', target.get('Email'))
else:
    print('No User Found')

