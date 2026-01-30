import os
import pymongo
import datetime
import json
from bson import json_util

uri = os.getenv("MONGODB_CONNECTION_STRING")
client = pymongo.MongoClient(uri)
db = client["PLI_Leaderboard"]

# ID from previous Breakdown Test (Sumit)
target = db.Zoho_Users.find_one({'id': '2969103000154276001'})
if target:
    print('Found Keys:', list(target.keys()))
    print('Email:', target.get('Email'))
    print('email:', target.get('email'))
else:
    print('No User Found for 2969103000154276001. Trying string vs int...')
    target = db.Zoho_Users.find_one({'id': 2969103000154276001})
    if target:
        print('Found Keys (Int):', list(target.keys()))
        print('Email:', target.get('Email'))
    else:
        print('Still No User Found')
