from pymongo import MongoClient
import os

CONN = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority"
c = MongoClient(CONN)
db_v2 = c['PLI_Leaderboard_v2']

print("Updating Zoho_Users in v2...")
# Using pipeline update to copy field
res = db_v2.Zoho_Users.update_many(
    {"Full Name": {"$exists": False}},
    [{"$set": {"Full Name": "$full_name"}}]
)
print(f"Modified {res.modified_count} docs")

# Verify
sample = db_v2.Zoho_Users.find_one({"Full Name": {"$exists": True}})
if sample:
    print(f"Sample verification: id={sample.get('id')}, Full Name='{sample.get('Full Name')}'")
else:
    print("Warning: No docs have 'Full Name' after update?")
