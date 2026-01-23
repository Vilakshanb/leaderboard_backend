
import pymongo
import os
import datetime
from bson.son import SON

# Replicate the start of build_public_leaderboard_pipeline from Leaderboard/__init__.py
month = "2025-05"
start = datetime.datetime(2025, 5, 1)
end = datetime.datetime(2025, 6, 1)

client = pymongo.MongoClient("mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority")
db = client["PLI_Leaderboard_v2"]

print(f"Tracing pipeline for {month}...")

# STAGE 1: MATCH
stage1 = {
    "$match": {
        "$and": [
            {"$or": [{"period_month": month}, {"month": month}]},
            {"module": "SIP_Scorer"},
        ]
    }
}
count1 = len(list(db.MF_SIP_Leaderboard.aggregate([stage1])))
print(f"Stage 1 (Match SIP): {count1} records")

# STAGE 2: PROJECT
stage2 = {
    "$project": {
        "period_month": {"$ifNull": ["$period_month", {"$ifNull": ["$month", month]}]},
        "rm_name": 1,
        "employee_id": {"$toString": "$employee_id"},
        "bucket": {"$literal": "MF"},
    }
}
count2 = len(list(db.MF_SIP_Leaderboard.aggregate([stage1, stage2])))
print(f"Stage 2 (Project): {count2} records")

# SKIP UNIONS FOR NOW (Assume they just add, don't filter)
# Let's jump to Group
stage6 = {
    "$group": {
        "_id": {"rm_name": "$rm_name", "employee_id": "$employee_id", "m": "$period_month"},
        "mf_count": {"$sum": 1}
    }
}
# Note: Unions happen before group in real pipeline. If I skip them, I only verify SIP flow.
# That is sufficient for Kawal since he is in SIP.
pipeline_prefix = [stage1, stage2, stage6]
count6 = len(list(db.MF_SIP_Leaderboard.aggregate(pipeline_prefix)))
print(f"Stage 6 (Group SIP-only): {count6} records")

# STAGE 8: LOOKUP ZOHO
stage8 = {
    "$lookup": {
        "from": "Zoho_Users",
        "let": {"emp": {"$toString": "$_id.employee_id"}},
        "pipeline": [
            {"$match": {"$expr": {"$eq": [{"$toString": "$id"}, "$$emp"]}}},
            {"$project": {"status": "$status", "active": "$active"}} # limited fields for debug
        ],
        "as": "zu",
    }
}
pipeline_prefix.append(stage8)
count8 = len(list(db.MF_SIP_Leaderboard.aggregate(pipeline_prefix)))
print(f"Stage 8 (Lookup Zoho): {count8} records")

# STAGE 9: CALC FIELDS (simplified for debug)
stage9 = {
    "$addFields": {
        "is_active": False, # Mock
        "skip_by_inactive_no_empid": {
             "$let": {
                 "vars": {
                     "st": {"$toLower": {"$ifNull": [{"$first": "$zu.status"}, ""]}},
                     "empid": {"$ifNull": [{"$first": "$zu.employee_id"}, ""]}
                 },
                 "in": {
                     "$and": [
                         {"$eq": ["$$st", "inactive"]},
                         {"$eq": ["$$empid", ""]}
                     ]
                 }
             }
        }
    }
}
pipeline_prefix.append(stage9)

# STAGE 10: MATCH VALID INVALID
stage10 = {
    "$match": {
        "skip_by_inactive_no_empid": {"$ne": True}
    }
}
pipeline_prefix.append(stage10)
count10 = len(list(db.MF_SIP_Leaderboard.aggregate(pipeline_prefix)))
print(f"Stage 10 (Final Filter): {count10} records")

if count10 == 0:
    print("Dropped at Stage 10! Checking sample doc before filter...")
    # debug last state
    debug_cursor = db.MF_SIP_Leaderboard.aggregate(pipeline_prefix[:-1])
    for doc in debug_cursor:
        print("Pre-Filter Doc:")
        print(doc)
