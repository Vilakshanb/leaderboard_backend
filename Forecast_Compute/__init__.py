import logging
import datetime
import os
import pymongo
from pymongo import UpdateOne
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info(f"Forecast Compute triggering at {utc_timestamp}")

    uri = os.getenv("MONGODB_CONNECTION_STRING")
    client = pymongo.MongoClient(uri)
    db_name = os.getenv("PLI_DB_NAME", "PLI_Leaderboard")
    db = client[db_name]

    # 1. Identify active forecast months
    # Simplification: Compute for Current Month and Next Month?
    # Or scan all events updated recently?
    # For robustness, we aggregate by (month) from Forecast_Events

    months = db.Forecast_Events.distinct("month")

    # Coefficients
    COEFF_SIP = 0.0288
    COEFF_LUMPSUM = 0.001
    COEFF_INS = 1.0

    ops = []

    for m in months:
        # Aggregation Pipeline
        # Group by Employee, Product
        # Project Probability for Channels

        logging.info(f"Processing Forecast for {m}")

        pipeline = [
            {"$match": {"month": m}},
            {"$project": {
                "employee_id": 1,
                "product": 1,
                "expected_amount": 1,
                "prob": "$probability",
                # Channels
                "prob_conservative": {"$min": ["$probability", 0.5]},
                "prob_base": "$probability",
                "prob_aggressive": {"$max": ["$probability", 0.8]}
            }},
            {"$group": {
                "_id": "$employee_id",
                "sip_base": {"$sum": {"$cond": [{"$eq": ["$product", "SIP"]}, {"$multiply": ["$expected_amount", "$prob_base"]}, 0]}},
                "sip_cons": {"$sum": {"$cond": [{"$eq": ["$product", "SIP"]}, {"$multiply": ["$expected_amount", "$prob_conservative"]}, 0]}},
                "sip_aggr": {"$sum": {"$cond": [{"$eq": ["$product", "SIP"]}, {"$multiply": ["$expected_amount", "$prob_aggressive"]}, 0]}},

                "lumpsum_base": {"$sum": {"$cond": [{"$eq": ["$product", "LUMPSUM"]}, {"$multiply": ["$expected_amount", "$prob_base"]}, 0]}},
                "lumpsum_cons": {"$sum": {"$cond": [{"$eq": ["$product", "LUMPSUM"]}, {"$multiply": ["$expected_amount", "$prob_conservative"]}, 0]}},
                "lumpsum_aggr": {"$sum": {"$cond": [{"$eq": ["$product", "LUMPSUM"]}, {"$multiply": ["$expected_amount", "$prob_aggressive"]}, 0]}},

                "ins_base": {"$sum": {"$cond": [{"$eq": ["$product", "INSURANCE"]}, {"$multiply": ["$expected_amount", "$prob_base"]}, 0]}},
                "ins_cons": {"$sum": {"$cond": [{"$eq": ["$product", "INSURANCE"]}, {"$multiply": ["$expected_amount", "$prob_conservative"]}, 0]}},
                "ins_aggr": {"$sum": {"$cond": [{"$eq": ["$product", "INSURANCE"]}, {"$multiply": ["$expected_amount", "$prob_aggressive"]}, 0]}},
            }}
        ]

        cursor = db.Forecast_Events.aggregate(pipeline)

        for doc in cursor:
            eid = doc["_id"]

            # Calculate Points per Channel
            def calc(s, l, i):
                return (s * COEFF_SIP) + (l * COEFF_LUMPSUM) + (i * COEFF_INS)

            pts_base = calc(doc["sip_base"], doc["lumpsum_base"], doc["ins_base"])
            pts_cons = calc(doc["sip_cons"], doc["lumpsum_cons"], doc["ins_cons"])
            pts_aggr = calc(doc["sip_aggr"], doc["lumpsum_aggr"], doc["ins_aggr"])

            # Prepare Upserts
            ts = datetime.datetime.utcnow()

            # Base
            ops.append(UpdateOne(
                {"employee_id": eid, "month": m, "channel": "BASE"},
                {"$set": {"forecast_points": pts_base, "computed_at": ts, "buckets": {"sip": doc["sip_base"], "lumpsum": doc["lumpsum_base"], "insurance": doc["ins_base"]}}},
                upsert=True
            ))
            # Conservative
            ops.append(UpdateOne(
                {"employee_id": eid, "month": m, "channel": "CONSERVATIVE"},
                {"$set": {"forecast_points": pts_cons, "computed_at": ts, "buckets": {"sip": doc["sip_cons"], "lumpsum": doc["lumpsum_cons"], "insurance": doc["ins_cons"]}}},
                upsert=True
            ))
            # Aggressive
            ops.append(UpdateOne(
                {"employee_id": eid, "month": m, "channel": "AGGRESSIVE"},
                {"$set": {"forecast_points": pts_aggr, "computed_at": ts, "buckets": {"sip": doc["sip_aggr"], "lumpsum": doc["lumpsum_aggr"], "insurance": doc["ins_aggr"]}}},
                upsert=True
            ))

    if ops:
        db.Forecast_Leaderboard.bulk_write(ops)
        logging.info(f"Forecast Compute: Updated {len(ops)} channel records.")
