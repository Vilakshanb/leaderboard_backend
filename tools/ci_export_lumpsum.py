import sys
import os
import json
from pymongo import MongoClient

def main():
    uri = os.environ.get('MONGO_URI')
    if not uri:
        print("Error: MONGO_URI not set")
        sys.exit(1)

    client = MongoClient(uri)
    db = client.PLI_Leaderboard_v2

    # Export __engine__Leaderboard_Lumpsum
    docs = list(db['__engine__Leaderboard_Lumpsum'].find({}))

    # Normalize (remove _id, timestamps, etc.)
    IGNORE_FIELDS = {'_id', 'createdAt', 'created_at', 'updatedAt', 'updated_at', 'updated_at_audit', 'version', '__v', 'config_hash', 'AuditMeta'}

    def normalize(val):
        if val is None or val == '':
            return val
        if isinstance(val, list):
            return [normalize(v) for v in val]
        if isinstance(val, dict):
            return {k: normalize(v) for k, v in val.items() if k not in IGNORE_FIELDS}
        return val

    normalized = [normalize(doc) for doc in docs]
    normalized.sort(key=lambda x: (x.get('employee_id') or '', x.get('month') or ''))

    os.makedirs('engine_artifacts', exist_ok=True)
    with open('engine_artifacts/Leaderboard_Lumpsum.json', 'w') as f:
        json.dump(normalized, f, indent=2, default=str, sort_keys=True)

    print(f'Exported {len(normalized)} records from __engine__Leaderboard_Lumpsum')

if __name__ == "__main__":
    main()
