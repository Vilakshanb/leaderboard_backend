"""
Extract Legacy Configuration to Match V2
"""
import os
from pymongo import MongoClient
import json

MONGO_URI = (
    os.getenv('MongoDb-Connection-String')
    or os.getenv('MONGO_CONN')
    or os.getenv('MONGO_URI')
    or os.getenv('MONGODB_URI')
)

if not MONGO_URI:
    raise ValueError("MongoDB connection string not found.")

client = MongoClient(MONGO_URI)

legacy_db = client['PLI_Leaderboard']
v2_db = client['PLI_Leaderboard_v2']

print("="*120)
print("EXTRACTING LEGACY CONFIGURATION")
print("="*120)

# Get a sample legacy record to extract config
legacy_record = legacy_db['Leaderboard_Lumpsum'].find_one({'month': '2025-04'})

if not legacy_record:
    print("ERROR: No legacy records found")
    exit(1)

print(f"\nLegacy Config Hash: {legacy_record.get('config_hash')}")
print(f"Legacy Config Schema Version: {legacy_record.get('config_schema_version')}")

# Check if there's a config collection
legacy_config_coll = legacy_db['config']
v2_config_coll = v2_db['config']

print(f"\n{'='*120}")
print("LEGACY CONFIG COLLECTION")
print(f"{'='*120}")

legacy_configs = list(legacy_config_coll.find({'config_type': 'lumpsum_weights'}))
print(f"\nFound {len(legacy_configs)} lumpsum config(s) in Legacy")

for idx, cfg in enumerate(legacy_configs):
    print(f"\nConfig {idx + 1}:")
    print(f"  Config ID: {cfg.get('_id')}")
    print(f"  Config Type: {cfg.get('config_type')}")
    print(f"  Created: {cfg.get('created_at')}")
    print(f"  Updated: {cfg.get('updated_at')}")

    # Print the actual weights config
    if 'config_data' in cfg:
        print(f"\n  Config Data:")
        print(json.dumps(cfg['config_data'], indent=4, default=str))

print(f"\n{'='*120}")
print("V2 CONFIG COLLECTION")
print(f"{'='*120}")

v2_configs = list(v2_config_coll.find({'config_type': 'lumpsum_weights'}))
print(f"\nFound {len(v2_configs)} lumpsum config(s) in V2")

for idx, cfg in enumerate(v2_configs):
    print(f"\nConfig {idx + 1}:")
    print(f"  Config ID: {cfg.get('_id')}")
    print(f"  Config Type: {cfg.get('config_type')}")
    print(f"  Created: {cfg.get('created_at')}")
    print(f"  Updated: {cfg.get('updated_at')}")

    # Print the actual weights config
    if 'config_data' in cfg:
        print(f"\n  Config Data:")
        print(json.dumps(cfg['config_data'], indent=4, default=str))

print(f"\n{'='*120}")
print("PENALTY CONFIGURATION COMPARISON")
print(f"{'='*120}")

# Get penalty details from sample records
legacy_penalty_meta = legacy_record.get('incentive_penalty_meta', {})
print(f"\nLegacy Penalty Strategy: {legacy_penalty_meta.get('ls_penalty_strategy')}")
print(f"Legacy Penalty Config:")
print(json.dumps(legacy_penalty_meta, indent=2))

v2_record = v2_db['Leaderboard_Lumpsum'].find_one({'month': '2025-04'})
if v2_record:
    v2_penalty_meta = v2_record.get('incentive_penalty_meta', {})
    print(f"\nV2 Penalty Strategy: {v2_penalty_meta.get('ls_penalty_strategy')}")
    print(f"V2 Penalty Config:")
    print(json.dumps(v2_penalty_meta, indent=2))

print(f"\n{'='*120}")
print("CATEGORY RULES COMPARISON")
print(f"{'='*120}")

# Check if category rules exist in config
if legacy_configs:
    legacy_cat_rules = legacy_configs[0].get('config_data', {}).get('category_rules', {})
    print("\nLegacy Category Rules:")
    print(json.dumps(legacy_cat_rules, indent=2))

if v2_configs:
    v2_cat_rules = v2_configs[0].get('config_data', {}).get('category_rules', {})
    print("\nV2 Category Rules:")
    print(json.dumps(v2_cat_rules, indent=2))
