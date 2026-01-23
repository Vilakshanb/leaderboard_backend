from pymongo import MongoClient
import os

CONN = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/?retryWrites=true&w=majority"
c = MongoClient(CONN)

db_prod = c['PLI_Leaderboard']
db_v2 = c['PLI_Leaderboard_v2']

print(f"--- Raw Data Verification ---")
print(f"PLI_Leaderboard.purchase_txn count: {db_prod.purchase_txn.estimated_document_count()}")
print(f"PLI_Leaderboard_v2.purchase_txn count: {db_v2.purchase_txn.estimated_document_count()}")

if 'internal' in c.list_database_names():
    print(f"internal.transactions count: {c['internal']['transactions'].estimated_document_count()}")
else:
    print("internal DB not found")

if 'iwell' in c.list_database_names():
    print(f"iwell.purchase_txn count: {c['iwell']['purchase_txn'].estimated_document_count()}")
else:
    print("iwell DB not found")
