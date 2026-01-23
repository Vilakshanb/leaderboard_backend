
import logging
import os
import pymongo
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SKIP_RM_ALIASES = {
    "vilakshan bhutani",
    "vilakshan p bhutani",
    "pramod bhutani",
    "dilip kumar singh",
    "dillip kumar",
    "dilip kumar",
    "ruby",
    "manisha p tendulkar",
    "ankur khurana",
    "amaya -virtual assistant",
    "amaya - virtual assistant",
    "anchal chandra",
    "kanchan bhalla",
    "himanshu",
    "poonam gulati",
}

def get_secret(name: str):
    # Try env first
    if os.getenv(name):
        return os.getenv(name)

    # Try Key Vault
    kv_url = "https://milestonetsl1.vault.azure.net/"
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=kv_url, credential=credential)
        return client.get_secret(name).value
    except Exception as e:
        logging.error(f"Failed to fetch secret {name}: {e}")
        return None

def main():
    conn_str = get_secret("MONGODB_CONNECTION_STRING")
    if not conn_str:
        logging.error("Could not find MongoDb-Connection-String")
        return

    client = pymongo.MongoClient(conn_str)
    db = client["PLI_Leaderboard"]
    col = db["referralLeaderboard"]

    logging.info("Connected to MongoDB. Starting cleanup...")

    # Normalize aliases for case-insensitive matching if needed,
    # but the scorer uses title case mostly. We'll try to match exact or case-insensitive name.

    deleted_count = 0

    # Fetch all records to check names against skip list (safest approach)
    # OR use a delete_many with $in if we are confident about the exact string format.
    # Given the logs showed "Pramod Bhutani" (Title Case), let's iterate and check.

    cursor = col.find({}, {"employee_name": 1, "employee_id": 1})
    ids_to_delete = []

    for doc in cursor:
        name = doc.get("employee_name")
        if not name:
            continue

        # Normalize name for comparison
        norm_name = " ".join(str(name).lower().split())

        if norm_name in SKIP_RM_ALIASES:
            ids_to_delete.append(doc["_id"])
            logging.info(f"Marking for deletion: {name} (ID: {doc['_id']})")

    if ids_to_delete:
        result = col.delete_many({"_id": {"$in": ids_to_delete}})
        logging.info(f"Deleted {result.deleted_count} records matching skipped RMs.")
    else:
        logging.info("No records found to delete.")

if __name__ == "__main__":
    main()
