import os
import pymongo
import logging

# Global cache for the MongoDB client to enable connection pooling across invocations
_CLIENT_CACHE = None

def get_db_client(**kwargs):
    """
    Returns a PyMongo client using the connection string from environment variables.
    Uses a global cache to reuse the client across Azure Function invocations.
    """
    global _CLIENT_CACHE

    if _CLIENT_CACHE:
        return _CLIENT_CACHE

    # List of keys to check in order
    keys = [
        "MongoDb-Connection-String",
        "MONGODB_CONNECTION_STRING",
        "CUSTOMCONNSTR_MongoDb-Connection-String",
        "MongoDbConnectionString",
        "DB_CONNECTION_STRING"
    ]

    uri = None
    for key in keys:
        val = os.getenv(key)
        if val:
            uri = val
            break

    if not uri:
        # CRITICAL: Prevent fallback to localhost:27017
        error_msg = f"MongoDB Connection String not found in environment variables. Checked: {keys}"
        logging.critical(error_msg)
        raise Exception(error_msg)

    try:
        # Create new client and cache it
        client = pymongo.MongoClient(uri, **kwargs)
        _CLIENT_CACHE = client
        return client
    except Exception as e:
        logging.critical(f"Failed to create MongoClient: {e}")
        raise

def get_db(db_name_env="PLI_DB_NAME", default_db="PLI_Leaderboard_v2"):
    """
    Returns the database object.
    """
    client = get_db_client()
    db_name = os.getenv(db_name_env, os.getenv("DB_NAME", default_db))
    return client[db_name]
