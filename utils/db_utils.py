import os
import pymongo
import logging

def get_db_client(**kwargs):
    """
    Returns a PyMongo client using the connection string from environment variables.
    Checks multiple common key names. Raises generic Exception if missing to avoid localhost fallback.
    Passes any additional kwargs to pymongo.MongoClient.
    """
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
        return pymongo.MongoClient(uri, **kwargs)
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
