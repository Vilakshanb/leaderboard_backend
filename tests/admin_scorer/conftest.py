"""
Pytest configuration and fixtures for Admin Scorer API tests.

SAFETY: All tests MUST run against PLI_Leaderboard_v2 only.
"""

import os
import pytest
import requests
from pymongo import MongoClient

# Test database safety guard
TEST_DB_NAME = "PLI_Leaderboard_v2"
PROD_DB_NAME = "PLI_Leaderboard"

@pytest.fixture(scope="session", autouse=True)
def enforce_test_db():
    """Ensure tests never run against production database."""
    db_name = os.getenv("DB_NAME", PROD_DB_NAME)

    if db_name == PROD_DB_NAME:
        pytest.fail(
            f"SAFETY GUARD: Tests cannot run against production DB. "
            f"Set DB_NAME={TEST_DB_NAME} in environment."
        )

    if db_name != TEST_DB_NAME:
        pytest.fail(
            f"SAFETY GUARD: DB_NAME={db_name} is not the expected test DB. "
            f"Set DB_NAME={TEST_DB_NAME}"
        )

    print(f"\n✓ Test DB safety check passed: DB_NAME={db_name}")
    return db_name

@pytest.fixture(scope="session")
def api_base_url():
    """Base URL for Admin Scorer API."""
    return "http://localhost:7071/api/scoring-admin/config"

@pytest.fixture(scope="session")
def mongo_client():
    """MongoDB client for test database."""
    mongo_uri = os.getenv("MongoDb-Connection-String")
    if not mongo_uri:
        pytest.skip("MongoDb-Connection-String not set")

    client = MongoClient(mongo_uri)
    yield client
    client.close()

@pytest.fixture(scope="session")
def test_db(mongo_client):
    """Test database instance."""
    return mongo_client[TEST_DB_NAME]

@pytest.fixture(scope="function")
def clean_config(test_db):
    """Clean config collection before each test."""
    # Drop config docs to ensure clean state
    test_db.config.delete_many({"_id": {"$in": ["Leaderboard_Lumpsum", "Leaderboard_SIP"]}})
    yield
    # Cleanup after test
    test_db.config.delete_many({"_id": {"$in": ["Leaderboard_Lumpsum", "Leaderboard_SIP"]}})

@pytest.fixture
def session():
    """Requests session for API calls."""
    s = requests.Session()
    yield s
    s.close()
