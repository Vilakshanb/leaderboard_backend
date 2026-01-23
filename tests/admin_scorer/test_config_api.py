"""
Golden tests for Admin Scorer API config endpoints.

These tests lock the schema, defaults, and validation logic to Python 1:1
and prevent drift between the API and the scorer implementations.

All tests run against PLI_Leaderboard_v2 only (enforced by conftest.py).
"""

import pytest
import sys
import os

# Add parent dir to import scorer constants
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Lumpsum_Scorer import (
    DEFAULT_RATE_SLABS, DEFAULT_MEETING_SLABS, DEFAULT_QTR_BONUS_JSON,
    DEFAULT_ANNUAL_BONUS_JSON, DEFAULT_LS_PENALTY_CFG, DEFAULT_WEIGHTS,
    SCHEMA_VERSION
)
from SIP_Scorer import (
    TIER_THRESHOLDS, TIER_MONTHLY_FACTORS, SIP_POINTS_COEFF,
    SCHEMA_VERSION_SIP
)


class TestLumpsumConfigDefaults:
    """Test that Lumpsum config GET returns exact Python defaults when config missing."""

    def test_get_lumpsum_default_state(self, session, api_base_url, clean_config):
        """GET lumpsum with no config doc should return Python DEFAULT_* constants exactly."""
        response = session.get(f"{api_base_url}/lumpsum")

        assert response.status_code == 200
        assert response.headers.get("X-DB-Name") == "PLI_Leaderboard_v2"

        data = response.json()
        assert data["module"] == "lumpsum"
        assert data["schema_version"] == SCHEMA_VERSION

        effective = data["effective_config"]

        # Assert exact match with Python defaults
        assert effective["rate_slabs"] == DEFAULT_RATE_SLABS
        assert effective["meeting_slabs"] == DEFAULT_MEETING_SLABS
        assert effective["qtr_bonus_template"] == DEFAULT_QTR_BONUS_JSON
        assert effective["annual_bonus_template"] == DEFAULT_ANNUAL_BONUS_JSON
        assert effective["ls_penalty"] == DEFAULT_LS_PENALTY_CFG
        assert effective["weights"] == DEFAULT_WEIGHTS

        # raw_config should be empty when no doc exists
        assert data["raw_config"] == {}


class TestSIPConfigDefaults:
    """Test that SIP config GET returns exact Python defaults when config missing."""

    def test_get_sip_default_state(self, session, api_base_url, clean_config):
        """GET sip with no config doc should return Python DEFAULT_* constants exactly."""
        response = session.get(f"{api_base_url}/sip")

        assert response.status_code == 200
        assert response.headers.get("X-DB-Name") == "PLI_Leaderboard_v2"

        data = response.json()
        assert data["module"] == "sip"
        assert data["schema_version"] == SCHEMA_VERSION_SIP

        effective = data["effective_config"]

        # Assert exact match with Python defaults
        # Note: JSON serialization converts tuples to lists
        assert effective["tier_thresholds"] == [list(t) for t in TIER_THRESHOLDS]
        assert effective["tier_monthly_factors"] == TIER_MONTHLY_FACTORS
        assert effective["sip_points_coeff"] == SIP_POINTS_COEFF


class TestLumpsumConfigPersistence:
    """Test that Lumpsum config PUT validates and persists correctly."""

    def test_put_lumpsum_valid_config_persists(self, session, api_base_url, test_db, clean_config):
        """PUT valid lumpsum config should persist with version tracking."""
        payload = {
            "rate_slabs": [
                {"min_pct": 0.0, "max_pct": 1.0, "rate": 0.001, "label": "Test Slab"}
            ],
            "options": {
                "range_mode": "fy",
                "fy_mode": "CAL"
            }
        }

        response = session.put(f"{api_base_url}/lumpsum", json=payload)

        assert response.status_code == 200
        data = response.json()

        # Verify effective_config reflects the change
        assert data["effective_config"]["rate_slabs"] == payload["rate_slabs"]
        assert data["effective_config"]["options"]["range_mode"] == "fy"
        assert data["effective_config"]["options"]["fy_mode"] == "CAL"

        # Verify raw_config has audit metadata
        raw = data["raw_config"]
        assert raw["_id"] == "Leaderboard_Lumpsum"
        assert raw["version"] == 1
        assert "updatedAt" in raw
        assert raw["schema"] == "Leaderboard_Lumpsum"
        assert raw["schema_version"] == SCHEMA_VERSION

        # Verify MongoDB persistence
        doc = test_db.config.find_one({"_id": "Leaderboard_Lumpsum"})
        assert doc is not None
        assert doc["version"] == 1
        assert doc["rate_slabs"] == payload["rate_slabs"]

    def test_version_increment_on_update(self, session, api_base_url, test_db, clean_config):
        """Subsequent PUT should increment version."""
        payload1 = {"rate_slabs": [{"min_pct": 0.0, "max_pct": 1.0, "rate": 0.001, "label": "V1"}]}
        payload2 = {"rate_slabs": [{"min_pct": 0.0, "max_pct": 2.0, "rate": 0.002, "label": "V2"}]}

        # First PUT
        r1 = session.put(f"{api_base_url}/lumpsum", json=payload1)
        assert r1.status_code == 200
        assert r1.json()["raw_config"]["version"] == 1

        # Second PUT
        r2 = session.put(f"{api_base_url}/lumpsum", json=payload2)
        assert r2.status_code == 200
        assert r2.json()["raw_config"]["version"] == 2

        # Verify in DB
        doc = test_db.config.find_one({"_id": "Leaderboard_Lumpsum"})
        assert doc["version"] == 2


class TestSIPConfigPersistence:
    """Test that SIP config PUT validates and persists correctly."""

    def test_put_sip_valid_config_persists(self, session, api_base_url, test_db, clean_config):
        """PUT valid sip config should persist with version tracking."""
        payload = {
            "sip_points_coeff": 0.035,
            "options": {
                "ls_gate_pct": -5.0,
                "sip_net_mode": "sip_plus_swp"
            }
        }

        response = session.put(f"{api_base_url}/sip", json=payload)

        assert response.status_code == 200
        data = response.json()

        # Verify effective_config reflects the change
        assert data["effective_config"]["sip_points_coeff"] == 0.035
        assert data["effective_config"]["options"]["ls_gate_pct"] == -5.0
        assert data["effective_config"]["options"]["sip_net_mode"] == "sip_plus_swp"

        # Verify audit metadata
        raw = data["raw_config"]
        assert raw["_id"] == "Leaderboard_SIP"
        assert raw["version"] == 1
        assert "updatedAt" in raw
        assert raw["schema"] == "Leaderboard_Sip"
        assert raw["schema_version"] == SCHEMA_VERSION_SIP


class TestLumpsumValidation:
    """Test that Lumpsum config validation prevents invalid data."""

    def test_put_lumpsum_invalid_rate_slabs_no_write(self, session, api_base_url, test_db, clean_config):
        """PUT with min_pct >= max_pct should return 400 and NOT write to DB."""
        payload = {
            "rate_slabs": [
                {"min_pct": 1.0, "max_pct": 0.5, "rate": 0.001, "label": "Invalid"}
            ]
        }

        response = session.put(f"{api_base_url}/lumpsum", json=payload)

        assert response.status_code == 400
        errors = response.json()["errors"]
        assert any("min_pct" in err and "max_pct" in err for err in errors)

        # Verify NO write to DB
        doc = test_db.config.find_one({"_id": "Leaderboard_Lumpsum"})
        assert doc is None

    def test_put_lumpsum_invalid_meeting_slabs_no_write(self, session, api_base_url, test_db, clean_config):
        """PUT with non-increasing max_count should return 400 and NOT write."""
        payload = {
            "meeting_slabs": [
                {"max_count": 10, "mult": 1.0, "label": "First"},
                {"max_count": 5, "mult": 1.05, "label": "Second"}  # Invalid: 5 < 10
            ]
        }

        response = session.put(f"{api_base_url}/lumpsum", json=payload)

        assert response.status_code == 400
        errors = response.json()["errors"]
        assert any("max_count" in err for err in errors)

        # Verify NO write to DB
        doc = test_db.config.find_one({"_id": "Leaderboard_Lumpsum"})
        assert doc is None

    def test_put_lumpsum_invalid_range_mode_no_write(self, session, api_base_url, test_db, clean_config):
        """PUT with invalid range_mode enum should return 400 and NOT write."""
        payload = {
            "options": {
                "range_mode": "invalid_mode"
            }
        }

        response = session.put(f"{api_base_url}/lumpsum", json=payload)

        assert response.status_code == 400
        errors = response.json()["errors"]
        assert any("range_mode" in err and "invalid" in err for err in errors)

        # Verify NO write to DB
        doc = test_db.config.find_one({"_id": "Leaderboard_Lumpsum"})
        assert doc is None


class TestSIPValidation:
    """Test that SIP config validation prevents invalid data."""

    def test_put_sip_invalid_tier_thresholds_no_write(self, session, api_base_url, test_db, clean_config):
        """PUT with invalid tier_thresholds format should return 400 and NOT write."""
        payload = {
            "tier_thresholds": [
                ["T1", 5000],
                "invalid_format"  # Should be [name, amount]
            ]
        }

        response = session.put(f"{api_base_url}/sip", json=payload)

        assert response.status_code == 400
        errors = response.json()["errors"]
        assert any("tier_thresholds" in err for err in errors)

        # Verify NO write to DB
        doc = test_db.config.find_one({"_id": "Leaderboard_SIP"})
        assert doc is None

    def test_put_sip_invalid_net_mode_no_write(self, session, api_base_url, test_db, clean_config):
        """PUT with invalid sip_net_mode enum should return 400 and NOT write."""
        payload = {
            "options": {
                "sip_net_mode": "invalid_mode"
            }
        }

        response = session.put(f"{api_base_url}/sip", json=payload)

        assert response.status_code == 400
        errors = response.json()["errors"]
        assert any("sip_net_mode" in err for err in errors)

        # Verify NO write to DB
        doc = test_db.config.find_one({"_id": "Leaderboard_SIP"})
        assert doc is None
