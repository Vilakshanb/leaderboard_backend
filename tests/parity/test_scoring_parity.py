
import os
import sys
import pytest
import subprocess
import json
from compare_json import compare_snapshots

# Setup Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

GOLD_DIR = os.path.join(ROOT_DIR, "gold", "2025-11")
RUNNER_SCRIPT = os.path.join(ROOT_DIR, "tools", "export_gold.py")

SCENARIOS = ["default", "override_lumpsum", "override_sip"]
COLLECTIONS = ["Leaderboard_Lumpsum", "MF_SIP_Leaderboard", "Public_Leaderboard"]

# Shared fixture to run all scenarios once into a tmp dir
@pytest.fixture(scope="module")
def run_all_scenarios(tmp_path_factory):
    """
    Run the scoring pipeline for EACH scenario into a temporary directory
    using SEPARATE subprocesses to ensure isolation.
    Returns the base temporary directory containing subdirs for each scenario.
    """
    out_base = tmp_path_factory.mktemp("actual_run")

    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT_DIR
    env["CONFIRM_DROP"] = "yes"
    if not env.get("MongoDb-Connection-String") and not env.get("MONGO_URI"):
        pytest.skip("MONGO_URI or MongoDb-Connection-String required")

    print(f"\n[Fixture] Running scoring pipelines into {out_base}...")

    for sc in SCENARIOS:
        print(f"  > Subprocess: {sc}")
        cmd = [
            sys.executable,
            RUNNER_SCRIPT,
            "--scenario", sc,
            "--output-dir", str(out_base)
        ]
        try:
            subprocess.run(cmd, env=env, check=True)
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Scoring pipeline failed for {sc}: {e}")

    return out_base

@pytest.mark.parametrize("scenario", SCENARIOS)
@pytest.mark.parametrize("collection", COLLECTIONS)
def test_identity_parity(run_all_scenarios, scenario, collection):
    """
    Verify that the current code (Actual) produces output IDENTICAL to the committed Gold snapshots.
    This ensures we haven't broken the 'oracle'.
    """
    gold_path = os.path.join(GOLD_DIR, scenario, f"{collection}.json")
    actual_path =  run_all_scenarios / scenario / f"{collection}.json"

    # Check existence
    assert os.path.exists(gold_path), f"Gold snapshot missing: {scenario}/{collection}"
    assert actual_path.exists(), f"Actual snapshot missing: {scenario}/{collection}"

    # Compare
    errors = compare_snapshots(str(gold_path), str(actual_path))
    if errors:
        error_msg = "\n".join(errors[:10]) # Show first 10
        if len(errors) > 10:
            error_msg += f"\n... and {len(errors)-10} more."
        pytest.fail(f"Snapshot mismatch for {scenario}/{collection}:\n{error_msg}")

def test_override_diffs_lumpsum(run_all_scenarios):
    """
    Verify that applying the Lumpsum override actually changes the output
    compared to default (Anti-Parity test).
    """
    default_path = run_all_scenarios / "default" / "Leaderboard_Lumpsum.json"
    override_path = run_all_scenarios / "override_lumpsum" / "Leaderboard_Lumpsum.json"

    with open(default_path, 'r') as f: default = json.load(f)
    with open(override_path, 'r') as f: override = json.load(f)

    # We expect differences in Lumpsum calculation (Incentive/Rate changed)
    assert default != override, "Lumpsum override produced identical Leaderboard_Lumpsum output!"

    # Note: Lumpsum override (Rate Boost) affects Final Incentive (Payout), not Points.
    # Public_Leaderboard aggregates Points. So it might NOT change.
    # We skip Public_Leaderboard assertion for Lumpsum override.

def test_override_diffs_sip(run_all_scenarios):
    """
    Verify that applying the SIP override actually changes the output.
    """
    default_path = run_all_scenarios / "default" / "MF_SIP_Leaderboard.json"
    override_path = run_all_scenarios / "override_sip" / "MF_SIP_Leaderboard.json"

    with open(default_path, 'r') as f: default = json.load(f)
    with open(override_path, 'r') as f: override = json.load(f)

    assert default != override, "SIP override produced identical MF_SIP_Leaderboard output!"

    # Verify Public Leaderboard also changed
    default_pub = run_all_scenarios / "default" / "Public_Leaderboard.json"
    override_pub = run_all_scenarios / "override_sip" / "Public_Leaderboard.json"

    with open(default_pub, 'r') as f: d_pub = json.load(f)
    with open(override_pub, 'r') as f: o_pub = json.load(f)

    assert d_pub != o_pub, "SIP override failed to propagate to Public_Leaderboard!"
