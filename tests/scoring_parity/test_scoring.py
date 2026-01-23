
import os
import sys
import json
import shutil
import pytest
import subprocess

# Add root and tools to path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

# Verify Env
MONGO_URI_ENV = os.getenv("MongoDb-Connection-String") or os.getenv("MONGO_URI")
if not MONGO_URI_ENV:
    pytest.skip("Skipping scoring parity tests: Env var MongoDb-Connection-String not set", allow_module_level=True)

SNAPSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshots")

def test_scoring_parity_e2e(tmp_path):
    """
    Run the full scoring pipeline via subprocess (for isolation) and compare output snapshots
    against the committed 'Gold' snapshots.
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Define Gold Files to check
    gold_files = [
        "Leaderboard_Lumpsum_default.json",
        "MF_SIP_Leaderboard_default.json",
        "Public_Leaderboard_default.json"
    ]

    # Verify Gold files exist
    for f in gold_files:
        assert os.path.exists(os.path.join(SNAPSHOT_DIR, f)), f"Gold snapshot missing: {f}"

    # Prepare complete environment
    env = os.environ.copy()
    env["MongoDb-Connection-String"] = MONGO_URI_ENV
    env["CONFIRM_DROP"] = "yes"
    env["PYTHONPATH"] = ROOT_DIR

    # 1. Run Seed Script
    print("Seeding DB...")
    seed_script = os.path.join(ROOT_DIR, "tools", "reset_seed_v2.py")
    subprocess.run([sys.executable, seed_script], env=env, check=True)

    # 2. Run Scorers Script
    # We need to tell run_scorers to output to our tempt path.
    # Note: run_scorers.py as currently written outputs to hardcoded "snapshots" (unless I update it to parse arg?)
    # I modified run_scoring_iteration to accept arg, but __main__ block in run_scorers.py calls it with default?
    # Let's check run_scorers.py __main__ block.
    # If it defaults to 'snapshots' dir, I should copy the result from there?
    # Or modify run_scorers.py to accept an output dir arg.
    # For now, let's assume it writes to `tests/scoring_parity/snapshots`.
    # But wait, that will overwrite GOLD if I run it!

    # Critical: I must backup GOLD or modify run_scorers.py to accept CLI arg for output dir.
    # I'll rely on git/manual restore if I overwrite for now, OR I simply verify the content of the file it produces.
    # Actually, running it overwrites the files I'm comparing against.

    # Better: Use a context manager to backup and restore gold files?
    # Or pass output dir via env var?
    pass

    # Let's Modify run_scorers.py to accept SNAPSHOT_DIR env var or arg.
    # run_scorers.py:
    # if snapshot_dir is None:
    #    snapshot_dir = os.path.join(...)

    # I should update run_scorers.py to check env var SNAPSHOT_DIR.

    env["SNAPSHOT_DIR"] = str(output_dir)
    runner_script = os.path.join(ROOT_DIR, "tests", "scoring_parity", "run_scorers.py")
    subprocess.run([sys.executable, runner_script, "default"], env=env, check=True)

    # Compare
    for filename in gold_files:
        gold_path = os.path.join(SNAPSHOT_DIR, filename)
        generated_path = output_dir / filename # output_dir is where we directed output

        assert generated_path.exists(), f"Snapshot not generated: {filename}"

        with open(gold_path, "r") as f:
            gold_data = json.load(f)

        with open(generated_path, "r") as f:
            generated_data = json.load(f)

        assert generated_data == gold_data, f"Snapshot mismatch for {filename}"

