import pytest
import os
import sys
import subprocess
import json
import shutil
from pathlib import Path
from compare_json import compare_snapshots

# Setup Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOLS_DIR = os.path.join(ROOT_DIR, "tools")
ENGINE_DIR = os.path.join(ROOT_DIR, "engine")

@pytest.fixture(scope="module")
def parity_env(tmp_path_factory):
    """
    Sets up a consolidated environment for comparison:
    1. Runs Python Oracle (via export_gold.py) -> <tmp>/python/default/...
    2. Runs TypeScript Engine (via npm start) -> <tmp>/ts/...
    Returns the paths to both output directories.
    """
    base_dir = tmp_path_factory.mktemp("parity_run")
    python_out = base_dir / "python"
    ts_out = base_dir / "ts"

    python_out.mkdir()
    ts_out.mkdir()

    # Pass connection string
    mongo_uri = os.environ.get("MONGO_URI") or os.environ.get("MongoDb-Connection-String")
    if not mongo_uri:
        # Try to read local.settings.json fallback
        try:
            settings_path = os.path.join(ROOT_DIR, "local.settings.json")
            with open(settings_path, "r") as f:
                settings = json.load(f)
                mongo_uri = settings["Values"]["MongoDb-Connection-String"]
        except Exception:
            pytest.skip("MONGO_URI not found and local.settings.json unavailable")

    env = os.environ.copy()
    env["MONGO_URI"] = mongo_uri
    env["MongoDb-Connection-String"] = mongo_uri
    env["CONFIRM_DROP"] = "yes"
    env["SNAPSHOT_DIR"] = str(ts_out)

    # 1. Run Python Oracle (Seeds DB + Runs logic + Exports)
    print("\n[Parity] Running Python Oracle...")
    cmd_py = [
        sys.executable,
        os.path.join(TOOLS_DIR, "export_gold.py"),
        "--scenario", "default",
        "--output-dir", str(python_out)
    ]
    subprocess.run(cmd_py, env=env, check=True, cwd=ROOT_DIR)

    # 2. Run TypeScript Engine (Reads seeded DB + Runs logic + Exports)
    # Ensure dependencies are installed or just run via ts-node if possible?
    # Assuming npm install is done.
    print(f"\n[Parity] Running TypeScript Engine... outputting to {ts_out}")
    cmd_ts = ["npm", "run", "start"]

    # We need to run this from engine dir
    subprocess.run(cmd_ts, env=env, check=True, cwd=ENGINE_DIR)

    return {
        "python": python_out / "default", # export_gold appends scenario name
        "ts": ts_out
    }

def test_sip_parity(parity_env):
    """
    Compare MF_SIP_Leaderboard.json from Python vs TypeScript
    """
    py_path = parity_env["python"] / "MF_SIP_Leaderboard.json"
    ts_path = parity_env["ts"] / "MF_SIP_Leaderboard.json"

    assert py_path.exists(), "Python oracle failed to produce MF_SIP_Leaderboard.json"
    assert ts_path.exists(), "TypeScript engine failed to produce MF_SIP_Leaderboard.json"

    print(f"\nComparing:\n  PY: {py_path}\n  TS: {ts_path}")

    errors = compare_snapshots(str(py_path), str(ts_path))

    if errors:
        # Dump diffs for debugging
        print("\n".join(errors[:20]))
        if len(errors) > 20:
            print(f"... and {len(errors) - 20} more.")

        pytest.fail(f"SIP Parity Mismatch: {len(errors)} differences found.")

def test_lumpsum_parity(parity_env):
    """
    Compare Leaderboard_Lumpsum.json from Python vs TypeScript
    """
    py_path = parity_env["python"] / "Leaderboard_Lumpsum.json"
    ts_path = parity_env["ts"] / "Leaderboard_Lumpsum.json"

    assert py_path.exists(), "Python oracle failed to produce Leaderboard_Lumpsum.json"
    assert ts_path.exists(), "TypeScript engine failed to produce Leaderboard_Lumpsum.json"

    errors = compare_snapshots(str(py_path), str(ts_path))
    if errors:
        print("\n".join(errors[:20]))
        pytest.fail(f"Lumpsum Parity Mismatch: {len(errors)} differences found.")

def test_public_parity(parity_env):
    """
    Compare Public_Leaderboard.json from Python vs TypeScript
    """
    py_path = parity_env["python"] / "Public_Leaderboard.json"
    ts_path = parity_env["ts"] / "Public_Leaderboard.json"

    assert py_path.exists(), "Python oracle failed to produce Public_Leaderboard.json"
    assert ts_path.exists(), "TypeScript engine failed to produce Public_Leaderboard.json"

    errors = compare_snapshots(str(py_path), str(ts_path))
    if errors:
        print("\n".join(errors[:20]))
        pytest.fail(f"Public Parity Mismatch: {len(errors)} differences found.")
