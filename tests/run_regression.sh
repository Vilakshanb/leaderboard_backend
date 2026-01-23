#!/bin/bash
set -e

# Default env vars for local run if not set
export MONGO_DB_NAME_REPLAY=${MONGO_DB_NAME_REPLAY:-"PLI_Leaderboard_REGRESSION_TEST"}
export MONGO_CORE_DB_NAME_REPLAY=${MONGO_CORE_DB_NAME_REPLAY:-"iwell_REGRESSION_TEST"}

echo "Starting PLI Leaderboard Regression Suite"
echo "Replay DB: $MONGO_DB_NAME_REPLAY"
echo "========================================="

# 1. Replay
echo "[1/2] Running Replay..."
# Ensure we have baseline metadata
if [ ! -f "tests/fixtures/baseline/metadata.json" ]; then
    echo "ERROR: No baseline found in tests/fixtures/baseline. Please run snapshot tool (carefully) if needed."
    exit 1
fi
python3 tools/leaderboard_replay.py

# 2. Diff
echo "[2/2] Calculating Diff..."
python3 tools/leaderboard_diff.py

echo "Done."
