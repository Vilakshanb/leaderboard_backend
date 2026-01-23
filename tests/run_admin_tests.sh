#!/bin/bash
#
# Run Admin Scorer API golden tests against PLI_Leaderboard_v2
#
# This script ensures tests run with the correct environment variables
# and provides clear output.
#

set -e

echo "=========================================="
echo "Admin Scorer API Golden Tests"
echo "=========================================="
echo ""

# Check if func is running
if ! curl -s http://localhost:7071/api/whoami > /dev/null 2>&1; then
    echo "ERROR: Azure Functions not running on localhost:7071"
    echo "Please start with: func start"
    exit 1
fi

# Set test environment
export DB_NAME=PLI_Leaderboard_v2
export APP_ENV=dev

# Check MongoDB connection string
if [ -z "$MongoDb-Connection-String" ]; then
    echo "WARNING: MongoDb-Connection-String not set in environment"
    echo "Reading from local.settings.json..."
    # This will be picked up by the function app
fi

echo "Environment:"
echo "  DB_NAME: $DB_NAME"
echo "  APP_ENV: $APP_ENV"
echo ""

# Run tests
echo "Running pytest..."
echo ""

pytest tests/admin_scorer/test_config_api.py \
    -v \
    --tb=short \
    --color=yes \
    -x

echo ""
echo "=========================================="
echo "✓ All tests passed!"
echo "=========================================="
