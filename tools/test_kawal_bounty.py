
import os
import sys
import json
import logging
import importlib.util
from unittest.mock import MagicMock
import pymongo
from datetime import datetime

# Setup Env
os.environ["MongoDb-Connection-String"] = "mongodb+srv://Vilakshanb:TIW0YwgQNaI8iMSc@milestone.wftaulr.mongodb.net/PLI_Leaderboard?retryWrites=true&w=majority"
os.environ["PLI_DB_NAME"] = "PLI_Leaderboard_v2"

# Mock Azure Functions
import types
az_func = types.ModuleType("azure.functions")
az_func.HttpRequest = MagicMock()
az_func.HttpResponse = MagicMock() # Use an instance so calls are recorded
sys.modules["azure.functions"] = az_func

# Mock rbac (in case it's used elsewhere, though we will strip the import)
rbac_mock = types.ModuleType("utils.rbac")
rbac_mock.get_user_email = MagicMock(return_value="admin@example.com")
rbac_mock.is_admin = MagicMock(return_value=True)
sys.modules["utils.rbac"] = rbac_mock
sys.modules["..utils"] = rbac_mock # Try to catch relative

# Load incentive_logic so it can be imported as .incentive_logic or incentive_logic
# We adding Leaderboard_API to path
leaderboard_api_path = os.path.abspath("Leaderboard_API")
sys.path.append(leaderboard_api_path)

# Manual Load of __init__.py to strip relative import
fn_path = os.path.join(leaderboard_api_path, "__init__.py")
with open(fn_path, "r") as f:
    code = f.read()

# Strip the problematic import
code = code.replace("from ..utils import rbac", "# from ..utils import rbac")
# Also fix relative import of incentive_logic if we are not running as package
# code = code.replace("from .incentive_logic", "import incentive_logic") # Depends on how we exec

# Prepare context
context = {}
context["__file__"] = fn_path
# We need to make sure 'incentive_logic' is importable.
# Since we added Leaderboard_API to sys.path, 'import incentive_logic' works.
# But original code uses 'from .incentive_logic'.
# We'll replace it to be safe, or rely on execution context.
code = code.replace("from .incentive_logic", "from incentive_logic")

exec(code, context)
fetch_user_breakdown = context["fetch_user_breakdown"]

# Mock Request
class MockReq:
    def __init__(self, params):
        self.params = params

def test_kawal():
    print("Testing Kawal Singh for May 2025...")
    req = MockReq({"month": "2025-05"})
    eid = "Kawal Singh"

    resp_mock = fetch_user_breakdown(req, eid)

    # The result in our mock context is a Mock object for HttpResponse?
    # Wait, get_db in the exec context uses pymongo, which is real.
    # The return value uses func.HttpResponse which we mocked.
    # We need to see how we mocked HttpResponse.

    # If we mocked it as a class, we instantiated it?
    # func.HttpResponse(json.dumps(...))

    # Our mock: az_func.HttpResponse = MagicMock
    # So resp_mock is an instance of MagicMock.
    # The first arg to the constructor was the body.

    # Inspect the mock class (constructor) calls
    # resp_mock is the return value, but we want the args passed *to* HttpResponse
    http_response_mock = sys.modules["azure.functions"].HttpResponse
    call_args = http_response_mock.call_args
    if call_args:
        body = call_args[0][0] # first arg to constructor
        data = json.loads(body)

        print("\n--- Result ---")
        print(f"RM Name: {data.get('rm_name')}")
        inc = data.get("rupee_incentive")
        print(f"Rupee Incentive: {json.dumps(inc, indent=2)}")

        if inc and inc.get("total_incentive", 0) != 0:
            print("\nSUCCESS: Incentive data found (non-zero)!")
        elif inc and inc.get("audit", {}).get("calculated_at_runtime"):
             print("\nSUCCESS: Calculated at runtime (Value: {})!".format(inc.get("ins_rupees_total")))
        else:
            print("\nFAILURE: No incentive data or not calculated.")

    else:
        # Maybe it's a class and we need to inspect how it was called
        # If az_func.HttpResponse is the class, then fetch_user_breakdown returned an instance.
        # We can also check if fetch_user_breakdown returned something else?
        pass

if __name__ == "__main__":
    test_kawal()
