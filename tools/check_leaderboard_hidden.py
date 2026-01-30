
import requests
import json
import sys

# Call local API to get leaderboard for May 2025
url = "http://localhost:7071/api/leaderboard?month=2025-05"
headers = {
    "x-ms-client-principal-name": "vilakshan@niveshonline.com",
    "Access-Control-Allow-Origin": "http://localhost:5173",
    "Access-Control-Allow-Credentials": "true",
}

try:
    print(f"Calling {url}...")
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f"Error: {res.status_code} - {res.text}")
        sys.exit(1)

    data = res.json()

    # Check for Kawal
    found = False
    for row in data:
        name = row.get("name", "").lower()
        if "kawal" in name:
            found = True
            print(f"FAILURE: Kawal found in Leaderboard! {row}")
            break

    if not found:
        print("SUCCESS: Kawal NOT found in Leaderboard.")

except Exception as e:
    print(f"Exception: {e}")
