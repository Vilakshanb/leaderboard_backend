import json
import azure.functions as func
import os

def cors_headers():
    return {
        "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN"),
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
    }

def respond(body=None, status=200):
    return func.HttpResponse(
        json.dumps(body) if body is not None else "",
        status_code=status,
        mimetype="application/json",
        headers=cors_headers()
    )

def options_response():
    return func.HttpResponse("", status_code=204, headers=cors_headers())
