
import os
import logging
import jwt
from http.cookies import SimpleCookie
import azure.functions as func
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_public_key():
    """
    Retrieves and formats the public key from environment variables.
    """
    public_key_str = os.getenv("JWT_PUBLIC_KEY")
    if not public_key_str:
        logging.warning("JWT_PUBLIC_KEY environment variable is not set.")
        return None

    # Ensure correct PEM format if possibly stripped
    # Ensure correct PEM format if possibly stripped or has escaped newlines
    if "\\n" in public_key_str:
        public_key_str = public_key_str.replace("\\n", "\n")
        
    if not public_key_str.startswith("-----BEGIN PUBLIC KEY-----"):
        public_key_str = f"-----BEGIN PUBLIC KEY-----\n{public_key_str}\n-----END PUBLIC KEY-----"

    try:
        return serialization.load_pem_public_key(
            public_key_str.encode(),
            backend=default_backend()
        )
    except Exception as e:
        logging.error(f"Failed to load public key: {e}")
        return None

def verify_jwt_token(token: str) -> dict | None:
    """
    Verifies the JWT token using RS256 and the configured public key.
    Returns the decoded payload if valid, None otherwise.
    """
    public_key = get_public_key()
    if not public_key:
        return None

    try:
        # Decode and verify
        # Audience and Issuer checks can be added if needed, currently skipping for flexibility
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"verify_aud": False, "verify_iss": False}
        )
        return payload
    except jwt.ExpiredSignatureError:
        logging.warning("JWT Token has expired.")
    except jwt.InvalidTokenError as e:
        logging.warning(f"Invalid JWT Token: {e}")
    except Exception as e:
        logging.error(f"Unexpected error verifying JWT: {e}")

    return None

def get_email_from_jwt_cookie(req: func.HttpRequest) -> str | None:
    """
    Extracts the JWT from the cookie and returns the email if valid.
    """
    cookie_header = req.headers.get("Cookie")
    if not cookie_header:
        # Try checking Authorization header as fallback (Bearer token)
        auth_header = req.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            payload = verify_jwt_token(token)
            if payload:
                return payload.get("email") or payload.get("preferred_username") or payload.get("upn")
        return None

    cookie_name = os.getenv("JWT_COOKIE_NAME", "auth_token")

    try:
        simple_cookie = SimpleCookie()
        simple_cookie.load(cookie_header)

        if cookie_name in simple_cookie:
            token = simple_cookie[cookie_name].value
            payload = verify_jwt_token(token)
            if payload:
                # Adjust these keys based on your specific JWT payload structure
                return payload.get("email") or payload.get("preferred_username") or payload.get("upn")

    except Exception as e:
        logging.error(f"Error parsing cookies: {e}")

    return None
