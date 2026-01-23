
def get_scoring_referral(req):
    """Get Referral scoring configuration."""
    return get_config_generic(SCORING_CONFIG_ID_REFERRAL, DEFAULT_REFERRAL_CONFIG)

def update_scoring_referral(req):
    """Update Referral scoring configuration."""
    try:
        req_body = req.get_json()
        reason = req_body.get('reason', 'Manual update via Settings API')
        new_config = req_body.get('config')

        if not new_config or not isinstance(new_config, dict):
             return func.HttpResponse(
                json.dumps({"error": "Invalid configuration payload"}),
                status_code=400,
                mimetype="application/json"
            )

        return update_config_generic(SCORING_CONFIG_ID_REFERRAL, new_config, reason, req)

    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json"
        )

def reset_scoring_referral(req):
    """Reset Referral configuration to defaults."""
    req_body = {}
    try:
        req_body = req.get_json()
    except ValueError:
        pass

    reason = req_body.get('reason', 'Reset to defaults')
    return update_config_generic(SCORING_CONFIG_ID_REFERRAL, DEFAULT_REFERRAL_CONFIG, reason, req)
