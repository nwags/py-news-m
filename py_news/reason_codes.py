"""Machine-readable reason code authority for resolver/operator surfaces."""

from __future__ import annotations

LOCAL_METADATA_HIT = "local_metadata_hit"
LOCAL_CONTENT_HIT = "local_content_hit"
METADATA_REFRESHED = "metadata_refreshed"
METADATA_REFRESH_NOT_SUPPORTED = "metadata_refresh_not_supported"
CONTENT_MISSING_LOCAL = "content_missing_local"
ARTICLE_NOT_FOUND = "article_not_found"
NO_MATCH = "no_match"
NOT_FOUND = "not_found"
STRATEGY_NOT_SUPPORTED = "strategy_not_supported"
STRATEGY_NOT_SUPPORTED_FOR_PROVIDER = "strategy_not_supported_for_provider"
DIRECT_URL_NOT_ALLOWED = "direct_url_not_allowed"
HTTP_FAILURE = "http_failure"
NON_HTML_RESPONSE = "non_html_response"
EMPTY_BODY = "empty_body"
PARSE_FAILURE = "parse_failure"
SUCCESS = "success"
PROVIDER_NOT_REGISTERED = "provider_not_registered"
NO_PERMITTED_OR_SUCCESSFUL_STRATEGY = "no_permitted_or_successful_strategy"
MISSING_URL = "missing_url"
AUTH_NOT_CONFIGURED = "auth_not_configured"
AUTH_INVALID_OR_MISSING = "auth_invalid_or_missing"

ALL_REASON_CODES = {
    LOCAL_METADATA_HIT,
    LOCAL_CONTENT_HIT,
    METADATA_REFRESHED,
    METADATA_REFRESH_NOT_SUPPORTED,
    CONTENT_MISSING_LOCAL,
    ARTICLE_NOT_FOUND,
    NO_MATCH,
    NOT_FOUND,
    STRATEGY_NOT_SUPPORTED,
    STRATEGY_NOT_SUPPORTED_FOR_PROVIDER,
    DIRECT_URL_NOT_ALLOWED,
    HTTP_FAILURE,
    NON_HTML_RESPONSE,
    EMPTY_BODY,
    PARSE_FAILURE,
    SUCCESS,
    PROVIDER_NOT_REGISTERED,
    NO_PERMITTED_OR_SUCCESSFUL_STRATEGY,
    MISSING_URL,
    AUTH_NOT_CONFIGURED,
    AUTH_INVALID_OR_MISSING,
}
