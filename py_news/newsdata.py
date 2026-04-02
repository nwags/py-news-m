"""Shared NewsData auth/request/normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Any
import json

from py_news.adapters.article_bulk_utils import normalize_datetime, normalize_text, normalize_url
from py_news.providers import ProviderRule

NEWSDATA_SAFE_MAX_RECORDS = 10
NEWSDATA_DEFAULT_QUERY = "news"
NEWSDATA_DEFAULT_LANGUAGE = "en"
NEWSDATA_DEFAULT_AUTH_ENV_VAR = "NEWSDATA_API_KEY"


@dataclass(frozen=True, slots=True)
class NewsDataAuth:
    auth_type: str
    auth_env_var: str
    auth_configured: bool
    auth_value: str


@dataclass(frozen=True, slots=True)
class NewsDataSize:
    requested_max_records: int
    effective_max_records: int
    max_records_clamped: bool


def resolve_newsdata_auth(rule: ProviderRule | None) -> NewsDataAuth:
    auth_type = (rule.auth_type if rule and rule.auth_type else "api_key").strip().lower()
    auth_env_var = (rule.auth_env_var if rule and rule.auth_env_var else NEWSDATA_DEFAULT_AUTH_ENV_VAR).strip()
    auth_value = ""
    if auth_type == "api_key" and auth_env_var:
        import os

        auth_value = os.getenv(auth_env_var, "").strip()
    return NewsDataAuth(
        auth_type=auth_type,
        auth_env_var=auth_env_var or NEWSDATA_DEFAULT_AUTH_ENV_VAR,
        auth_configured=bool(auth_value),
        auth_value=auth_value,
    )


def clamp_newsdata_size(max_records: int | None) -> NewsDataSize:
    requested = int(max_records or NEWSDATA_SAFE_MAX_RECORDS)
    requested = max(1, requested)
    effective = min(requested, NEWSDATA_SAFE_MAX_RECORDS)
    return NewsDataSize(
        requested_max_records=requested,
        effective_max_records=effective,
        max_records_clamped=effective != requested,
    )


def build_newsdata_params(
    *,
    query: str | None,
    max_records: int | None,
    auth: NewsDataAuth,
) -> tuple[dict[str, Any], NewsDataSize]:
    size = clamp_newsdata_size(max_records=max_records)
    params: dict[str, Any] = {
        "q": normalize_text(query) or NEWSDATA_DEFAULT_QUERY,
        "language": NEWSDATA_DEFAULT_LANGUAGE,
        "size": size.effective_max_records,
    }
    if auth.auth_type == "api_key" and auth.auth_configured:
        params["apikey"] = auth.auth_value
    return params, size


def normalize_newsdata_item(item: dict[str, Any]) -> dict[str, Any]:
    url = normalize_url(item.get("link"))
    source_name = normalize_text(item.get("source_id") or item.get("source_name"))

    return {
        "provider_document_id": normalize_text(item.get("article_id") or item.get("link") or item.get("title")),
        "source_name": source_name,
        "source_domain": normalize_newsdata_domain(item.get("source_url") or item.get("source_id")),
        "url": url,
        "canonical_url": url,
        "title": normalize_text(item.get("title")),
        "published_at": normalize_datetime(item.get("pubDate")),
        "language": normalize_text(item.get("language")),
        "section": normalize_newsdata_section(item.get("category")),
        "byline": normalize_newsdata_byline(item.get("creator")),
        "summary_text": normalize_text(item.get("description")),
        "snippet": normalize_text(item.get("content") or item.get("description")),
        "article_text": normalize_text(item.get("content")),
    }


def normalize_newsdata_domain(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    lowered = text.lower().strip().rstrip("/")
    candidate = lowered
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    host = normalize_text(parsed.netloc)
    if host:
        return host.lower()
    # Fallback for unusual non-url values.
    if "/" in lowered:
        lowered = lowered.split("/", 1)[0]
    return lowered or None


def normalize_newsdata_section(value: Any) -> str | None:
    if isinstance(value, list):
        for item in value:
            normalized = normalize_text(item)
            if normalized:
                return normalized
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text.replace("'", '"'))
                if isinstance(parsed, list):
                    return normalize_newsdata_section(parsed)
            except ValueError:
                pass
    return normalize_text(value)


def normalize_newsdata_byline(value: Any) -> str | None:
    if isinstance(value, list):
        names = [normalize_text(item) for item in value]
        filtered = [name for name in names if name]
        if not filtered:
            return None
        if len(filtered) == 1:
            return filtered[0]
        return ", ".join(filtered)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text.replace("'", '"'))
                if isinstance(parsed, list):
                    return normalize_newsdata_byline(parsed)
            except ValueError:
                pass
    return normalize_text(value)
