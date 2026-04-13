"""Provider-aware article resolution with canonical provenance."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from py_news.config import AppConfig
from py_news.cache_layout import ensure_storage_mapping, mapped_article_ids_for_storage, mapped_storage_id_for_article
from py_news.html_extract import extract_visible_text_from_html
from py_news.http import HttpClient, HttpFailure
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS, RESOLUTION_EVENT_COLUMNS
from py_news.newsdata import build_newsdata_params, normalize_newsdata_item, resolve_newsdata_auth
from py_news.providers import ProviderRule, load_provider_rule
from py_news.reason_codes import (
    AUTH_INVALID_OR_MISSING,
    AUTH_NOT_CONFIGURED,
    ARTICLE_NOT_FOUND,
    CONTENT_MISSING_LOCAL,
    DIRECT_URL_NOT_ALLOWED,
    EMPTY_BODY,
    HTTP_FAILURE,
    LOCAL_CONTENT_HIT,
    LOCAL_METADATA_HIT,
    METADATA_REFRESH_NOT_SUPPORTED,
    METADATA_REFRESHED,
    MISSING_URL,
    NON_HTML_RESPONSE,
    NO_MATCH,
    NOT_FOUND,
    NO_PERMITTED_OR_SUCCESSFUL_STRATEGY,
    PARSE_FAILURE,
    PROVIDER_NOT_REGISTERED,
    STRATEGY_NOT_SUPPORTED,
    STRATEGY_NOT_SUPPORTED_FOR_PROVIDER,
    SUCCESS,
)
from py_news.storage.paths import (
    derive_publisher_slug,
    normalized_artifact_path,
    publisher_article_artifact_path,
    publisher_article_meta_path,
)
from py_news.storage.writes import append_parquet_rows, upsert_parquet_rows, write_json, write_text


_STRATEGIES = {"provider_payload_content", "provider_api_lookup", "direct_url_fetch"}


@dataclass(slots=True)
class ResolutionResult:
    article_id: str
    representation: str
    resolved: bool
    source: str
    reason_code: str
    provider: str
    strategy: str | None = None
    status_code: int | None = None
    message: str | None = None
    auth_env_var: str | None = None
    auth_configured: bool | None = None
    content_available: bool = False
    local_write_performed: bool = False
    artifact_paths: list[str] | None = None
    meta_sidecar_path: str | None = None
    resolution_mode: str | None = None
    provider_requested: str | None = None
    provider_used: str | None = None
    served_from: str | None = None
    remote_attempted: bool = False
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: str | None = None
    provider_skip_reasons: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_article(
    config: AppConfig,
    *,
    article_id: str,
    representation: str,
    allow_remote: bool,
    force_remote: bool = False,
    http_client: HttpClient | None = None,
) -> ResolutionResult:
    """Resolve article representation with local-first and provider-aware fallback."""

    if representation not in {"metadata", "content"}:
        raise ValueError(f"Unsupported representation: {representation}")

    articles_df = _load_articles(config)
    matched = articles_df[articles_df["article_id"] == article_id]
    if matched.empty:
        result = ResolutionResult(
            article_id=article_id,
            representation=representation,
            resolved=False,
            source="none",
            reason_code=ARTICLE_NOT_FOUND,
            provider="",
            strategy="none",
            message="Article not found in local metadata",
        )
        result = _enrich_result(result, allow_remote=allow_remote, force_remote=force_remote, provider_requested=None)
        _record_event(config, result, strategy="none", artifact_path=None, provenance={"allow_remote": allow_remote})
        return result

    row = matched.iloc[0].to_dict()
    provider = _clean_optional_text(row.get("provider")) or ""

    if representation == "metadata":
        result = _resolve_metadata(
            config=config,
            row=row,
            provider=provider,
            allow_remote=allow_remote,
            http_client=http_client,
        )
        result = _enrich_result(
            result,
            allow_remote=allow_remote,
            force_remote=False,
            provider_requested=provider or None,
        )
        _record_event(
            config,
            result,
            strategy=result.strategy or "none",
            artifact_path=_first_path(result.artifact_paths),
            provenance=_merge_provenance(
                {"allow_remote": allow_remote, "representation": representation},
                result,
            ),
        )
        return result

    result = _resolve_content(
        config=config,
        row=row,
        provider=provider,
        allow_remote=allow_remote,
        force_remote=force_remote,
        http_client=http_client,
    )
    result = _enrich_result(
        result,
        allow_remote=allow_remote,
        force_remote=force_remote,
        provider_requested=provider or None,
    )
    _record_event(
        config,
        result,
        strategy=result.strategy or "none",
        artifact_path=_first_path(result.artifact_paths),
        provenance=_merge_provenance(
            {"allow_remote": allow_remote, "force_remote": force_remote, "representation": representation},
            result,
        ),
    )
    return result


def _resolve_metadata(
    *,
    config: AppConfig,
    row: dict[str, Any],
    provider: str,
    allow_remote: bool,
    http_client: HttpClient | None,
) -> ResolutionResult:
    article_id = _clean_optional_text(row.get("article_id")) or ""

    if not allow_remote:
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=True,
            source="local_metadata",
            reason_code=LOCAL_METADATA_HIT,
            provider=provider,
            strategy="local_metadata",
        )

    rule = load_provider_rule(config, provider)
    if rule is None:
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=True,
            source="local_metadata",
            reason_code=PROVIDER_NOT_REGISTERED,
            provider=provider,
            strategy="local_metadata",
            message="No provider registry rule found; metadata unchanged",
        )

    client = http_client or HttpClient(config)
    attempted = False
    last_failure: ResolutionResult | None = None

    for strategy in rule.preferred_resolution_order:
        if strategy not in _STRATEGIES:
            continue
        attempt = _run_metadata_strategy(config=config, row=row, rule=rule, strategy=strategy, client=client)
        if attempt is None:
            continue
        attempted = True
        if attempt.resolved:
            return attempt
        last_failure = attempt

    if attempted and last_failure is not None:
        return last_failure

    return ResolutionResult(
        article_id=article_id,
        representation="metadata",
        resolved=True,
        source="local_metadata",
        reason_code=METADATA_REFRESH_NOT_SUPPORTED,
        provider=provider,
        strategy="local_metadata",
        message="No supported metadata refresh strategy for provider",
    )


def _resolve_content(
    *,
    config: AppConfig,
    row: dict[str, Any],
    provider: str,
    allow_remote: bool,
    force_remote: bool,
    http_client: HttpClient | None,
) -> ResolutionResult:
    article_id = _clean_optional_text(row.get("article_id")) or ""

    local_content = _local_content_status(config, article_id)
    if local_content["available"] and not force_remote:
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=True,
            source="local_artifact",
            reason_code=LOCAL_CONTENT_HIT,
            provider=provider,
            strategy="local_artifact",
            content_available=True,
            artifact_paths=local_content["paths"],
        )

    if not allow_remote:
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=False,
            source="local_only",
            reason_code=CONTENT_MISSING_LOCAL,
            provider=provider,
            message="Remote resolution disabled",
            strategy="local_only",
        )

    rule = load_provider_rule(config, provider)
    if rule is None:
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=False,
            source="provider_rules",
            reason_code=PROVIDER_NOT_REGISTERED,
            provider=provider,
            message="No provider registry rule found",
            strategy="provider_rules",
        )

    client = http_client or HttpClient(config)
    last_attempt: ResolutionResult | None = None

    for strategy in rule.preferred_resolution_order:
        if strategy not in _STRATEGIES:
            continue
        attempt = _run_content_strategy(config=config, row=row, rule=rule, strategy=strategy, client=client)
        if attempt is None:
            continue
        if attempt.resolved:
            return attempt
        last_attempt = attempt

    if last_attempt is not None:
        return last_attempt

    return ResolutionResult(
        article_id=article_id,
        representation="content",
        resolved=False,
        source="provider_resolution",
        reason_code=NO_PERMITTED_OR_SUCCESSFUL_STRATEGY,
        provider=provider,
        message="No strategy produced content",
        strategy="none",
    )


def _run_metadata_strategy(
    *,
    config: AppConfig,
    row: dict[str, Any],
    rule: ProviderRule,
    strategy: str,
    client: HttpClient,
) -> ResolutionResult | None:
    article_id = _clean_optional_text(row.get("article_id")) or ""
    provider = _clean_optional_text(row.get("provider")) or ""

    if strategy != "provider_api_lookup":
        return None

    if provider != "newsdata":
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=False,
            source="provider_rules",
            reason_code=STRATEGY_NOT_SUPPORTED_FOR_PROVIDER,
            provider=provider,
            strategy=strategy,
            message="provider_api_lookup is only implemented for newsdata",
        )

    endpoint = rule.api_base_url or "https://newsdata.io/api/1/news"
    query = _build_newsdata_query(row)
    auth = resolve_newsdata_auth(rule)
    params, size = build_newsdata_params(query=query, max_records=20, auth=auth)
    if auth.auth_type in {"api_key", "api_key_query"} and not auth.auth_configured:
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=False,
            source="provider_api",
            reason_code=AUTH_NOT_CONFIGURED,
            provider=provider,
            strategy=strategy,
            message=f"Missing provider auth; set {auth.auth_env_var}",
            auth_env_var=auth.auth_env_var,
            auth_configured=False,
        )

    try:
        payload = client.request_json("GET", endpoint, params=params)
    except HttpFailure as exc:
        lowered_reason = (exc.reason or "").lower()
        auth_missing = exc.status_code == 401 and ("api key" in lowered_reason or "apikey" in lowered_reason)
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=False,
            source="provider_api",
            reason_code=AUTH_INVALID_OR_MISSING if auth_missing else HTTP_FAILURE,
            provider=provider,
            strategy=strategy,
            status_code=exc.status_code,
            message=exc.reason,
            auth_env_var=auth.auth_env_var,
            auth_configured=auth.auth_configured,
            retry_count=max(0, int(exc.attempts) - 1),
            rate_limited=exc.status_code == 429,
        )

    candidates = payload.get("results")
    if not isinstance(candidates, list):
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=False,
            source="provider_api",
            reason_code=NOT_FOUND,
            provider=provider,
            strategy=strategy,
            message="No results list in provider payload",
            auth_env_var=auth.auth_env_var,
            auth_configured=auth.auth_configured,
        )

    match = _select_newsdata_match(row, [item for item in candidates if isinstance(item, dict)])
    if match is None:
        return ResolutionResult(
            article_id=article_id,
            representation="metadata",
            resolved=False,
            source="provider_api",
            reason_code=NO_MATCH,
            provider=provider,
            strategy=strategy,
            message="No deterministic match found",
            auth_env_var=auth.auth_env_var,
            auth_configured=auth.auth_configured,
        )

    merged = _merge_metadata_row_with_newsdata(row, match)
    _upsert_article_row(config, merged)

    return ResolutionResult(
        article_id=article_id,
        representation="metadata",
        resolved=True,
        source="provider_api",
        reason_code=METADATA_REFRESHED,
        provider=provider,
        strategy=strategy,
        message=(
            "Metadata refreshed from newsdata provider_api_lookup"
            + ("; size clamped to safe max=10" if size.max_records_clamped else "")
        ),
        auth_env_var=auth.auth_env_var,
        auth_configured=auth.auth_configured,
        local_write_performed=True,
        retry_count=max(0, int(getattr(client, "last_attempts", 1)) - 1),
        rate_limited=bool(getattr(client, "last_rate_limited", False)),
    )


def _run_content_strategy(
    *,
    config: AppConfig,
    row: dict[str, Any],
    rule: ProviderRule,
    strategy: str,
    client: HttpClient,
) -> ResolutionResult | None:
    article_id = _clean_optional_text(row.get("article_id")) or ""
    provider = _clean_optional_text(row.get("provider")) or ""

    if strategy == "provider_payload_content":
        text = _extract_text_from_metadata(row)
        if not text:
            return None

        persisted = _persist_content_artifacts(config=config, row=row, text=text, strategy=strategy)
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=True,
            source="provider_payload",
            reason_code=SUCCESS,
            provider=provider,
            strategy=strategy,
            content_available=True,
            local_write_performed=True,
            artifact_paths=persisted["artifact_paths"],
            meta_sidecar_path=persisted["meta_sidecar_path"],
        )

    if strategy == "provider_api_lookup":
        # Intentionally not implemented for content in this phase.
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=False,
            source="provider_api",
            reason_code=STRATEGY_NOT_SUPPORTED,
            provider=provider,
            strategy=strategy,
            message="provider_api_lookup is metadata-only in this phase",
        )

    if strategy == "direct_url_fetch":
        if not rule.direct_url_allowed:
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="provider_rules",
                reason_code=DIRECT_URL_NOT_ALLOWED,
                provider=provider,
                strategy=strategy,
            )

        url = _clean_optional_text(row.get("canonical_url")) or _clean_optional_text(row.get("url")) or ""
        if not url:
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="provider_rules",
                reason_code=MISSING_URL,
                provider=provider,
                strategy=strategy,
            )

        try:
            response = client.request_response("GET", url)
        except HttpFailure as exc:
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="remote_http",
                reason_code=HTTP_FAILURE,
                provider=provider,
                strategy=strategy,
                status_code=exc.status_code,
                message=exc.reason,
                retry_count=max(0, int(exc.attempts) - 1),
                rate_limited=exc.status_code == 429,
            )

        content_type = _clean_optional_text(response.headers.get("Content-Type")) or ""
        if "html" not in content_type.lower():
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="remote_http",
                reason_code=NON_HTML_RESPONSE,
                provider=provider,
                strategy=strategy,
                status_code=response.status_code,
                message=content_type,
            )

        html = response.text or ""
        if not html.strip():
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="remote_http",
                reason_code=EMPTY_BODY,
                provider=provider,
                strategy=strategy,
                status_code=response.status_code,
            )

        text = extract_visible_text_from_html(html)
        if not text:
            persisted = _persist_html_only(config=config, row=row, html=html, strategy=strategy, status_code=response.status_code)
            return ResolutionResult(
                article_id=article_id,
                representation="content",
                resolved=False,
                source="remote_http",
                reason_code=PARSE_FAILURE,
                provider=provider,
                strategy=strategy,
                status_code=response.status_code,
                message="html_without_visible_text",
                local_write_performed=True,
                artifact_paths=[persisted["html_path"]],
                meta_sidecar_path=persisted["meta_sidecar_path"],
            )

        persisted = _persist_content_artifacts(
            config=config,
            row=row,
            text=text,
            html=html,
            strategy=strategy,
            status_code=response.status_code,
            message=content_type,
        )
        return ResolutionResult(
            article_id=article_id,
            representation="content",
            resolved=True,
            source="remote_http",
            reason_code=SUCCESS,
            provider=provider,
            strategy=strategy,
            status_code=response.status_code,
            content_available=True,
            local_write_performed=True,
            artifact_paths=persisted["artifact_paths"],
            meta_sidecar_path=persisted["meta_sidecar_path"],
            retry_count=max(0, int(getattr(client, "last_attempts", 1)) - 1),
            rate_limited=bool(getattr(client, "last_rate_limited", False)),
        )

    return None


def _build_newsdata_query(row: dict[str, Any]) -> str:
    for field in ["provider_document_id", "canonical_url", "url", "title"]:
        value = _clean_optional_text(row.get(field))
        if value:
            return value
    return "news"


def _select_newsdata_match(local_row: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    local_doc_id = _clean_optional_text(local_row.get("provider_document_id"))
    local_canonical = _clean_optional_text(local_row.get("canonical_url"))
    local_url = _clean_optional_text(local_row.get("url"))
    local_title = _normalize_title(_clean_optional_text(local_row.get("title")) or "")
    local_pub_date = _date_part(local_row.get("published_at"))

    # Precedence 1: provider_document_id
    if local_doc_id:
        for item in candidates:
            remote_doc = _clean_optional_text(item.get("article_id")) or _clean_optional_text(item.get("link"))
            if remote_doc and remote_doc == local_doc_id:
                return item

    # Precedence 2: URL exact match
    targets = {value for value in [local_canonical, local_url] if value}
    if targets:
        for item in candidates:
            remote_url = _clean_optional_text(item.get("link"))
            if remote_url and remote_url in targets:
                return item

    # Precedence 3: title + date proximity
    if local_title:
        for item in candidates:
            remote_title = _normalize_title(_clean_optional_text(item.get("title")) or "")
            if not remote_title or remote_title != local_title:
                continue
            remote_date = _date_part(item.get("pubDate"))
            if remote_date == local_pub_date or local_pub_date == "1970-01-01":
                return item

    return None


def _merge_metadata_row_with_newsdata(local_row: dict[str, Any], match: dict[str, Any]) -> dict[str, Any]:
    row = dict(local_row)
    normalized = normalize_newsdata_item(match)
    updates = {
        "source_name": normalized["source_name"],
        "source_domain": normalized["source_domain"],
        "url": normalized["url"],
        "canonical_url": normalized["canonical_url"],
        "title": normalized["title"],
        "published_at": normalized["published_at"],
        "language": normalized["language"],
        "section": normalized["section"],
        "byline": normalized["byline"],
        "summary_text": normalized["summary_text"],
        "snippet": normalized["snippet"],
        "article_text": normalized["article_text"],
        "metadata_json": json.dumps({"provider_native": match}, sort_keys=True, separators=(",", ":")),
        "imported_at": _utc_now(),
    }

    # Preserve identity invariants and update only mutable non-empty fields.
    for key, value in updates.items():
        if value:
            row[key] = value

    row["article_id"] = local_row.get("article_id")
    row["provider"] = local_row.get("provider")
    row["provider_document_id"] = local_row.get("provider_document_id")
    row["resolved_document_identity"] = local_row.get("resolved_document_identity")

    return row


def _upsert_article_row(config: AppConfig, row: dict[str, Any]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[row],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )


def _extract_text_from_metadata(row: dict[str, Any]) -> str | None:
    direct = _clean_optional_text(row.get("article_text"))
    if direct:
        return direct

    metadata_json = row.get("metadata_json")
    if metadata_json is None:
        return None

    parsed: dict[str, Any] | None = None
    if isinstance(metadata_json, dict):
        parsed = metadata_json
    elif isinstance(metadata_json, str):
        try:
            loaded = json.loads(metadata_json)
            if isinstance(loaded, dict):
                parsed = loaded
        except ValueError:
            parsed = None

    if not parsed:
        return None

    candidates = [
        parsed.get("content"),
        parsed.get("article_text"),
        parsed.get("full_text"),
    ]

    native = parsed.get("provider_native")
    if isinstance(native, dict):
        candidates.extend([native.get("content"), native.get("full_content"), native.get("description")])

    for value in candidates:
        text = _clean_optional_text(value)
        if text:
            return text

    return None


def _persist_content_artifacts(
    *,
    config: AppConfig,
    row: dict[str, Any],
    text: str,
    strategy: str,
    html: str | None = None,
    status_code: int | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    article_id = _clean_optional_text(row.get("article_id")) or ""
    provider = _clean_optional_text(row.get("provider")) or ""
    source_name = _clean_optional_text(row.get("source_name")) or ""
    source_domain = _clean_optional_text(row.get("source_domain")) or ""
    published_at = row.get("published_at")
    mapping = ensure_storage_mapping(config, row)
    storage_article_id = mapping["storage_article_id"]

    publisher_slug = derive_publisher_slug(source_domain=source_domain, source_name=source_name, provider=provider)

    text_path = publisher_article_artifact_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=storage_article_id,
        extension="txt",
    )
    _write_if_stronger(text_path, text)

    json_path = publisher_article_artifact_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=storage_article_id,
        extension="json",
    )
    if not _is_nonempty_path(json_path):
        write_json(
            json_path,
            {
                "article_id": article_id,
                "storage_article_id": storage_article_id,
                "provider": provider,
                "strategy": strategy,
                "fetched_at": _utc_now(),
                "status_code": status_code,
                "message": message,
                "text_char_count": len(text),
            },
        )

    artifact_rows = [
        {
            "article_id": article_id,
            "storage_article_id": storage_article_id,
            "artifact_type": "article_text",
            "artifact_path": str(text_path),
            "provider": provider,
            "source_domain": source_domain,
            "published_date": _date_part(published_at),
            "exists_locally": True,
        },
        {
            "article_id": article_id,
            "storage_article_id": storage_article_id,
            "artifact_type": "article_json",
            "artifact_path": str(json_path),
            "provider": provider,
            "source_domain": source_domain,
            "published_date": _date_part(published_at),
            "exists_locally": True,
        },
    ]

    html_path: Path | None = None
    if html is not None:
        html_path = publisher_article_artifact_path(
            config,
            publisher_slug=publisher_slug,
            published_at=published_at,
            article_id=storage_article_id,
            extension="html",
        )
        _write_if_stronger(html_path, html)
        artifact_rows.append(
            {
                "article_id": article_id,
                "storage_article_id": storage_article_id,
                "artifact_type": "article_html",
                "artifact_path": str(html_path),
                "provider": provider,
                "source_domain": source_domain,
                "published_date": _date_part(published_at),
                "exists_locally": True,
            }
        )

    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_artifacts"),
        rows=artifact_rows,
        dedupe_keys=["article_id", "storage_article_id", "artifact_type"],
        column_order=ARTICLE_ARTIFACT_COLUMNS,
        drop_extra_columns=False,
    )

    meta_path = publisher_article_meta_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=storage_article_id,
    )
    meta_payload = {
        "article_id": article_id,
        "storage_article_id": storage_article_id,
        "provider": provider,
        "publisher_slug": publisher_slug,
        "source_name": source_name,
        "source_domain": source_domain,
        "published_at": str(published_at or ""),
        "last_resolution": {
            "strategy": strategy,
            "resolved_at": _utc_now(),
            "status_code": status_code,
            "message": message,
        },
    }
    write_json(meta_path, meta_payload)

    paths = [str(text_path), str(json_path)]
    if html_path is not None:
        paths.append(str(html_path))

    return {
        "artifact_paths": paths,
        "meta_sidecar_path": str(meta_path),
    }


def _persist_html_only(
    *,
    config: AppConfig,
    row: dict[str, Any],
    html: str,
    strategy: str,
    status_code: int | None,
) -> dict[str, str]:
    article_id = _clean_optional_text(row.get("article_id")) or ""
    provider = _clean_optional_text(row.get("provider")) or ""
    source_name = _clean_optional_text(row.get("source_name")) or ""
    source_domain = _clean_optional_text(row.get("source_domain")) or ""
    published_at = row.get("published_at")
    mapping = ensure_storage_mapping(config, row)
    storage_article_id = mapping["storage_article_id"]
    publisher_slug = derive_publisher_slug(source_domain=source_domain, source_name=source_name, provider=provider)

    html_path = publisher_article_artifact_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=storage_article_id,
        extension="html",
    )
    _write_if_stronger(html_path, html)
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_artifacts"),
        rows=[
            {
                "article_id": article_id,
                "storage_article_id": storage_article_id,
                "artifact_type": "article_html",
                "artifact_path": str(html_path),
                "provider": provider,
                "source_domain": source_domain,
                "published_date": _date_part(published_at),
                "exists_locally": True,
            }
        ],
        dedupe_keys=["article_id", "storage_article_id", "artifact_type"],
        column_order=ARTICLE_ARTIFACT_COLUMNS,
        drop_extra_columns=False,
    )
    meta_path = publisher_article_meta_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=storage_article_id,
    )
    write_json(
        meta_path,
        {
            "article_id": article_id,
            "storage_article_id": storage_article_id,
            "provider": provider,
            "last_resolution": {
                "strategy": strategy,
                "resolved_at": _utc_now(),
                "status_code": status_code,
                "message": "html_without_visible_text",
            },
        },
    )
    return {"html_path": str(html_path), "meta_sidecar_path": str(meta_path)}


def _record_event(
    config: AppConfig,
    result: ResolutionResult,
    *,
    strategy: str,
    artifact_path: str | None,
    provenance: dict[str, Any],
) -> None:
    event_at = _utc_now()
    material = (
        f"{event_at}|{result.article_id}|{result.provider}|{result.representation}|{strategy}|{result.reason_code}|{artifact_path or ''}"
    )
    event_id = f"evt_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:20]}"

    allow_remote = bool(provenance.get("allow_remote"))
    force_remote = bool(provenance.get("force_remote"))
    resolution_mode = result.resolution_mode or _derive_resolution_mode(allow_remote=allow_remote, force_remote=force_remote)
    remote_attempted = bool(result.remote_attempted or _derive_remote_attempted(result=result, allow_remote=allow_remote))
    served_from = result.served_from or _derive_served_from(result=result)
    canonical_key = f"article:{result.article_id}"
    provider_requested = _clean_optional_text(result.provider_requested) or _clean_optional_text(result.provider) or None
    provider_used = _clean_optional_text(result.provider_used) or _clean_optional_text(result.provider) or None

    event_row = {
        "event_id": event_id,
        "event_at": event_at,
        "article_id": result.article_id,
        "provider": result.provider,
        "representation": result.representation,
        "strategy": strategy,
        "success": result.resolved,
        "reason_code": result.reason_code,
        "message": result.message,
        "status_code": result.status_code,
        "artifact_path": artifact_path,
        "meta_sidecar_path": result.meta_sidecar_path,
        "provenance_json": json.dumps(provenance, sort_keys=True),
        "domain": "news",
        "content_domain": "article",
        "canonical_key": canonical_key,
        "resolution_mode": resolution_mode,
        "provider_requested": provider_requested,
        "provider_used": provider_used,
        "method_used": strategy,
        "served_from": served_from,
        "remote_attempted": remote_attempted,
        "persisted_locally": bool(result.local_write_performed),
        "http_status": result.status_code,
        "retry_count": int(result.retry_count),
        "rate_limited": bool(result.rate_limited),
        "deferred_until": result.deferred_until,
        "latency_ms": None,
    }
    append_parquet_rows(
        path=normalized_artifact_path(config, "resolution_events"),
        rows=[event_row],
        column_order=RESOLUTION_EVENT_COLUMNS,
    )


def _load_articles(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "articles")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLES_COLUMNS)
    df = pd.read_parquet(path)
    for column in ARTICLES_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[ARTICLES_COLUMNS].copy()


def load_resolution_events(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "resolution_events")
    if not path.exists():
        return pd.DataFrame(columns=RESOLUTION_EVENT_COLUMNS)
    df = pd.read_parquet(path)
    for column in RESOLUTION_EVENT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[RESOLUTION_EVENT_COLUMNS].copy()


def query_resolution_events(
    config: AppConfig,
    *,
    article_id: str | None = None,
    provider: str | None = None,
    representation: str | None = None,
    reason_code: str | None = None,
    success: bool | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    events = load_resolution_events(config)
    if article_id:
        events = events[events["article_id"] == article_id]
    if provider:
        events = events[events["provider"] == provider]
    if representation:
        events = events[events["representation"] == representation]
    if reason_code:
        events = events[events["reason_code"] == reason_code]
    if success is not None:
        events = events[events["success"] == success]

    ordered = events.copy()
    ordered["_event_ts"] = pd.to_datetime(ordered["event_at"], errors="coerce", utc=True)
    ordered = ordered.sort_values(by=["_event_ts", "event_id"], ascending=[False, False], na_position="last")
    ordered = ordered.drop(columns=["_event_ts"])
    if limit > 0:
        ordered = ordered.head(limit)
    return ordered.reset_index(drop=True)


def _local_content_status(config: AppConfig, article_id: str) -> dict[str, Any]:
    artifacts_path = normalized_artifact_path(config, "article_artifacts")
    if not artifacts_path.exists():
        return {"available": False, "paths": []}

    df = pd.read_parquet(artifacts_path)
    if df.empty:
        return {"available": False, "paths": []}

    matched = df[(df["article_id"] == article_id) & (df["artifact_type"] == "article_text")]
    storage_id = mapped_storage_id_for_article(config, article_id)
    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    if storage_id and "storage_article_id" in df.columns:
        mapped_ids = mapped_article_ids_for_storage(config, storage_id)
        storage_matches = df[
            (df["storage_article_id"] == storage_id)
            & (df["artifact_type"] == "article_text")
            & (df["article_id"].isin(mapped_ids))
        ]
        if not storage_matches.empty:
            matched = pd.concat([matched, storage_matches], ignore_index=True).drop_duplicates()
    paths: list[str] = []
    for path in matched.get("artifact_path", []):
        text = _clean_optional_text(path)
        if not text:
            continue
        if storage_id and not str(Path(text).resolve()).startswith(canonical_prefix):
            continue
        if Path(text).exists():
            paths.append(str(Path(text).resolve()))

    return {"available": bool(paths), "paths": paths}


def _write_if_stronger(path: Path, content: str) -> None:
    if _is_nonempty_path(path):
        return
    write_text(path, content)


def _is_nonempty_path(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _derive_domain(url: str | None) -> str | None:
    value = _clean_optional_text(url)
    if not value:
        return None
    parsed = urlparse(value)
    domain = _clean_optional_text(parsed.netloc)
    if not domain:
        return None
    return domain.lower()


def _normalize_title(value: str) -> str:
    return " ".join(value.lower().split())


def _date_part(value: Any) -> str:
    text = _clean_optional_text(value)
    if not text:
        return "1970-01-01"
    return text[:10] if len(text) >= 10 else "1970-01-01"


def _first_path(paths: list[str] | None) -> str | None:
    if not paths:
        return None
    return paths[0]


def _merge_provenance(base: dict[str, Any], result: ResolutionResult) -> dict[str, Any]:
    merged = dict(base)
    if result.auth_env_var:
        merged["auth_env_var"] = result.auth_env_var
    if result.auth_configured is not None:
        merged["auth_configured"] = bool(result.auth_configured)
    merged["retry_count"] = int(result.retry_count)
    merged["rate_limited"] = bool(result.rate_limited)
    if result.deferred_until:
        merged["deferred_until"] = result.deferred_until
    if result.provider_requested:
        merged["provider_requested"] = result.provider_requested
    if result.provider_used:
        merged["provider_used"] = result.provider_used
    if result.served_from:
        merged["served_from"] = result.served_from
    if result.provider_skip_reasons:
        merged["provider_skip_reasons"] = result.provider_skip_reasons
    return merged


def _enrich_result(
    result: ResolutionResult,
    *,
    allow_remote: bool,
    force_remote: bool,
    provider_requested: str | None,
) -> ResolutionResult:
    result.resolution_mode = _derive_resolution_mode(allow_remote=allow_remote, force_remote=force_remote)
    if provider_requested:
        result.provider_requested = provider_requested
    if result.provider and not result.provider_used:
        result.provider_used = result.provider
    if not result.served_from:
        result.served_from = _derive_served_from(result=result)
    result.remote_attempted = _derive_remote_attempted(result=result, allow_remote=allow_remote)
    if result.provider_skip_reasons is None:
        result.provider_skip_reasons = []
    return result


def _derive_resolution_mode(*, allow_remote: bool, force_remote: bool) -> str:
    if not allow_remote:
        return "local_only"
    if force_remote:
        return "refresh_if_stale"
    return "resolve_if_missing"


def _derive_remote_attempted(*, result: ResolutionResult, allow_remote: bool) -> bool:
    if not allow_remote:
        return False
    local_sources = {"local_artifact", "local_metadata", "local_only"}
    if result.source in local_sources:
        return False
    local_strategies = {"local_artifact", "local_metadata", "local_only"}
    return (result.strategy or "") not in local_strategies


def _derive_served_from(*, result: ResolutionResult) -> str:
    if result.source == "local_artifact":
        return "local_cache"
    if result.source in {"local_metadata", "local_only"}:
        return "local_normalized"
    if result.local_write_performed:
        return "remote_then_persisted"
    if result.source in {"remote_http", "provider_api", "provider_payload"}:
        return "remote_ephemeral"
    return "none"


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "nat", "none"}:
        return None
    return text
