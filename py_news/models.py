"""Core domain records and identity utilities for metadata-first ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
import hashlib
import json
from typing import Any

def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def _normalize_provider(value: Any, default: str = "local_tabular") -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return default
    return normalized.lower()

ARTICLES_COLUMNS = [
    "article_id",
    "provider",
    "provider_document_id",
    "resolved_document_identity",
    "source_name",
    "source_domain",
    "url",
    "canonical_url",
    "title",
    "published_at",
    "language",
    "section",
    "byline",
    "article_text",
    "summary_text",
    "snippet",
    "metadata_json",
    "imported_at",
]

ARTICLE_ARTIFACT_COLUMNS = [
    "article_id",
    "storage_article_id",
    "artifact_type",
    "artifact_path",
    "provider",
    "source_domain",
    "published_date",
    "exists_locally",
]

STORAGE_ARTICLES_COLUMNS = [
    "storage_article_id",
    "publisher_slug",
    "storage_anchor_date",
    "storage_folder_path",
    "equivalence_basis",
    "equivalence_value",
    "created_at",
    "updated_at",
]

ARTICLE_STORAGE_MAP_COLUMNS = [
    "article_id",
    "provider",
    "resolved_document_identity",
    "storage_article_id",
    "mapping_basis",
    "mapped_at",
]

LOOKUP_ARTICLE_COLUMNS = [
    "article_id",
    "provider",
    "source_name",
    "source_domain",
    "published_at",
    "title",
    "summary_text",
    "snippet",
    "canonical_url",
    "url",
    "language",
    "section",
]

PROVIDER_REGISTRY_COLUMNS = [
    "provider_id",
    "provider_name",
    "provider_type",
    "is_active",
    "history_mode",
    "recent_mode",
    "content_mode",
    "api_base_url",
    "auth_type",
    "auth_env_var",
    "rate_limit_policy",
    "preferred_resolution_order",
    "direct_url_allowed",
    "supports_metadata_only",
    "supports_partial_content",
    "supports_full_content",
    "requires_js",
    "provider_instructions",
    "version",
    "updated_at",
]

RESOLUTION_EVENT_COLUMNS = [
    "event_id",
    "event_at",
    "article_id",
    "provider",
    "representation",
    "strategy",
    "success",
    "reason_code",
    "message",
    "status_code",
    "artifact_path",
    "meta_sidecar_path",
    "provenance_json",
]


def _iso(value: datetime | date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True, slots=True)
class ArticleIdentity:
    resolved_document_identity: str
    article_id: str


def derive_article_identity(
    provider: str | None,
    provider_document_id: str | None,
    canonical_url: str | None,
    url: str | None,
    source_name: str | None,
    title: str | None,
    published_at: str | None,
) -> ArticleIdentity:
    normalized_provider = _normalize_provider(provider)
    normalized_document_id = _normalize_text(provider_document_id)
    normalized_canonical_url = _normalize_text(canonical_url)
    normalized_url = _normalize_text(url)
    normalized_source_name = _normalize_text(source_name)
    normalized_title = _normalize_text(title)
    normalized_published_at = _normalize_text(published_at)

    if normalized_document_id:
        identity = f"provider={normalized_provider}|provider_document_id={normalized_document_id}"
    elif normalized_canonical_url or normalized_url:
        identity_url = normalized_canonical_url or normalized_url
        identity = f"provider={normalized_provider}|url={identity_url}"
    else:
        source_value = normalized_source_name or "unknown"
        title_value = normalized_title or "unknown"
        published_value = normalized_published_at or "unknown"
        identity = (
            f"provider={normalized_provider}|"
            f"source_name={source_value}|title={title_value}|published_at={published_value}"
        )

    article_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return ArticleIdentity(
        resolved_document_identity=identity,
        article_id=f"art_{article_hash}",
    )


@dataclass(slots=True)
class NewsArticleRecord:
    """Canonical article metadata where full text is optional."""

    article_id: str
    provider: str
    provider_document_id: str | None = None
    resolved_document_identity: str | None = None
    source_name: str | None = None
    source_domain: str | None = None
    url: str | None = None
    canonical_url: str | None = None
    title: str | None = None
    published_at: str | None = None
    language: str | None = None
    section: str | None = None
    byline: str | None = None
    article_text: str | None = None
    summary_text: str | None = None
    snippet: str | None = None
    metadata_json: dict[str, Any] | str | None = None

    def to_record(self, imported_at: str | None = None) -> dict[str, Any]:
        return {
            "article_id": self.article_id,
            "provider": self.provider,
            "provider_document_id": self.provider_document_id,
            "resolved_document_identity": self.resolved_document_identity,
            "source_name": self.source_name,
            "source_domain": self.source_domain,
            "url": self.url,
            "canonical_url": self.canonical_url,
            "title": self.title,
            "published_at": _iso(self.published_at),
            "language": self.language,
            "section": self.section,
            "byline": self.byline,
            "article_text": self.article_text,
            "summary_text": self.summary_text,
            "snippet": self.snippet,
            "metadata_json": _serialize_metadata_json(self.metadata_json),
            "imported_at": imported_at,
        }


def _serialize_metadata_json(value: dict[str, Any] | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    text = str(value).strip()
    return text or None


@dataclass(slots=True)
class ArticleArtifactRecord:
    article_id: str
    artifact_type: str
    artifact_path: str
    storage_article_id: str | None = None
    provider: str | None = None
    source_domain: str | None = None
    published_date: date | str | None = None
    exists_locally: bool = True

    def to_record(self) -> dict[str, Any]:
        return {
            "article_id": self.article_id,
            "storage_article_id": self.storage_article_id,
            "artifact_type": self.artifact_type,
            "artifact_path": self.artifact_path,
            "provider": self.provider,
            "source_domain": self.source_domain,
            "published_date": _iso(self.published_date),
            "exists_locally": self.exists_locally,
        }


@dataclass(slots=True)
class SourceWindowRecord:
    provider: str
    window_date: date | datetime | str
    window_key: str
    request_id: str
    payload_path: str
    fetch_status: str = "placeholder"
    fetched_at: datetime | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "window_date": _iso(self.window_date),
            "window_key": self.window_key,
            "request_id": self.request_id,
            "payload_path": self.payload_path,
            "fetch_status": self.fetch_status,
            "fetched_at": _iso(self.fetched_at),
        }
