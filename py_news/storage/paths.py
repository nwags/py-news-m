"""Centralized deterministic path construction."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import re
from urllib.parse import urlparse

from py_news.config import AppConfig

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    normalized = (value or "unknown").strip().lower()
    normalized = _SLUG_RE.sub("-", normalized).strip("-")
    return normalized or "unknown"


def _to_date(value: date | datetime | str | None) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value or "").strip()
    if not text:
        return date(1970, 1, 1)

    # Fast ISO date handling first.
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return date(1970, 1, 1)


def normalized_artifact_path(config: AppConfig, artifact_name: str) -> Path:
    stem = artifact_name.removesuffix(".parquet")
    return config.refdata_normalized_root / f"{stem}.parquet"


def derive_publisher_slug(
    *,
    source_domain: str | None,
    source_name: str | None,
    provider: str | None,
) -> str:
    """Deterministic publisher slug derivation.

    Order:
    1) canonical domain when present,
    2) source/publisher name,
    3) provider id fallback.
    """

    domain = normalized_domain(source_domain)
    if domain:
        return slugify(domain)

    name = str(source_name or "").strip()
    if name:
        return slugify(name)

    return slugify(str(provider or "unknown"))


def normalized_domain(value: str | None) -> str | None:
    text = str(value or "").strip().lower().rstrip("/")
    if not text:
        return None
    candidate = text if "://" in text else f"https://{text}"
    parsed = urlparse(candidate)
    host = parsed.netloc.strip().lower()
    if host:
        return host
    # Fallback for malformed inputs.
    return text.split("/", 1)[0] or None


def publisher_storage_article_dir_path(
    config: AppConfig,
    *,
    publisher_slug: str,
    published_at: date | datetime | str | None,
    storage_article_id: str,
) -> Path:
    published = _to_date(published_at)
    return (
        config.cache_root
        / "publisher"
        / "data"
        / slugify(publisher_slug)
        / f"{published.year:04d}"
        / f"{published.month:02d}"
        / slugify(storage_article_id)
    )


def provider_full_index_dir_path(config: AppConfig, *, provider_id: str) -> Path:
    return config.cache_root / "provider" / "full-index" / slugify(provider_id)


def publisher_article_artifact_path(
    config: AppConfig,
    *,
    publisher_slug: str,
    published_at: date | datetime | str | None,
    article_id: str,
    extension: str,
) -> Path:
    # Backward-compatible function name with new article-folder layout semantics.
    folder = publisher_storage_article_dir_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        storage_article_id=article_id,
    )
    ext = extension.lstrip(".").lower()
    filename = "article.txt"
    if ext == "html":
        filename = "article.html"
    elif ext == "json":
        filename = "article.json"
    return folder / filename


def publisher_article_meta_path(
    config: AppConfig,
    *,
    publisher_slug: str,
    published_at: date | datetime | str | None,
    article_id: str,
) -> Path:
    # Backward-compatible function name with new article-folder layout semantics.
    folder = publisher_storage_article_dir_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        storage_article_id=article_id,
    )
    return folder / "meta.json"
