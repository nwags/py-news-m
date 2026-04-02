"""Local NYT archive historical adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import json

from py_news.adapters.article_bulk_utils import (
    normalize_datetime,
    normalize_text,
    normalize_url,
)
from py_news.adapters.base import ArticleBulkAdapter
from py_news.models import NewsArticleRecord, derive_article_identity


class NytArchiveArticleAdapter(ArticleBulkAdapter):
    """Load NYT archive-style JSON payloads from local disk."""

    provider = "nyt_archive"

    def __init__(self) -> None:
        self.last_total_rows = 0
        self.last_skipped_rows = 0

    def load_articles(self, dataset_path: str) -> list[NewsArticleRecord]:
        path = Path(dataset_path)
        if path.suffix.lower() != ".json":
            raise ValueError(f"Unsupported dataset extension for nyt_archive: {path.suffix.lower()}")

        payload = json.loads(path.read_text(encoding="utf-8"))
        docs = _extract_docs(payload)
        self.last_total_rows = len(docs)
        self.last_skipped_rows = 0

        records: list[NewsArticleRecord] = []
        for doc in docs:
            mapped = _map_doc(doc)
            if not _row_is_viable(mapped):
                self.last_skipped_rows += 1
                continue

            identity = derive_article_identity(
                provider=self.provider,
                provider_document_id=mapped["provider_document_id"],
                canonical_url=mapped["canonical_url"],
                url=mapped["url"],
                source_name=mapped["source_name"],
                title=mapped["title"],
                published_at=mapped["published_at"],
            )

            records.append(
                NewsArticleRecord(
                    article_id=identity.article_id,
                    provider=self.provider,
                    provider_document_id=mapped["provider_document_id"],
                    resolved_document_identity=identity.resolved_document_identity,
                    source_name=mapped["source_name"],
                    source_domain=mapped["source_domain"],
                    url=mapped["url"],
                    canonical_url=mapped["canonical_url"],
                    title=mapped["title"],
                    published_at=mapped["published_at"],
                    language=mapped["language"],
                    section=mapped["section"],
                    byline=mapped["byline"],
                    article_text=mapped["article_text"],
                    summary_text=mapped["summary_text"],
                    snippet=mapped["snippet"],
                    metadata_json=mapped["metadata_json"],
                )
            )

        return records


def _extract_docs(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [doc for doc in payload if isinstance(doc, dict)]

    if not isinstance(payload, dict):
        return []

    response = payload.get("response")
    if isinstance(response, dict) and isinstance(response.get("docs"), list):
        return [doc for doc in response["docs"] if isinstance(doc, dict)]

    docs = payload.get("docs")
    if isinstance(docs, list):
        return [doc for doc in docs if isinstance(doc, dict)]

    return []


def _map_doc(doc: dict[str, Any]) -> dict[str, Any]:
    web_url = normalize_url(doc.get("web_url"))
    headline = doc.get("headline") if isinstance(doc.get("headline"), dict) else {}
    byline_obj = doc.get("byline") if isinstance(doc.get("byline"), dict) else {}

    source_name = normalize_text(doc.get("source")) or "New York Times"
    source_domain = _domain_from_url(web_url) or "nytimes.com"
    title = normalize_text(headline.get("main")) or normalize_text(headline.get("print_headline"))
    section = normalize_text(doc.get("section_name")) or normalize_text(doc.get("news_desk"))
    snippet = normalize_text(doc.get("snippet")) or normalize_text(doc.get("lead_paragraph"))
    provider_document_id = (
        normalize_text(doc.get("_id"))
        or normalize_text(doc.get("uri"))
        or web_url
    )

    return {
        "provider_document_id": provider_document_id,
        "source_name": source_name,
        "source_domain": source_domain,
        "url": web_url,
        "canonical_url": web_url,
        "title": title,
        "published_at": normalize_datetime(doc.get("pub_date")),
        "language": normalize_text(doc.get("language")),
        "section": section,
        "byline": normalize_text(byline_obj.get("original")),
        "article_text": normalize_text(doc.get("article_text"))
        or normalize_text(doc.get("full_text"))
        or normalize_text(doc.get("body"))
        or normalize_text(doc.get("content")),
        "summary_text": normalize_text(doc.get("abstract")),
        "snippet": snippet,
        "metadata_json": _build_metadata_json(doc),
    }


def _domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    domain = normalize_text(parsed.netloc)
    if not domain:
        return None
    return domain.lower()


def _build_metadata_json(doc: dict[str, Any]) -> dict[str, Any] | None:
    metadata: dict[str, Any] = {}
    for key in [
        "keywords",
        "multimedia",
        "document_type",
        "type_of_material",
        "news_desk",
        "section_name",
        "subsection_name",
        "word_count",
        "print_page",
        "uri",
    ]:
        value = doc.get(key)
        if value is not None:
            metadata[key] = value

    normalized_keys = {
        "_id",
        "source",
        "web_url",
        "headline",
        "pub_date",
        "language",
        "byline",
        "abstract",
        "snippet",
        "lead_paragraph",
        "article_text",
        "full_text",
        "body",
        "content",
    }
    extra = {k: v for k, v in doc.items() if k not in normalized_keys and k not in metadata}
    if extra:
        metadata["raw_doc"] = extra

    return metadata or None


def _row_is_viable(values: dict[str, Any]) -> bool:
    keys = [
        "provider_document_id",
        "url",
        "canonical_url",
        "title",
        "published_at",
        "article_text",
        "summary_text",
        "snippet",
    ]
    return any(values.get(key) for key in keys)
