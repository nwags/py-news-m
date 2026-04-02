"""Source-neutral local tabular article history adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from py_news.adapters.article_bulk_utils import (
    extract_first,
    normalize_datetime,
    normalize_provider,
    normalize_text,
    normalize_url,
    row_has_meaningful_metadata,
)
from py_news.adapters.base import ArticleBulkAdapter
from py_news.models import NewsArticleRecord, derive_article_identity


class LocalTabularArticleAdapter(ArticleBulkAdapter):
    """Load articles from CSV, JSONL, or parquet datasets."""

    def __init__(self) -> None:
        self.last_total_rows = 0
        self.last_skipped_rows = 0

    def load_articles(self, dataset_path: str) -> list[NewsArticleRecord]:
        path = Path(dataset_path)
        frame = _load_frame(path)
        self.last_total_rows = len(frame)
        self.last_skipped_rows = 0

        records: list[NewsArticleRecord] = []
        columns = list(frame.columns)

        for row in frame.to_dict(orient="records"):
            mapped = _map_row(row=row, columns=columns)
            if not row_has_meaningful_metadata(mapped):
                self.last_skipped_rows += 1
                continue

            identity = derive_article_identity(
                provider=mapped["provider"],
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
                    provider=mapped["provider"],
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


def _load_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported dataset extension: {suffix}")


def _map_row(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    provider = normalize_provider(extract_first(row, columns, ["provider", "source_provider"]))

    metadata_columns = [
        "metadata_json",
        "metadata",
        "meta",
        "extra_metadata",
    ]
    metadata_json = extract_first(row, columns, metadata_columns)
    if isinstance(metadata_json, float) and pd.isna(metadata_json):
        metadata_json = None
    if metadata_json is not None and not isinstance(metadata_json, dict):
        metadata_json = {"raw": str(metadata_json)}

    return {
        "provider": provider,
        "provider_document_id": normalize_text(
            extract_first(row, columns, ["provider_document_id", "document_id", "id", "article_id"])
        ),
        "source_name": normalize_text(
            extract_first(row, columns, ["source_name", "publication", "source", "news_source"])
        ),
        "source_domain": normalize_text(extract_first(row, columns, ["source_domain", "domain"])),
        "url": normalize_url(extract_first(row, columns, ["url", "article_url", "source_url", "link"])),
        "canonical_url": normalize_url(extract_first(row, columns, ["canonical_url"])),
        "title": normalize_text(extract_first(row, columns, ["title", "headline"])),
        "published_at": normalize_datetime(
            extract_first(row, columns, ["published_at", "publish_date", "published", "date", "datetime"])
        ),
        "language": normalize_text(extract_first(row, columns, ["language", "lang"])),
        "section": normalize_text(extract_first(row, columns, ["section", "desk", "category"])),
        "byline": normalize_text(extract_first(row, columns, ["byline", "author"])),
        "article_text": normalize_text(extract_first(row, columns, ["article_text", "content", "body", "text", "full_text"])),
        "summary_text": normalize_text(extract_first(row, columns, ["summary", "abstract", "lead_paragraph"])),
        "snippet": normalize_text(extract_first(row, columns, ["snippet", "description"])),
        "metadata_json": metadata_json,
    }
