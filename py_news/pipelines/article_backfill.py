"""Phase 3 narrow recent-window metadata backfill pipeline."""

from __future__ import annotations

from datetime import date

from py_news.adapters import GdeltRecentArticleAdapter, NewsDataRecentArticleAdapter
from py_news.adapters.article_bulk_utils import utc_now_iso
from py_news.config import AppConfig, ensure_runtime_dirs
from py_news.models import ARTICLES_COLUMNS
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


SUPPORTED_RECENT_PROVIDERS = {
    "gdelt_recent": GdeltRecentArticleAdapter,
    "newsdata": NewsDataRecentArticleAdapter,
}


def run_article_backfill(
    config: AppConfig,
    *,
    provider: str,
    window_date: date,
    window_key: str,
    query: str | None = None,
    max_records: int | None = None,
) -> dict:
    ensure_runtime_dirs(config)

    adapter_class = SUPPORTED_RECENT_PROVIDERS.get(provider)
    if adapter_class is None:
        raise ValueError(f"Unsupported provider for Phase 3 backfill: {provider}")

    adapter = adapter_class(config)
    result = adapter.fetch_window(
        window_date=window_date,
        window_key=window_key,
        query=query,
        max_records=max_records,
    )

    imported_at = utc_now_iso()
    article_rows = [record.to_record(imported_at=imported_at) for record in result.articles]
    upsert_details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=article_rows,
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )

    return {
        "stage": "article_backfill",
        "status": "ok",
        "provider": provider,
        "window_date": window_date.isoformat(),
        "window_key": window_key,
        "request_id": result.request_id,
        "raw_payload_path": result.raw_payload_path,
        "fetched_rows": result.fetched_rows,
        "normalized_rows": result.normalized_rows,
        "skipped_rows": result.skipped_rows,
        "requested_max_records": result.requested_max_records,
        "effective_max_records": result.effective_max_records,
        "max_records_clamped": result.max_records_clamped,
        "deduped_rows": int(upsert_details["deduped_count"]),
        "articles_after_count": int(upsert_details["after_count"]),
        "articles_path": str(upsert_details["path"]),
        "project_root": str(config.project_root),
    }
