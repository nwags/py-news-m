"""Phase 2 article history import pipeline."""

from __future__ import annotations

from pathlib import Path

from py_news.adapters import LocalTabularArticleAdapter, NytArchiveArticleAdapter
from py_news.adapters.article_bulk_utils import utc_now_iso
from py_news.config import AppConfig, ensure_runtime_dirs
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS, NewsArticleRecord
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


ADAPTER_SPECS = {
    "local_tabular": {
        "adapter_class": LocalTabularArticleAdapter,
        "extensions": {".csv", ".jsonl", ".parquet"},
    },
    "nyt_archive": {
        "adapter_class": NytArchiveArticleAdapter,
        "extensions": {".json"},
    },
}


def run_article_import_history(
    config: AppConfig,
    dataset: str,
    adapter_name: str,
) -> dict:
    ensure_runtime_dirs(config)

    dataset_path = Path(dataset)
    if not dataset_path.exists() or not dataset_path.is_file():
        raise click_usage_error(f"Dataset file not found: {dataset_path}")

    spec = ADAPTER_SPECS.get(adapter_name)
    if spec is None:
        raise click_usage_error(f"Unsupported adapter: {adapter_name}")
    suffix = dataset_path.suffix.lower()
    if suffix not in spec["extensions"]:
        supported = ", ".join(sorted(spec["extensions"]))
        raise click_usage_error(f"Unsupported dataset extension for {adapter_name}: {suffix} (supported: {supported})")

    adapter_class = spec["adapter_class"]
    adapter = adapter_class()
    records = adapter.load_articles(str(dataset_path))

    imported_at = utc_now_iso()
    article_rows = [record.to_record(imported_at=imported_at) for record in records]

    articles_details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=article_rows,
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )

    artifact_rows = _build_artifact_rows(records)
    artifacts_details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_artifacts"),
        rows=artifact_rows,
        dedupe_keys=["article_id", "artifact_type", "artifact_path"],
        column_order=ARTICLE_ARTIFACT_COLUMNS,
    )

    total_loaded = getattr(adapter, "last_total_rows", len(records))
    skipped_rows = getattr(adapter, "last_skipped_rows", max(0, total_loaded - len(records)))

    return {
        "stage": "article_import_history",
        "status": "ok",
        "adapter": adapter_name,
        "dataset": str(dataset_path),
        "loaded_rows": total_loaded,
        "imported_rows": len(records),
        "skipped_rows": skipped_rows,
        "deduped_rows": int(articles_details["deduped_count"]),
        "article_artifacts_written": int(artifacts_details["incoming_count"]),
        "articles_path": str(articles_details["path"]),
        "article_artifacts_path": str(artifacts_details["path"]),
        "project_root": str(config.project_root),
    }



def _build_artifact_rows(records: list[NewsArticleRecord]) -> list[dict]:
    # Phase 2 local-tabular import writes only metadata authority rows.
    # No parsed/html artifact files are generated in this stage.
    _ = records
    return []



def click_usage_error(message: str) -> ValueError:
    return ValueError(message)
