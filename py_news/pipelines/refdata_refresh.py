"""Phase 1 refdata refresh placeholder."""

from __future__ import annotations

from py_news.config import AppConfig, ensure_runtime_dirs
from py_news.models import (
    ARTICLE_STORAGE_MAP_COLUMNS,
    AUGMENTATION_ARTIFACT_COLUMNS,
    AUGMENTATION_EVENT_COLUMNS,
    AUGMENTATION_RUN_COLUMNS,
    RECONCILIATION_DISCREPANCY_COLUMNS,
    RECONCILIATION_EVENT_COLUMNS,
    RESOLUTION_EVENT_COLUMNS,
    STORAGE_ARTICLES_COLUMNS,
)
from py_news.providers import refresh_provider_registry
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import append_parquet_rows, upsert_parquet


def run_refdata_refresh(config: AppConfig) -> dict:
    ensure_runtime_dirs(config)

    provider_summary = refresh_provider_registry(config)
    created = []
    for artifact_name, dedupe_key in [
        ("source_catalog", ["source_id"]),
        ("provider_registry", ["provider_id"]),
    ]:
        artifact = normalized_artifact_path(config, artifact_name)
        if not artifact.exists():
            upsert_parquet(artifact, rows=[], dedupe_keys=dedupe_key)
            created.append(artifact.name)

    resolution_events = normalized_artifact_path(config, "resolution_events")
    if not resolution_events.exists():
        append_parquet_rows(
            resolution_events,
            rows=[],
            column_order=RESOLUTION_EVENT_COLUMNS,
        )
        created.append(resolution_events.name)

    for artifact_name, cols in [
        ("reconciliation_events", RECONCILIATION_EVENT_COLUMNS),
        ("reconciliation_discrepancies", RECONCILIATION_DISCREPANCY_COLUMNS),
        ("augmentation_runs", AUGMENTATION_RUN_COLUMNS),
        ("augmentation_events", AUGMENTATION_EVENT_COLUMNS),
        ("augmentation_artifacts", AUGMENTATION_ARTIFACT_COLUMNS),
    ]:
        path = normalized_artifact_path(config, artifact_name)
        if not path.exists():
            append_parquet_rows(path, rows=[], column_order=cols)
            created.append(path.name)

    for artifact_name, cols in [
        ("storage_articles", STORAGE_ARTICLES_COLUMNS),
        ("article_storage_map", ARTICLE_STORAGE_MAP_COLUMNS),
    ]:
        path = normalized_artifact_path(config, artifact_name)
        if not path.exists():
            append_parquet_rows(path, rows=[], column_order=cols)
            created.append(path.name)

    return {
        "stage": "refdata_refresh",
        "status": "ok",
        "artifacts_created": len(created),
        "created_artifacts": ",".join(created) if created else "",
        "providers_count": provider_summary["providers_count"],
        "provider_registry_path": provider_summary["provider_registry_path"],
        "project_root": str(config.project_root),
    }
