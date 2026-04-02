"""Cache layout rebuild/migration pipeline."""

from __future__ import annotations

from py_news.cache_layout import rebuild_cache_layout_with_options
from py_news.config import AppConfig, ensure_runtime_dirs


def run_cache_rebuild_layout(
    config: AppConfig,
    *,
    cleanup_legacy: bool = False,
    repair_metadata: bool = False,
) -> dict:
    ensure_runtime_dirs(config)
    summary = rebuild_cache_layout_with_options(
        config,
        cleanup_legacy=cleanup_legacy,
        repair_metadata=repair_metadata,
    )
    summary["project_root"] = str(config.project_root)
    return summary
