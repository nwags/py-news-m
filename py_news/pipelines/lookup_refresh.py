"""Phase 2 lookup refresh pipeline for article lookup artifacts."""

from __future__ import annotations

import pandas as pd

from py_news.config import AppConfig, ensure_runtime_dirs
from py_news.models import ARTICLES_COLUMNS, LOOKUP_ARTICLE_COLUMNS
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


def run_lookup_refresh(config: AppConfig) -> dict:
    ensure_runtime_dirs(config)

    articles_path = normalized_artifact_path(config, "articles")
    if articles_path.exists():
        articles_df = pd.read_parquet(articles_path)
    else:
        articles_df = pd.DataFrame(columns=ARTICLES_COLUMNS)

    lookup_rows = _build_lookup_rows(articles_df)
    details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "local_lookup_articles"),
        rows=lookup_rows,
        dedupe_keys=["article_id"],
        column_order=LOOKUP_ARTICLE_COLUMNS,
    )

    return {
        "stage": "lookup_refresh",
        "status": "ok",
        "articles_read": len(articles_df),
        "lookup_rows": len(lookup_rows),
        "deduped_rows": int(details["deduped_count"]),
        "lookup_path": str(details["path"]),
        "project_root": str(config.project_root),
    }



def _build_lookup_rows(articles_df: pd.DataFrame) -> list[dict]:
    if articles_df.empty:
        return []

    for column in LOOKUP_ARTICLE_COLUMNS:
        if column not in articles_df.columns:
            articles_df[column] = None

    lookup_df = articles_df[LOOKUP_ARTICLE_COLUMNS].copy()
    return lookup_df.to_dict(orient="records")
