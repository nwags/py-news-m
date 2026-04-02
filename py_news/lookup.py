"""Local lookup helpers for article-only querying."""

from __future__ import annotations

import pandas as pd

from py_news.config import AppConfig
from py_news.models import LOOKUP_ARTICLE_COLUMNS
from py_news.storage.paths import normalized_artifact_path



def load_lookup_articles(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "local_lookup_articles")
    if not path.exists():
        return pd.DataFrame(columns=LOOKUP_ARTICLE_COLUMNS)

    df = pd.read_parquet(path)
    for column in LOOKUP_ARTICLE_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[LOOKUP_ARTICLE_COLUMNS].copy()



def query_lookup_articles(
    config: AppConfig,
    provider: str | None = None,
    source: str | None = None,
    domain: str | None = None,
    article_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    title_contains: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    df = load_lookup_articles(config)
    if df.empty:
        return df

    if provider:
        df = df[df["provider"] == provider]
    if source:
        df = df[df["source_name"] == source]
    if domain:
        df = df[df["source_domain"] == domain]
    if article_id:
        df = df[df["article_id"] == article_id]

    published = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    if start:
        start_ts = pd.to_datetime(start, errors="coerce", utc=True)
        if not pd.isna(start_ts):
            df = df[published >= start_ts]
            published = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    if end:
        end_ts = pd.to_datetime(end, errors="coerce", utc=True)
        if not pd.isna(end_ts):
            df = df[published <= end_ts]

    if title_contains:
        pattern = str(title_contains).strip().lower()
        if pattern:
            df = df[df["title"].fillna("").astype(str).str.lower().str.contains(pattern, regex=False)]

    return df.head(max(0, limit)).copy()
