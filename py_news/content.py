"""Content-fetch selection and outcome helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from py_news.config import AppConfig
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS
from py_news.storage.paths import normalized_artifact_path

CONTENT_REASON_CODES = {
    "missing_url",
    "already_fetched",
    "http_failure",
    "non_html_response",
    "access_denied",
    "empty_body",
    "parse_failure",
    "success",
}


@dataclass(slots=True)
class ContentFetchAttempt:
    article_id: str
    provider: str
    reason_code: str
    url: str | None = None
    status_code: int | None = None
    message: str | None = None
    html_path: str | None = None
    text_path: str | None = None
    json_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_articles(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "articles")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLES_COLUMNS)
    df = pd.read_parquet(path)
    for column in ARTICLES_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[ARTICLES_COLUMNS].copy()


def load_article_artifacts(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "article_artifacts")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLE_ARTIFACT_COLUMNS)
    df = pd.read_parquet(path)
    for column in ARTICLE_ARTIFACT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[ARTICLE_ARTIFACT_COLUMNS].copy()


def select_articles_for_content_fetch(
    articles_df: pd.DataFrame,
    *,
    provider: str | None,
    article_id: str | None,
    start: str | None,
    end: str | None,
    limit: int,
) -> pd.DataFrame:
    df = articles_df.copy()

    if provider:
        df = df[df["provider"] == provider]
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

    if limit > 0:
        df = df.head(limit)

    return df


def has_existing_text_artifact(artifacts_df: pd.DataFrame, article_id: str) -> bool:
    if artifacts_df.empty:
        return False

    matches = artifacts_df[
        (artifacts_df["article_id"] == article_id)
        & (artifacts_df["artifact_type"] == "article_text")
        & (artifacts_df["exists_locally"] == True)
    ]
    if matches.empty:
        return False

    for artifact_path in matches["artifact_path"].dropna().astype(str).tolist():
        if Path(artifact_path).exists():
            return True

    return False
