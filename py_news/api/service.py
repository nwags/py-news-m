"""Small local parquet/file-backed API service helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from py_news.api.models import (
    ArticleContentArtifactResponse,
    ArticleContentResponse,
    ArticleSummaryResponse,
    ArticlesListResponse,
)
from py_news.config import AppConfig
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS
from py_news.cache_layout import mapped_article_ids_for_storage, mapped_storage_id_for_article
from py_news.reason_codes import CONTENT_MISSING_LOCAL, LOCAL_METADATA_HIT, SUCCESS
from py_news.resolution import resolve_article
from py_news.storage.paths import normalized_artifact_path

_CONTENT_ARTIFACT_TYPES = {"article_html", "article_text", "article_json"}


@dataclass(slots=True)
class ApiService:
    config: AppConfig

    def list_articles(
        self,
        *,
        provider: str | None,
        source: str | None,
        domain: str | None,
        start: str | None,
        end: str | None,
        title_contains: str | None,
        limit: int,
        offset: int,
    ) -> ArticlesListResponse:
        df = self._load_articles()
        if provider:
            df = df[df["provider"] == provider]
        if source:
            df = df[df["source_name"] == source]
        if domain:
            df = df[df["source_domain"] == domain]

        if start or end:
            published = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
            start_ts = _parse_filter_ts(start, is_end=False)
            end_ts = _parse_filter_ts(end, is_end=True)
            if start_ts is not None:
                df = df[published >= start_ts]
                published = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
            if end_ts is not None:
                df = df[published <= end_ts]

        if title_contains:
            pattern = str(title_contains).strip().lower()
            if pattern:
                df = df[df["title"].fillna("").astype(str).str.lower().str.contains(pattern, regex=False)]

        ordered = _order_articles(df)
        sliced = ordered.iloc[offset : offset + limit].copy()

        items = [
            ArticleSummaryResponse(
                article_id=str(row.get("article_id") or ""),
                provider=str(row.get("provider") or ""),
                provider_document_id=_optional_str(row.get("provider_document_id")),
                source_name=_optional_str(row.get("source_name")),
                source_domain=_optional_str(row.get("source_domain")),
                url=_optional_str(row.get("url")),
                canonical_url=_optional_str(row.get("canonical_url")),
                title=_optional_str(row.get("title")),
                published_at=_optional_str(row.get("published_at")),
                language=_optional_str(row.get("language")),
                section=_optional_str(row.get("section")),
                byline=_optional_str(row.get("byline")),
                summary_text=_optional_str(row.get("summary_text")),
                snippet=_optional_str(row.get("snippet")),
                imported_at=_optional_str(row.get("imported_at")),
            )
            for row in sliced.to_dict(orient="records")
        ]

        return ArticlesListResponse(items=items, count=len(items), limit=limit, offset=offset)

    def get_article(self, article_id: str, *, resolve_remote_enabled: bool = False) -> ArticleSummaryResponse | None:
        resolution = None
        if resolve_remote_enabled:
            resolution = resolve_article(
                self.config,
                article_id=article_id,
                representation="metadata",
                allow_remote=True,
            )
        df = self._load_articles()
        matches = df[df["article_id"] == article_id]
        if matches.empty:
            return None
        row = matches.iloc[0]
        return ArticleSummaryResponse(
            article_id=str(row.get("article_id") or ""),
            provider=str(row.get("provider") or ""),
            provider_document_id=_optional_str(row.get("provider_document_id")),
            source_name=_optional_str(row.get("source_name")),
            source_domain=_optional_str(row.get("source_domain")),
            url=_optional_str(row.get("url")),
            canonical_url=_optional_str(row.get("canonical_url")),
            title=_optional_str(row.get("title")),
            published_at=_optional_str(row.get("published_at")),
            language=_optional_str(row.get("language")),
            section=_optional_str(row.get("section")),
            byline=_optional_str(row.get("byline")),
            summary_text=_optional_str(row.get("summary_text")),
            snippet=_optional_str(row.get("snippet")),
            imported_at=_optional_str(row.get("imported_at")),
            resolution_source=resolution.source if resolution else "local_metadata",
            resolution_strategy=resolution.strategy if resolution else "local_metadata",
            resolution_reason_code=resolution.reason_code if resolution else LOCAL_METADATA_HIT,
            resolution_status_code=resolution.status_code if resolution else None,
            resolution_message=resolution.message if resolution else None,
            resolution_auth_env_var=resolution.auth_env_var if resolution else None,
            resolution_auth_configured=resolution.auth_configured if resolution else None,
            resolution_remote_attempted=bool(resolve_remote_enabled),
            local_write_performed=resolution.local_write_performed if resolution else False,
        )

    def article_exists(self, article_id: str) -> bool:
        df = self._load_articles()
        return not df[df["article_id"] == article_id].empty

    def get_article_content(self, article_id: str, *, resolve_remote_enabled: bool = False) -> ArticleContentResponse:
        if resolve_remote_enabled:
            resolution = resolve_article(
                self.config,
                article_id=article_id,
                representation="content",
                allow_remote=True,
            )
        else:
            resolution = None

        artifacts_df = self._load_article_artifacts()
        matches = artifacts_df[artifacts_df["article_id"] == article_id]
        storage_id = mapped_storage_id_for_article(self.config, article_id)
        if storage_id and "storage_article_id" in artifacts_df.columns:
            mapped_ids = mapped_article_ids_for_storage(self.config, storage_id)
            storage_matches = artifacts_df[
                (artifacts_df["storage_article_id"] == storage_id)
                & (artifacts_df["article_id"].isin(mapped_ids))
            ]
            if not storage_matches.empty:
                matches = pd.concat([matches, storage_matches], ignore_index=True).drop_duplicates()
        if matches.empty:
            return ArticleContentResponse(
                article_id=article_id,
                content_available=False,
                preferred_text=None,
                resolution_source=resolution.source if resolution else "local_only",
                resolution_strategy=resolution.strategy if resolution else "local_only",
                resolution_reason_code=resolution.reason_code if resolution else CONTENT_MISSING_LOCAL,
                resolution_status_code=resolution.status_code if resolution else None,
                resolution_message=resolution.message if resolution else None,
                resolution_auth_env_var=resolution.auth_env_var if resolution else None,
                resolution_auth_configured=resolution.auth_configured if resolution else None,
                resolution_remote_attempted=bool(resolve_remote_enabled),
                local_write_performed=resolution.local_write_performed if resolution else False,
                artifacts=[],
            )

        artifact_models: list[ArticleContentArtifactResponse] = []
        preferred_text: str | None = None

        for row in matches.to_dict(orient="records"):
            artifact_type = str(row.get("artifact_type") or "")
            artifact_path = str(row.get("artifact_path") or "")
            if artifact_type not in _CONTENT_ARTIFACT_TYPES or not artifact_path:
                continue
            if storage_id:
                canonical_prefix = str((self.config.cache_root / "publisher" / "data").resolve())
                if not str(Path(artifact_path).resolve()).startswith(canonical_prefix):
                    continue

            path_obj = Path(artifact_path)
            file_exists = path_obj.exists()
            exists_locally = bool(row.get("exists_locally")) and file_exists

            artifact_models.append(
                ArticleContentArtifactResponse(
                    artifact_type=artifact_type,
                    artifact_path=artifact_path,
                    exists_locally=exists_locally,
                    file_exists=file_exists,
                )
            )

            if artifact_type == "article_text" and file_exists and preferred_text is None:
                preferred_text = path_obj.read_text(encoding="utf-8")

        content_available = any(artifact.file_exists for artifact in artifact_models)
        return ArticleContentResponse(
            article_id=article_id,
            content_available=content_available,
            preferred_text=preferred_text if content_available else None,
            resolution_source=resolution.source if resolution else "local_artifact",
            resolution_strategy=resolution.strategy if resolution else "local_artifact",
            resolution_reason_code=resolution.reason_code if resolution else SUCCESS,
            resolution_status_code=resolution.status_code if resolution else None,
            resolution_message=resolution.message if resolution else None,
            resolution_auth_env_var=resolution.auth_env_var if resolution else None,
            resolution_auth_configured=resolution.auth_configured if resolution else None,
            resolution_remote_attempted=bool(resolve_remote_enabled),
            local_write_performed=resolution.local_write_performed if resolution else False,
            artifacts=artifact_models,
        )

    def _load_articles(self) -> pd.DataFrame:
        path = normalized_artifact_path(self.config, "articles")
        if not path.exists():
            return pd.DataFrame(columns=ARTICLES_COLUMNS)
        df = pd.read_parquet(path)
        for column in ARTICLES_COLUMNS:
            if column not in df.columns:
                df[column] = None
        return df[ARTICLES_COLUMNS].copy()

    def _load_article_artifacts(self) -> pd.DataFrame:
        path = normalized_artifact_path(self.config, "article_artifacts")
        if not path.exists():
            return pd.DataFrame(columns=ARTICLE_ARTIFACT_COLUMNS)
        df = pd.read_parquet(path)
        for column in ARTICLE_ARTIFACT_COLUMNS:
            if column not in df.columns:
                df[column] = None
        filtered = df[
            (df["artifact_type"].isin(sorted(_CONTENT_ARTIFACT_TYPES)))
            & (df["exists_locally"] == True)
        ]
        return filtered[ARTICLE_ARTIFACT_COLUMNS].copy()


def _parse_filter_ts(value: str | None, *, is_end: bool) -> pd.Timestamp | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    ts = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(ts):
        return None

    if len(text) == 10:
        dt = ts.to_pydatetime().astimezone(timezone.utc)
        if is_end:
            dt = dt + timedelta(days=1) - timedelta(microseconds=1)
        ts = pd.Timestamp(dt)
    return ts


def _order_articles(df: pd.DataFrame) -> pd.DataFrame:
    ordered = df.copy()
    ordered["_published_ts"] = pd.to_datetime(ordered["published_at"], errors="coerce", utc=True)
    ordered = ordered.sort_values(
        by=["_published_ts", "article_id"],
        ascending=[False, True],
        na_position="last",
    )
    return ordered.drop(columns=["_published_ts"])


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "nat"}:
        return None
    return text
