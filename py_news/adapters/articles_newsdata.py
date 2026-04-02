"""NewsData.io recent-window metadata adapter."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
from typing import Any

from py_news.adapters.article_bulk_utils import normalize_text, row_has_meaningful_metadata
from py_news.adapters.base import ArticleRecentWindowAdapter, RecentWindowResult
from py_news.config import AppConfig
from py_news.http import HttpClient
from py_news.models import NewsArticleRecord, derive_article_identity
from py_news.newsdata import build_newsdata_params, normalize_newsdata_item, resolve_newsdata_auth
from py_news.providers import load_provider_rule
from py_news.rate_limit import SharedRateLimiter


class NewsDataRecentArticleAdapter(ArticleRecentWindowAdapter):
    provider = "newsdata"

    def __init__(
        self,
        config: AppConfig,
        *,
        http_client: HttpClient | None = None,
        rate_limiter: SharedRateLimiter | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        self.config = config
        self.rate_limiter = rate_limiter or SharedRateLimiter(config.max_requests_per_second)
        self.http_client = http_client or HttpClient(config, rate_limiter=self.rate_limiter)
        self.endpoint_url = endpoint_url or "https://newsdata.io/api/1/news"

    def fetch_window(
        self,
        *,
        window_date: date,
        window_key: str,
        query: str | None = None,
        max_records: int | None = None,
    ) -> RecentWindowResult:
        query_text = normalize_text(query) or "news"
        rule = load_provider_rule(self.config, self.provider)
        auth = resolve_newsdata_auth(rule)
        params, size = build_newsdata_params(query=query_text, max_records=max_records, auth=auth)

        start_ts, end_ts = _window_bounds(window_date=window_date, window_key=window_key)
        request_id = _deterministic_request_id(
            provider=self.provider,
            endpoint_url=self.endpoint_url,
            query=query_text,
            window_start=start_ts,
            window_end=end_ts,
            max_records=size.effective_max_records,
        )

        payload = self.http_client.request_json(
            method="GET",
            url=self.endpoint_url,
            params=params,
        )

        raw_items = _extract_items(payload)
        articles: list[NewsArticleRecord] = []
        skipped = 0

        for item in raw_items:
            mapped = normalize_newsdata_item(item)
            if not row_has_meaningful_metadata(mapped):
                skipped += 1
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

            articles.append(
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
                    article_text=None,
                    summary_text=mapped["summary_text"],
                    snippet=mapped["snippet"],
                    metadata_json={"provider_native": item},
                )
            )

        return RecentWindowResult(
            provider=self.provider,
            window_date=window_date,
            window_key=window_key,
            request_id=request_id,
            raw_payload_path="",
            articles=articles,
            fetched_rows=len(raw_items),
            normalized_rows=len(articles),
            skipped_rows=skipped,
            requested_max_records=size.requested_max_records,
            effective_max_records=size.effective_max_records,
            max_records_clamped=size.max_records_clamped,
        )


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    value = payload.get("results")
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _window_bounds(window_date: date, window_key: str) -> tuple[datetime, datetime]:
    duration = _parse_window_key(window_key)
    end = datetime.combine(window_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    start = end - duration
    return start, end


def _parse_window_key(window_key: str) -> timedelta:
    key = window_key.strip().lower()
    if key.endswith("m"):
        return timedelta(minutes=int(key[:-1]))
    if key.endswith("h"):
        return timedelta(hours=int(key[:-1]))
    if key.endswith("d"):
        return timedelta(days=int(key[:-1]))
    raise ValueError(f"Unsupported window_key: {window_key}")


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def _deterministic_request_id(
    *,
    provider: str,
    endpoint_url: str,
    query: str,
    window_start: datetime,
    window_end: datetime,
    max_records: int,
) -> str:
    material = (
        f"provider={provider}|endpoint={endpoint_url}|query={query}|"
        f"start={_format_utc(window_start)}|"
        f"end={_format_utc(window_end)}|max_records={max_records}"
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return f"req_{digest}"
