"""Abstract adapter interfaces for bulk and recent article ingestion."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date

from py_news.models import NewsArticleRecord


@dataclass(slots=True)
class RecentWindowResult:
    provider: str
    window_date: date
    window_key: str
    request_id: str
    raw_payload_path: str
    articles: list[NewsArticleRecord]
    fetched_rows: int
    normalized_rows: int
    skipped_rows: int
    requested_max_records: int | None = None
    effective_max_records: int | None = None
    max_records_clamped: bool = False


class ArticleBulkAdapter(ABC):
    """Interface for source-neutral historical article loaders."""

    @abstractmethod
    def load_articles(self, dataset_path: str) -> list[NewsArticleRecord]:
        """Load normalized article records from a local dataset path."""


class ArticleRecentWindowAdapter(ABC):
    """Interface for provider-specific recent metadata window loaders."""

    @abstractmethod
    def fetch_window(
        self,
        *,
        window_date: date,
        window_key: str,
        query: str | None = None,
        max_records: int | None = None,
    ) -> RecentWindowResult:
        """Fetch, persist raw payload, and normalize recent-window metadata rows."""
