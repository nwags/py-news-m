"""Bulk and incremental source adapters."""

from py_news.adapters.base import ArticleBulkAdapter, ArticleRecentWindowAdapter, RecentWindowResult
from py_news.adapters.articles_gdelt_recent import GdeltRecentArticleAdapter
from py_news.adapters.articles_local_tabular import LocalTabularArticleAdapter
from py_news.adapters.articles_newsdata import NewsDataRecentArticleAdapter
from py_news.adapters.articles_nyt_archive import NytArchiveArticleAdapter

__all__ = [
    "ArticleBulkAdapter",
    "ArticleRecentWindowAdapter",
    "RecentWindowResult",
    "LocalTabularArticleAdapter",
    "NytArchiveArticleAdapter",
    "GdeltRecentArticleAdapter",
    "NewsDataRecentArticleAdapter",
]
