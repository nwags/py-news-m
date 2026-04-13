"""Bulk and incremental source adapters."""

from py_news.adapters.base import ArticleBulkAdapter, ArticleRecentWindowAdapter, RecentWindowResult

__all__ = [
    "ArticleBulkAdapter",
    "ArticleRecentWindowAdapter",
    "RecentWindowResult",
    "LocalTabularArticleAdapter",
    "NytArchiveArticleAdapter",
    "GdeltRecentArticleAdapter",
    "NewsDataRecentArticleAdapter",
]


def __getattr__(name: str):
    if name == "LocalTabularArticleAdapter":
        from py_news.adapters.articles_local_tabular import LocalTabularArticleAdapter

        return LocalTabularArticleAdapter
    if name == "NytArchiveArticleAdapter":
        from py_news.adapters.articles_nyt_archive import NytArchiveArticleAdapter

        return NytArchiveArticleAdapter
    if name == "GdeltRecentArticleAdapter":
        from py_news.adapters.articles_gdelt_recent import GdeltRecentArticleAdapter

        return GdeltRecentArticleAdapter
    if name == "NewsDataRecentArticleAdapter":
        from py_news.adapters.articles_newsdata import NewsDataRecentArticleAdapter

        return NewsDataRecentArticleAdapter
    raise AttributeError(name)
