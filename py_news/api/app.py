"""FastAPI application factory for local-first py-news APIs."""

from pathlib import Path

from fastapi import FastAPI
from fastapi import HTTPException, Query

from py_news.api.models import ArticleContentResponse, ArticleSummaryResponse, ArticlesListResponse
from py_news.api.service import ApiService
from py_news.config import load_config


def create_app(project_root: Path | None = None, cache_root: Path | None = None) -> FastAPI:
    app = FastAPI(title="py-news-m", version="0.1.0")
    service = ApiService(load_config(project_root=project_root, cache_root=cache_root))

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "py-news-m"}

    @app.get("/articles", response_model=ArticlesListResponse)
    def list_articles(
        provider: str | None = None,
        source: str | None = None,
        domain: str | None = None,
        start: str | None = None,
        end: str | None = None,
        title_contains: str | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> ArticlesListResponse:
        return service.list_articles(
            provider=provider,
            source=source,
            domain=domain,
            start=start,
            end=end,
            title_contains=title_contains,
            limit=limit,
            offset=offset,
        )

    @app.get("/articles/{article_id}", response_model=ArticleSummaryResponse)
    def get_article(article_id: str, resolve_remote: bool = Query(default=False)) -> ArticleSummaryResponse:
        article = service.get_article(article_id, resolve_remote_enabled=resolve_remote)
        if article is None:
            raise HTTPException(status_code=404, detail="Article not found in local metadata")
        return article

    @app.get("/articles/{article_id}/content", response_model=ArticleContentResponse)
    def get_article_content(article_id: str, resolve_remote: bool = Query(default=False)) -> ArticleContentResponse:
        if not service.article_exists(article_id):
            raise HTTPException(status_code=404, detail="Article not found in local metadata")
        return service.get_article_content(article_id, resolve_remote_enabled=resolve_remote)

    return app
