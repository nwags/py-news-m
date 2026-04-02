"""Provider-aware content-fetch pipeline for existing article rows."""

from __future__ import annotations

from py_news.config import AppConfig, ensure_runtime_dirs
from py_news.content import load_articles, select_articles_for_content_fetch
from py_news.http import HttpClient
from py_news.resolution import resolve_article


def run_content_fetch(
    config: AppConfig,
    *,
    provider: str | None,
    article_id: str | None,
    start: str | None,
    end: str | None,
    limit: int,
    refetch: bool,
) -> dict:
    ensure_runtime_dirs(config)

    articles_df = load_articles(config)
    selected = select_articles_for_content_fetch(
        articles_df,
        provider=provider,
        article_id=article_id,
        start=start,
        end=end,
        limit=limit,
    )

    client = HttpClient(config)
    attempts = []
    reason_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    strategy_counts: dict[str, int] = {}
    local_write_rows = 0

    for row in selected.to_dict(orient="records"):
        if not refetch:
            local = resolve_article(
                config,
                article_id=str(row.get("article_id") or ""),
                representation="content",
                allow_remote=False,
                http_client=client,
            )
            if local.resolved:
                attempts.append(local.to_dict())
                reason_counts["already_fetched"] = reason_counts.get("already_fetched", 0) + 1
                source_counts[local.source] = source_counts.get(local.source, 0) + 1
                strategy_key = local.strategy or "none"
                strategy_counts[strategy_key] = strategy_counts.get(strategy_key, 0) + 1
                continue

        result = resolve_article(
            config,
            article_id=str(row.get("article_id") or ""),
            representation="content",
            allow_remote=True,
            force_remote=refetch,
            http_client=client,
        )
        attempts.append(result.to_dict())
        reason_counts[result.reason_code] = reason_counts.get(result.reason_code, 0) + 1
        source_counts[result.source] = source_counts.get(result.source, 0) + 1
        strategy_key = result.strategy or "none"
        strategy_counts[strategy_key] = strategy_counts.get(strategy_key, 0) + 1
        if result.local_write_performed:
            local_write_rows += 1

    return {
        "stage": "content_fetch",
        "status": "ok",
        "selected_rows": len(selected),
        "attempted_rows": len(attempts),
        "success_rows": reason_counts.get("success", 0),
        "reason_counts": reason_counts,
        "resolution_source_counts": source_counts,
        "resolution_strategy_counts": strategy_counts,
        "local_write_rows": local_write_rows,
        "attempts": attempts,
        "project_root": str(config.project_root),
    }
