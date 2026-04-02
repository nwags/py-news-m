"""Operator-facing CLI for py-news."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import click
import pandas as pd
import uvicorn

from py_news.audit import run_audit_article, run_audit_cache, run_audit_provider, run_audit_summary
from py_news.config import AppConfig, load_config
from py_news.cache_layout import (
    mapped_article_ids_for_storage,
    mapped_storage_id_for_article,
    storage_folder_path_for_storage,
)
from py_news.lookup import query_lookup_articles
from py_news.pipelines.cache_rebuild import run_cache_rebuild_layout
from py_news.pipelines.article_backfill import run_article_backfill
from py_news.pipelines.content_fetch import run_content_fetch
from py_news.pipelines.article_import import run_article_import_history
from py_news.pipelines.lookup_refresh import run_lookup_refresh
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.providers import load_provider_registry, refresh_provider_registry
from py_news.providers import load_provider_rule
from py_news.resolution import query_resolution_events, resolve_article
from py_news.runtime_output import render_summary_block
from py_news.storage.paths import normalized_artifact_path, publisher_article_meta_path


@click.group()
@click.option("--project-root", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=None)
@click.option("--cache-root", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=None)
@click.pass_context
def cli(ctx: click.Context, project_root: Path | None, cache_root: Path | None) -> None:
    """Local-first news ingestion and retrieval CLI."""
    config = load_config(project_root=project_root, cache_root=cache_root)
    ctx.obj = {"config": config}


@cli.group("refdata")
def refdata_group() -> None:
    """Reference data commands."""


@refdata_group.command("refresh")
@click.pass_context
def refdata_refresh(ctx: click.Context) -> None:
    summary = run_refdata_refresh(_config(ctx))
    click.echo(render_summary_block("Refdata Refresh", summary))


@cli.group("providers")
def providers_group() -> None:
    """Provider registry commands."""


@providers_group.command("refresh")
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def providers_refresh(ctx: click.Context, summary_json: bool) -> None:
    details = refresh_provider_registry(_config(ctx))
    summary = {
        "stage": "providers_refresh",
        "status": "ok",
        **details,
    }
    if summary_json:
        click.echo(json.dumps(summary, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Providers Refresh", summary))


@providers_group.command("list")
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def providers_list(ctx: click.Context, as_json: bool) -> None:
    df = load_provider_registry(_config(ctx))
    if as_json:
        click.echo(df.to_json(orient="records", indent=2))
        return
    click.echo(df.to_string(index=False))


@cli.group("articles")
def articles_group() -> None:
    """Article ingest commands."""


@articles_group.command("import-history")
@click.option("--dataset", required=True, type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option("--adapter", required=True, default="local_tabular", show_default=True)
@click.pass_context
def articles_import_history(ctx: click.Context, dataset: Path, adapter: str) -> None:
    try:
        summary = run_article_import_history(_config(ctx), dataset=str(dataset), adapter_name=adapter)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    click.echo(render_summary_block("Article Import History", summary))


@articles_group.command("backfill")
@click.option("--provider", required=True, default="gdelt_recent", show_default=True)
@click.option("--date", "backfill_date", required=True)
@click.option("--window-key", required=True)
@click.option("--query", default=None)
@click.option("--max-records", default=250, type=int, show_default=True)
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_backfill(
    ctx: click.Context,
    provider: str,
    backfill_date: str,
    window_key: str,
    query: str | None,
    max_records: int,
    summary_json: bool,
) -> None:
    try:
        window_date = date.fromisoformat(backfill_date)
    except ValueError as exc:
        raise click.UsageError("--date must be in YYYY-MM-DD format") from exc

    try:
        summary = run_article_backfill(
            _config(ctx),
            provider=provider,
            window_date=window_date,
            window_key=window_key,
            query=query,
            max_records=max_records,
        )
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    if summary_json:
        click.echo(json.dumps(summary, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Article Backfill", summary))


@articles_group.command("fetch-content")
@click.option("--provider", default=None)
@click.option("--article-id", default=None)
@click.option("--start", default=None)
@click.option("--end", default=None)
@click.option("--limit", default=100, type=int, show_default=True)
@click.option("--refetch", is_flag=True, default=False, show_default=True)
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_fetch_content(
    ctx: click.Context,
    provider: str | None,
    article_id: str | None,
    start: str | None,
    end: str | None,
    limit: int,
    refetch: bool,
    summary_json: bool,
) -> None:
    summary = run_content_fetch(
        _config(ctx),
        provider=provider,
        article_id=article_id,
        start=start,
        end=end,
        limit=limit,
        refetch=refetch,
    )

    if summary_json:
        click.echo(json.dumps(summary, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Article Fetch Content", summary))


@articles_group.command("resolve")
@click.option("--article-id", required=True)
@click.option("--representation", type=click.Choice(["metadata", "content"]), default="content", show_default=True)
@click.option("--allow-remote/--local-only", default=True, show_default=True)
@click.option("--force-remote", is_flag=True, default=False, show_default=True)
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_resolve(
    ctx: click.Context,
    article_id: str,
    representation: str,
    allow_remote: bool,
    force_remote: bool,
    summary_json: bool,
) -> None:
    result = resolve_article(
        _config(ctx),
        article_id=article_id,
        representation=representation,
        allow_remote=allow_remote,
        force_remote=force_remote,
    )
    payload = {
        "stage": "article_resolve",
        "status": "ok" if result.resolved else "miss",
        "resolution_source": result.source,
        "resolution_strategy": result.strategy,
        "resolution_reason_code": result.reason_code,
        "resolution_status_code": result.status_code,
        "resolution_message": result.message,
        "local_write_performed": result.local_write_performed,
        **result.to_dict(),
    }
    if summary_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Article Resolve", payload))


@articles_group.command("inspect")
@click.option("--article-id", required=True)
@click.option("--events-limit", default=5, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_inspect(ctx: click.Context, article_id: str, events_limit: int, as_json: bool) -> None:
    config = _config(ctx)

    articles = _load_table(config, "articles")
    artifacts = _load_table(config, "article_artifacts")
    row_matches = articles[articles["article_id"] == article_id] if not articles.empty else pd.DataFrame()
    row = row_matches.iloc[0].to_dict() if not row_matches.empty else {}

    provider = _safe_text(row.get("provider"))
    provider_rule = load_provider_rule(config, provider) if provider else None

    article_artifacts = artifacts[artifacts["article_id"] == article_id] if not artifacts.empty else pd.DataFrame()
    storage_article_id = None
    if not article_artifacts.empty and "storage_article_id" in article_artifacts.columns:
        first_sid = article_artifacts.iloc[0].get("storage_article_id")
        storage_article_id = _safe_text(first_sid)
    if not storage_article_id:
        storage_article_id = mapped_storage_id_for_article(config, article_id)
    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    artifact_items = []
    for record in article_artifacts.to_dict(orient="records"):
        artifact_type = _safe_text(record.get("artifact_type"))
        artifact_path = _safe_text(record.get("artifact_path"))
        if not artifact_path:
            continue
        resolved_path = str(Path(artifact_path).resolve())
        if storage_article_id and not resolved_path.startswith(canonical_prefix):
            continue
        file_exists = bool(artifact_path) and Path(artifact_path).exists()
        artifact_items.append(
            {
                "artifact_type": artifact_type,
                "artifact_path": resolved_path,
                "exists_locally": bool(record.get("exists_locally")),
                "file_exists": file_exists,
            }
        )

    sidecar_path = None
    if storage_article_id:
        folder = storage_folder_path_for_storage(config, storage_article_id)
        if folder:
            canonical_meta = Path(folder) / "meta.json"
            if canonical_meta.exists():
                sidecar_path = str(canonical_meta.resolve())
    if sidecar_path is None and row and storage_article_id:
        sidecar_guess = publisher_article_meta_path(
            config,
            publisher_slug=_publisher_slug_from_row(row),
            published_at=row.get("published_at"),
            article_id=storage_article_id,
        )
        if sidecar_guess.exists():
            sidecar_path = str(sidecar_guess.resolve())

    events = query_resolution_events(config, article_id=article_id, limit=max(0, events_limit))
    if sidecar_path is None and not events.empty:
        latest_sidecar = next((str(v) for v in events["meta_sidecar_path"].tolist() if _safe_text(v)), None)
        if latest_sidecar:
            sidecar_path = latest_sidecar

    payload = {
        "stage": "article_inspect",
        "article_id": article_id,
        "metadata_present": bool(row),
        "provider": provider or None,
        "source_name": _safe_text(row.get("source_name")) if row else None,
        "source_domain": _safe_text(row.get("source_domain")) if row else None,
        "url": _safe_text(row.get("url")) if row else None,
        "canonical_url": _safe_text(row.get("canonical_url")) if row else None,
        "title": _safe_text(row.get("title")) if row else None,
        "published_at": _safe_text(row.get("published_at")) if row else None,
        "storage_article_id": storage_article_id,
        "storage_mapped_article_ids": mapped_article_ids_for_storage(config, storage_article_id) if storage_article_id else [],
        "local_artifacts": artifact_items,
        "meta_sidecar_path": sidecar_path,
        "provider_rule": (
            {
                "provider_id": provider_rule.provider_id,
                "provider_name": provider_rule.provider_name,
                "preferred_resolution_order": provider_rule.preferred_resolution_order,
                "direct_url_allowed": provider_rule.direct_url_allowed,
                "content_mode": provider_rule.content_mode,
            }
            if provider_rule
            else None
        ),
        "resolution_events": events.to_dict(orient="records"),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Article Inspect", payload))


@cli.group("resolution")
def resolution_group() -> None:
    """Resolution/provenance inspection commands."""


@resolution_group.command("events")
@click.option("--article-id", default=None)
@click.option("--provider", default=None)
@click.option("--representation", type=click.Choice(["metadata", "content"]), default=None)
@click.option("--reason-code", default=None)
@click.option("--success", type=click.Choice(["true", "false"]), default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def resolution_events(
    ctx: click.Context,
    article_id: str | None,
    provider: str | None,
    representation: str | None,
    reason_code: str | None,
    success: str | None,
    limit: int,
    as_json: bool,
) -> None:
    success_filter = None if success is None else success == "true"
    config = _config(ctx)
    events = query_resolution_events(
        config,
        article_id=article_id,
        provider=provider,
        representation=representation,
        reason_code=reason_code,
        success=success_filter,
        limit=max(0, limit),
    )
    payload = {
        "stage": "resolution_events",
        "count": len(events),
        "filters": {
            "article_id": article_id,
            "provider": provider,
            "representation": representation,
            "reason_code": reason_code,
            "success": success_filter,
            "limit": max(0, limit),
        },
        "items": events.to_dict(orient="records"),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(render_summary_block("Resolution Events", {"count": payload["count"], **payload["filters"]}))
    if events.empty:
        click.echo("No matching events found.")
    else:
        click.echo(events.to_string(index=False))


@cli.group("audit")
def audit_group() -> None:
    """Read-only audit/reconciliation commands."""


@audit_group.command("summary")
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_summary(ctx: click.Context, summary_json: bool) -> None:
    payload = run_audit_summary(_config(ctx))
    if summary_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Audit Summary", payload))


@audit_group.command("cache")
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_cache(ctx: click.Context, summary_json: bool) -> None:
    payload = run_audit_cache(_config(ctx))
    if summary_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Audit Cache", payload))


@audit_group.command("article")
@click.option("--article-id", required=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_article(ctx: click.Context, article_id: str, as_json: bool) -> None:
    payload = run_audit_article(_config(ctx), article_id=article_id)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Audit Article", payload))


@audit_group.command("provider")
@click.option("--provider", "provider_id", required=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_provider(ctx: click.Context, provider_id: str, as_json: bool) -> None:
    payload = run_audit_provider(_config(ctx), provider_id=provider_id)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Audit Provider", payload))


@cli.group("cache")
def cache_group() -> None:
    """Cache layout and migration commands."""


@cache_group.command("rebuild-layout")
@click.option("--cleanup-legacy", is_flag=True, default=False, show_default=True)
@click.option("--repair-metadata", is_flag=True, default=False, show_default=True)
@click.option("--summary-json", is_flag=True, default=False, show_default=True)
@click.pass_context
def cache_rebuild_layout(ctx: click.Context, cleanup_legacy: bool, repair_metadata: bool, summary_json: bool) -> None:
    summary = run_cache_rebuild_layout(
        _config(ctx),
        cleanup_legacy=cleanup_legacy,
        repair_metadata=repair_metadata,
    )
    if summary_json:
        click.echo(json.dumps(summary, indent=2, sort_keys=True))
    else:
        click.echo(render_summary_block("Cache Rebuild Layout", summary))


@cli.group("lookup")
def lookup_group() -> None:
    """Local lookup commands (article-only in Phase 2/3)."""


@lookup_group.command("refresh")
@click.pass_context
def lookup_refresh(ctx: click.Context) -> None:
    summary = run_lookup_refresh(_config(ctx))
    click.echo(render_summary_block("Lookup Refresh", summary))


@lookup_group.command("query")
@click.option("--scope", required=True)
@click.option("--provider", default=None)
@click.option("--source", default=None)
@click.option("--domain", default=None)
@click.option("--article-id", default=None)
@click.option("--start", default=None)
@click.option("--end", default=None)
@click.option("--title-contains", default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def lookup_query(
    ctx: click.Context,
    scope: str,
    provider: str | None,
    source: str | None,
    domain: str | None,
    article_id: str | None,
    start: str | None,
    end: str | None,
    title_contains: str | None,
    limit: int,
    as_json: bool,
) -> None:
    if scope != "articles":
        raise click.UsageError("Only --scope articles is supported in this phase")

    config = _config(ctx)
    rows = query_lookup_articles(
        config,
        provider=provider,
        source=source,
        domain=domain,
        article_id=article_id,
        start=start,
        end=end,
        title_contains=title_contains,
        limit=limit,
    )

    if as_json:
        click.echo(rows.to_json(orient="records", indent=2))
        return

    summary = {
        "status": "ok",
        "rows": len(rows),
        "scope": scope,
    }
    click.echo(render_summary_block("Lookup Query", summary))
    if rows.empty:
        click.echo("No matching articles found.")
    else:
        click.echo(rows.to_string(index=False))


@cli.group("api")
def api_group() -> None:
    """API commands."""


@api_group.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8000, type=int, show_default=True)
@click.option("--reload", is_flag=True, default=False, show_default=True)
def api_serve(host: str, port: int, reload: bool) -> None:
    """Serve API with local-first behavior."""
    uvicorn.run("py_news.api.app:create_app", host=host, port=port, reload=reload, factory=True)


def _config(ctx: click.Context) -> AppConfig:
    return ctx.obj["config"]


def _load_table(config: AppConfig, artifact_name: str) -> pd.DataFrame:
    path = normalized_artifact_path(config, artifact_name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _safe_text(value: object) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return None
    return text


def _publisher_slug_from_row(row: dict) -> str:
    source_domain = _safe_text(row.get("source_domain"))
    if source_domain:
        return source_domain
    source_name = _safe_text(row.get("source_name"))
    if source_name:
        return source_name
    return _safe_text(row.get("provider")) or "unknown"
