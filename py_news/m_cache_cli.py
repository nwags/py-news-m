"""Canonical Wave 1 m-cache CLI surface for the news domain."""

from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
from typing import Any

import click
import pandas as pd
import uvicorn

from py_news.augmentation import (
    build_producer_target_descriptor,
    inspect_article_target,
    list_augmentation_types,
    list_producer_kinds,
    list_status_values,
    load_augmentation_artifacts,
    load_augmentation_events,
    load_augmentation_runs,
    read_wave_schema,
    submit_artifact_envelope,
    submit_run_envelope,
    validate_outer_metadata_shape,
)
from py_news.audit import (
    audit_status_brief,
    compare_audit_reports,
    render_audit_compare_human,
    render_audit_human,
    render_audit_report_ndjson,
    run_audit_article,
    run_audit_cache,
    run_audit_provider,
    run_audit_report,
    run_audit_summary,
)
from py_news.cache_layout import mapped_article_ids_for_storage, mapped_storage_id_for_article, storage_folder_path_for_storage
from py_news.cli import _bundle_summary_text, _canonical_provider_ids, _dedupe_sorted, _prepare_bundle_dir, _safe_name_map, _safe_text
from py_news.config import AppConfig
from py_news.lookup import query_lookup_articles
from py_news.m_cache_shared_shim import get_shared_symbol
from py_news.m_cache_config import app_config_from_effective, load_effective_config
from py_news.m_cache_runtime import ProgressEmitter, RuntimeContext, render_runtime_summary, utc_now_iso
from py_news.pipelines.article_backfill import run_article_backfill
from py_news.pipelines.article_import import run_article_import_history
from py_news.pipelines.cache_rebuild import run_cache_rebuild_layout
from py_news.pipelines.content_fetch import run_content_fetch
from py_news.pipelines.lookup_refresh import run_lookup_refresh
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.providers import load_provider_registry, load_provider_rule, refresh_provider_registry
from py_news.reason_codes import MODE_UNSUPPORTED
from py_news.resolution import query_resolution_events, resolve_article
from py_news.runtime_output import render_summary_block
from py_news.storage.paths import normalized_artifact_path, publisher_article_meta_path

pack_events_view = get_shared_symbol("pack_events_view")
pack_run_status_view = get_shared_symbol("pack_run_status_view")
parse_json_input_payload = get_shared_symbol("parse_json_input_payload")


@click.group("m-cache")
@click.option("--config", "config_path", type=click.Path(path_type=Path, exists=True, dir_okay=False), default=None)
@click.option("--project-root", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=None)
@click.option("--cache-root", type=click.Path(path_type=Path, file_okay=False, dir_okay=True), default=None)
@click.option("--summary-json/--no-summary-json", "summary_json", default=None)
@click.option("--progress-json/--no-progress-json", "progress_json", default=None)
@click.option("--progress-heartbeat-seconds", type=float, default=None)
@click.option("--verbose", is_flag=True, default=False, show_default=True)
@click.option("--quiet", is_flag=True, default=False, show_default=True)
@click.option("--log-level", default=None)
@click.option("--log-file", type=click.Path(path_type=Path, dir_okay=False), default=None)
@click.pass_context
def m_cache_cli(
    ctx: click.Context,
    config_path: Path | None,
    project_root: Path | None,
    cache_root: Path | None,
    summary_json: bool | None,
    progress_json: bool | None,
    progress_heartbeat_seconds: float | None,
    verbose: bool,
    quiet: bool,
    log_level: str | None,
    log_file: Path | None,
) -> None:
    """Canonical shared CLI surface."""
    effective = load_effective_config(
        domain="news",
        project_root_hint=project_root,
        explicit_config_path=config_path,
        explicit_cache_root=cache_root,
    )
    global_cfg = effective.global_config
    app_config = app_config_from_effective(effective_config=effective, domain="news", project_root_hint=project_root)
    ctx.obj = {
        "config": app_config,
        "effective_config": effective,
        "summary_json": bool(global_cfg.default_summary_json if summary_json is None else summary_json),
        "progress_json": bool(global_cfg.default_progress_json if progress_json is None else progress_json),
        "progress_heartbeat_seconds": float(
            global_cfg.default_progress_heartbeat_seconds
            if progress_heartbeat_seconds is None
            else progress_heartbeat_seconds
        ),
        "verbose": bool(verbose),
        "quiet": bool(quiet),
        "log_level": log_level or global_cfg.log_level,
        "log_file": str(log_file) if log_file is not None else None,
    }


@m_cache_cli.group("news")
def news_group() -> None:
    """News domain commands."""


@news_group.group("refdata")
def refdata_group() -> None:
    """Reference data commands."""


@refdata_group.command("refresh")
@click.pass_context
def refdata_refresh(ctx: click.Context) -> None:
    runtime = _runtime_context(ctx, ["m-cache", "news", "refdata", "refresh"])
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={})
    started_at = utc_now_iso()
    summary = run_refdata_refresh(_app_config(ctx))
    progress.emit(event="completed", phase="finalize", counters={"persisted_count": int(summary.get("artifacts_created", 0))})
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status=str(summary.get("status", "ok")),
            remote_attempted=False,
            provider_used=None,
            rate_limited=False,
            retry_count=0,
            persisted_locally=True,
            counters={"persisted_count": int(summary.get("artifacts_created", 0))},
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_summary_block("Refdata Refresh", summary))


@news_group.group("providers")
def providers_group() -> None:
    """Provider registry commands."""


@providers_group.command("refresh")
@click.pass_context
def providers_refresh(ctx: click.Context) -> None:
    runtime = _runtime_context(ctx, ["m-cache", "news", "providers", "refresh"])
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={})
    started_at = utc_now_iso()
    details = refresh_provider_registry(_app_config(ctx))
    summary = {"stage": "providers_refresh", "status": "ok", **details}
    progress.emit(event="completed", phase="finalize", counters={"persisted_count": int(summary.get("providers_count", 0))})
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status="ok",
            remote_attempted=False,
            provider_used=None,
            rate_limited=False,
            retry_count=0,
            persisted_locally=True,
            counters={"persisted_count": int(summary.get("providers_count", 0))},
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_summary_block("Providers Refresh", summary))


@providers_group.command("list")
@click.option("--content-domain", default=None)
@click.option("--active-only", is_flag=True, default=False, show_default=True)
@click.option("--provider-type", default=None)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def providers_list(
    ctx: click.Context,
    content_domain: str | None,
    active_only: bool,
    provider_type: str | None,
    as_json: bool,
) -> None:
    df = load_provider_registry(_app_config(ctx))
    if content_domain:
        df = df[df["content_domain"] == content_domain]
    if active_only:
        df = df[df["is_active"] == True]
    if provider_type:
        df = df[df["provider_type"] == provider_type]
    cols = [
        "provider_id",
        "domain",
        "content_domain",
        "display_name",
        "provider_type",
        "is_active",
        "fallback_priority",
        "rate_limit_policy",
        "supports_direct_resolution",
        "supports_incremental_refresh",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = None
    out = df[cols].copy().sort_values(by=["fallback_priority", "provider_id"], na_position="last")
    out["direct_resolution_allowed"] = df.get("direct_url_allowed")
    out = out[
        [
            "provider_id",
            "domain",
            "content_domain",
            "display_name",
            "provider_type",
            "is_active",
            "fallback_priority",
            "rate_limit_policy",
            "direct_resolution_allowed",
            "supports_direct_resolution",
            "supports_incremental_refresh",
        ]
    ]
    if as_json:
        click.echo(out.to_json(orient="records", indent=2))
        return
    click.echo(out.to_string(index=False))


@providers_group.command("show")
@click.option("--provider", "provider_id", required=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def providers_show(ctx: click.Context, provider_id: str, as_json: bool) -> None:
    config = _app_config(ctx)
    df = load_provider_registry(config)
    matched = df[df["provider_id"] == provider_id]
    if matched.empty:
        raise click.UsageError(f"Unknown provider: {provider_id}")
    row = matched.iloc[0].to_dict()
    payload = _provider_detail_payload(ctx, row)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block(f"Provider Show: {provider_id}", payload))


@providers_group.command("explain-resolution")
@click.option("--provider", "provider_id", required=True)
@click.option("--representation", type=click.Choice(["metadata", "content"]), default="content", show_default=True)
@click.option(
    "--resolution-mode",
    type=click.Choice(["local_only", "resolve_if_missing", "refresh_if_stale"]),
    default="resolve_if_missing",
    show_default=True,
)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def providers_explain_resolution(
    ctx: click.Context,
    provider_id: str,
    representation: str,
    resolution_mode: str,
    as_json: bool,
) -> None:
    df = load_provider_registry(_app_config(ctx))
    matched = df[df["provider_id"] == provider_id]
    if matched.empty:
        raise click.UsageError(f"Unknown provider: {provider_id}")
    row = matched.iloc[0].to_dict()
    preferred = [item.strip() for item in str(row.get("preferred_resolution_order") or "").split(",") if item.strip()]
    if not preferred:
        preferred = ["provider_payload_content", "direct_url_fetch"]

    skip_reasons: list[dict[str, str]] = []
    eligible = True
    if not bool(row.get("is_active")):
        eligible = False
        skip_reasons.append({"provider_id": provider_id, "reason_code": "inactive"})
    if resolution_mode == "local_only":
        eligible = False
        skip_reasons.append({"provider_id": provider_id, "reason_code": "mode_unsupported"})
    if resolution_mode == "refresh_if_stale":
        eligible = False
        skip_reasons.append({"provider_id": provider_id, "reason_code": "mode_unsupported"})
    if representation == "content" and not bool(row.get("supports_direct_resolution")):
        skip_reasons.append({"provider_id": provider_id, "reason_code": "policy_denied"})

    payload = {
        "provider_id": provider_id,
        "representation": representation,
        "resolution_mode": resolution_mode,
        "eligible": eligible,
        "preferred_resolution_order": preferred,
        "graceful_degradation_policy": row.get("graceful_degradation_policy"),
        "provider_skip_reasons": skip_reasons,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block(f"Provider Explain Resolution: {provider_id}", payload))


@news_group.group("articles")
def articles_group() -> None:
    """Article ingest commands."""


@articles_group.command("import-history")
@click.option("--dataset", required=True, type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option("--adapter", required=True, default="local_tabular", show_default=True)
@click.pass_context
def articles_import_history(ctx: click.Context, dataset: Path, adapter: str) -> None:
    runtime = _runtime_context(ctx, ["m-cache", "news", "articles", "import-history"], provider_requested=adapter)
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={})
    started_at = utc_now_iso()
    summary = run_article_import_history(_app_config(ctx), dataset=str(dataset), adapter_name=adapter)
    counters = {
        "candidate_count": int(summary.get("loaded_rows", 0)),
        "succeeded_count": int(summary.get("imported_rows", 0)),
        "skipped_count": int(summary.get("skipped_rows", 0)),
        "persisted_count": int(summary.get("imported_rows", 0)),
    }
    progress.emit(event="completed", phase="finalize", counters=counters, provider=adapter)
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status=str(summary.get("status", "ok")),
            remote_attempted=False,
            provider_used=adapter,
            rate_limited=False,
            retry_count=0,
            persisted_locally=True,
            counters=counters,
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_summary_block("Article Import History", summary))


@articles_group.command("backfill")
@click.option("--provider", required=True, default="gdelt_recent", show_default=True)
@click.option("--date", "backfill_date", required=True)
@click.option("--window-key", required=True)
@click.option("--query", default=None)
@click.option("--max-records", default=250, type=int, show_default=True)
@click.pass_context
def articles_backfill(
    ctx: click.Context,
    provider: str,
    backfill_date: str,
    window_key: str,
    query: str | None,
    max_records: int,
) -> None:
    try:
        window_date = date.fromisoformat(backfill_date)
    except ValueError as exc:
        raise click.UsageError("--date must be in YYYY-MM-DD format") from exc
    runtime = _runtime_context(
        ctx,
        ["m-cache", "news", "articles", "backfill"],
        resolution_mode="resolve_if_missing",
        provider_requested=provider,
    )
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={}, provider=provider)
    started_at = utc_now_iso()
    summary = run_article_backfill(
        _app_config(ctx),
        provider=provider,
        window_date=window_date,
        window_key=window_key,
        query=query,
        max_records=max_records,
    )
    counters = {
        "candidate_count": int(summary.get("fetched_rows", 0)),
        "attempted_count": int(summary.get("fetched_rows", 0)),
        "succeeded_count": int(summary.get("normalized_rows", 0)),
        "skipped_count": int(summary.get("skipped_rows", 0)),
        "persisted_count": int(summary.get("normalized_rows", 0)),
    }
    progress.emit(event="completed", phase="finalize", counters=counters, provider=provider)
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status=str(summary.get("status", "ok")),
            remote_attempted=True,
            provider_used=provider,
            rate_limited=False,
            retry_count=0,
            persisted_locally=True,
            counters=counters,
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_summary_block("Article Backfill", summary))


@articles_group.command("fetch-content")
@click.option("--provider", default=None)
@click.option("--article-id", default=None)
@click.option("--start", default=None)
@click.option("--end", default=None)
@click.option("--limit", default=100, type=int, show_default=True)
@click.option("--refetch", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_fetch_content(
    ctx: click.Context,
    provider: str | None,
    article_id: str | None,
    start: str | None,
    end: str | None,
    limit: int,
    refetch: bool,
) -> None:
    runtime = _runtime_context(
        ctx,
        ["m-cache", "news", "articles", "fetch-content"],
        resolution_mode="resolve_if_missing",
        provider_requested=provider,
    )
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={})
    started_at = utc_now_iso()
    summary = run_content_fetch(
        _app_config(ctx),
        provider=provider,
        article_id=article_id,
        start=start,
        end=end,
        limit=limit,
        refetch=refetch,
    )
    counters = {
        "candidate_count": int(summary.get("selected_rows", 0)),
        "attempted_count": int(summary.get("attempted_rows", 0)),
        "succeeded_count": int(summary.get("success_rows", 0)),
        "persisted_count": int(summary.get("local_write_rows", 0)),
    }
    progress.emit(event="completed", phase="finalize", counters=counters)
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status=str(summary.get("status", "ok")),
            remote_attempted=True,
            provider_used=provider,
            rate_limited=False,
            retry_count=0,
            persisted_locally=bool(summary.get("local_write_rows", 0)),
            counters=counters,
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_summary_block("Article Fetch Content", summary))


@articles_group.command("inspect")
@click.option("--article-id", required=True)
@click.option("--events-limit", default=5, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def articles_inspect(ctx: click.Context, article_id: str, events_limit: int, as_json: bool) -> None:
    config = _app_config(ctx)
    articles = _load_table(config, "articles")
    artifacts = _load_table(config, "article_artifacts")
    row_matches = articles[articles["article_id"] == article_id] if not articles.empty else pd.DataFrame()
    row = row_matches.iloc[0].to_dict() if not row_matches.empty else {}
    provider = _safe_text(row.get("provider"))
    provider_rule = load_provider_rule(config, provider) if provider else None
    article_artifacts = artifacts[artifacts["article_id"] == article_id] if not artifacts.empty else pd.DataFrame()
    storage_article_id = None
    if not article_artifacts.empty and "storage_article_id" in article_artifacts.columns:
        storage_article_id = _safe_text(article_artifacts.iloc[0].get("storage_article_id"))
    if not storage_article_id:
        storage_article_id = mapped_storage_id_for_article(config, article_id)
    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    artifact_items = []
    for record in article_artifacts.to_dict(orient="records"):
        artifact_path = _safe_text(record.get("artifact_path"))
        if not artifact_path:
            continue
        resolved_path = str(Path(artifact_path).resolve())
        if storage_article_id and not resolved_path.startswith(canonical_prefix):
            continue
        artifact_items.append(
            {
                "artifact_type": _safe_text(record.get("artifact_type")),
                "artifact_path": resolved_path,
                "exists_locally": bool(record.get("exists_locally")),
                "file_exists": Path(artifact_path).exists(),
            }
        )

    sidecar_path = None
    if storage_article_id:
        folder = storage_folder_path_for_storage(config, storage_article_id)
        if folder and (Path(folder) / "meta.json").exists():
            sidecar_path = str((Path(folder) / "meta.json").resolve())
    if sidecar_path is None and row and storage_article_id:
        guess = publisher_article_meta_path(
            config,
            publisher_slug=_publisher_slug_from_row(row),
            published_at=row.get("published_at"),
            article_id=storage_article_id,
        )
        if guess.exists():
            sidecar_path = str(guess.resolve())

    events = query_resolution_events(config, article_id=article_id, limit=max(0, events_limit))
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
        return
    click.echo(render_summary_block("Article Inspect", payload))


@news_group.group("lookup")
def lookup_group() -> None:
    """Local lookup commands."""


@lookup_group.command("refresh")
@click.pass_context
def lookup_refresh(ctx: click.Context) -> None:
    runtime = _runtime_context(ctx, ["m-cache", "news", "lookup", "refresh"])
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={})
    started_at = utc_now_iso()
    summary = run_lookup_refresh(_app_config(ctx))
    counters = {
        "candidate_count": int(summary.get("articles_read", 0)),
        "lookup_refreshed_count": int(summary.get("lookup_rows", 0)),
        "persisted_count": int(summary.get("lookup_rows", 0)),
    }
    progress.emit(event="completed", phase="finalize", counters=counters)
    if runtime.summary_json:
        payload = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status="ok",
            remote_attempted=False,
            provider_used=None,
            rate_limited=False,
            retry_count=0,
            persisted_locally=True,
            counters=counters,
            effective_config=_effective_config(ctx),
        )
        click.echo(json.dumps(payload, sort_keys=True))
        return
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
    rows = query_lookup_articles(
        _app_config(ctx),
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
    click.echo(render_summary_block("Lookup Query", {"status": "ok", "rows": len(rows), "scope": scope}))
    if rows.empty:
        click.echo("No matching articles found.")
    else:
        click.echo(rows.to_string(index=False))


@news_group.group("resolve")
def resolve_group() -> None:
    """Resolve commands."""


@resolve_group.command("article")
@click.option("--article-id", required=True)
@click.option("--representation", type=click.Choice(["metadata", "content"]), default="content", show_default=True)
@click.option(
    "--resolution-mode",
    type=click.Choice(["local_only", "resolve_if_missing", "refresh_if_stale"]),
    default=None,
)
@click.option("--allow-remote/--local-only", default=True, show_default=True)
@click.option("--force-remote", is_flag=True, default=False, show_default=True)
@click.pass_context
def resolve_article_cmd(
    ctx: click.Context,
    article_id: str,
    representation: str,
    resolution_mode: str | None,
    allow_remote: bool,
    force_remote: bool,
) -> None:
    mode = resolution_mode
    if mode is None:
        mode = "local_only" if not allow_remote else ("refresh_if_stale" if force_remote else "resolve_if_missing")

    if mode == "refresh_if_stale":
        runtime = _runtime_context(
            ctx,
            ["m-cache", "news", "resolve", "article"],
            resolution_mode=mode,
        )
        started_at = utc_now_iso()
        summary = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status="failed",
            remote_attempted=False,
            provider_used=None,
            rate_limited=False,
            retry_count=0,
            persisted_locally=False,
            counters={"attempted_count": 0, "failed_count": 1},
            warnings=[],
            errors=[MODE_UNSUPPORTED],
            effective_config=_effective_config(ctx),
        )
        summary["reason_code"] = MODE_UNSUPPORTED
        summary["message"] = "refresh_if_stale is not implemented on this path in Wave 2"
        if runtime.summary_json:
            click.echo(json.dumps(summary, sort_keys=True))
            return
        click.echo(render_summary_block("Article Resolve", summary))
        return

    allow_remote_effective = mode != "local_only"
    force_remote_effective = False
    runtime = _runtime_context(
        ctx,
        ["m-cache", "news", "resolve", "article"],
        resolution_mode=mode,
    )
    progress = ProgressEmitter(runtime)
    progress.emit(event="started", phase="startup", counters={}, canonical_key=f"article:{article_id}")
    started_at = utc_now_iso()
    result = resolve_article(
        _app_config(ctx),
        article_id=article_id,
        representation=representation,
        allow_remote=allow_remote_effective,
        force_remote=force_remote_effective,
    )
    counters = {
        "attempted_count": 1,
        "succeeded_count": 1 if result.resolved else 0,
        "failed_count": 0 if result.resolved else 1,
        "persisted_count": 1 if result.local_write_performed else 0,
    }
    progress.emit(
        event="completed" if result.resolved else "failed",
        phase="finalize",
        counters=counters,
        provider=result.provider or None,
        canonical_key=f"article:{article_id}",
        rate_limit_state="rate_limited" if result.rate_limited else "ok",
        detail=result.reason_code,
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
    if runtime.summary_json:
        summary = render_runtime_summary(
            context=runtime,
            started_at=started_at,
            status=str(payload["status"]),
            remote_attempted=bool(result.remote_attempted),
            provider_used=result.provider_used or result.provider or None,
            rate_limited=bool(result.rate_limited),
            retry_count=int(result.retry_count),
            persisted_locally=bool(result.local_write_performed),
            counters=counters,
            warnings=[],
            errors=[] if result.resolved else [result.reason_code],
            effective_config=_effective_config(ctx),
        )
        summary["selection_outcome"] = _selection_outcome_from_result(result)
        summary["deferred"] = bool(result.deferred_until)
        summary["deferred_until"] = result.deferred_until
        summary["provider_skip_reasons"] = result.provider_skip_reasons or []
        click.echo(json.dumps(summary, sort_keys=True))
        return
    click.echo(render_summary_block("Article Resolve", payload))


@news_group.group("resolution")
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
def resolution_events_cmd(
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
    events = query_resolution_events(
        _app_config(ctx),
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


@news_group.group("aug")
def aug_group() -> None:
    """Wave 4.1 augmentation command family with compatibility aliases."""


@aug_group.command("list-types")
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
def aug_list_types(as_json: bool) -> None:
    payload = {
        "stage": "augmentation_list_types",
        "domain": "news",
        "resource_family": "articles",
        "augmentation_types": list_augmentation_types(),
        "producer_kinds": list_producer_kinds(),
        "status_values": list_status_values(),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Types", payload))


@aug_group.command("inspect-target")
@click.option("--article-id", required=True)
@click.option("--text-source", type=click.Choice(["auto", "metadata", "content"]), default="auto", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_inspect_target(ctx: click.Context, article_id: str, text_source: str, as_json: bool) -> None:
    inspection = inspect_article_target(_app_config(ctx), article_id=article_id, text_source=text_source)
    payload = {
        "stage": "augmentation_inspect_target",
        "domain": inspection.domain,
        "resource_family": inspection.resource_family,
        "article_id": inspection.article_id,
        "canonical_key": inspection.canonical_key,
        "text_bearing": inspection.text_bearing,
        "augmentation_applicable": inspection.augmentation_applicable,
        "text_source": inspection.text_source,
        "source_text_version": inspection.source_text_version,
        "text_present": inspection.text_present,
        "text_length": inspection.text_length,
        "reason": inspection.reason,
        "inspect_runs_path": inspection.inspect_runs_path,
        "inspect_artifacts_path": inspection.inspect_artifacts_path,
        "language": inspection.language,
        "document_time_reference": inspection.document_time_reference,
        "producer_hints": inspection.producer_hints or {},
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Target Inspection", payload))


@aug_group.command("target-descriptor")
@click.option("--article-id", required=True)
@click.option("--text-source", type=click.Choice(["auto", "metadata", "content"]), default="auto", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_target_descriptor(ctx: click.Context, article_id: str, text_source: str, as_json: bool) -> None:
    """Compatibility alias for producer target-descriptor output."""
    descriptor = build_producer_target_descriptor(_app_config(ctx), article_id=article_id, text_source=text_source)
    if descriptor is None:
        raise click.UsageError("No eligible text/source_text_version found for this article target.")
    payload = {
        "stage": "augmentation_target_descriptor",
        "domain": descriptor.domain,
        "resource_family": descriptor.resource_family,
        "canonical_key": descriptor.canonical_key,
        "text_source": descriptor.text_source,
        "source_text_version": descriptor.source_text_version,
        "language": descriptor.language,
        "document_time_reference": descriptor.document_time_reference,
        "producer_hints": descriptor.producer_hints or {},
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Target Descriptor", payload))


@aug_group.command("inspect-runs")
@click.option("--article-id", default=None)
@click.option("--augmentation-type", default=None)
@click.option("--status", default=None)
@click.option("--producer-name", default=None)
@click.option("--producer-version", default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_inspect_runs(
    ctx: click.Context,
    article_id: str | None,
    augmentation_type: str | None,
    status: str | None,
    producer_name: str | None,
    producer_version: str | None,
    limit: int,
    as_json: bool,
) -> None:
    runs = load_augmentation_runs(_app_config(ctx))
    if article_id:
        runs = runs[runs["canonical_key"] == f"article:{article_id}"]
    if augmentation_type:
        runs = runs[runs["augmentation_type"] == augmentation_type]
    if status:
        runs = runs[runs["status"] == status]
    if producer_name:
        runs = runs[runs["producer_name"] == producer_name]
    if producer_version:
        runs = runs[runs["producer_version"] == producer_version]
    if limit > 0:
        runs = runs.head(limit)

    payload = {
        "stage": "augmentation_inspect_runs",
        "domain": "news",
        "resource_family": "articles",
        "count": int(len(runs)),
        "filters": {
            "article_id": article_id,
            "augmentation_type": augmentation_type,
            "status": status,
            "producer_name": producer_name,
            "producer_version": producer_version,
            "limit": limit,
        },
        "items": runs.to_dict(orient="records"),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Runs", {"count": payload["count"], **payload["filters"]}))
    if runs.empty:
        click.echo("No augmentation runs found.")
    else:
        click.echo(runs.to_string(index=False))


@aug_group.command("inspect-artifacts")
@click.option("--article-id", default=None)
@click.option("--augmentation-type", default=None)
@click.option("--success", type=click.Choice(["true", "false"]), default=None)
@click.option("--producer-name", default=None)
@click.option("--producer-version", default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_inspect_artifacts(
    ctx: click.Context,
    article_id: str | None,
    augmentation_type: str | None,
    success: str | None,
    producer_name: str | None,
    producer_version: str | None,
    limit: int,
    as_json: bool,
) -> None:
    artifacts = load_augmentation_artifacts(_app_config(ctx))
    if article_id:
        artifacts = artifacts[artifacts["canonical_key"] == f"article:{article_id}"]
    if augmentation_type:
        artifacts = artifacts[artifacts["augmentation_type"] == augmentation_type]
    if success is not None:
        artifacts = artifacts[artifacts["success"] == (success == "true")]
    if producer_name:
        artifacts = artifacts[artifacts["producer_name"] == producer_name]
    if producer_version:
        artifacts = artifacts[artifacts["producer_version"] == producer_version]
    if limit > 0:
        artifacts = artifacts.head(limit)

    payload = {
        "stage": "augmentation_inspect_artifacts",
        "domain": "news",
        "resource_family": "articles",
        "count": int(len(artifacts)),
        "filters": {
            "article_id": article_id,
            "augmentation_type": augmentation_type,
            "success": None if success is None else success == "true",
            "producer_name": producer_name,
            "producer_version": producer_version,
            "limit": limit,
        },
        "items": artifacts.to_dict(orient="records"),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Artifacts", {"count": payload["count"], **payload["filters"]}))
    if artifacts.empty:
        click.echo("No augmentation artifacts found.")
    else:
        click.echo(artifacts.to_string(index=False))


def _load_submission_payload(input_json: Path | None, json_payload: str | None) -> dict[str, Any]:
    return parse_json_input_payload(input_json, json_payload)


def _submit_producer_envelope(
    ctx: click.Context,
    *,
    kind: str,
    payload: dict[str, Any],
    inline_payload_max_bytes: int,
) -> dict[str, Any]:
    schema_name = "producer-run-submission.schema.json" if kind == "run" else "producer-artifact-submission.schema.json"
    schema = read_wave_schema(schema_name, project_root=_app_config(ctx).project_root, wave_version="v4")
    validation_errors = validate_outer_metadata_shape(payload, schema=schema)
    if validation_errors:
        raise click.UsageError(f"Submission failed schema validation: {', '.join(validation_errors)}")
    if kind == "run":
        return submit_run_envelope(_app_config(ctx), payload)
    return submit_artifact_envelope(
        _app_config(ctx),
        payload,
        inline_payload_max_bytes=max(0, int(inline_payload_max_bytes)),
    )


@aug_group.command("submit-run")
@click.option("--input-json", type=click.Path(path_type=Path, exists=True, dir_okay=False), default=None)
@click.option("--json-payload", default=None)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_submit_run(
    ctx: click.Context,
    input_json: Path | None,
    json_payload: str | None,
    as_json: bool,
) -> None:
    """Canonical producer run submission surface."""
    payload = _load_submission_payload(input_json, json_payload)
    result = _submit_producer_envelope(ctx, kind="run", payload=payload, inline_payload_max_bytes=65536)
    if as_json:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Submit Run", result))


@aug_group.command("submit-artifact")
@click.option("--input-json", type=click.Path(path_type=Path, exists=True, dir_okay=False), default=None)
@click.option("--json-payload", default=None)
@click.option("--inline-payload-max-bytes", default=65536, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_submit_artifact(
    ctx: click.Context,
    input_json: Path | None,
    json_payload: str | None,
    inline_payload_max_bytes: int,
    as_json: bool,
) -> None:
    """Canonical producer artifact submission surface."""
    payload = _load_submission_payload(input_json, json_payload)
    result = _submit_producer_envelope(
        ctx,
        kind="artifact",
        payload=payload,
        inline_payload_max_bytes=inline_payload_max_bytes,
    )
    if as_json:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Submit Artifact", result))


@aug_group.command("submit")
@click.option("--kind", type=click.Choice(["run", "artifact"]), required=True)
@click.option("--input-json", type=click.Path(path_type=Path, exists=True, dir_okay=False), default=None)
@click.option("--json-payload", default=None)
@click.option("--inline-payload-max-bytes", default=65536, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_submit_alias(
    ctx: click.Context,
    kind: str,
    input_json: Path | None,
    json_payload: str | None,
    inline_payload_max_bytes: int,
    as_json: bool,
) -> None:
    """Compatibility alias for Wave 4.0 submit submodes."""
    payload = _load_submission_payload(input_json, json_payload)
    result = _submit_producer_envelope(
        ctx,
        kind=kind,
        payload=payload,
        inline_payload_max_bytes=inline_payload_max_bytes,
    )
    if as_json:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Submit", {"kind": kind, **result}))


@aug_group.command("status")
@click.option("--run-id", default=None)
@click.option("--idempotency-key", default=None)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_status(ctx: click.Context, run_id: str | None, idempotency_key: str | None, as_json: bool) -> None:
    if not run_id and not idempotency_key:
        raise click.UsageError("Provide --run-id or --idempotency-key.")
    runs = load_augmentation_runs(_app_config(ctx))
    if run_id:
        runs = runs[runs["run_id"] == run_id]
    if idempotency_key:
        runs = runs[runs["idempotency_key"] == idempotency_key]
    packed_items = pack_run_status_view(runs.to_dict(orient="records"))
    payload = {
        "stage": "augmentation_status",
        "count": int(len(runs)),
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "items": packed_items,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Status", payload))


@aug_group.command("events")
@click.option("--article-id", default=None)
@click.option("--augmentation-type", default=None)
@click.option("--status", default=None)
@click.option("--producer-name", default=None)
@click.option("--producer-version", default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def aug_events(
    ctx: click.Context,
    article_id: str | None,
    augmentation_type: str | None,
    status: str | None,
    producer_name: str | None,
    producer_version: str | None,
    limit: int,
    as_json: bool,
) -> None:
    events = load_augmentation_events(_app_config(ctx))
    if article_id:
        events = events[events["canonical_key"] == f"article:{article_id}"]
    if augmentation_type:
        events = events[events["augmentation_type"] == augmentation_type]
    if status:
        events = events[events["status"] == status]
    if producer_name:
        events = events[events["producer_name"] == producer_name]
    if producer_version:
        events = events[events["producer_version"] == producer_version]
    if limit > 0:
        events = events.head(limit)
    packed_items = pack_events_view(events.to_dict(orient="records"))
    payload = {
        "stage": "augmentation_events",
        "domain": "news",
        "resource_family": "articles",
        "count": int(len(events)),
        "filters": {
            "article_id": article_id,
            "augmentation_type": augmentation_type,
            "status": status,
            "producer_name": producer_name,
            "producer_version": producer_version,
            "limit": limit,
        },
        "items": packed_items,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(render_summary_block("Augmentation Events", payload))


@news_group.group("audit")
def audit_group() -> None:
    """Read-only audit/reconciliation commands."""


@audit_group.command("summary")
@click.pass_context
def audit_summary(ctx: click.Context) -> None:
    payload = run_audit_summary(_app_config(ctx))
    if _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_human(payload, title="Audit Summary"))


@audit_group.command("cache")
@click.pass_context
def audit_cache(ctx: click.Context) -> None:
    payload = run_audit_cache(_app_config(ctx))
    if _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_human(payload, title="Audit Cache"))


@audit_group.command("article")
@click.option("--article-id", required=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_article(ctx: click.Context, article_id: str, as_json: bool) -> None:
    payload = run_audit_article(_app_config(ctx), article_id=article_id)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_human(payload, title="Audit Article"))


@audit_group.command("provider")
@click.option("--provider", "provider_id", required=True)
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_provider(ctx: click.Context, provider_id: str, as_json: bool) -> None:
    payload = run_audit_provider(_app_config(ctx), provider_id=provider_id)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_human(payload, title="Audit Provider"))


@audit_group.command("report")
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.option("--ndjson", is_flag=True, default=False, show_default=True)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False, file_okay=True), default=None)
@click.pass_context
def audit_report(ctx: click.Context, as_json: bool, ndjson: bool, output: Path | None) -> None:
    if as_json and ndjson:
        raise click.UsageError("Choose only one of --json or --ndjson")
    payload = run_audit_report(_app_config(ctx))
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        if ndjson:
            output.write_text(render_audit_report_ndjson(payload), encoding="utf-8")
        else:
            output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if ndjson:
        click.echo(render_audit_report_ndjson(payload))
        return
    if as_json or _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_human(payload, title="Audit Report"))


@audit_group.command("compare")
@click.option("--left", "left_path", required=True, type=click.Path(path_type=Path, exists=True, dir_okay=False, file_okay=True))
@click.option("--right", "right_path", required=True, type=click.Path(path_type=Path, exists=True, dir_okay=False, file_okay=True))
@click.option("--json", "as_json", is_flag=True, default=False, show_default=True)
@click.pass_context
def audit_compare(ctx: click.Context, left_path: Path, right_path: Path, as_json: bool) -> None:
    payload = compare_audit_reports(
        left_report=json.loads(left_path.read_text(encoding="utf-8")),
        right_report=json.loads(right_path.read_text(encoding="utf-8")),
        left_label=str(left_path.resolve()),
        right_label=str(right_path.resolve()),
    )
    if as_json or _summary_json(ctx):
        click.echo(json.dumps(payload, sort_keys=True))
        return
    click.echo(render_audit_compare_human(payload))


@audit_group.command("bundle")
@click.option("--output-dir", required=True, type=click.Path(path_type=Path, file_okay=False, dir_okay=True))
@click.option("--overwrite", is_flag=True, default=False, show_default=True)
@click.option("--include-ndjson", is_flag=True, default=False, show_default=True)
@click.option("--article-id", "article_ids", multiple=True)
@click.option("--provider", "provider_ids", multiple=True)
@click.pass_context
def audit_bundle(
    ctx: click.Context,
    output_dir: Path,
    overwrite: bool,
    include_ndjson: bool,
    article_ids: tuple[str, ...],
    provider_ids: tuple[str, ...],
) -> None:
    config = _app_config(ctx)
    output_dir = output_dir.resolve()
    _prepare_bundle_dir(output_dir, overwrite=overwrite)
    requested_providers = _dedupe_sorted(provider_ids)
    requested_articles = _dedupe_sorted(article_ids)
    canonical_provider_ids = _canonical_provider_ids(config)
    if requested_providers:
        unknown = sorted([provider for provider in requested_providers if provider not in canonical_provider_ids])
        if unknown:
            raise click.UsageError(f"Unknown provider IDs: {', '.join(unknown)}")
        export_provider_ids = requested_providers
    else:
        export_provider_ids = canonical_provider_ids

    summary_payload = run_audit_summary(config)
    cache_payload = run_audit_cache(config)
    report_payload = run_audit_report(config)
    status_brief = audit_status_brief(report_payload)
    written_payloads: dict[str, Any] = {
        "audit_summary.json": summary_payload,
        "audit_cache.json": cache_payload,
        "audit_report.json": report_payload,
    }
    if include_ndjson:
        written_payloads["audit_report.ndjson"] = render_audit_report_ndjson(report_payload)
    provider_safe = _safe_name_map(export_provider_ids)
    for provider_id in export_provider_ids:
        written_payloads[f"audit_provider_{provider_safe[provider_id]}.json"] = run_audit_provider(config, provider_id=provider_id)
    article_safe = _safe_name_map(requested_articles)
    for article_id in requested_articles:
        written_payloads[f"audit_article_{article_safe[article_id]}.json"] = run_audit_article(config, article_id=article_id)
    for name in sorted(written_payloads):
        path = output_dir / name
        value = written_payloads[name]
        if name.endswith(".ndjson"):
            path.write_text(str(value) + "\n", encoding="utf-8")
        else:
            path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = {
        "stage": "audit_bundle",
        "generated_at": report_payload.get("generated_at"),
        "repo_root": report_payload.get("repo_root"),
        "arguments": {
            "output_dir": str(output_dir),
            "overwrite": bool(overwrite),
            "include_ndjson": bool(include_ndjson),
            "provider_ids": export_provider_ids,
            "article_ids": requested_articles,
        },
        "status": status_brief["status"],
        "ok": status_brief["ok"],
        "hard_failure_count": status_brief["hard_failure_count"],
        "warning_count": status_brief["warning_count"],
        "observation_count": status_brief["observation_count"],
        "files_written": sorted([*written_payloads.keys(), "manifest.json", "SUMMARY.txt"]),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "SUMMARY.txt").write_text(
        _bundle_summary_text(
            report_payload=report_payload,
            manifest=manifest,
            provider_files=sorted([name for name in written_payloads if name.startswith("audit_provider_")]),
            article_files=sorted([name for name in written_payloads if name.startswith("audit_article_")]),
            next_actions=status_brief["next_actions"],
        ),
        encoding="utf-8",
    )
    click.echo(f"output_dir: {output_dir}")
    click.echo(f"status: {status_brief['status']}")
    click.echo(f"files_written: {len(manifest['files_written'])}")
    click.echo(f"hard_failures_present: {status_brief['hard_failure_count'] > 0}")


@news_group.group("cache")
def cache_group() -> None:
    """Cache layout and migration commands."""


@cache_group.command("rebuild-layout")
@click.option("--cleanup-legacy", is_flag=True, default=False, show_default=True)
@click.option("--repair-metadata", is_flag=True, default=False, show_default=True)
@click.pass_context
def cache_rebuild_layout(ctx: click.Context, cleanup_legacy: bool, repair_metadata: bool) -> None:
    summary = run_cache_rebuild_layout(_app_config(ctx), cleanup_legacy=cleanup_legacy, repair_metadata=repair_metadata)
    if _summary_json(ctx):
        click.echo(json.dumps(summary, sort_keys=True))
        return
    click.echo(render_summary_block("Cache Rebuild Layout", summary))


@news_group.group("api")
def api_group() -> None:
    """API commands."""


@api_group.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8000, type=int, show_default=True)
@click.option("--reload", is_flag=True, default=False, show_default=True)
def api_serve(host: str, port: int, reload: bool) -> None:
    uvicorn.run("py_news.api.app:create_app", host=host, port=port, reload=reload, factory=True)


@news_group.group("monitor", invoke_without_command=True)
@click.pass_context
def monitor_group(ctx: click.Context) -> None:
    """Reserved canonical family for later waves."""
    if ctx.invoked_subcommand is None:
        raise click.UsageError("`m-cache news monitor ...` is reserved for a later wave.")


@news_group.group("reconcile", invoke_without_command=True)
@click.pass_context
def reconcile_group(ctx: click.Context) -> None:
    """Reserved canonical family for later waves."""
    if ctx.invoked_subcommand is None:
        raise click.UsageError("`m-cache news reconcile ...` is reserved for a later wave.")


@news_group.group("storage", invoke_without_command=True)
@click.pass_context
def storage_group(ctx: click.Context) -> None:
    """Reserved canonical family for later waves."""
    if ctx.invoked_subcommand is None:
        raise click.UsageError("`m-cache news storage ...` is reserved for a later wave.")


def _runtime_context(
    ctx: click.Context,
    command_path: list[str],
    *,
    resolution_mode: str | None = None,
    provider_requested: str | None = None,
) -> RuntimeContext:
    return RuntimeContext(
        domain="news",
        command_path=command_path,
        summary_json=_summary_json(ctx),
        progress_json=bool(ctx.obj["progress_json"]),
        progress_heartbeat_seconds=float(ctx.obj["progress_heartbeat_seconds"]),
        quiet=bool(ctx.obj["quiet"]),
        verbose=bool(ctx.obj["verbose"]),
        log_level=str(ctx.obj["log_level"]) if ctx.obj.get("log_level") is not None else None,
        log_file=str(ctx.obj["log_file"]) if ctx.obj.get("log_file") else None,
        resolution_mode=resolution_mode,
        provider_requested=provider_requested,
    )


def _summary_json(ctx: click.Context) -> bool:
    return bool(ctx.obj["summary_json"])


def _app_config(ctx: click.Context) -> AppConfig:
    return ctx.obj["config"]


def _effective_config(ctx: click.Context) -> dict[str, Any]:
    return ctx.obj["effective_config"].to_dict()


def _load_table(config: AppConfig, artifact_name: str) -> pd.DataFrame:
    path = normalized_artifact_path(config, artifact_name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _publisher_slug_from_row(row: dict[str, Any]) -> str:
    source_domain = _safe_text(row.get("source_domain"))
    if source_domain:
        return source_domain
    source_name = _safe_text(row.get("source_name"))
    if source_name:
        return source_name
    return _safe_text(row.get("provider")) or "unknown"


def _provider_detail_payload(ctx: click.Context, row: dict[str, Any]) -> dict[str, Any]:
    provider_id = str(row.get("provider_id") or "")
    effective = _effective_config(ctx)
    providers_cfg = (
        effective.get("domains", {})
        .get("news", {})
        .get("providers", {})
    )
    provider_cfg = providers_cfg.get(provider_id, {}) if isinstance(providers_cfg, dict) else {}
    auth_env_var = str(row.get("auth_env_var") or "").strip()
    effective_auth_present = bool(auth_env_var and bool(os.getenv(auth_env_var, "").strip()))
    effective_enabled = bool(row.get("is_active"))
    if isinstance(provider_cfg, dict) and "enabled" in provider_cfg:
        effective_enabled = bool(provider_cfg.get("enabled"))

    payload = {
        "provider_id": row.get("provider_id"),
        "domain": row.get("domain"),
        "content_domain": row.get("content_domain"),
        "display_name": row.get("display_name") or row.get("provider_name"),
        "provider_type": row.get("provider_type"),
        "auth_type": row.get("auth_type"),
        "auth_env_var": row.get("auth_env_var"),
        "rate_limit_policy": row.get("rate_limit_policy"),
        "direct_resolution_allowed": bool(row.get("direct_url_allowed")),
        "browse_discovery_allowed": bool(row.get("browse_discovery_allowed")),
        "supports_bulk_history": bool(row.get("supports_bulk_history")),
        "supports_incremental_refresh": bool(row.get("supports_incremental_refresh")),
        "supports_direct_resolution": bool(row.get("supports_direct_resolution")),
        "supports_public_resolve_if_missing": bool(row.get("supports_public_resolve_if_missing")),
        "supports_admin_refresh_if_stale": bool(row.get("supports_admin_refresh_if_stale")),
        "graceful_degradation_policy": row.get("graceful_degradation_policy"),
        "is_active": bool(row.get("is_active")),
        "soft_limit": row.get("soft_limit"),
        "hard_limit": row.get("hard_limit"),
        "burst_limit": row.get("burst_limit"),
        "retry_budget": row.get("retry_budget"),
        "backoff_policy": row.get("backoff_policy"),
        "fallback_priority": row.get("fallback_priority"),
        "default_timeout_seconds": row.get("default_timeout_seconds"),
        "quota_window_seconds": row.get("quota_window_seconds"),
        "quota_reset_hint": row.get("quota_reset_hint"),
        "expected_error_modes": [item.strip() for item in str(row.get("expected_error_modes") or "").split(",") if item.strip()],
        "user_agent_required": row.get("user_agent_required"),
        "contact_requirement": row.get("contact_requirement"),
        "terms_url": row.get("terms_url"),
        "effective_auth_present": effective_auth_present,
        "effective_enabled": effective_enabled,
        "notes": row.get("notes"),
    }
    return payload


def _selection_outcome_from_result(result: Any) -> str:
    if not bool(getattr(result, "remote_attempted", False)):
        return "served_locally"
    provider_requested = getattr(result, "provider_requested", None)
    provider_used = getattr(result, "provider_used", None)
    if provider_requested and provider_used and provider_requested == provider_used:
        return "used_requested_provider"
    if provider_requested and provider_used and provider_requested != provider_used:
        return "used_fallback_provider"
    if bool(getattr(result, "deferred_until", None)):
        return "deferred"
    if bool(getattr(result, "resolved", False)):
        return "used_requested_provider"
    return "failed"
