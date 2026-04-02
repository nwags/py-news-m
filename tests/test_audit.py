from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from py_news.audit import run_audit_article, run_audit_provider, run_audit_summary
from py_news.cache_layout import build_provider_full_index
from py_news.cli import cli
from py_news.config import load_config
from py_news.models import (
    ARTICLE_ARTIFACT_COLUMNS,
    ARTICLES_COLUMNS,
    ARTICLE_STORAGE_MAP_COLUMNS,
    LOOKUP_ARTICLE_COLUMNS,
    RESOLUTION_EVENT_COLUMNS,
    STORAGE_ARTICLES_COLUMNS,
)
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.storage.paths import normalized_artifact_path, publisher_article_meta_path
from py_news.storage.writes import append_parquet_rows, upsert_parquet_rows, write_json, write_text


def _article_row(article_id: str = "art_1", provider: str = "newsdata") -> dict:
    return {
        "article_id": article_id,
        "provider": provider,
        "provider_document_id": f"doc-{article_id}",
        "resolved_document_identity": f"provider={provider}|provider_document_id=doc-{article_id}",
        "source_name": "Example Source",
        "source_domain": "example.com",
        "url": f"https://example.com/{article_id}",
        "canonical_url": f"https://example.com/{article_id}",
        "title": f"Title {article_id}",
        "published_at": "2026-03-05T01:00:00Z",
        "language": "en",
        "section": "news",
        "byline": "Reporter",
        "article_text": None,
        "summary_text": "Summary",
        "snippet": "Snippet",
        "metadata_json": None,
        "imported_at": "2026-03-05T02:00:00Z",
    }


def _seed(config, artifact: str, rows: list[dict], columns: list[str], dedupe_keys: list[str]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, artifact),
        rows=rows,
        dedupe_keys=dedupe_keys,
        column_order=columns,
        drop_extra_columns=False,
    )


def _seed_resolution_events(config, rows: list[dict]) -> None:
    append_parquet_rows(
        path=normalized_artifact_path(config, "resolution_events"),
        rows=rows,
        column_order=RESOLUTION_EVENT_COLUMNS,
        drop_extra_columns=False,
    )


def _coherent_state(tmp_path: Path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)

    article = _article_row("art_1", "newsdata")
    _seed(config, "articles", [article], ARTICLES_COLUMNS, ["provider", "resolved_document_identity"])

    sid = "sto_1"
    folder = publisher_article_meta_path(
        config,
        publisher_slug="example.com",
        published_at=article["published_at"],
        article_id=sid,
    ).parent
    folder.mkdir(parents=True, exist_ok=True)
    txt_path = folder / "article.txt"
    txt_path.write_text("hello world", encoding="utf-8")
    meta_path = folder / "meta.json"
    write_json(
        meta_path,
        {
            "storage_article_id": sid,
            "mapped_article_ids": ["art_1"],
        },
    )

    _seed(
        config,
        "storage_articles",
        [
            {
                "storage_article_id": sid,
                "publisher_slug": "example-com",
                "storage_anchor_date": "2026-03-05",
                "storage_folder_path": str(folder),
                "equivalence_basis": "canonical_url",
                "equivalence_value": "example.com/art_1",
                "created_at": "2026-03-05T02:00:00Z",
                "updated_at": "2026-03-05T02:00:00Z",
            }
        ],
        STORAGE_ARTICLES_COLUMNS,
        ["storage_article_id"],
    )
    _seed(
        config,
        "article_storage_map",
        [
            {
                "article_id": "art_1",
                "provider": "newsdata",
                "resolved_document_identity": article["resolved_document_identity"],
                "storage_article_id": sid,
                "mapping_basis": "canonical_url",
                "mapped_at": "2026-03-05T02:00:00Z",
            }
        ],
        ARTICLE_STORAGE_MAP_COLUMNS,
        ["article_id"],
    )
    _seed(
        config,
        "article_artifacts",
        [
            {
                "article_id": "art_1",
                "storage_article_id": sid,
                "artifact_type": "article_text",
                "artifact_path": str(txt_path),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            }
        ],
        ARTICLE_ARTIFACT_COLUMNS,
        ["article_id", "storage_article_id", "artifact_type"],
    )
    _seed(
        config,
        "local_lookup_articles",
        [
            {
                "article_id": "art_1",
                "provider": "newsdata",
                "source_name": "Example Source",
                "source_domain": "example.com",
                "published_at": "2026-03-05T01:00:00Z",
                "title": "Title art_1",
                "summary_text": "Summary",
                "snippet": "Snippet",
                "canonical_url": "https://example.com/art_1",
                "url": "https://example.com/art_1",
                "language": "en",
                "section": "news",
            }
        ],
        LOOKUP_ARTICLE_COLUMNS,
        ["article_id"],
    )
    _seed_resolution_events(
        config,
        [
            {
                "event_id": "evt_1",
                "event_at": "2026-03-05T03:00:00Z",
                "article_id": "art_1",
                "provider": "newsdata",
                "representation": "content",
                "strategy": "local_artifact",
                "success": True,
                "reason_code": "local_content_hit",
                "message": None,
                "status_code": None,
                "artifact_path": str(txt_path),
                "meta_sidecar_path": str(meta_path),
                "provenance_json": json.dumps({"allow_remote": False}, sort_keys=True),
            }
        ],
    )
    build_provider_full_index(config)
    return config, sid, folder, txt_path


def _has_issue(payload: dict, bucket: str, issue_code: str) -> bool:
    return any(item["issue_code"] == issue_code for item in payload[bucket])


def test_audit_summary_ok_for_coherent_state(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    payload = run_audit_summary(config)
    assert payload["ok"] is True
    assert payload["status"] == "ok"
    assert payload["hard_failures"] == []


def test_audit_detects_missing_and_duplicate_article_storage_mapping(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    map_path = normalized_artifact_path(config, "article_storage_map")
    df = pd.read_parquet(map_path)
    df = df.iloc[0:0]
    df.to_parquet(map_path, index=False)
    payload = run_audit_summary(config)
    assert _has_issue(payload, "hard_failures", "missing_article_storage_mapping")

    _coherent_state(tmp_path / "dup")
    cfg2 = load_config(project_root=tmp_path / "dup", cache_root=(tmp_path / "dup") / ".news_cache")
    dup = pd.read_parquet(normalized_artifact_path(cfg2, "article_storage_map"))
    dup2 = dup.copy()
    dup2.loc[:, "storage_article_id"] = "sto_2"
    pd.concat([dup, dup2], ignore_index=True).to_parquet(normalized_artifact_path(cfg2, "article_storage_map"), index=False)
    payload2 = run_audit_summary(cfg2)
    assert _has_issue(payload2, "hard_failures", "duplicate_article_storage_mapping")


def test_audit_detects_artifact_path_and_file_failures(tmp_path):
    config, sid, folder, _ = _coherent_state(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("x", encoding="utf-8")
    _seed(
        config,
        "article_artifacts",
        [
            {
                "article_id": "art_1",
                "storage_article_id": sid,
                "artifact_type": "article_json",
                "artifact_path": str(outside),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            }
        ],
        ARTICLE_ARTIFACT_COLUMNS,
        ["article_id", "storage_article_id", "artifact_type"],
    )
    payload = run_audit_summary(config)
    assert _has_issue(payload, "hard_failures", "outside_canonical_cache_path")

    missing = folder / "article.html"
    _seed(
        config,
        "article_artifacts",
        [
            {
                "article_id": "art_1",
                "storage_article_id": sid,
                "artifact_type": "article_html",
                "artifact_path": str(missing),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            }
        ],
        ARTICLE_ARTIFACT_COLUMNS,
        ["article_id", "storage_article_id", "artifact_type"],
    )
    payload2 = run_audit_summary(config)
    assert _has_issue(payload2, "hard_failures", "artifact_file_missing")


def test_audit_detects_artifact_storage_folder_missing(tmp_path):
    config, sid, folder, txt_path = _coherent_state(tmp_path)
    storage = pd.read_parquet(normalized_artifact_path(config, "storage_articles"))
    storage.loc[:, "storage_folder_path"] = str(folder / "missing-folder")
    storage.to_parquet(normalized_artifact_path(config, "storage_articles"), index=False)
    assert txt_path.exists()
    payload = run_audit_summary(config)
    assert _has_issue(payload, "hard_failures", "artifact_storage_folder_missing")


def test_audit_storage_sidecar_classification(tmp_path):
    config, _, folder, _ = _coherent_state(tmp_path)
    # Orphan folder
    orphan = config.cache_root / "publisher" / "data" / "orphan-pub" / "2026" / "03" / "sto-orphan"
    orphan.mkdir(parents=True, exist_ok=True)
    write_json(orphan / "meta.json", {"storage_article_id": "sto_orphan"})
    payload = run_audit_summary(config)
    assert _has_issue(payload, "warnings", "orphan_storage_folder")

    # Metadata-only folder observation (canonical mapped folder with no article files)
    (folder / "article.txt").unlink()
    payload2 = run_audit_summary(config)
    assert _has_issue(payload2, "observations", "metadata_only_folder")

    # Invalid/corrupt sidecar warning
    (folder / "meta.json").write_text("{invalid", encoding="utf-8")
    payload3 = run_audit_summary(config)
    assert _has_issue(payload3, "warnings", "invalid_meta_sidecar")


def test_audit_detects_provider_index_mismatch(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    idx = config.cache_root / "provider" / "full-index" / "newsdata" / "article_map.parquet"
    df = pd.read_parquet(idx)
    df = df.iloc[0:0]
    df.to_parquet(idx, index=False)
    payload = run_audit_summary(config)
    assert _has_issue(payload, "hard_failures", "provider_article_map_mismatch")


def test_audit_lookup_drift_and_lookup_missing_article(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    lookup_path = normalized_artifact_path(config, "local_lookup_articles")
    lookup = pd.read_parquet(lookup_path)
    extra = lookup.iloc[[0]].copy()
    extra.loc[:, "article_id"] = "missing_article"
    lookup = pd.concat([lookup, extra], ignore_index=True)
    lookup.to_parquet(lookup_path, index=False)
    payload = run_audit_summary(config)
    assert _has_issue(payload, "warnings", "lookup_row_count_drift")
    assert _has_issue(payload, "hard_failures", "lookup_missing_article")

    lookup_only_missing = lookup.iloc[[1]].copy()
    lookup_only_missing.to_parquet(lookup_path, index=False)
    payload_article = run_audit_article(config, article_id="art_1")
    assert _has_issue(payload_article, "hard_failures", "lookup_article_missing")


def test_audit_historical_provenance_paths_are_observations_only(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    _seed_resolution_events(
        config,
        [
            {
                "event_id": "evt_legacy",
                "event_at": "2026-03-05T03:30:00Z",
                "article_id": "art_1",
                "provider": "newsdata",
                "representation": "content",
                "strategy": "direct_url_fetch",
                "success": False,
                "reason_code": "http_failure",
                "message": "legacy",
                "status_code": 404,
                "artifact_path": "/tmp/pytest-of-user/legacy.txt",
                "meta_sidecar_path": "/tmp/pytest-of-user/legacy-meta.json",
                "provenance_json": json.dumps({"old_path": ".news_cache/articles/raw/x.html"}, sort_keys=True),
            }
        ],
    )
    payload = run_audit_summary(config)
    assert _has_issue(payload, "observations", "historical_tmp_or_pytest_path")
    assert payload["ok"] is True
    assert payload["status"] == "ok"


def test_audit_provider_command_accepts_canonical_provider_id(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    payload = run_audit_provider(config, provider_id="newsdata")
    assert payload["provider"] == "newsdata"
    assert "newsdata" in payload["provider_index_counts"]


def test_audit_cli_json_contract(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()

    summary = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "summary", "--summary-json"])
    assert summary.exit_code == 0
    payload = json.loads(summary.output)
    for field in [
        "stage",
        "status",
        "ok",
        "articles_rows",
        "lookup_rows",
        "mapping_rows",
        "storage_rows",
        "artifact_rows",
        "resolution_events_rows",
        "provider_index_counts",
        "counts_by_issue",
        "hard_failures",
        "warnings",
        "observations",
    ]:
        assert field in payload

    cache = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "cache", "--summary-json"])
    assert cache.exit_code == 0
    article = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "article", "--article-id", "art_1", "--json"])
    assert article.exit_code == 0
    provider = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "provider", "--provider", "newsdata", "--json"],
    )
    assert provider.exit_code == 0
