from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from py_news.audit import run_audit_article, run_audit_provider, run_audit_report, run_audit_summary
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


def test_audit_cli_human_output_grouped_sections(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()

    commands = [
        ["audit", "summary"],
        ["audit", "cache"],
        ["audit", "article", "--article-id", "art_1"],
        ["audit", "provider", "--provider", "newsdata"],
    ]
    for args in commands:
        result = runner.invoke(cli, ["--project-root", str(tmp_path), *args])
        assert result.exit_code == 0
        output = result.output
        assert "Hard failures" in output
        assert "Warnings" in output
        assert "Observations" in output
        assert "Key counts" in output
        assert "Suggested next operator actions" in output


def test_audit_human_output_includes_remediation_hint(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    idx = config.cache_root / "provider" / "full-index" / "newsdata" / "article_map.parquet"
    idx.unlink()
    runner = CliRunner()
    result = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "summary"])
    assert result.exit_code == 0
    assert "missing_provider_index_file" in result.output
    assert "rebuild" in result.output.lower()


def test_audit_report_json_contract(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "report", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    for field in [
        "stage",
        "generated_at",
        "status",
        "ok",
        "repo_root",
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


def _artifact_hashes(config) -> dict[str, str]:
    out: dict[str, str] = {}
    for artifact in (
        "articles",
        "local_lookup_articles",
        "article_storage_map",
        "storage_articles",
        "article_artifacts",
        "resolution_events",
    ):
        path = normalized_artifact_path(config, artifact)
        if not path.exists():
            out[artifact] = "missing"
            continue
        out[artifact] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def _cache_hashes(config) -> dict[str, str]:
    out: dict[str, str] = {}
    if not config.cache_root.exists():
        return out
    for path in sorted([p for p in config.cache_root.rglob("*") if p.is_file()]):
        out[str(path.relative_to(config.cache_root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def test_audit_report_output_writes_file_and_is_read_only(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    before = _artifact_hashes(config)
    output_path = tmp_path / "audit_report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "report", "--output", str(output_path)],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["stage"] == "audit_report"
    after = _artifact_hashes(config)
    assert after == before


def test_audit_report_ndjson_shape(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "report", "--ndjson"])
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) >= 2
    records = [json.loads(line) for line in lines]
    assert records[0]["record_type"] == "summary"
    assert records[1]["record_type"] == "key_counts"
    bucket_records = [row for row in records if row.get("record_type") == "issue_bucket"]
    assert isinstance(bucket_records, list)


def test_audit_report_rejects_json_and_ndjson_together(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "report", "--json", "--ndjson"])
    assert result.exit_code != 0
    assert "Choose only one of --json or --ndjson" in result.output


def test_audit_compare_detects_bucket_deltas_and_is_read_only(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    left = run_audit_report(config)

    map_path = normalized_artifact_path(config, "article_storage_map")
    map_df = pd.read_parquet(map_path)
    map_df = map_df.iloc[0:0]
    map_df.to_parquet(map_path, index=False)
    right = run_audit_report(config)

    left_path = tmp_path / "left.json"
    right_path = tmp_path / "right.json"
    left_path.write_text(json.dumps(left, indent=2, sort_keys=True), encoding="utf-8")
    right_path.write_text(json.dumps(right, indent=2, sort_keys=True), encoding="utf-8")

    # restore canonical files before compare so compare command remains pure file-read only
    _coherent_state(tmp_path)
    before = _artifact_hashes(load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache"))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "audit",
            "compare",
            "--left",
            str(left_path),
            "--right",
            str(right_path),
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "audit_compare"
    assert "new_hard_failures" in payload
    assert "resolved_hard_failures" in payload
    assert "new_warnings" in payload
    assert "resolved_warnings" in payload
    assert "count_deltas" in payload
    assert "observations_delta" in payload
    assert any(item["issue_code"] == "missing_article_storage_mapping" for item in payload["new_hard_failures"])
    after = _artifact_hashes(load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache"))
    assert after == before


def test_audit_bundle_creates_output_dir_and_writes_expected_files(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle" / "audit"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir)],
    )
    assert result.exit_code == 0
    assert out_dir.exists()
    expected = {
        "audit_summary.json",
        "audit_cache.json",
        "audit_report.json",
        "manifest.json",
        "SUMMARY.txt",
    }
    files = {path.name for path in out_dir.iterdir()}
    assert expected.issubset(files)
    assert "audit_report.ndjson" not in files


def test_audit_bundle_fails_on_nonempty_dir_without_overwrite(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle_nonempty"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "keep.txt").write_text("x", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir)],
    )
    assert result.exit_code != 0
    assert "Output directory is not empty" in result.output


def test_audit_bundle_overwrite_clears_contents_and_writes_fresh_bundle(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle_overwrite"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "old.json").write_text("old", encoding="utf-8")
    (out_dir / "nested").mkdir(parents=True, exist_ok=True)
    (out_dir / "nested" / "old.txt").write_text("old", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir), "--overwrite"],
    )
    assert result.exit_code == 0
    files = {path.name for path in out_dir.iterdir()}
    assert "old.json" not in files
    assert "nested" not in files
    assert "audit_report.json" in files
    assert "manifest.json" in files


def test_audit_bundle_explicit_provider_and_article_selection(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle_subset"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "audit",
            "bundle",
            "--output-dir",
            str(out_dir),
            "--provider",
            "newsdata",
            "--provider",
            "newsdata",
            "--article-id",
            "art_1",
            "--article-id",
            "art_1",
        ],
    )
    assert result.exit_code == 0
    files = sorted(path.name for path in out_dir.iterdir())
    provider_files = [name for name in files if name.startswith("audit_provider_")]
    article_files = [name for name in files if name.startswith("audit_article_")]
    assert provider_files == ["audit_provider_newsdata.json"]
    assert article_files == ["audit_article_art_1.json"]


def test_audit_bundle_default_provider_exports_all_canonical_registry_providers(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    # derive authoritative count from provider_registry parquet itself
    provider_registry = pd.read_parquet(normalized_artifact_path(config, "provider_registry"))
    canonical_ids = sorted({str(v).strip() for v in provider_registry["provider_id"].tolist() if str(v).strip()})
    out_dir = tmp_path / "bundle_default_providers"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir)],
    )
    assert result.exit_code == 0
    provider_files = sorted([path.name for path in out_dir.iterdir() if path.name.startswith("audit_provider_")])
    assert len(provider_files) == len(canonical_ids)


def test_audit_bundle_rejects_unknown_provider_id(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle_unknown_provider"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "audit",
            "bundle",
            "--output-dir",
            str(out_dir),
            "--provider",
            "unknown_provider",
        ],
    )
    assert result.exit_code != 0
    assert "Unknown provider IDs" in result.output


def test_audit_bundle_ndjson_emission_is_optional(tmp_path):
    _coherent_state(tmp_path)
    runner = CliRunner()
    out_no = tmp_path / "bundle_no_ndjson"
    result_no = runner.invoke(cli, ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_no)])
    assert result_no.exit_code == 0
    assert not (out_no / "audit_report.ndjson").exists()

    out_yes = tmp_path / "bundle_with_ndjson"
    result_yes = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "audit",
            "bundle",
            "--output-dir",
            str(out_yes),
            "--include-ndjson",
        ],
    )
    assert result_yes.exit_code == 0
    assert (out_yes / "audit_report.ndjson").exists()


def test_audit_bundle_manifest_and_summary_shape(tmp_path):
    _coherent_state(tmp_path)
    out_dir = tmp_path / "bundle_manifest"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir), "--article-id", "art_1"],
    )
    assert result.exit_code == 0
    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    for field in (
        "generated_at",
        "repo_root",
        "arguments",
        "status",
        "ok",
        "hard_failure_count",
        "warning_count",
        "observation_count",
        "files_written",
    ):
        assert field in manifest
    assert manifest["arguments"]["article_ids"] == ["art_1"]
    assert manifest["files_written"] == sorted(manifest["files_written"])

    summary = (out_dir / "SUMMARY.txt").read_text(encoding="utf-8")
    assert "status:" in summary
    assert "hard_failures_present:" in summary
    assert "Key row counts" in summary
    assert "provider_files_included:" in summary
    assert "article_files_included:" in summary


def test_audit_bundle_is_read_only_for_canonical_and_cache_state(tmp_path):
    config, _, _, _ = _coherent_state(tmp_path)
    before_artifacts = _artifact_hashes(config)
    before_cache = _cache_hashes(config)
    out_dir = tmp_path / "bundle_read_only"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "audit", "bundle", "--output-dir", str(out_dir), "--include-ndjson"],
    )
    assert result.exit_code == 0
    after_artifacts = _artifact_hashes(config)
    after_cache = _cache_hashes(config)
    assert after_artifacts == before_artifacts
    assert after_cache == before_cache
