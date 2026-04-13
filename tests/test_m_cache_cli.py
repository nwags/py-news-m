import json
from pathlib import Path

from click.testing import CliRunner
import pandas as pd

from py_news.cli import cli
from py_news.config import load_config
from py_news.http import HttpFailure
from py_news.m_cache_cli import m_cache_cli
from py_news.models import ARTICLES_COLUMNS, AUGMENTATION_ARTIFACT_COLUMNS, AUGMENTATION_RUN_COLUMNS
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


FIXTURES = Path(__file__).parent / "fixtures"


def test_m_cache_help_includes_news_domain():
    runner = CliRunner()
    result = runner.invoke(m_cache_cli, ["--help"])
    assert result.exit_code == 0
    assert "news" in result.output


def test_m_cache_refdata_refresh_summary_json_shape(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "--summary-json", "news", "refdata", "refresh"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    for key in (
        "status",
        "domain",
        "command_path",
        "started_at",
        "finished_at",
        "elapsed_seconds",
        "resolution_mode",
        "remote_attempted",
        "provider_requested",
        "provider_used",
        "rate_limited",
        "retry_count",
        "persisted_locally",
        "counters",
        "warnings",
        "errors",
        "effective_config",
    ):
        assert key in payload
    assert payload["domain"] == "news"
    assert payload["command_path"] == ["m-cache", "news", "refdata", "refresh"]


def test_m_cache_progress_json_emits_ndjson_events(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "--summary-json",
            "--progress-json",
            "news",
            "refdata",
            "refresh",
        ],
    )
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) >= 3
    first = json.loads(lines[0])
    assert first["event"] == "started"
    assert first["domain"] == "news"
    assert first["command_path"] == ["m-cache", "news", "refdata", "refresh"]
    assert "phase" in first
    assert "elapsed_seconds" in first
    assert isinstance(first["counters"], dict)
    summary = json.loads(lines[-1])
    assert summary["domain"] == "news"


def test_py_news_compatibility_cli_still_works(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "articles_sample.csv"
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "local_tabular",
        ],
    )
    assert result.exit_code == 0
    assert "imported_rows" in result.output


def test_m_cache_providers_show_json(tmp_path):
    runner = CliRunner()
    runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "news", "providers", "refresh"],
    )
    result = runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "news", "providers", "show", "--provider", "newsdata", "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    for key in (
        "provider_id",
        "domain",
        "content_domain",
        "display_name",
        "provider_type",
        "auth_type",
        "rate_limit_policy",
        "direct_resolution_allowed",
        "graceful_degradation_policy",
        "is_active",
        "effective_auth_present",
        "effective_enabled",
    ):
        assert key in payload
    assert payload["provider_id"] == "newsdata"


def test_m_cache_resolve_refresh_if_stale_fails_transparently(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "--summary-json",
            "news",
            "resolve",
            "article",
            "--article-id",
            "art_missing",
            "--resolution-mode",
            "refresh_if_stale",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "mode_unsupported"
    assert payload["resolution_mode"] == "refresh_if_stale"


def test_m_cache_resolve_rate_limit_telemetry(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[
            {
                "article_id": "art_rate",
                "provider": "gdelt_recent",
                "provider_document_id": "doc-rate",
                "resolved_document_identity": "provider=gdelt_recent|provider_document_id=doc-rate",
                "source_name": "Reuters",
                "source_domain": "reuters.com",
                "url": "https://example.com/rate",
                "canonical_url": "https://example.com/rate",
                "title": "Rate limited",
                "published_at": "2026-03-05T01:00:00Z",
                "language": "en",
                "section": "news",
                "byline": None,
                "article_text": None,
                "summary_text": "summary",
                "snippet": "snippet",
                "metadata_json": None,
                "imported_at": "2026-03-05T02:00:00Z",
            }
        ],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )

    def fake_request_response(self, method, url, params=None, headers=None):
        raise HttpFailure(method=method, url=url, reason="throttled", attempts=3, status_code=429, is_transient=True)

    monkeypatch.setattr("py_news.http.HttpClient.request_response", fake_request_response)

    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "--summary-json",
            "news",
            "resolve",
            "article",
            "--article-id",
            "art_rate",
            "--resolution-mode",
            "resolve_if_missing",
            "--representation",
            "content",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["rate_limited"] is True
    assert payload["retry_count"] == 2

    events = pd.read_parquet(normalized_artifact_path(config, "resolution_events"))
    row = events.iloc[-1].to_dict()
    assert bool(row["rate_limited"]) is True
    assert int(row["retry_count"]) == 2


def test_m_cache_aug_list_types_is_read_only_surface(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "news", "aug", "list-types", "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "augmentation_list_types"
    assert payload["resource_family"] == "articles"
    assert payload["augmentation_types"] == ["entity_tagging", "temporal_expression_tagging"]


def test_m_cache_aug_inspect_target_article_text_sources(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[
            {
                "article_id": "art_aug_target",
                "provider": "nyt_archive",
                "provider_document_id": "doc-aug-target",
                "resolved_document_identity": "provider=nyt_archive|provider_document_id=doc-aug-target",
                "source_name": "New York Times",
                "source_domain": "nytimes.com",
                "url": "https://example.com/aug-target",
                "canonical_url": "https://example.com/aug-target",
                "title": "Aug target",
                "published_at": "2026-04-01T01:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "By Reporter",
                "article_text": None,
                "summary_text": "Summary text body",
                "snippet": "Snippet text body",
                "metadata_json": None,
                "imported_at": "2026-04-01T02:00:00Z",
            }
        ],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )

    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "inspect-target",
            "--article-id",
            "art_aug_target",
            "--text-source",
            "metadata",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["augmentation_applicable"] is True
    assert payload["text_source"] == "metadata"
    assert payload["text_present"] is True
    assert payload["source_text_version"].startswith("sha256:")


def test_m_cache_aug_target_descriptor_alias_preserved(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[
            {
                "article_id": "art_desc",
                "provider": "newsdata",
                "provider_document_id": "doc-desc",
                "resolved_document_identity": "provider=newsdata|provider_document_id=doc-desc",
                "source_name": "Source",
                "source_domain": "example.com",
                "url": "https://example.com/desc",
                "canonical_url": "https://example.com/desc",
                "title": "Descriptor",
                "published_at": "2026-04-02T01:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "Byline",
                "article_text": "content body",
                "summary_text": "summary body",
                "snippet": "snippet body",
                "metadata_json": None,
                "imported_at": "2026-04-02T02:00:00Z",
            }
        ],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )

    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "target-descriptor",
            "--article-id",
            "art_desc",
            "--text-source",
            "content",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["domain"] == "news"
    assert payload["resource_family"] == "articles"
    assert payload["canonical_key"] == "article:art_desc"
    assert payload["text_source"] == "api:/articles/art_desc/content"
    assert payload["source_text_version"].startswith("sha256:")


def test_m_cache_aug_submit_run_and_status_idempotent(tmp_path):
    runner = CliRunner()
    run_payload = {
        "run_id": "run-001",
        "domain": "news",
        "resource_family": "articles",
        "canonical_key": "article:art_run",
        "augmentation_type": "entity_tagging",
        "source_text_version": "sha256:abc",
        "producer_kind": "llm",
        "producer_name": "entity-producer",
        "producer_version": "1.0.0",
        "payload_schema_name": "com.example.entity-span-set",
        "payload_schema_version": "2026-04-01",
        "status": "completed",
        "success": True,
        "reason_code": "completed",
        "persisted_locally": True,
    }
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit-run",
            "--json-payload",
            json.dumps(run_payload),
            "--json",
        ],
    )
    assert result.exit_code == 0
    first = json.loads(result.output)
    assert first["action"] == "submit_run"
    assert first["deduped"] is False

    replay = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit-run",
            "--json-payload",
            json.dumps(run_payload),
            "--json",
        ],
    )
    assert replay.exit_code == 0
    replay_payload = json.loads(replay.output)
    assert replay_payload["deduped"] is True

    status = runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "news", "aug", "status", "--run-id", "run-001", "--json"],
    )
    assert status.exit_code == 0
    status_payload = json.loads(status.output)
    assert status_payload["count"] == 1
    assert status_payload["items"][0]["producer_version"] == "1.0.0"


def test_m_cache_aug_submit_artifact_large_payload_uses_locator(tmp_path):
    runner = CliRunner()
    large_payload = {"annotations": [{"span_start": 0, "span_end": 10000, "text": "x" * 20000, "label": "ORG"}]}
    artifact_submission = {
        "domain": "news",
        "resource_family": "articles",
        "canonical_key": "article:art_big",
        "augmentation_type": "entity_tagging",
        "source_text_version": "sha256:big",
        "producer_name": "entity-producer",
        "producer_version": "2.0.0",
        "payload_schema_name": "com.example.entity-span-set",
        "payload_schema_version": "2026-04-01",
        "payload": large_payload,
        "success": True,
    }
    submit = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit-artifact",
            "--inline-payload-max-bytes",
            "128",
            "--json-payload",
            json.dumps(artifact_submission),
            "--json",
        ],
    )
    assert submit.exit_code == 0
    payload = json.loads(submit.output)
    assert payload["action"] == "submit_artifact"
    assert payload["payload_truncated"] is True
    assert payload["stored_inline"] is False
    assert payload["artifact_locator"] is not None

    events = runner.invoke(
        m_cache_cli,
        ["--project-root", str(tmp_path), "news", "aug", "events", "--producer-name", "entity-producer", "--json"],
    )
    assert events.exit_code == 0
    events_payload = json.loads(events.output)
    assert events_payload["stage"] == "augmentation_events"


def test_m_cache_aug_submit_alias_preserved(tmp_path):
    runner = CliRunner()
    run_payload = {
        "run_id": "run-alias",
        "domain": "news",
        "resource_family": "articles",
        "canonical_key": "article:art_alias",
        "augmentation_type": "entity_tagging",
        "source_text_version": "sha256:alias",
        "producer_kind": "llm",
        "producer_name": "entity-producer",
        "producer_version": "1.0.0",
        "payload_schema_name": "com.example.entity-span-set",
        "payload_schema_version": "2026-04-01",
        "status": "completed",
        "success": True,
        "reason_code": "completed",
    }
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit",
            "--kind",
            "run",
            "--json-payload",
            json.dumps(run_payload),
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["action"] == "submit_run"


def test_m_cache_aug_inspect_artifacts_filters(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "augmentation_artifacts"),
        rows=[
            {
                "run_id": "run-a",
                "idempotency_key": "art-key-a",
                "domain": "news",
                "resource_family": "articles",
                "canonical_key": "article:art_aug_a",
                "augmentation_type": "entity_tagging",
                "artifact_locator": ".news_cache/augmentations/article-art_aug_a/entity_tagging.json",
                "source_text_version": "sha256:abc",
                "producer_name": "entity-v1",
                "producer_version": "1.0.0",
                "payload_schema_name": "com.example.entity-span-set",
                "payload_schema_version": "2026-04-01",
                "payload_inline_json": None,
                "payload_size_bytes": 0,
                "payload_truncated": False,
                "event_at": "2026-04-08T17:04:00Z",
                "success": True,
            },
            {
                "run_id": "run-b",
                "idempotency_key": "art-key-b",
                "domain": "news",
                "resource_family": "articles",
                "canonical_key": "article:art_aug_a",
                "augmentation_type": "temporal_expression_tagging",
                "artifact_locator": ".news_cache/augmentations/article-art_aug_a/temporal_expression_tagging.json",
                "source_text_version": "sha256:def",
                "producer_name": "temporal-v1",
                "producer_version": "1.1.0",
                "payload_schema_name": "com.example.temporal-span-set",
                "payload_schema_version": "2026-04-01",
                "payload_inline_json": None,
                "payload_size_bytes": 0,
                "payload_truncated": False,
                "event_at": "2026-04-08T18:04:00Z",
                "success": False,
            },
        ],
        dedupe_keys=["idempotency_key"],
        column_order=AUGMENTATION_ARTIFACT_COLUMNS,
    )

    runner = CliRunner()
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "inspect-artifacts",
            "--article-id",
            "art_aug_a",
            "--success",
            "true",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["count"] == 1
    assert payload["items"][0]["augmentation_type"] == "entity_tagging"


def test_m_cache_aug_submit_run_schema_validation_error(tmp_path):
    runner = CliRunner()
    bad_payload = {"domain": "news", "resource_family": "articles"}
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit-run",
            "--json-payload",
            json.dumps(bad_payload),
            "--json",
        ],
    )
    assert result.exit_code != 0
    assert "schema validation" in result.output.lower()


def test_m_cache_aug_submit_rejects_non_article_resource_family(tmp_path):
    runner = CliRunner()
    payload = {
        "run_id": "run-bad-resource",
        "domain": "news",
        "resource_family": "provider_registry",
        "canonical_key": "provider:newsdata",
        "augmentation_type": "entity_tagging",
        "source_text_version": "sha256:abc",
        "producer_kind": "llm",
        "producer_name": "entity-producer",
        "producer_version": "1.0.0",
        "payload_schema_name": "com.example.entity-span-set",
        "payload_schema_version": "2026-04-01",
        "status": "failed",
        "success": False,
        "reason_code": "not_applicable",
        "persisted_locally": False,
    }
    result = runner.invoke(
        m_cache_cli,
        [
            "--project-root",
            str(tmp_path),
            "news",
            "aug",
            "submit-run",
            "--json-payload",
            json.dumps(payload),
            "--json",
        ],
    )
    assert result.exit_code != 0
    assert result.exception is not None
    assert "resource_family=articles" in str(result.exception)
