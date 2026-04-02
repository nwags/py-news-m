import json
from pathlib import Path

from click.testing import CliRunner

from py_news.cli import cli
from py_news.http import HttpClient


FIXTURES = Path(__file__).parent / "fixtures"


def test_cli_help_shape():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for command in ["refdata", "articles", "lookup", "api", "resolution", "cache", "audit"]:
        assert command in result.output


def test_cli_import_refresh_and_query_json(tmp_path):
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

    result = runner.invoke(cli, ["--project-root", str(tmp_path), "lookup", "refresh"])
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "lookup",
            "query",
            "--scope",
            "articles",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload) == 3


def test_cli_enforces_article_only_scope(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "lookup", "query", "--scope", "events"],
    )
    assert result.exit_code != 0
    assert "Only --scope articles is supported" in result.output


def test_cli_backfill_gdelt_recent_with_stubbed_http(monkeypatch, tmp_path):
    runner = CliRunner()
    payload = json.loads((FIXTURES / "gdelt_recent_sample.json").read_text(encoding="utf-8"))

    def fake_request_json(self, method, url, params=None, headers=None):
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "backfill",
            "--provider",
            "gdelt_recent",
            "--date",
            "2026-03-05",
            "--window-key",
            "1d",
            "--summary-json",
        ],
    )

    assert result.exit_code == 0
    summary = json.loads(result.output)
    assert summary["provider"] == "gdelt_recent"
    assert summary["normalized_rows"] == 2
    assert "raw_payload_path" in summary


def test_cli_backfill_newsdata_reports_clamp(monkeypatch, tmp_path):
    runner = CliRunner()
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    payload = {
        "results": [
            {
                "article_id": "n1",
                "source_id": "cnn",
                "source_url": "https://cnn.com/path",
                "link": "https://cnn.com/story-1",
                "title": "Story 1",
                "pubDate": "2026-03-05T10:00:00Z",
                "description": "Summary",
                "content": "Snippet",
                "language": "en",
            }
        ]
    }

    def fake_request_json(self, method, url, params=None, headers=None):
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "backfill",
            "--provider",
            "newsdata",
            "--date",
            "2026-03-05",
            "--window-key",
            "1d",
            "--max-records",
            "50",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    summary = json.loads(result.output)
    assert summary["provider"] == "newsdata"
    assert summary["requested_max_records"] == 50
    assert summary["effective_max_records"] == 10
    assert summary["max_records_clamped"] is True


def test_cli_fetch_content_with_stubbed_http(monkeypatch, tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "articles_sample.csv"
    html = (FIXTURES / "content_html_sample.html").read_text(encoding="utf-8")

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text
            self.status_code = 200
            self.headers = {"Content-Type": "text/html"}

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse(html)

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    runner.invoke(
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

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "fetch-content",
            "--provider",
            "nyt",
            "--limit",
            "5",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    summary = json.loads(result.output)
    assert summary["stage"] == "content_fetch"
    assert "reason_counts" in summary
    assert "resolution_source_counts" in summary
    assert "resolution_strategy_counts" in summary
    assert "local_write_rows" in summary


def test_cli_empty_lookup_message(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "lookup", "query", "--scope", "articles"],
    )
    assert result.exit_code == 0
    assert "No matching articles found." in result.output


def test_cli_import_history_nyt_archive_adapter(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"

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
            "nyt_archive",
        ],
    )
    assert result.exit_code == 0
    assert "nyt_archive" in result.output
    assert "imported_rows" in result.output


def test_cli_providers_refresh_and_list_json(tmp_path):
    runner = CliRunner()
    refresh = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "providers", "refresh", "--summary-json"],
    )
    assert refresh.exit_code == 0
    summary = json.loads(refresh.output)
    assert summary["stage"] == "providers_refresh"
    assert summary["providers_count"] >= 3

    listed = runner.invoke(
        cli,
        ["--project-root", str(tmp_path), "providers", "list", "--json"],
    )
    assert listed.exit_code == 0
    payload = json.loads(listed.output)
    ids = {row["provider_id"] for row in payload}
    assert {"nyt_archive", "gdelt_recent", "newsdata"}.issubset(ids)


def test_cli_articles_resolve_summary_json(monkeypatch, tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"

    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            "art_fc7e09459882abbe",
            "--representation",
            "content",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "article_resolve"
    assert payload["article_id"] == "art_fc7e09459882abbe"


def test_cli_articles_resolve_local_only_flag(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            "art_fc7e09459882abbe",
            "--representation",
            "content",
            "--local-only",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["reason_code"] == "content_missing_local"
    assert payload["resolution_reason_code"] == "content_missing_local"
    assert payload["resolution_strategy"] == "local_only"
    assert payload["local_write_performed"] is False


def test_cli_resolution_events_filters_and_json(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            "art_fc7e09459882abbe",
            "--representation",
            "content",
            "--local-only",
            "--summary-json",
        ],
    )
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            "art_17ea6ed34f91889f",
            "--representation",
            "content",
            "--local-only",
            "--summary-json",
        ],
    )

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "resolution",
            "events",
            "--article-id",
            "art_fc7e09459882abbe",
            "--representation",
            "content",
            "--reason-code",
            "content_missing_local",
            "--success",
            "false",
            "--limit",
            "5",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "resolution_events"
    assert payload["filters"]["article_id"] == "art_fc7e09459882abbe"
    assert payload["filters"]["success"] is False
    assert payload["count"] >= 1
    assert all(item["article_id"] == "art_fc7e09459882abbe" for item in payload["items"])
    assert payload["items"][0]["event_at"] >= payload["items"][-1]["event_at"]


def test_cli_articles_inspect_json(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )
    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            "art_fc7e09459882abbe",
            "--representation",
            "content",
            "--local-only",
            "--summary-json",
        ],
    )

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "inspect",
            "--article-id",
            "art_fc7e09459882abbe",
            "--events-limit",
            "5",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "article_inspect"
    assert payload["metadata_present"] is True
    assert payload["provider"] == "nyt_archive"
    assert isinstance(payload["resolution_events"], list)
    assert payload["provider_rule"]["provider_id"] == "nyt_archive"
    assert "storage_article_id" in payload


def test_cli_articles_inspect_with_local_artifacts(monkeypatch, tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    html = (FIXTURES / "content_html_sample.html").read_text(encoding="utf-8")

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text
            self.status_code = 200
            self.headers = {"Content-Type": "text/html"}

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse(html)

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )
    article_id = "art_fc7e09459882abbe"

    runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "resolve",
            "--article-id",
            article_id,
            "--representation",
            "content",
            "--summary-json",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "inspect",
            "--article-id",
            article_id,
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    artifact_types = {item["artifact_type"] for item in payload["local_artifacts"]}
    assert {"article_html", "article_text", "article_json"}.issubset(artifact_types)
    assert payload["meta_sidecar_path"] is not None
    assert "/publisher/data/" in payload["meta_sidecar_path"]
    assert payload["meta_sidecar_path"].endswith("/meta.json")


def test_cli_cache_rebuild_layout_summary_json(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    imported = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )
    assert imported.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "cache",
            "rebuild-layout",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["stage"] == "cache_rebuild_layout"
    assert payload["status"] == "ok"


def test_cli_cache_rebuild_layout_repair_metadata_counts(tmp_path):
    runner = CliRunner()
    dataset = FIXTURES / "nyt_archive_sample.json"
    imported = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "articles",
            "import-history",
            "--dataset",
            str(dataset),
            "--adapter",
            "nyt_archive",
        ],
    )
    assert imported.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "--project-root",
            str(tmp_path),
            "cache",
            "rebuild-layout",
            "--repair-metadata",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    repair = payload["metadata_repair"]
    assert "rows_scanned" in repair
    assert "rows_repaired_source_domain" in repair
    assert "rows_repaired_section" in repair
    assert "rows_repaired_byline" in repair
    assert "rows_unchanged" in repair
    assert "rows_skipped_unrepairable" in repair
