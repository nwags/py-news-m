from pathlib import Path

import pandas as pd
import json

from py_news.config import load_config
from py_news.http import HttpClient
from py_news.models import ARTICLES_COLUMNS
from py_news.pipelines.article_import import run_article_import_history
from py_news.providers import refresh_provider_registry
from py_news.resolution import resolve_article
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows

FIXTURES = Path(__file__).parent / "fixtures"


def _seed_article(config, row: dict) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[row],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )


def test_resolution_local_first_and_provenance_append(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)

    run_article_import_history(
        config,
        dataset=str(FIXTURES / "nyt_archive_sample.json"),
        adapter_name="nyt_archive",
    )

    result = resolve_article(config, article_id="art_fc7e09459882abbe", representation="content", allow_remote=False)
    assert result.resolved is False
    assert result.reason_code == "content_missing_local"

    events = pd.read_parquet(normalized_artifact_path(config, "resolution_events"))
    assert len(events) >= 1
    assert events.iloc[-1]["reason_code"] == "content_missing_local"


def test_resolution_remote_success_writes_artifacts_sidecar_and_event(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)

    _seed_article(
        config,
        {
            "article_id": "art_remote",
            "provider": "gdelt_recent",
            "provider_document_id": "doc",
            "resolved_document_identity": "provider=gdelt_recent|provider_document_id=doc",
            "source_name": "Reuters",
            "source_domain": "reuters.com",
            "url": "https://example.com/reuters",
            "canonical_url": "https://example.com/reuters",
            "title": "Title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    class FakeResponse:
        text = "<html><body><h1>Hello</h1><p>Readable body</p></body></html>"
        status_code = 200
        headers = {"Content-Type": "text/html"}

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse()

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    result = resolve_article(config, article_id="art_remote", representation="content", allow_remote=True)
    assert result.resolved is True
    assert result.reason_code == "success"
    assert result.meta_sidecar_path is not None
    assert Path(result.meta_sidecar_path).exists()

    artifacts = pd.read_parquet(normalized_artifact_path(config, "article_artifacts"))
    assert set(artifacts["artifact_type"].tolist()) == {"article_html", "article_text", "article_json"}

    events = pd.read_parquet(normalized_artifact_path(config, "resolution_events"))
    assert events.iloc[-1]["success"] == True


def test_resolution_failure_writes_event_without_artifact(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)

    _seed_article(
        config,
        {
            "article_id": "art_fail",
            "provider": "gdelt_recent",
            "provider_document_id": "doc-fail",
            "resolved_document_identity": "provider=gdelt_recent|provider_document_id=doc-fail",
            "source_name": "Reuters",
            "source_domain": "reuters.com",
            "url": "",
            "canonical_url": "",
            "title": "Title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    result = resolve_article(config, article_id="art_fail", representation="content", allow_remote=True)
    assert result.resolved is False

    events = pd.read_parquet(normalized_artifact_path(config, "resolution_events"))
    assert events.iloc[-1]["success"] == False
    assert pd.isna(events.iloc[-1]["artifact_path"]) or events.iloc[-1]["artifact_path"] == ""


def test_metadata_resolution_local_only_does_not_call_remote(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_local",
            "provider": "newsdata",
            "provider_document_id": "n1",
            "resolved_document_identity": "provider=newsdata|provider_document_id=n1",
            "source_name": "Source",
            "source_domain": "example.com",
            "url": "https://example.com/a",
            "canonical_url": "https://example.com/a",
            "title": "Original title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "orig summary",
            "snippet": "orig snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    def fail_request_json(self, method, url, params=None, headers=None):  # pragma: no cover
        raise AssertionError("request_json should not be called for local-only metadata")

    monkeypatch.setattr(HttpClient, "request_json", fail_request_json)

    result = resolve_article(config, article_id="art_meta_local", representation="metadata", allow_remote=False)
    assert result.resolved is True
    assert result.reason_code == "local_metadata_hit"


def test_metadata_resolution_newsdata_provider_api_lookup_success(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_refresh",
            "provider": "newsdata",
            "provider_document_id": "doc-123",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-123",
            "source_name": "Old Source",
            "source_domain": "old.example.com",
            "url": "https://example.com/old",
            "canonical_url": "https://example.com/old",
            "title": "Old title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "old summary",
            "snippet": "old snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    payload = {
        "results": [
            {
                "article_id": "doc-123",
                "source_name": "NewsData Source",
                "source_url": "https://news.example.com",
                "link": "https://news.example.com/story-1",
                "title": "Updated title",
                "pubDate": "2026-03-06T05:00:00Z",
                "language": "en",
                "category": "business",
                "creator": "Reporter",
                "description": "Updated summary",
                "content": "Updated snippet and text",
            }
        ]
    }

    seen = {}

    def fake_request_json(self, method, url, params=None, headers=None):
        seen["params"] = params
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    result = resolve_article(config, article_id="art_meta_refresh", representation="metadata", allow_remote=True)
    assert result.resolved is True
    assert result.reason_code == "metadata_refreshed"

    articles = pd.read_parquet(normalized_artifact_path(config, "articles"))
    row = articles[articles["article_id"] == "art_meta_refresh"].iloc[0]
    assert row["title"] == "Updated title"
    assert row["source_domain"] == "news.example.com"
    assert "provider_native" in str(row["metadata_json"])
    assert seen["params"]["apikey"] == "test-key"
    assert seen["params"]["size"] == 10


def test_metadata_resolution_newsdata_no_match(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_no_match",
            "provider": "newsdata",
            "provider_document_id": "doc-xyz",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-xyz",
            "source_name": "Old Source",
            "source_domain": "old.example.com",
            "url": "https://example.com/old2",
            "canonical_url": "https://example.com/old2",
            "title": "Old title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "old summary",
            "snippet": "old snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    def fake_request_json(self, method, url, params=None, headers=None):
        return {"results": [{"article_id": "different", "title": "Another"}]}

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    result = resolve_article(config, article_id="art_meta_no_match", representation="metadata", allow_remote=True)
    assert result.resolved is False
    assert result.reason_code == "no_match"


def test_metadata_resolution_newsdata_missing_auth(monkeypatch, tmp_path):
    monkeypatch.delenv("NEWSDATA_API_KEY", raising=False)
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_auth_missing",
            "provider": "newsdata",
            "provider_document_id": "doc-auth",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-auth",
            "source_name": "Source",
            "source_domain": "example.com",
            "url": "https://example.com/auth",
            "canonical_url": "https://example.com/auth",
            "title": "Auth title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    def fail_request_json(self, method, url, params=None, headers=None):  # pragma: no cover
        raise AssertionError("request_json should not be called when auth is missing")

    monkeypatch.setattr(HttpClient, "request_json", fail_request_json)
    result = resolve_article(config, article_id="art_meta_auth_missing", representation="metadata", allow_remote=True)
    assert result.resolved is False
    assert result.reason_code == "auth_not_configured"
    assert result.auth_env_var == "NEWSDATA_API_KEY"
    assert result.auth_configured is False


def test_metadata_resolution_newsdata_401_maps_to_auth_reason(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "present-key")
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_auth_401",
            "provider": "newsdata",
            "provider_document_id": "doc-auth-401",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-auth-401",
            "source_name": "Source",
            "source_domain": "example.com",
            "url": "https://example.com/auth401",
            "canonical_url": "https://example.com/auth401",
            "title": "Auth 401",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    from py_news.http import HttpFailure

    def fake_request_json(self, method, url, params=None, headers=None):
        raise HttpFailure(method=method, url=url, reason="API key is missing", attempts=1, status_code=401, is_transient=False)

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    result = resolve_article(config, article_id="art_meta_auth_401", representation="metadata", allow_remote=True)
    assert result.resolved is False
    assert result.reason_code == "auth_invalid_or_missing"
    assert result.auth_env_var == "NEWSDATA_API_KEY"
    assert result.auth_configured is True


def test_metadata_resolution_newsdata_normalizes_scalar_fields(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_norm",
            "provider": "newsdata",
            "provider_document_id": "doc-norm",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-norm",
            "source_name": "Old Source",
            "source_domain": "old.example.com",
            "url": "https://example.com/old-norm",
            "canonical_url": "https://example.com/old-norm",
            "title": "Old title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "old summary",
            "snippet": "old snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    payload = {
        "results": [
            {
                "article_id": "doc-norm",
                "source_name": "NewsData Source",
                "source_url": "https://ktul.com/politics/story",
                "link": "https://ktul.com/news/story-1",
                "title": "Updated title",
                "pubDate": "2026-03-06T05:00:00Z",
                "language": "en",
                "category": ["politics", "top"],
                "creator": ["the national news desk", "wire"],
                "description": "Updated summary",
                "content": "Updated snippet and text",
            }
        ]
    }

    def fake_request_json(self, method, url, params=None, headers=None):
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    result = resolve_article(config, article_id="art_meta_norm", representation="metadata", allow_remote=True)
    assert result.resolved is True
    articles = pd.read_parquet(normalized_artifact_path(config, "articles"))
    row = articles[articles["article_id"] == "art_meta_norm"].iloc[0]
    assert row["source_domain"] == "ktul.com"
    assert row["section"] == "politics"
    assert row["byline"] == "the national news desk, wire"


def test_metadata_resolution_non_newsdata_does_not_attempt_provider_api_lookup(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(
        config,
        {
            "article_id": "art_meta_gdelt",
            "provider": "gdelt_recent",
            "provider_document_id": "g1",
            "resolved_document_identity": "provider=gdelt_recent|provider_document_id=g1",
            "source_name": "GDELT",
            "source_domain": "gdeltproject.org",
            "url": "https://example.com/g1",
            "canonical_url": "https://example.com/g1",
            "title": "Title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": json.dumps({"provider_native": {"description": "native"}}),
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    def fail_request_json(self, method, url, params=None, headers=None):  # pragma: no cover
        raise AssertionError("request_json should not be called for gdelt metadata refresh")

    monkeypatch.setattr(HttpClient, "request_json", fail_request_json)
    result = resolve_article(config, article_id="art_meta_gdelt", representation="metadata", allow_remote=True)
    assert result.resolved is True
    assert result.reason_code == "metadata_refresh_not_supported"


def test_content_direct_url_strategy_respects_provider_rule(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    registry_path = normalized_artifact_path(config, "provider_registry")
    registry_df = pd.read_parquet(registry_path)
    registry_df.loc[registry_df["provider_id"] == "newsdata", "direct_url_allowed"] = False
    registry_df.to_parquet(registry_path, index=False)

    _seed_article(
        config,
        {
            "article_id": "art_direct_rule",
            "provider": "newsdata",
            "provider_document_id": "doc-dir",
            "resolved_document_identity": "provider=newsdata|provider_document_id=doc-dir",
            "source_name": "NewsData",
            "source_domain": "example.com",
            "url": "https://example.com/direct",
            "canonical_url": "https://example.com/direct",
            "title": "Title",
            "published_at": "2026-03-05T01:00:00Z",
            "language": "en",
            "section": "news",
            "byline": None,
            "article_text": None,
            "summary_text": "summary",
            "snippet": "snippet",
            "metadata_json": None,
            "imported_at": "2026-03-05T02:00:00Z",
        },
    )

    def fake_request_json(self, method, url, params=None, headers=None):
        return {"results": []}

    def fail_request_response(self, method, url, params=None, headers=None):  # pragma: no cover
        raise AssertionError("direct URL fetch should not be attempted when disallowed")

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    monkeypatch.setattr(HttpClient, "request_response", fail_request_response)

    result = resolve_article(config, article_id="art_direct_rule", representation="content", allow_remote=True)
    assert result.resolved is False
    assert result.reason_code in {"no_match", "direct_url_not_allowed"}
