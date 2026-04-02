from __future__ import annotations

from pathlib import Path

import pandas as pd

from py_news.config import load_config
from py_news.http import HttpClient
from py_news.models import ARTICLES_COLUMNS
from py_news.pipelines.content_fetch import run_content_fetch
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


FIXTURES = Path(__file__).parent / "fixtures"


class FakeResponse:
    def __init__(self, text: str, content_type: str = "text/html", status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}



def _seed_articles(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=rows,
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )



def _article_row(article_id: str, provider: str, identity: str, url: str | None) -> dict:
    return {
        "article_id": article_id,
        "provider": provider,
        "provider_document_id": None,
        "resolved_document_identity": identity,
        "source_name": "Example Source",
        "source_domain": "example.com",
        "url": url,
        "canonical_url": url,
        "title": "Example Title",
        "published_at": "2026-03-05T12:00:00Z",
        "language": "en",
        "section": "news",
        "byline": None,
        "article_text": None,
        "summary_text": "summary",
        "snippet": "snippet",
        "imported_at": "2026-03-05T12:10:00Z",
    }



def test_content_fetch_success_writes_artifacts_and_rows(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path)
    html = (FIXTURES / "content_html_sample.html").read_text(encoding="utf-8")

    _seed_articles(
        config,
        [
            _article_row("art_success", "gdelt_recent", "provider=gdelt_recent|url=https://example.com/a", "https://example.com/a")
        ],
    )

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse(html, content_type="text/html")

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    summary = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )

    assert summary["reason_counts"].get("success") == 1

    artifacts_path = normalized_artifact_path(config, "article_artifacts")
    artifacts_df = pd.read_parquet(artifacts_path)
    assert set(artifacts_df["artifact_type"].tolist()) == {"article_html", "article_text", "article_json"}

    text_row = artifacts_df[artifacts_df["artifact_type"] == "article_text"].iloc[0]
    text_path = Path(text_row["artifact_path"])
    assert text_path.exists()
    extracted = text_path.read_text(encoding="utf-8")
    assert "Policy Update" in extracted
    assert "console.log" not in extracted



def test_content_fetch_skips_missing_url_and_non_html(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path)

    _seed_articles(
        config,
        [
            _article_row("art_missing", "gdelt_recent", "provider=gdelt_recent|id=missing", None),
            _article_row("art_binary", "gdelt_recent", "provider=gdelt_recent|id=binary", "https://example.com/binary"),
        ],
    )

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse("%PDF-1.7", content_type="application/pdf")

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    summary = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )

    assert summary["reason_counts"].get("missing_url") == 1
    assert summary["reason_counts"].get("non_html_response") == 1



def test_content_fetch_already_fetched_and_refetch(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path)
    html = (FIXTURES / "content_html_sample.html").read_text(encoding="utf-8")

    _seed_articles(
        config,
        [
            _article_row("art_refetch", "gdelt_recent", "provider=gdelt_recent|id=refetch", "https://example.com/refetch")
        ],
    )

    calls = {"count": 0}

    def fake_request_response(self, method, url, params=None, headers=None):
        calls["count"] += 1
        return FakeResponse(html, content_type="text/html")

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    first = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )
    second = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )
    third = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=True,
    )

    assert first["reason_counts"].get("success") == 1
    assert second["reason_counts"].get("already_fetched") == 1
    assert third["reason_counts"].get("success") == 1
    assert calls["count"] == 2



def test_content_fetch_failure_does_not_invalidate_articles(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path)
    rows = [
        _article_row("art_keep", "gdelt_recent", "provider=gdelt_recent|id=keep", "https://example.com/keep")
    ]
    _seed_articles(config, rows)

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse("", content_type="text/html")

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    summary = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )
    assert summary["reason_counts"].get("empty_body") == 1

    articles_df = pd.read_parquet(normalized_artifact_path(config, "articles"))
    assert len(articles_df) == 1
    assert articles_df.iloc[0]["article_id"] == "art_keep"


def test_unusable_html_records_html_artifact_and_empty_body(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_unusable", "gdelt_recent", "provider=gdelt_recent|id=unusable", "https://example.com/u")],
    )

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse("<html><body><script>hidden()</script></body></html>", content_type="text/html")

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)

    summary = run_content_fetch(
        config,
        provider="gdelt_recent",
        article_id=None,
        start=None,
        end=None,
        limit=10,
        refetch=False,
    )

    assert summary["reason_counts"].get("parse_failure") == 1
    artifacts_df = pd.read_parquet(normalized_artifact_path(config, "article_artifacts"))
    assert set(artifacts_df["artifact_type"].tolist()) == {"article_html"}
