import json
from datetime import date
from pathlib import Path

import pandas as pd

from py_news.config import load_config
from py_news.http import HttpClient
from py_news.pipelines.article_backfill import run_article_backfill
from py_news.storage.paths import normalized_artifact_path


FIXTURES = Path(__file__).parent / "fixtures"


def test_backfill_is_idempotent_and_raw_path_deterministic(monkeypatch, tmp_path):
    payload = json.loads((FIXTURES / "gdelt_recent_sample.json").read_text(encoding="utf-8"))

    def fake_request_json(self, method, url, params=None, headers=None):
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)

    config = load_config(project_root=tmp_path)
    summary1 = run_article_backfill(
        config,
        provider="gdelt_recent",
        window_date=date(2026, 3, 5),
        window_key="1d",
        query="fed",
        max_records=20,
    )
    summary2 = run_article_backfill(
        config,
        provider="gdelt_recent",
        window_date=date(2026, 3, 5),
        window_key="1d",
        query="fed",
        max_records=20,
    )

    assert summary1["raw_payload_path"] == summary2["raw_payload_path"]

    articles_df = pd.read_parquet(normalized_artifact_path(config, "articles"))
    assert len(articles_df) == 2
    assert articles_df[["provider", "resolved_document_identity"]].duplicated().sum() == 0


def test_newsdata_backfill_clamps_max_records(monkeypatch, tmp_path):
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
    seen = {}

    def fake_request_json(self, method, url, params=None, headers=None):
        seen["params"] = params
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")

    config = load_config(project_root=tmp_path)
    summary = run_article_backfill(
        config,
        provider="newsdata",
        window_date=date(2026, 3, 5),
        window_key="1d",
        query="fed",
        max_records=50,
    )
    assert summary["requested_max_records"] == 50
    assert summary["effective_max_records"] == 10
    assert summary["max_records_clamped"] is True
    assert seen["params"]["size"] == 10
    assert seen["params"]["apikey"] == "test-key"
