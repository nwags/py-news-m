import json
from datetime import date
from pathlib import Path

from py_news.adapters.articles_gdelt_recent import GdeltRecentArticleAdapter
from py_news.config import ensure_runtime_dirs, load_config
from py_news.http import HttpClient


FIXTURES = Path(__file__).parent / "fixtures"


def test_gdelt_recent_normalizes_metadata_and_persists_raw_payload(monkeypatch, tmp_path):
    payload = json.loads((FIXTURES / "gdelt_recent_sample.json").read_text(encoding="utf-8"))
    config = load_config(project_root=tmp_path)
    ensure_runtime_dirs(config)

    def fake_request_json(self, method, url, params=None, headers=None):
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)

    adapter = GdeltRecentArticleAdapter(config)
    result = adapter.fetch_window(window_date=date(2026, 3, 5), window_key="1d", query="fed", max_records=20)

    assert result.provider == "gdelt_recent"
    assert result.fetched_rows == 3
    assert result.normalized_rows == 2
    assert result.skipped_rows == 1
    assert result.request_id.startswith("req_")
    assert result.raw_payload_path == ""
    assert all(record.article_text is None for record in result.articles)
