from datetime import date

from py_news.adapters.articles_newsdata import NewsDataRecentArticleAdapter
from py_news.config import load_config
from py_news.http import HttpClient


def test_newsdata_recent_adapter_normalizes_rows(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    payload = {
        "results": [
            {
                "article_id": "n1",
                "source_id": "cnn",
                "link": "https://example.com/n1",
                "title": "Title",
                "pubDate": "2026-03-05T10:00:00Z",
                "description": "Summary",
                "content": "Snippet",
                "language": "en",
                "source_url": "https://ktul.com/path",
                "category": ["politics", "top"],
                "creator": ["the national news desk", "wire"],
            },
            {"article_id": "", "title": "   "},
        ]
    }

    seen = {}

    def fake_request_json(self, method, url, params=None, headers=None):
        seen["params"] = params
        return payload

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)

    config = load_config(cache_root=tmp_path / ".news_cache")
    adapter = NewsDataRecentArticleAdapter(config)
    result = adapter.fetch_window(window_date=date(2026, 3, 5), window_key="1d", query="fed", max_records=20)

    assert result.provider == "newsdata"
    assert result.fetched_rows == 2
    assert result.normalized_rows == 1
    assert result.skipped_rows == 1
    assert result.requested_max_records == 20
    assert result.effective_max_records == 10
    assert result.max_records_clamped is True
    assert seen["params"]["apikey"] == "test-key"
    assert seen["params"]["size"] == 10
    record = result.articles[0]
    assert record.source_domain == "ktul.com"
    assert record.section == "politics"
    assert record.byline == "the national news desk, wire"
