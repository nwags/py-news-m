from pathlib import Path
import json

from py_news.adapters.articles_nyt_archive import NytArchiveArticleAdapter


FIXTURES = Path(__file__).parent / "fixtures"


def test_nyt_archive_standard_shape_imports_metadata_first_rows():
    adapter = NytArchiveArticleAdapter()
    records = adapter.load_articles(str(FIXTURES / "nyt_archive_sample.json"))

    assert adapter.last_total_rows == 3
    assert adapter.last_skipped_rows == 1
    assert len(records) == 2
    assert all(record.provider == "nyt_archive" for record in records)
    assert all(record.article_text is None for record in records)


def test_nyt_archive_fallback_mappings_and_defaults():
    adapter = NytArchiveArticleAdapter()
    records = adapter.load_articles(str(FIXTURES / "nyt_archive_sample.json"))
    fallback = records[1]

    assert fallback.provider_document_id == "nyt://article/22222222-2222-2222-2222-222222222222"
    assert fallback.title == "Election Night Brings Coalition Talks"
    assert fallback.section == "Foreign"
    assert fallback.byline is None
    assert fallback.source_name == "New York Times"
    assert fallback.source_domain == "www.nytimes.com"
    assert fallback.summary_text is None
    assert fallback.snippet == "Leaders met overnight to discuss coalition options."
    assert isinstance(fallback.metadata_json, dict)
    assert fallback.metadata_json.get("document_type") == "article"


def test_nyt_archive_supports_direct_docs_list_shape(tmp_path):
    payload = [
        {
            "_id": "nyt://article/abc",
            "web_url": "https://www.nytimes.com/2024/02/01/us/sample.html",
            "headline": {"main": "Sample Headline"},
            "pub_date": "2024-02-01T10:00:00Z",
        }
    ]
    path = tmp_path / "nyt_docs_list.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    adapter = NytArchiveArticleAdapter()
    records = adapter.load_articles(str(path))

    assert len(records) == 1
    assert records[0].provider == "nyt_archive"
    assert records[0].title == "Sample Headline"


def test_nyt_archive_supports_top_level_docs_shape(tmp_path):
    payload = {
        "docs": [
            {
                "_id": "nyt://article/top-docs",
                "web_url": "https://www.nytimes.com/2024/02/02/us/another.html",
                "headline": {"main": "Top Docs Shape"},
                "pub_date": "2024-02-02T08:00:00Z",
            }
        ]
    }
    path = tmp_path / "nyt_top_docs.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    adapter = NytArchiveArticleAdapter()
    records = adapter.load_articles(str(path))

    assert len(records) == 1
    assert records[0].provider_document_id == "nyt://article/top-docs"
