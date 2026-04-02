from pathlib import Path

from py_news.adapters.articles_local_tabular import LocalTabularArticleAdapter


FIXTURES = Path(__file__).parent / "fixtures"


def test_local_tabular_csv_import_maps_flexible_columns():
    adapter = LocalTabularArticleAdapter()
    records = adapter.load_articles(str(FIXTURES / "articles_sample.csv"))

    assert adapter.last_total_rows == 4
    assert adapter.last_skipped_rows == 1
    assert len(records) == 3
    assert records[0].provider == "nyt"
    assert records[0].provider_document_id == "nyt-1"
    assert records[0].article_text is None


def test_local_tabular_jsonl_import_supports_missing_full_text():
    adapter = LocalTabularArticleAdapter()
    records = adapter.load_articles(str(FIXTURES / "articles_sample.jsonl"))

    assert adapter.last_total_rows == 4
    assert adapter.last_skipped_rows == 1
    assert len(records) == 3
    assert any(record.article_text is None for record in records)


def test_local_tabular_parquet_import_supported():
    adapter = LocalTabularArticleAdapter()
    records = adapter.load_articles(str(FIXTURES / "articles_sample.parquet"))

    assert len(records) == 3
    assert records[1].provider == "reuters"
