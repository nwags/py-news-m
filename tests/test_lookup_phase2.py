from pathlib import Path

import pandas as pd

from py_news.config import load_config
from py_news.lookup import query_lookup_articles
from py_news.pipelines.article_import import run_article_import_history
from py_news.pipelines.lookup_refresh import run_lookup_refresh
from py_news.storage.paths import normalized_artifact_path


FIXTURES = Path(__file__).parent / "fixtures"


def test_lookup_refresh_builds_article_lookup_from_articles(tmp_path):
    config = load_config(project_root=tmp_path)
    run_article_import_history(
        config,
        dataset=str(FIXTURES / "articles_sample.parquet"),
        adapter_name="local_tabular",
    )

    summary = run_lookup_refresh(config)
    lookup_path = normalized_artifact_path(config, "local_lookup_articles")

    assert summary["status"] == "ok"
    assert lookup_path.exists()
    df = pd.read_parquet(lookup_path)
    assert len(df) == 3


def test_lookup_query_filters_return_rows(tmp_path):
    config = load_config(project_root=tmp_path)
    run_article_import_history(
        config,
        dataset=str(FIXTURES / "articles_sample.parquet"),
        adapter_name="local_tabular",
    )
    run_lookup_refresh(config)

    by_provider = query_lookup_articles(config, provider="nyt")
    assert len(by_provider) == 1

    by_domain = query_lookup_articles(config, domain="reuters.com")
    assert len(by_domain) == 1

    by_title = query_lookup_articles(config, title_contains="fallback")
    assert len(by_title) == 1

    by_range = query_lookup_articles(config, start="2024-01-03", end="2024-01-04")
    assert len(by_range) == 1

    article_id = by_provider.iloc[0]["article_id"]
    by_id = query_lookup_articles(config, article_id=article_id)
    assert len(by_id) == 1
