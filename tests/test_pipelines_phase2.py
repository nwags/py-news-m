from pathlib import Path
import json

import pandas as pd

from py_news.config import load_config
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS
from py_news.pipelines.article_import import run_article_import_history
from py_news.storage.paths import normalized_artifact_path


FIXTURES = Path(__file__).parent / "fixtures"


def test_import_history_writes_canonical_articles_and_artifacts(tmp_path):
    config = load_config(project_root=tmp_path)

    summary = run_article_import_history(
        config,
        dataset=str(FIXTURES / "articles_sample.csv"),
        adapter_name="local_tabular",
    )

    articles_path = normalized_artifact_path(config, "articles")
    artifacts_path = normalized_artifact_path(config, "article_artifacts")

    assert summary["status"] == "ok"
    assert articles_path.exists()
    assert artifacts_path.exists()

    articles_df = pd.read_parquet(articles_path)
    artifact_df = pd.read_parquet(artifacts_path)

    assert list(articles_df.columns) == ARTICLES_COLUMNS
    assert list(artifact_df.columns) == ARTICLE_ARTIFACT_COLUMNS
    assert len(articles_df) == 3
    assert len(artifact_df) == 0
    assert articles_df["article_text"].isna().any()


def test_reimport_is_idempotent_with_provider_plus_resolved_identity(tmp_path):
    config = load_config(project_root=tmp_path)

    for _ in range(2):
        run_article_import_history(
            config,
            dataset=str(FIXTURES / "articles_sample.jsonl"),
            adapter_name="local_tabular",
        )

    articles_df = pd.read_parquet(normalized_artifact_path(config, "articles"))
    assert len(articles_df) == 3
    assert articles_df[["provider", "resolved_document_identity"]].duplicated().sum() == 0


def test_import_history_nyt_archive_writes_rows_with_metadata_json(tmp_path):
    config = load_config(project_root=tmp_path)
    summary = run_article_import_history(
        config,
        dataset=str(FIXTURES / "nyt_archive_sample.json"),
        adapter_name="nyt_archive",
    )

    assert summary["status"] == "ok"
    assert summary["imported_rows"] == 2
    assert summary["skipped_rows"] == 1

    articles_df = pd.read_parquet(normalized_artifact_path(config, "articles"))
    artifacts_df = pd.read_parquet(normalized_artifact_path(config, "article_artifacts"))

    assert len(articles_df) == 2
    assert len(artifacts_df) == 0
    assert "metadata_json" in articles_df.columns
    assert articles_df["metadata_json"].notna().all()
    parsed = json.loads(articles_df.iloc[0]["metadata_json"])
    assert isinstance(parsed, dict)


def test_import_history_nyt_archive_is_idempotent(tmp_path):
    config = load_config(project_root=tmp_path)
    dataset = str(FIXTURES / "nyt_archive_sample.json")

    run_article_import_history(config, dataset=dataset, adapter_name="nyt_archive")
    run_article_import_history(config, dataset=dataset, adapter_name="nyt_archive")

    articles_df = pd.read_parquet(normalized_artifact_path(config, "articles"))
    assert len(articles_df) == 2
    assert articles_df[["provider", "resolved_document_identity"]].duplicated().sum() == 0
