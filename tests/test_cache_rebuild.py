from pathlib import Path

import pandas as pd

from py_news.config import load_config
from py_news.models import ARTICLE_ARTIFACT_COLUMNS, ARTICLES_COLUMNS
from py_news.pipelines.cache_rebuild import run_cache_rebuild_layout
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


def _seed_articles(config, rows):
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=rows,
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )


def _seed_artifacts(config, rows):
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_artifacts"),
        rows=rows,
        dedupe_keys=["article_id", "artifact_type", "artifact_path"],
        column_order=ARTICLE_ARTIFACT_COLUMNS,
        drop_extra_columns=False,
    )


def test_cache_rebuild_maps_two_providers_to_one_storage_when_canonical_url_matches(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)

    _seed_articles(
        config,
        [
            {
                "article_id": "art_a",
                "provider": "newsdata",
                "provider_document_id": "n1",
                "resolved_document_identity": "provider=newsdata|provider_document_id=n1",
                "source_name": "Source",
                "source_domain": "https://example.com",
                "url": "https://example.com/story-1",
                "canonical_url": "https://example.com/story-1",
                "title": "Story",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "A",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            },
            {
                "article_id": "art_b",
                "provider": "gdelt_recent",
                "provider_document_id": "g1",
                "resolved_document_identity": "provider=gdelt_recent|provider_document_id=g1",
                "source_name": "Source",
                "source_domain": "example.com",
                "url": "https://example.com/story-1",
                "canonical_url": "https://example.com/story-1",
                "title": "Story",
                "published_at": "2026-03-01T10:01:00Z",
                "language": "en",
                "section": "news",
                "byline": "B",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            },
        ],
    )

    old_txt = tmp_path / ".news_cache" / "legacy" / "art_a.txt"
    old_txt.parent.mkdir(parents=True, exist_ok=True)
    old_txt.write_text("hello", encoding="utf-8")
    _seed_artifacts(
        config,
        [
            {
                "article_id": "art_a",
                "artifact_type": "article_text",
                "artifact_path": str(old_txt),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-01",
                "exists_locally": True,
            }
        ],
    )

    summary = run_cache_rebuild_layout(config)
    assert summary["status"] == "ok"

    mapping = pd.read_parquet(normalized_artifact_path(config, "article_storage_map"))
    sid_a = mapping[mapping["article_id"] == "art_a"].iloc[0]["storage_article_id"]
    sid_b = mapping[mapping["article_id"] == "art_b"].iloc[0]["storage_article_id"]
    assert sid_a == sid_b

    storage = pd.read_parquet(normalized_artifact_path(config, "storage_articles"))
    folder = Path(storage[storage["storage_article_id"] == sid_a].iloc[0]["storage_folder_path"])
    assert (folder / "article.txt").exists()
    assert (folder / "meta.json").exists()
    artifacts = pd.read_parquet(normalized_artifact_path(config, "article_artifacts"))
    assert artifacts["storage_article_id"].isna().sum() == 0
    assert artifacts["artifact_path"].astype(str).str.contains("/publisher/data/", regex=False).all()


def test_cache_rebuild_keeps_separate_storage_when_equivalence_is_weak(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)

    _seed_articles(
        config,
        [
            {
                "article_id": "art_x",
                "provider": "newsdata",
                "provider_document_id": None,
                "resolved_document_identity": "provider=newsdata|x",
                "source_name": "S1",
                "source_domain": "x.com",
                "url": None,
                "canonical_url": None,
                "title": "X",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "A",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            },
            {
                "article_id": "art_y",
                "provider": "gdelt_recent",
                "provider_document_id": None,
                "resolved_document_identity": "provider=gdelt_recent|y",
                "source_name": "S2",
                "source_domain": "y.com",
                "url": None,
                "canonical_url": None,
                "title": "Y",
                "published_at": "2026-03-01T10:01:00Z",
                "language": "en",
                "section": "news",
                "byline": "B",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            },
        ],
    )

    run_cache_rebuild_layout(config)
    mapping = pd.read_parquet(normalized_artifact_path(config, "article_storage_map"))
    sid_x = mapping[mapping["article_id"] == "art_x"].iloc[0]["storage_article_id"]
    sid_y = mapping[mapping["article_id"] == "art_y"].iloc[0]["storage_article_id"]
    assert sid_x != sid_y


def test_cache_rebuild_cleanup_legacy_is_opt_in_and_runs_after_success(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)
    _seed_articles(
        config,
        [
            {
                "article_id": "art_cleanup",
                "provider": "newsdata",
                "provider_document_id": "n-clean",
                "resolved_document_identity": "provider=newsdata|provider_document_id=n-clean",
                "source_name": "Source",
                "source_domain": "example.com",
                "url": "https://example.com/cleanup",
                "canonical_url": "https://example.com/cleanup",
                "title": "Cleanup",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "A",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            }
        ],
    )
    legacy_dir = tmp_path / ".news_cache" / "publisher" / "legacy-slug" / "articles"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "old.txt").write_text("old", encoding="utf-8")

    summary = run_cache_rebuild_layout(config, cleanup_legacy=True)
    assert summary["verification_ok"] is True
    assert summary["canonical_artifact_safety"]["ok"] is True
    assert summary["legacy_cleanup_performed"] is True
    assert not (tmp_path / ".news_cache" / "publisher" / "legacy-slug").exists()


def test_cache_rebuild_cleanup_fails_when_canonical_safety_check_fails(monkeypatch, tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)
    _seed_articles(
        config,
        [
            {
                "article_id": "art_fail_cleanup",
                "provider": "newsdata",
                "provider_document_id": "n-fail",
                "resolved_document_identity": "provider=newsdata|provider_document_id=n-fail",
                "source_name": "Source",
                "source_domain": "example.com",
                "url": "https://example.com/fail",
                "canonical_url": "https://example.com/fail",
                "title": "Fail",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "A",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            }
        ],
    )

    from py_news import cache_layout

    def fake_safety(config):
        return {
            "ok": False,
            "rows_checked": 1,
            "null_storage_article_id_rows": 1,
            "outside_canonical_cache_rows": 0,
        }

    monkeypatch.setattr(cache_layout, "_canonical_artifact_safety_check", fake_safety)
    try:
        run_cache_rebuild_layout(config, cleanup_legacy=True)
        assert False, "expected cleanup to fail when canonical safety check fails"
    except ValueError as exc:
        assert "verification failed" in str(exc)


def test_cache_rebuild_removes_legacy_and_pytest_artifact_rows_from_canonical_index(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)
    _seed_articles(
        config,
        [
            {
                "article_id": "art_clean",
                "provider": "newsdata",
                "provider_document_id": "n-clean",
                "resolved_document_identity": "provider=newsdata|provider_document_id=n-clean",
                "source_name": "Source",
                "source_domain": "example.com",
                "url": "https://example.com/clean",
                "canonical_url": "https://example.com/clean",
                "title": "Clean",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "news",
                "byline": "A",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            }
        ],
    )
    legacy = tmp_path / ".news_cache" / "legacy" / "art_clean.txt"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text("legacy", encoding="utf-8")
    pytest_tmp = Path("/tmp/pytest-of-nick/fake_art.txt")
    _seed_artifacts(
        config,
        [
            {
                "article_id": "art_clean",
                "storage_article_id": None,
                "artifact_type": "article_text",
                "artifact_path": str(legacy),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-01",
                "exists_locally": True,
            },
            {
                "article_id": "art_clean",
                "storage_article_id": None,
                "artifact_type": "article_text",
                "artifact_path": str(pytest_tmp),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-01",
                "exists_locally": True,
            },
        ],
    )

    run_cache_rebuild_layout(config)
    artifacts = pd.read_parquet(normalized_artifact_path(config, "article_artifacts"))
    assert artifacts["storage_article_id"].isna().sum() == 0
    paths = artifacts["artifact_path"].astype(str)
    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    assert paths.map(lambda p: str(Path(p).resolve()).startswith(canonical_prefix)).all()


def test_cache_rebuild_repair_metadata_emits_machine_counts(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)
    _seed_articles(
        config,
        [
            {
                "article_id": "art_rep",
                "provider": "newsdata",
                "provider_document_id": "n-rep",
                "resolved_document_identity": "provider=newsdata|provider_document_id=n-rep",
                "source_name": "Source",
                "source_domain": "https://ktul.com/path",
                "url": "https://ktul.com/story",
                "canonical_url": "https://ktul.com/story",
                "title": "Repair",
                "published_at": "2026-03-01T10:00:00Z",
                "language": "en",
                "section": "['politics','top']",
                "byline": "['desk','wire']",
                "article_text": None,
                "summary_text": "s",
                "snippet": "sn",
                "metadata_json": None,
                "imported_at": "2026-03-01T10:00:00Z",
            }
        ],
    )
    summary = run_cache_rebuild_layout(config, repair_metadata=True)
    repair = summary["metadata_repair"]
    assert repair is not None
    assert repair["rows_scanned"] >= 1
    assert repair["rows_repaired_source_domain"] >= 1
    assert repair["rows_repaired_section"] >= 1
    assert repair["rows_repaired_byline"] >= 1
    assert "rows_unchanged" in repair
    assert "rows_skipped_unrepairable" in repair

    articles = pd.read_parquet(normalized_artifact_path(config, "articles"))
    row = articles[articles["article_id"] == "art_rep"].iloc[0]
    assert row["source_domain"] == "ktul.com"
    assert row["section"] == "politics"
    assert row["byline"] == "desk, wire"
