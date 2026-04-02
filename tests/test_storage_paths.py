from datetime import date

from py_news.config import load_config
from py_news.storage.paths import (
    derive_publisher_slug,
    normalized_artifact_path,
    provider_full_index_dir_path,
    publisher_article_artifact_path,
    publisher_article_meta_path,
    publisher_storage_article_dir_path,
    slugify,
)


def test_slugify_is_deterministic():
    assert slugify("NY Times / U.S.") == "ny-times-u-s"
    assert slugify("") == "unknown"


def test_publisher_slug_derivation_order():
    assert (
        derive_publisher_slug(source_domain="WWW.NYTIMES.COM", source_name="New York Times", provider="gdelt_recent")
        == "www-nytimes-com"
    )
    assert derive_publisher_slug(source_domain=None, source_name="New York Times", provider="gdelt_recent") == "new-york-times"
    assert derive_publisher_slug(source_domain=None, source_name=None, provider="gdelt_recent") == "gdelt-recent"


def test_storage_paths_are_deterministic(tmp_path):
    config = load_config(cache_root=tmp_path / ".news_cache")

    assert normalized_artifact_path(config, "source_catalog") == config.project_root / "refdata" / "normalized" / "source_catalog.parquet"
    assert publisher_article_artifact_path(
        config,
        publisher_slug="www.nytimes.com",
        published_at=date(2026, 3, 1),
        article_id="ABC 123",
        extension="html",
    ) == (
        tmp_path
        / ".news_cache"
        / "publisher"
        / "data"
        / "www-nytimes-com"
        / "2026"
        / "03"
        / "abc-123"
        / "article.html"
    )
    assert publisher_article_meta_path(
        config,
        publisher_slug="www.nytimes.com",
        published_at="2026-03-01",
        article_id="ABC 123",
    ).name == "meta.json"
    assert publisher_storage_article_dir_path(
        config,
        publisher_slug="www.nytimes.com",
        published_at="2026-03-01",
        storage_article_id="ABC 123",
    ).name == "abc-123"
    assert provider_full_index_dir_path(config, provider_id="newsdata") == (
        tmp_path / ".news_cache" / "provider" / "full-index" / "newsdata"
    )
