import pandas as pd

from py_news.config import load_config
from py_news.models import ARTICLES_COLUMNS
from py_news.providers import refresh_provider_registry
from py_news.resolution import resolve_article
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


def _seed_article(config) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=[
            {
                "article_id": "art_schema",
                "provider": "gdelt_recent",
                "provider_document_id": "doc-schema",
                "resolved_document_identity": "provider=gdelt_recent|provider_document_id=doc-schema",
                "source_name": "Reuters",
                "source_domain": "reuters.com",
                "url": "https://example.com/schema",
                "canonical_url": "https://example.com/schema",
                "title": "Schema test",
                "published_at": "2026-03-05T01:00:00Z",
                "language": "en",
                "section": "news",
                "byline": None,
                "article_text": None,
                "summary_text": "summary",
                "snippet": "snippet",
                "metadata_json": None,
                "imported_at": "2026-03-05T02:00:00Z",
            }
        ],
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )


def test_resolution_event_additive_canonical_fields(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    refresh_provider_registry(config)
    _seed_article(config)

    result = resolve_article(config, article_id="art_schema", representation="content", allow_remote=False)
    assert result.resolved is False

    events = pd.read_parquet(normalized_artifact_path(config, "resolution_events"))
    row = events.iloc[-1].to_dict()
    for key in (
        "event_at",
        "domain",
        "content_domain",
        "canonical_key",
        "resolution_mode",
        "provider_requested",
        "provider_used",
        "method_used",
        "served_from",
        "remote_attempted",
        "success",
        "reason_code",
        "persisted_locally",
    ):
        assert key in row
    assert row["domain"] == "news"
    assert row["content_domain"] == "article"
    assert row["canonical_key"] == "article:art_schema"
    assert row["resolution_mode"] == "local_only"
    assert row["served_from"] in {
        "local_cache",
        "local_normalized",
        "remote_then_persisted",
        "remote_ephemeral",
        "none",
    }
