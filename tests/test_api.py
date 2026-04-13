from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
import pandas as pd

from py_news.api.app import create_app
from py_news.config import load_config
from py_news.http import HttpClient
from py_news.models import (
    ARTICLE_ARTIFACT_COLUMNS,
    ARTICLES_COLUMNS,
    ARTICLE_STORAGE_MAP_COLUMNS,
    AUGMENTATION_ARTIFACT_COLUMNS,
    STORAGE_ARTICLES_COLUMNS,
)
from py_news.providers import refresh_provider_registry
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


def _seed_articles(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "articles"),
        rows=rows,
        dedupe_keys=["provider", "resolved_document_identity"],
        column_order=ARTICLES_COLUMNS,
    )


def _seed_article_artifacts(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_artifacts"),
        rows=rows,
        dedupe_keys=["article_id", "artifact_type", "artifact_path"],
        column_order=ARTICLE_ARTIFACT_COLUMNS,
    )


def _seed_storage_map(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_storage_map"),
        rows=rows,
        dedupe_keys=["article_id"],
        column_order=ARTICLE_STORAGE_MAP_COLUMNS,
    )


def _seed_storage_articles(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "storage_articles"),
        rows=rows,
        dedupe_keys=["storage_article_id"],
        column_order=STORAGE_ARTICLES_COLUMNS,
    )


def _seed_augmentation_artifacts(config, rows: list[dict]) -> None:
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "augmentation_artifacts"),
        rows=rows,
        dedupe_keys=["canonical_key", "augmentation_type", "artifact_locator"],
        column_order=AUGMENTATION_ARTIFACT_COLUMNS,
    )


def _article_row(
    article_id: str,
    provider: str,
    identity: str,
    source_name: str,
    source_domain: str,
    title: str,
    published_at: str,
) -> dict:
    return {
        "article_id": article_id,
        "provider": provider,
        "provider_document_id": None,
        "resolved_document_identity": identity,
        "source_name": source_name,
        "source_domain": source_domain,
        "url": f"https://{source_domain}/{article_id}",
        "canonical_url": f"https://{source_domain}/{article_id}",
        "title": title,
        "published_at": published_at,
        "language": "en",
        "section": "news",
        "byline": None,
        "article_text": None,
        "summary_text": "summary",
        "snippet": "snippet",
        "imported_at": "2026-03-10T01:00:00Z",
    }


def test_health_endpoint() -> None:
    client = TestClient(create_app())
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "py-news-m"}


def test_get_articles_returns_seeded_rows_and_supports_limit_offset(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [
            _article_row("art_1", "nyt", "provider=nyt|id=1", "New York Times", "nytimes.com", "Fed Watch", "2026-03-03T05:00:00Z"),
            _article_row("art_2", "reuters", "provider=reuters|id=2", "Reuters", "reuters.com", "Market Rally", "2026-03-02T05:00:00Z"),
            _article_row("art_3", "gdelt_recent", "provider=gdelt_recent|id=3", "Example", "example.com", "Older Story", "2026-03-01T05:00:00Z"),
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles", params={"limit": 2, "offset": 1})
    assert response.status_code == 200

    payload = response.json()
    assert payload["count"] == 2
    assert payload["limit"] == 2
    assert payload["offset"] == 1
    assert [item["article_id"] for item in payload["items"]] == ["art_2", "art_3"]


def test_get_articles_filters_provider_domain_and_title(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [
            _article_row("art_a", "nyt", "provider=nyt|id=a", "New York Times", "nytimes.com", "Fed signals pause", "2026-03-03T05:00:00Z"),
            _article_row("art_b", "reuters", "provider=reuters|id=b", "Reuters", "reuters.com", "Fed comments move markets", "2026-03-02T05:00:00Z"),
            _article_row("art_c", "reuters", "provider=reuters|id=c", "Reuters", "reuters.com", "Commodities edge lower", "2026-03-01T05:00:00Z"),
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    by_provider = client.get("/articles", params={"provider": "reuters"})
    assert by_provider.status_code == 200
    assert by_provider.json()["count"] == 2

    by_domain = client.get("/articles", params={"domain": "nytimes.com"})
    assert by_domain.status_code == 200
    assert by_domain.json()["count"] == 1

    by_title = client.get("/articles", params={"title_contains": "move markets"})
    assert by_title.status_code == 200
    assert by_title.json()["count"] == 1
    assert by_title.json()["items"][0]["article_id"] == "art_b"


def test_get_articles_filters_date_range_inclusive(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [
            _article_row("art_d1", "nyt", "provider=nyt|id=d1", "NYT", "nytimes.com", "Story 1", "2026-03-01T01:00:00Z"),
            _article_row("art_d2", "nyt", "provider=nyt|id=d2", "NYT", "nytimes.com", "Story 2", "2026-03-02T12:00:00Z"),
            _article_row("art_d3", "nyt", "provider=nyt|id=d3", "NYT", "nytimes.com", "Story 3", "2026-03-03T23:00:00Z"),
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles", params={"start": "2026-03-02", "end": "2026-03-03"})
    assert response.status_code == 200
    payload = response.json()
    assert [item["article_id"] for item in payload["items"]] == ["art_d3", "art_d2"]


def test_get_article_by_id_and_404(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_known", "nyt", "provider=nyt|id=known", "NYT", "nytimes.com", "Known Story", "2026-03-04T01:00:00Z")],
    )
    client = TestClient(create_app(project_root=tmp_path))

    found = client.get("/articles/art_known")
    assert found.status_code == 200
    assert found.json()["article_id"] == "art_known"
    assert found.json()["resolution_source"] == "local_metadata"
    assert found.json()["resolution_strategy"] == "local_metadata"
    assert found.json()["resolution_reason_code"] == "local_metadata_hit"
    assert found.json()["resolution_remote_attempted"] is False
    assert found.json()["local_write_performed"] is False
    assert found.json()["augmentation_meta"]["augmentation_available"] is False
    assert found.json()["augmentation_meta"]["augmentation_types_present"] == []

    missing = client.get("/articles/does_not_exist")
    assert missing.status_code == 404


def test_get_article_resolve_remote_true_refreshes_metadata(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NEWSDATA_API_KEY", "test-key")
    config = load_config(project_root=tmp_path)
    refresh_provider_registry(config)
    _seed_articles(
        config,
        [
            _article_row(
                "art_meta_api",
                "newsdata",
                "provider=newsdata|provider_document_id=doc-api",
                "Old Source",
                "old.example.com",
                "Old title",
                "2026-03-04T01:00:00Z",
            )
        ],
    )
    # align provider_document_id with identity for deterministic provider_api_lookup match
    articles_path = normalized_artifact_path(config, "articles")
    df = pd.read_parquet(articles_path)
    df.loc[df["article_id"] == "art_meta_api", "provider_document_id"] = "doc-api"
    df.to_parquet(articles_path, index=False)

    def fake_request_json(self, method, url, params=None, headers=None):
        return {
            "results": [
                {
                    "article_id": "doc-api",
                    "source_name": "Refreshed Source",
                    "source_url": "https://fresh.example.com",
                    "link": "https://fresh.example.com/story",
                    "title": "Refreshed title",
                    "pubDate": "2026-03-05T10:00:00Z",
                    "description": "fresh summary",
                    "content": "fresh snippet",
                    "language": "en",
                }
            ]
        }

    monkeypatch.setattr(HttpClient, "request_json", fake_request_json)
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles/art_meta_api", params={"resolve_remote": "true"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "Refreshed title"
    assert payload["resolution_reason_code"] == "metadata_refreshed"
    assert payload["resolution_source"] == "provider_api"
    assert payload["resolution_strategy"] == "provider_api_lookup"
    assert payload["resolution_remote_attempted"] is True


def test_get_article_and_content_include_additive_augmentation_meta_when_present(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [
            {
                **_article_row(
                    "art_augmented",
                    "newsdata",
                    "provider=newsdata|provider_document_id=doc-aug",
                    "Source",
                    "example.com",
                    "Augmented Story",
                    "2026-03-10T01:00:00Z",
                ),
                "article_text": "Full text for augmentation freshness checks.",
            }
        ],
    )
    _seed_augmentation_artifacts(
        config,
        [
            {
                "domain": "news",
                "resource_family": "articles",
                "canonical_key": "article:art_augmented",
                "augmentation_type": "entity_tagging",
                "artifact_locator": ".news_cache/augmentations/article:art_augmented/entity_tagging.json",
                "source_text_version": "sha256:stale",
                "producer_name": "entity-v1",
                "event_at": "2026-04-08T17:04:00Z",
                "success": True,
            }
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    detail = client.get("/articles/art_augmented")
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["augmentation_meta"]["augmentation_available"] is True
    assert detail_payload["augmentation_meta"]["augmentation_types_present"] == ["entity_tagging"]
    assert detail_payload["augmentation_meta"]["inspect_path"] == "/articles/art_augmented/augmentations"

    content = client.get("/articles/art_augmented/content")
    assert content.status_code == 200
    content_payload = content.json()
    assert content_payload["augmentation_meta"]["augmentation_available"] is True
    assert content_payload["augmentation_meta"]["augmentation_types_present"] == ["entity_tagging"]
    assert isinstance(content_payload["augmentation_meta"]["augmentation_stale"], bool)


def test_get_article_resolve_remote_missing_auth_surfaces_diagnostics(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("NEWSDATA_API_KEY", raising=False)
    config = load_config(project_root=tmp_path)
    refresh_provider_registry(config)
    _seed_articles(
        config,
        [
            _article_row(
                "art_meta_missing_auth",
                "newsdata",
                "provider=newsdata|provider_document_id=doc-auth",
                "Source",
                "example.com",
                "Title",
                "2026-03-04T01:00:00Z",
            )
        ],
    )
    articles_path = normalized_artifact_path(config, "articles")
    df = pd.read_parquet(articles_path)
    df.loc[df["article_id"] == "art_meta_missing_auth", "provider_document_id"] = "doc-auth"
    df.to_parquet(articles_path, index=False)

    client = TestClient(create_app(project_root=tmp_path))
    response = client.get("/articles/art_meta_missing_auth", params={"resolve_remote": "true"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["resolution_reason_code"] == "auth_not_configured"
    assert payload["resolution_auth_env_var"] == "NEWSDATA_API_KEY"
    assert payload["resolution_auth_configured"] is False


def test_get_article_content_returns_local_artifacts_and_preferred_text(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_content", "reuters", "provider=reuters|id=content", "Reuters", "reuters.com", "Content Story", "2026-03-05T01:00:00Z")],
    )

    text_path = tmp_path / ".news_cache" / "articles" / "parsed" / "provider=reuters" / "source=reuters" / "date=2026-03-05" / "art_content.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text("Readable parsed content", encoding="utf-8")

    html_path = tmp_path / ".news_cache" / "articles" / "raw" / "provider=reuters" / "source=reuters" / "date=2026-03-05" / "art_content.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text("<html><body>Readable parsed content</body></html>", encoding="utf-8")

    source_window_path = tmp_path / ".news_cache" / "source_windows" / "raw" / "provider=gdelt_recent" / "date=2026-03-05" / "window=1d" / "abc.json"
    source_window_path.parent.mkdir(parents=True, exist_ok=True)
    source_window_path.write_text('{"rows": []}', encoding="utf-8")

    _seed_article_artifacts(
        config,
        [
            {
                "article_id": "art_content",
                "artifact_type": "article_text",
                "artifact_path": str(text_path),
                "provider": "reuters",
                "source_domain": "reuters.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
            {
                "article_id": "art_content",
                "artifact_type": "article_html",
                "artifact_path": str(html_path),
                "provider": "reuters",
                "source_domain": "reuters.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
            {
                "article_id": "art_content",
                "artifact_type": "provider_raw_payload",
                "artifact_path": str(source_window_path),
                "provider": "gdelt_recent",
                "source_domain": "gdeltproject.org",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles/art_content/content")
    assert response.status_code == 200
    payload = response.json()
    assert payload["content_available"] is True
    assert payload["preferred_text"] == "Readable parsed content"
    assert {item["artifact_type"] for item in payload["artifacts"]} == {"article_html", "article_text"}


def test_get_article_content_prefers_canonical_artifacts_over_legacy_paths(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_mixed", "newsdata", "provider=newsdata|id=mixed", "Source", "example.com", "Mixed", "2026-03-05T01:00:00Z")],
    )
    canonical_dir = tmp_path / ".news_cache" / "publisher" / "data" / "example-com" / "2026" / "03" / "sto-1"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_txt = canonical_dir / "article.txt"
    canonical_txt.write_text("Canonical", encoding="utf-8")
    canonical_html = canonical_dir / "article.html"
    canonical_html.write_text("<html><body>Canonical</body></html>", encoding="utf-8")
    legacy_txt = tmp_path / ".news_cache" / "legacy" / "art_mixed.txt"
    legacy_txt.parent.mkdir(parents=True, exist_ok=True)
    legacy_txt.write_text("Legacy", encoding="utf-8")

    _seed_storage_map(
        config,
        [
            {
                "article_id": "art_mixed",
                "provider": "newsdata",
                "resolved_document_identity": "provider=newsdata|id=mixed",
                "storage_article_id": "sto-1",
                "mapping_basis": "canonical_url",
                "mapped_at": "2026-03-05T02:00:00Z",
            }
        ],
    )
    _seed_storage_articles(
        config,
        [
            {
                "storage_article_id": "sto-1",
                "publisher_slug": "example-com",
                "storage_anchor_date": "2026-03-05",
                "storage_folder_path": str(canonical_dir),
                "equivalence_basis": "canonical_url",
                "equivalence_value": "example.com/mixed",
                "created_at": "2026-03-05T02:00:00Z",
                "updated_at": "2026-03-05T02:00:00Z",
            }
        ],
    )
    _seed_article_artifacts(
        config,
        [
            {
                "article_id": "art_mixed",
                "storage_article_id": "sto-1",
                "artifact_type": "article_text",
                "artifact_path": str(canonical_txt),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
            {
                "article_id": "art_mixed",
                "storage_article_id": "sto-1",
                "artifact_type": "article_html",
                "artifact_path": str(canonical_html),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
            {
                "article_id": "art_mixed",
                "storage_article_id": None,
                "artifact_type": "article_text",
                "artifact_path": str(legacy_txt),
                "provider": "newsdata",
                "source_domain": "example.com",
                "published_date": "2026-03-05",
                "exists_locally": True,
            },
        ],
    )

    client = TestClient(create_app(project_root=tmp_path))
    response = client.get("/articles/art_mixed/content")
    assert response.status_code == 200
    payload = response.json()
    paths = [item["artifact_path"] for item in payload["artifacts"]]
    assert all("/publisher/data/" in path for path in paths)


def test_get_article_content_returns_false_when_no_local_content(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_meta_only", "gdelt_recent", "provider=gdelt_recent|id=meta", "Example", "example.com", "Metadata only", "2026-03-06T01:00:00Z")],
    )
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles/art_meta_only/content")
    assert response.status_code == 200
    payload = response.json()
    assert payload["article_id"] == "art_meta_only"
    assert payload["content_available"] is False
    assert payload["preferred_text"] is None
    assert payload["artifacts"] == []
    assert payload["resolution_strategy"] == "local_only"
    assert payload["resolution_remote_attempted"] is False
    assert payload["augmentation_meta"]["augmentation_available"] is False
    assert payload["augmentation_meta"]["augmentation_types_present"] == []


def test_get_article_content_resolve_remote_true_triggers_provider_resolution(monkeypatch, tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    refresh_provider_registry(config)
    _seed_articles(
        config,
        [_article_row("art_remote_api", "gdelt_recent", "provider=gdelt_recent|id=remote", "Example", "example.com", "Remote", "2026-03-06T01:00:00Z")],
    )

    class FakeResponse:
        text = "<html><body>Resolved remotely</body></html>"
        status_code = 200
        headers = {"Content-Type": "text/html"}

    def fake_request_response(self, method, url, params=None, headers=None):
        return FakeResponse()

    monkeypatch.setattr(HttpClient, "request_response", fake_request_response)
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles/art_remote_api/content", params={"resolve_remote": "true"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["content_available"] is True
    assert payload["resolution_source"] in {"remote_http", "local_artifact"}
    assert payload["resolution_strategy"] in {"direct_url_fetch", "local_artifact"}
    assert payload["resolution_remote_attempted"] is True


def test_get_article_content_returns_404_for_unknown_article(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [_article_row("art_known_2", "nyt", "provider=nyt|id=known2", "NYT", "nytimes.com", "Known", "2026-03-04T01:00:00Z")],
    )
    client = TestClient(create_app(project_root=tmp_path))

    response = client.get("/articles/not_local/content")
    assert response.status_code == 404


def test_api_serializes_null_like_optional_fields_as_json_null(tmp_path: Path) -> None:
    config = load_config(project_root=tmp_path)
    _seed_articles(
        config,
        [
            {
                "article_id": "art_nulls",
                "provider": "nyt_archive",
                "provider_document_id": "doc-null",
                "resolved_document_identity": "provider=nyt_archive|provider_document_id=doc-null",
                "source_name": "New York Times",
                "source_domain": "nytimes.com",
                "url": "https://nytimes.com/nulls",
                "canonical_url": "https://nytimes.com/nulls",
                "title": "Null handling check",
                "published_at": "2026-03-10T01:00:00Z",
                "language": float("nan"),
                "section": "   ",
                "byline": pd.NaT,
                "article_text": None,
                "summary_text": "NaN",
                "snippet": "nAt",
                "metadata_json": None,
                "imported_at": "2026-03-10T01:00:00Z",
            }
        ],
    )
    client = TestClient(create_app(project_root=tmp_path))

    list_response = client.get("/articles")
    assert list_response.status_code == 200
    item = list_response.json()["items"][0]
    assert item["language"] is None
    assert item["section"] is None
    assert item["byline"] is None
    assert item["summary_text"] is None
    assert item["snippet"] is None

    detail_response = client.get("/articles/art_nulls")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["language"] is None
    assert detail["section"] is None
    assert detail["byline"] is None
    assert detail["summary_text"] is None
    assert detail["snippet"] is None
