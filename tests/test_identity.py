from py_news.models import derive_article_identity


def test_identity_prefers_provider_document_id():
    identity = derive_article_identity(
        provider="nyt",
        provider_document_id="doc-1",
        canonical_url="https://example.com/a",
        url="https://example.com/b",
        source_name="NYT",
        title="Title",
        published_at="2024-01-01T00:00:00Z",
    )
    assert "provider_document_id=doc-1" in identity.resolved_document_identity


def test_identity_falls_back_to_url_then_source_title_date():
    url_identity = derive_article_identity(
        provider="nyt",
        provider_document_id=None,
        canonical_url=None,
        url="https://example.com/a",
        source_name="NYT",
        title="Title",
        published_at="2024-01-01T00:00:00Z",
    )
    assert "url=https://example.com/a" in url_identity.resolved_document_identity

    fallback_identity = derive_article_identity(
        provider=None,
        provider_document_id=None,
        canonical_url=None,
        url=None,
        source_name="Source",
        title="Title",
        published_at="2024-01-01T00:00:00Z",
    )
    assert "provider=local_tabular" in fallback_identity.resolved_document_identity
    assert fallback_identity.article_id.startswith("art_")
