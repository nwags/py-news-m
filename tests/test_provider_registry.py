from py_news.config import load_config
from py_news.models import PROVIDER_REGISTRY_COLUMNS
from py_news.providers import load_provider_registry, refresh_provider_registry


def test_provider_registry_refresh_and_load(tmp_path):
    config = load_config(cache_root=tmp_path / ".news_cache")
    summary = refresh_provider_registry(config)

    assert summary["providers_count"] >= 3

    df = load_provider_registry(config)
    assert set(["nyt_archive", "gdelt_recent", "newsdata"]).issubset(set(df["provider_id"].tolist()))
    assert list(df.columns) == PROVIDER_REGISTRY_COLUMNS
    rules = {row["provider_id"]: row["preferred_resolution_order"] for row in df.to_dict(orient="records")}
    assert rules["gdelt_recent"] == "provider_payload_content,direct_url_fetch"
    assert rules["nyt_archive"] == "provider_payload_content,direct_url_fetch"
    assert "provider_api_lookup" in rules["newsdata"]
