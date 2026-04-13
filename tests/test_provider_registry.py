from py_news.config import load_config
from py_news.models import PROVIDER_REGISTRY_COLUMNS
from py_news.providers import load_provider_registry, refresh_provider_registry


def test_provider_registry_refresh_and_load(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    summary = refresh_provider_registry(config)

    assert summary["providers_count"] >= 3

    df = load_provider_registry(config)
    assert set(["nyt_archive", "gdelt_recent", "newsdata", "local_tabular"]).issubset(set(df["provider_id"].tolist()))
    assert list(df.columns) == PROVIDER_REGISTRY_COLUMNS
    rules = {row["provider_id"]: row["preferred_resolution_order"] for row in df.to_dict(orient="records")}
    assert rules["gdelt_recent"] == "provider_payload_content,direct_url_fetch"
    assert rules["nyt_archive"] == "provider_payload_content,direct_url_fetch"
    assert "provider_api_lookup" in rules["newsdata"]
    newsdata = df[df["provider_id"] == "newsdata"].iloc[0].to_dict()
    for key in (
        "domain",
        "content_domain",
        "display_name",
        "base_url",
        "soft_limit",
        "hard_limit",
        "burst_limit",
        "retry_budget",
        "backoff_policy",
        "supports_bulk_history",
        "supports_incremental_refresh",
        "supports_direct_resolution",
        "supports_public_resolve_if_missing",
        "supports_admin_refresh_if_stale",
        "graceful_degradation_policy",
    ):
        assert key in newsdata
    assert newsdata["domain"] == "news"
    assert newsdata["content_domain"] == "article"
    assert newsdata["auth_type"] == "api_key_query"


def test_provider_registry_override_csv(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    config.refdata_inputs_root.mkdir(parents=True, exist_ok=True)
    overrides = config.refdata_inputs_root / "provider_registry_overrides.csv"
    overrides.write_text(
        "provider_id,fallback_priority,is_active,notes\nnewsdata,99,false,disabled for maintenance\n",
        encoding="utf-8",
    )
    refresh_provider_registry(config)
    df = load_provider_registry(config)
    row = df[df["provider_id"] == "newsdata"].iloc[0].to_dict()
    assert int(row["fallback_priority"]) == 99
    assert bool(row["is_active"]) is False
    assert row["notes"] == "disabled for maintenance"
