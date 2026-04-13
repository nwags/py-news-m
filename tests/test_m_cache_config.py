from pathlib import Path

import pytest

from py_news.m_cache_config import load_effective_config


def test_load_effective_config_explicit_path_precedence(monkeypatch, tmp_path):
    explicit_cfg = tmp_path / "explicit.toml"
    explicit_cfg.write_text(
        """
[global]
log_level = "DEBUG"
default_summary_json = true
default_progress_json = true

[domains.news]
enabled = true
cache_root = "explicit_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    env_cfg = tmp_path / "env.toml"
    env_cfg.write_text(
        """
[domains.news]
enabled = true
cache_root = "env_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("M_CACHE_CONFIG", str(env_cfg))
    monkeypatch.setenv("PY_NEWS_CACHE_ROOT", str(tmp_path / "legacy_cache"))

    effective = load_effective_config(
        domain="news",
        project_root_hint=Path.cwd(),
        explicit_config_path=explicit_cfg,
    )
    assert effective.config_path == str(explicit_cfg.resolve())
    assert effective.domains["news"].cache_root == "explicit_cache"
    assert effective.global_config.log_level == "DEBUG"
    assert effective.global_config.default_summary_json is True


def test_load_effective_config_uses_env_path_when_explicit_missing(monkeypatch, tmp_path):
    cfg = tmp_path / "from_env.toml"
    cfg.write_text(
        """
[domains.news]
enabled = true
cache_root = "env_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("M_CACHE_CONFIG", str(cfg))
    effective = load_effective_config(domain="news", project_root_hint=Path.cwd())
    assert effective.config_path == str(cfg.resolve())
    assert effective.domains["news"].cache_root == "env_cache"


def test_load_effective_config_provider_validation(monkeypatch, tmp_path):
    cfg = tmp_path / "bad_provider.toml"
    cfg.write_text(
        """
[domains.news]
enabled = true
cache_root = ".news_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"

[domains.news.providers.newsdata]
auth_type = "api_key_query"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.delenv("M_CACHE_CONFIG", raising=False)
    with pytest.raises(ValueError, match="missing required keys"):
        load_effective_config(domain="news", project_root_hint=Path.cwd(), explicit_config_path=cfg)
