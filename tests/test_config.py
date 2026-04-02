from pathlib import Path

from py_news.config import ensure_runtime_dirs, load_config


def test_load_config_uses_cache_override(monkeypatch, tmp_path):
    monkeypatch.setenv("PY_NEWS_CACHE_ROOT", str(tmp_path / "alt_cache"))
    config = load_config()

    assert config.project_root == Path.cwd().resolve()
    assert config.cache_root == (tmp_path / "alt_cache").resolve()
    assert config.refdata_normalized_root == Path.cwd().resolve() / "refdata" / "normalized"


def test_load_config_deprecated_project_root_is_hint_only(monkeypatch, tmp_path):
    monkeypatch.setenv("PY_NEWS_PROJECT_ROOT", str(tmp_path))
    config = load_config()

    # tmp_path is not a valid repo root for this project, so canonical root remains discovered repo root.
    assert config.project_root == Path.cwd().resolve()
    assert config.refdata_root == Path.cwd().resolve() / "refdata"


def test_ensure_runtime_dirs_creates_expected_roots(tmp_path):
    config = load_config(cache_root=tmp_path / ".news_cache")
    created = ensure_runtime_dirs(config)

    assert created
    for path in created:
        assert path.exists()
        assert path.is_dir()
