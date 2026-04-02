"""Configuration and local path contracts for py-news-m."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import warnings

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    cache_root: Path
    refdata_root: Path
    refdata_source_catalog_root: Path
    refdata_taxonomies_root: Path
    refdata_inputs_root: Path
    refdata_normalized_root: Path
    user_agent: str
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_requests_per_second: float
    download_workers: int
    parse_workers: int


def load_config(project_root: Path | None = None, cache_root: Path | None = None) -> AppConfig:
    """Load runtime config.

    `project_root` is a compatibility hint for canonical repo-root discovery.
    It does not enable arbitrary refdata relocation.
    """

    load_dotenv()

    resolved_root = discover_project_root(project_root_hint=project_root)
    resolved_cache_root = _resolve_cache_root(default_root=resolved_root, explicit_cache_root=cache_root)
    refdata_root = resolved_root / "refdata"

    return AppConfig(
        project_root=resolved_root,
        cache_root=resolved_cache_root,
        refdata_root=refdata_root,
        refdata_source_catalog_root=refdata_root / "source_catalog",
        refdata_taxonomies_root=refdata_root / "taxonomies",
        refdata_inputs_root=refdata_root / "inputs",
        refdata_normalized_root=refdata_root / "normalized",
        user_agent=os.getenv("PY_NEWS_USER_AGENT", "py-news-m/0.1 (+local-first-ingestion)"),
        connect_timeout_seconds=float(os.getenv("PY_NEWS_CONNECT_TIMEOUT_SECONDS", "10.0")),
        read_timeout_seconds=float(os.getenv("PY_NEWS_READ_TIMEOUT_SECONDS", "30.0")),
        max_requests_per_second=float(os.getenv("PY_NEWS_MAX_REQUESTS_PER_SECOND", "2.0")),
        download_workers=int(os.getenv("PY_NEWS_DOWNLOAD_WORKERS", "4")),
        parse_workers=int(os.getenv("PY_NEWS_PARSE_WORKERS", "1")),
    )


def discover_project_root(project_root_hint: Path | None = None) -> Path:
    """Discover canonical project root.

    Preference order:
    1) explicit compatibility hint when it points to a valid repo root,
    2) deprecated PY_NEWS_PROJECT_ROOT hint when valid,
    3) current working directory ancestors,
    4) module path ancestors.
    """

    if project_root_hint is not None:
        return Path(project_root_hint).resolve()

    env_hint = os.getenv("PY_NEWS_PROJECT_ROOT")
    if env_hint:
        warnings.warn(
            "PY_NEWS_PROJECT_ROOT is deprecated for relocation. It is only used as a root-discovery hint and will be removed.",
            DeprecationWarning,
            stacklevel=2,
        )

    candidates: list[Path] = []
    if env_hint:
        candidates.append(Path(env_hint).resolve())

    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])

    module_dir = Path(__file__).resolve().parent
    candidates.extend([module_dir, *module_dir.parents])

    for candidate in candidates:
        if is_project_root(candidate):
            return candidate

    # Last-resort fallback to module parent for deterministic behavior.
    return module_dir.parent.resolve()


def is_project_root(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "pyproject.toml").exists()
        and (path / "py_news").exists()
        and (path / "AGENTS.md").exists()
    )


def _resolve_cache_root(default_root: Path, explicit_cache_root: Path | None) -> Path:
    if explicit_cache_root is not None:
        return Path(explicit_cache_root).resolve()

    env_cache = os.getenv("PY_NEWS_CACHE_ROOT")
    if env_cache:
        return Path(env_cache).resolve()

    return default_root / ".news_cache"


def ensure_runtime_dirs(config: AppConfig) -> list[Path]:
    dirs = [
        config.cache_root,
        config.refdata_root,
        config.refdata_source_catalog_root,
        config.refdata_taxonomies_root,
        config.refdata_inputs_root,
        config.refdata_normalized_root,
    ]
    for path in dirs:
        path.mkdir(parents=True, exist_ok=True)
    return dirs
