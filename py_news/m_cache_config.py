"""Canonical m-cache config loader and legacy mapping for the news domain."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
import tomllib
from typing import Any

from py_news.config import AppConfig, discover_project_root

_RESOLUTION_MODES = {"local_only", "resolve_if_missing", "refresh_if_stale"}
_ALLOWED_TOP_LEVEL_KEYS = {"global", "domains"}
_ALLOWED_GLOBAL_KEYS = {
    "app_root",
    "log_level",
    "default_summary_json",
    "default_progress_json",
    "default_progress_heartbeat_seconds",
    "default_http_timeout_seconds",
    "default_retry_budget",
    "default_user_agent",
}
_ALLOWED_DOMAIN_KEYS = {
    "enabled",
    "cache_root",
    "normalized_refdata_root",
    "lookup_root",
    "default_resolution_mode",
    "providers",
    "runtime",
}


@dataclass(frozen=True, slots=True)
class GlobalConfig:
    app_root: str = "."
    log_level: str = "INFO"
    default_summary_json: bool = False
    default_progress_json: bool = False
    default_progress_heartbeat_seconds: float = 30.0
    default_http_timeout_seconds: float = 30.0
    default_retry_budget: int = 2
    default_user_agent: str = "py-news-m/0.1 (+local-first-ingestion)"


@dataclass(frozen=True, slots=True)
class DomainConfig:
    enabled: bool = True
    cache_root: str = ".news_cache"
    normalized_refdata_root: str = "refdata/normalized"
    lookup_root: str = "refdata/normalized"
    default_resolution_mode: str = "local_only"
    providers: dict[str, dict[str, Any]] = field(default_factory=dict)
    runtime: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    global_config: GlobalConfig
    domains: dict[str, DomainConfig]
    config_path: str | None

    def to_dict(self) -> dict[str, Any]:
        out_domains: dict[str, Any] = {}
        for name, domain in sorted(self.domains.items()):
            out_domains[name] = {
                "enabled": bool(domain.enabled),
                "cache_root": domain.cache_root,
                "normalized_refdata_root": domain.normalized_refdata_root,
                "lookup_root": domain.lookup_root,
                "default_resolution_mode": domain.default_resolution_mode,
                "providers": domain.providers,
                "runtime": domain.runtime,
            }
        return {
            "global": {
                "app_root": self.global_config.app_root,
                "log_level": self.global_config.log_level,
                "default_summary_json": bool(self.global_config.default_summary_json),
                "default_progress_json": bool(self.global_config.default_progress_json),
                "default_progress_heartbeat_seconds": float(self.global_config.default_progress_heartbeat_seconds),
                "default_http_timeout_seconds": float(self.global_config.default_http_timeout_seconds),
                "default_retry_budget": int(self.global_config.default_retry_budget),
                "default_user_agent": self.global_config.default_user_agent,
            },
            "domains": out_domains,
            "config_path": self.config_path,
        }


def resolve_config_path(*, project_root: Path, explicit_config: Path | None) -> Path | None:
    if explicit_config is not None:
        return Path(explicit_config).resolve()
    env_config = os.getenv("M_CACHE_CONFIG", "").strip()
    if env_config:
        return Path(env_config).resolve()
    default_path = project_root / "m-cache.toml"
    if default_path.exists():
        return default_path.resolve()
    return None


def load_effective_config(
    *,
    domain: str = "news",
    project_root_hint: Path | None = None,
    explicit_config_path: Path | None = None,
    explicit_cache_root: Path | None = None,
) -> EffectiveConfig:
    project_root = discover_project_root(project_root_hint=project_root_hint)
    config_path = resolve_config_path(project_root=project_root, explicit_config=explicit_config_path)
    config_data = _load_toml(config_path) if config_path is not None and config_path.exists() else {}
    _validate_top_level_keys(config_data)

    global_data = _default_global_dict()
    domain_data = _default_domain_dict()
    _apply_legacy_env_defaults(project_root=project_root, global_data=global_data, domain_data=domain_data)

    file_global = config_data.get("global")
    if isinstance(file_global, dict):
        global_data.update(file_global)

    file_domains = config_data.get("domains")
    if isinstance(file_domains, dict):
        file_domain = file_domains.get(domain)
        if isinstance(file_domain, dict):
            _validate_domain_keys(domain, file_domain)
            domain_data.update({k: v for k, v in file_domain.items() if k in _ALLOWED_DOMAIN_KEYS})

    if explicit_cache_root is not None:
        domain_data["cache_root"] = str(Path(explicit_cache_root).resolve())

    _validate_domain_values(domain, domain_data)
    _validate_provider_values(domain, domain_data.get("providers") or {})

    global_config = GlobalConfig(
        app_root=str(global_data["app_root"]),
        log_level=str(global_data["log_level"]),
        default_summary_json=bool(global_data["default_summary_json"]),
        default_progress_json=bool(global_data["default_progress_json"]),
        default_progress_heartbeat_seconds=float(global_data["default_progress_heartbeat_seconds"]),
        default_http_timeout_seconds=float(global_data["default_http_timeout_seconds"]),
        default_retry_budget=int(global_data["default_retry_budget"]),
        default_user_agent=str(global_data["default_user_agent"]),
    )
    domain_config = DomainConfig(
        enabled=bool(domain_data["enabled"]),
        cache_root=str(domain_data["cache_root"]),
        normalized_refdata_root=str(domain_data["normalized_refdata_root"]),
        lookup_root=str(domain_data["lookup_root"]),
        default_resolution_mode=str(domain_data["default_resolution_mode"]),
        providers=dict(domain_data.get("providers") or {}),
        runtime=dict(domain_data.get("runtime") or {}),
    )
    return EffectiveConfig(
        global_config=global_config,
        domains={domain: domain_config},
        config_path=str(config_path) if config_path is not None else None,
    )


def app_config_from_effective(*, effective_config: EffectiveConfig, domain: str = "news", project_root_hint: Path | None = None) -> AppConfig:
    project_root = discover_project_root(project_root_hint=project_root_hint)
    domain_cfg = effective_config.domains[domain]
    runtime = domain_cfg.runtime

    cache_root = _resolve_path(project_root, domain_cfg.cache_root)
    refdata_normalized_root = _resolve_path(project_root, domain_cfg.normalized_refdata_root)
    refdata_root = refdata_normalized_root.parent

    return AppConfig(
        project_root=project_root,
        cache_root=cache_root,
        refdata_root=refdata_root,
        refdata_source_catalog_root=refdata_root / "source_catalog",
        refdata_taxonomies_root=refdata_root / "taxonomies",
        refdata_inputs_root=refdata_root / "inputs",
        refdata_normalized_root=refdata_normalized_root,
        user_agent=str(runtime.get("user_agent", effective_config.global_config.default_user_agent)),
        connect_timeout_seconds=float(runtime.get("connect_timeout_seconds", os.getenv("PY_NEWS_CONNECT_TIMEOUT_SECONDS", "10.0"))),
        read_timeout_seconds=float(runtime.get("read_timeout_seconds", os.getenv("PY_NEWS_READ_TIMEOUT_SECONDS", "30.0"))),
        max_requests_per_second=float(runtime.get("max_requests_per_second", os.getenv("PY_NEWS_MAX_REQUESTS_PER_SECOND", "2.0"))),
        download_workers=int(runtime.get("download_workers", os.getenv("PY_NEWS_DOWNLOAD_WORKERS", "4"))),
        parse_workers=int(runtime.get("parse_workers", os.getenv("PY_NEWS_PARSE_WORKERS", "1"))),
    )


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        loaded = tomllib.load(fh)
    if not isinstance(loaded, dict):
        raise ValueError("m-cache config must be a TOML table at top level")
    return loaded


def _default_global_dict() -> dict[str, Any]:
    return {
        "app_root": ".",
        "log_level": "INFO",
        "default_summary_json": False,
        "default_progress_json": False,
        "default_progress_heartbeat_seconds": 30.0,
        "default_http_timeout_seconds": 30.0,
        "default_retry_budget": 2,
        "default_user_agent": "py-news-m/0.1 (+local-first-ingestion)",
    }


def _default_domain_dict() -> dict[str, Any]:
    return {
        "enabled": True,
        "cache_root": ".news_cache",
        "normalized_refdata_root": "refdata/normalized",
        "lookup_root": "refdata/normalized",
        "default_resolution_mode": "local_only",
        "providers": {},
        "runtime": {},
    }


def _apply_legacy_env_defaults(*, project_root: Path, global_data: dict[str, Any], domain_data: dict[str, Any]) -> None:
    env_cache_root = os.getenv("PY_NEWS_CACHE_ROOT", "").strip()
    if env_cache_root:
        domain_data["cache_root"] = str(Path(env_cache_root).resolve())
    else:
        domain_data["cache_root"] = str((project_root / ".news_cache").resolve())

    runtime = domain_data.setdefault("runtime", {})
    user_agent = os.getenv("PY_NEWS_USER_AGENT", "").strip()
    if user_agent:
        runtime["user_agent"] = user_agent
    runtime["connect_timeout_seconds"] = float(os.getenv("PY_NEWS_CONNECT_TIMEOUT_SECONDS", "10.0"))
    runtime["read_timeout_seconds"] = float(os.getenv("PY_NEWS_READ_TIMEOUT_SECONDS", "30.0"))
    runtime["max_requests_per_second"] = float(os.getenv("PY_NEWS_MAX_REQUESTS_PER_SECOND", "2.0"))
    runtime["download_workers"] = int(os.getenv("PY_NEWS_DOWNLOAD_WORKERS", "4"))
    runtime["parse_workers"] = int(os.getenv("PY_NEWS_PARSE_WORKERS", "1"))

    log_level = os.getenv("PY_NEWS_LOG_LEVEL", "").strip()
    if log_level:
        global_data["log_level"] = log_level


def _validate_top_level_keys(config_data: dict[str, Any]) -> None:
    unknown = sorted([k for k in config_data if k not in _ALLOWED_TOP_LEVEL_KEYS])
    if unknown:
        raise ValueError(f"Unknown top-level config keys: {', '.join(unknown)}")
    global_block = config_data.get("global")
    if global_block is not None and not isinstance(global_block, dict):
        raise ValueError("Config key 'global' must be a table")
    if isinstance(global_block, dict):
        unknown_global = sorted([k for k in global_block if k not in _ALLOWED_GLOBAL_KEYS])
        if unknown_global:
            raise ValueError(f"Unknown [global] config keys: {', '.join(unknown_global)}")
    domains_block = config_data.get("domains")
    if domains_block is not None and not isinstance(domains_block, dict):
        raise ValueError("Config key 'domains' must be a table")


def _validate_domain_keys(domain: str, domain_data: dict[str, Any]) -> None:
    unknown = sorted([k for k in domain_data if k not in _ALLOWED_DOMAIN_KEYS])
    if unknown:
        raise ValueError(f"Unknown [domains.{domain}] config keys: {', '.join(unknown)}")


def _validate_domain_values(domain: str, domain_data: dict[str, Any]) -> None:
    if bool(domain_data.get("enabled", True)):
        if not str(domain_data.get("cache_root", "")).strip():
            raise ValueError(f"[domains.{domain}] cache_root is required when enabled=true")
        if not str(domain_data.get("normalized_refdata_root", "")).strip():
            raise ValueError(f"[domains.{domain}] normalized_refdata_root is required when enabled=true")
    mode = str(domain_data.get("default_resolution_mode", "local_only"))
    if mode not in _RESOLUTION_MODES:
        raise ValueError(
            f"[domains.{domain}] default_resolution_mode must be one of: {', '.join(sorted(_RESOLUTION_MODES))}"
        )


def _validate_provider_values(domain: str, provider_map: dict[str, Any]) -> None:
    if not isinstance(provider_map, dict):
        raise ValueError(f"[domains.{domain}.providers] must be a table")
    for provider_id, provider_data in sorted(provider_map.items()):
        if not isinstance(provider_data, dict):
            raise ValueError(f"[domains.{domain}.providers.{provider_id}] must be a table")
        required = ("auth_type", "rate_limit_policy", "direct_resolution_allowed")
        missing = [key for key in required if key not in provider_data]
        if missing:
            raise ValueError(
                f"[domains.{domain}.providers.{provider_id}] missing required keys: {', '.join(missing)}"
            )


def _resolve_path(project_root: Path, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    return (project_root / candidate).resolve()
