"""Provider registry authority helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from py_news.config import AppConfig
from py_news.models import PROVIDER_REGISTRY_COLUMNS
from py_news.storage.paths import normalized_artifact_path
from py_news.storage.writes import upsert_parquet_rows


@dataclass(frozen=True)
class ProviderRule:
    provider_id: str
    provider_name: str
    preferred_resolution_order: list[str]
    direct_url_allowed: bool
    supports_full_content: bool
    supports_partial_content: bool
    content_mode: str
    api_base_url: str | None
    auth_type: str
    auth_env_var: str


_DEF_PROVIDER_ROWS = [
    {
        "provider_id": "nyt_archive",
        "domain": "news",
        "content_domain": "article",
        "display_name": "New York Times Archive",
        "provider_name": "New York Times Archive",
        "provider_type": "bulk_dataset",
        "base_url": "https://developer.nytimes.com/docs/archive-product/1/overview",
        "soft_limit": 10,
        "hard_limit": 10,
        "burst_limit": 1,
        "retry_budget": 2,
        "backoff_policy": "exponential",
        "browse_discovery_allowed": False,
        "supports_bulk_history": True,
        "supports_incremental_refresh": False,
        "supports_direct_resolution": True,
        "supports_public_resolve_if_missing": True,
        "supports_admin_refresh_if_stale": False,
        "graceful_degradation_policy": "return_local_metadata_only",
        "free_tier_notes": "Archive import is metadata-first; direct URL fetch remains optional fallback.",
        "fallback_priority": 20,
        "notes": "Historical import provider; content may require direct URL fallback.",
        "default_timeout_seconds": 30,
        "quota_window_seconds": 60,
        "quota_reset_hint": "provider_controlled",
        "expected_error_modes": "http_429,http_5xx,metadata_only_payload",
        "user_agent_required": False,
        "contact_requirement": "",
        "terms_url": "https://developer.nytimes.com/terms",
        "is_active": True,
        "history_mode": "local_archive_import",
        "recent_mode": "none",
        "content_mode": "metadata_or_url",
        "api_base_url": "https://api.nytimes.com/svc/archive/v1",
        "auth_type": "none",
        "auth_env_var": "",
        "rate_limit_policy": "custom",
        "preferred_resolution_order": "provider_payload_content,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": False,
        "requires_js": False,
        "provider_instructions": "Archive import is historical metadata-first; URL fetch is optional fallback.",
        "provider_type_legacy": "archive",
        "version": "v1",
    },
    {
        "provider_id": "gdelt_recent",
        "domain": "news",
        "content_domain": "article",
        "display_name": "GDELT Recent",
        "provider_name": "GDELT Recent",
        "provider_type": "partner_api",
        "base_url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "soft_limit": 120,
        "hard_limit": 120,
        "burst_limit": 5,
        "retry_budget": 2,
        "backoff_policy": "exponential",
        "browse_discovery_allowed": True,
        "supports_bulk_history": False,
        "supports_incremental_refresh": True,
        "supports_direct_resolution": True,
        "supports_public_resolve_if_missing": True,
        "supports_admin_refresh_if_stale": False,
        "graceful_degradation_policy": "defer_and_report",
        "free_tier_notes": "Aggregator payloads may be partial and direct URL may be blocked by publisher policy.",
        "fallback_priority": 10,
        "notes": "Recent window metadata provider with bounded direct-url fallback.",
        "default_timeout_seconds": 30,
        "quota_window_seconds": 60,
        "quota_reset_hint": "provider_controlled",
        "expected_error_modes": "http_429,http_5xx,non_html_response",
        "user_agent_required": False,
        "contact_requirement": "",
        "terms_url": "https://www.gdeltproject.org",
        "is_active": True,
        "history_mode": "none",
        "recent_mode": "recent_window_api",
        "content_mode": "metadata_or_url",
        "api_base_url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "auth_type": "none",
        "auth_env_var": "",
        "rate_limit_policy": "per_minute",
        "preferred_resolution_order": "provider_payload_content,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": False,
        "requires_js": False,
        "provider_instructions": "Recent-window metadata adapter; direct URL may be blocked by publisher policy.",
        "provider_type_legacy": "aggregator",
        "version": "v1",
    },
    {
        "provider_id": "newsdata",
        "domain": "news",
        "content_domain": "article",
        "display_name": "NewsData.io",
        "provider_name": "NewsData.io",
        "provider_type": "partner_api",
        "base_url": "https://newsdata.io/api/1/news",
        "soft_limit": 60,
        "hard_limit": 60,
        "burst_limit": 5,
        "retry_budget": 2,
        "backoff_policy": "exponential",
        "browse_discovery_allowed": True,
        "supports_bulk_history": False,
        "supports_incremental_refresh": True,
        "supports_direct_resolution": True,
        "supports_public_resolve_if_missing": True,
        "supports_admin_refresh_if_stale": False,
        "graceful_degradation_policy": "defer_and_report",
        "free_tier_notes": "Free tier limits can throttle aggressively near quota boundaries.",
        "fallback_priority": 5,
        "notes": "API-backed provider with key-based auth.",
        "default_timeout_seconds": 30,
        "quota_window_seconds": 60,
        "quota_reset_hint": "provider_controlled",
        "expected_error_modes": "http_401,http_429,http_5xx,no_match",
        "user_agent_required": False,
        "contact_requirement": "",
        "terms_url": "https://newsdata.io/terms",
        "is_active": True,
        "history_mode": "none",
        "recent_mode": "recent_window_api",
        "content_mode": "metadata_partial_or_url",
        "api_base_url": "https://newsdata.io/api/1/news",
        "auth_type": "api_key_query",
        "auth_env_var": "NEWSDATA_API_KEY",
        "rate_limit_policy": "per_minute",
        "preferred_resolution_order": "provider_payload_content,provider_api_lookup,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": True,
        "requires_js": False,
        "provider_instructions": "Recent-window metadata path with optional content fields when provided.",
        "provider_type_legacy": "api",
        "version": "v1",
    },
    {
        "provider_id": "local_tabular",
        "domain": "news",
        "content_domain": "article",
        "display_name": "Local Tabular Dataset",
        "provider_name": "Local Tabular Dataset",
        "provider_type": "local_dataset",
        "base_url": None,
        "soft_limit": None,
        "hard_limit": None,
        "burst_limit": None,
        "retry_budget": 0,
        "backoff_policy": "none",
        "browse_discovery_allowed": False,
        "supports_bulk_history": True,
        "supports_incremental_refresh": False,
        "supports_direct_resolution": False,
        "supports_public_resolve_if_missing": False,
        "supports_admin_refresh_if_stale": False,
        "graceful_degradation_policy": "return_local_metadata_only",
        "free_tier_notes": "Local dataset import; no remote traffic.",
        "fallback_priority": 100,
        "notes": "Operator-provided local import adapter.",
        "default_timeout_seconds": None,
        "quota_window_seconds": None,
        "quota_reset_hint": "",
        "expected_error_modes": "",
        "user_agent_required": False,
        "contact_requirement": "",
        "terms_url": "",
        "is_active": True,
        "history_mode": "local_dataset_import",
        "recent_mode": "none",
        "content_mode": "metadata_only",
        "api_base_url": "",
        "auth_type": "none",
        "auth_env_var": "",
        "rate_limit_policy": "unknown",
        "preferred_resolution_order": "provider_payload_content",
        "direct_url_allowed": False,
        "supports_metadata_only": True,
        "supports_partial_content": False,
        "supports_full_content": False,
        "requires_js": False,
        "provider_instructions": "Local bulk import adapter only.",
        "provider_type_legacy": "local_dataset",
        "version": "v1",
    },
]


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def refresh_provider_registry(config: AppConfig) -> dict:
    rows = []
    now = utc_now_iso()
    for row in _DEF_PROVIDER_ROWS:
        copied = dict(row)
        copied["updated_at"] = now
        rows.append(copied)
    rows = _apply_provider_registry_overrides(config, rows)

    details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "provider_registry"),
        rows=rows,
        dedupe_keys=["provider_id"],
        column_order=PROVIDER_REGISTRY_COLUMNS,
    )

    # Keep source_catalog as lightweight provider/source authority mirror.
    source_rows = [
        {
            "source_id": row["provider_id"],
            "source_name": row["provider_name"],
            "source_domain": "",
            "source_type": row["provider_type"],
            "country_code": "",
            "language": "",
            "is_active": row["is_active"],
            "primary_source": False,
            "source_updated_at": now,
        }
        for row in rows
    ]
    source_details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "source_catalog"),
        rows=source_rows,
        dedupe_keys=["source_id"],
    )

    return {
        "provider_registry_path": str(details["path"]),
        "providers_count": len(rows),
        "source_catalog_path": str(source_details["path"]),
    }


def load_provider_registry(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "provider_registry")
    if not path.exists():
        refresh_provider_registry(config)

    df = pd.read_parquet(path)
    for column in PROVIDER_REGISTRY_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[PROVIDER_REGISTRY_COLUMNS].copy()


def load_provider_rule(config: AppConfig, provider_id: str) -> ProviderRule | None:
    df = load_provider_registry(config)
    matched = df[df["provider_id"] == provider_id]
    if matched.empty:
        return None

    row = matched.iloc[0].to_dict()
    preferred = str(row.get("preferred_resolution_order") or "").strip()
    order = [item.strip() for item in preferred.split(",") if item.strip()]
    if not order:
        order = ["provider_payload_content", "direct_url_fetch"]

    auth_type = str(row.get("auth_type") or "")
    auth_env_var = str(row.get("auth_env_var") or "")
    # Compatibility fallback for older provider_registry snapshots missing auth columns.
    if provider_id == "newsdata":
        auth_type = auth_type or "api_key_query"
        auth_env_var = auth_env_var or "NEWSDATA_API_KEY"

    return ProviderRule(
        provider_id=provider_id,
        provider_name=str(row.get("provider_name") or provider_id),
        preferred_resolution_order=order,
        direct_url_allowed=bool(row.get("direct_url_allowed")),
        supports_full_content=bool(row.get("supports_full_content")),
        supports_partial_content=bool(row.get("supports_partial_content")),
        content_mode=str(row.get("content_mode") or ""),
        api_base_url=str(row.get("api_base_url") or "") or None,
        auth_type=auth_type,
        auth_env_var=auth_env_var,
    )


def _apply_provider_registry_overrides(config: AppConfig, rows: list[dict]) -> list[dict]:
    rows_by_id = {str(row["provider_id"]): dict(row) for row in rows}
    for override_row in _load_override_rows(config):
        provider_id = str(override_row.get("provider_id") or "").strip()
        if not provider_id:
            continue
        current = rows_by_id.get(provider_id, {"provider_id": provider_id})
        merged = dict(current)
        for key, value in override_row.items():
            if key == "provider_id":
                continue
            if _is_nullish(value):
                continue
            merged[key] = value
        rows_by_id[provider_id] = merged
    return [rows_by_id[key] for key in sorted(rows_by_id)]


def _load_override_rows(config: AppConfig) -> list[dict]:
    base = config.refdata_inputs_root
    parquet_path = base / "provider_registry_overrides.parquet"
    csv_path = base / "provider_registry_overrides.csv"
    out: list[dict] = []
    for path in (parquet_path, csv_path):
        if not path.exists():
            continue
        if path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(path)
        else:
            frame = pd.read_csv(path)
        if frame.empty:
            continue
        frame = frame.sort_values(by=["provider_id"], na_position="last")
        out.extend(frame.to_dict(orient="records"))
    return out


def _is_nullish(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
