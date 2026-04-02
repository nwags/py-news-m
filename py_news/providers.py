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
        "provider_name": "New York Times Archive",
        "provider_type": "archive",
        "is_active": True,
        "history_mode": "local_archive_import",
        "recent_mode": "none",
        "content_mode": "metadata_or_url",
        "api_base_url": "",
        "auth_type": "none",
        "auth_env_var": "",
        "rate_limit_policy": "operator_controlled",
        "preferred_resolution_order": "provider_payload_content,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": False,
        "requires_js": False,
        "provider_instructions": "Archive import is historical metadata-first; URL fetch is optional fallback.",
        "version": "v1",
    },
    {
        "provider_id": "gdelt_recent",
        "provider_name": "GDELT Recent",
        "provider_type": "aggregator",
        "is_active": True,
        "history_mode": "none",
        "recent_mode": "recent_window_api",
        "content_mode": "metadata_or_url",
        "api_base_url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "auth_type": "none",
        "auth_env_var": "",
        "rate_limit_policy": "shared_rate_limiter",
        "preferred_resolution_order": "provider_payload_content,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": False,
        "requires_js": False,
        "provider_instructions": "Recent-window metadata adapter; direct URL may be blocked by publisher policy.",
        "version": "v1",
    },
    {
        "provider_id": "newsdata",
        "provider_name": "NewsData.io",
        "provider_type": "api",
        "is_active": True,
        "history_mode": "none",
        "recent_mode": "recent_window_api",
        "content_mode": "metadata_partial_or_url",
        "api_base_url": "https://newsdata.io/api/1/news",
        "auth_type": "api_key",
        "auth_env_var": "NEWSDATA_API_KEY",
        "rate_limit_policy": "shared_rate_limiter",
        "preferred_resolution_order": "provider_payload_content,provider_api_lookup,direct_url_fetch",
        "direct_url_allowed": True,
        "supports_metadata_only": True,
        "supports_partial_content": True,
        "supports_full_content": True,
        "requires_js": False,
        "provider_instructions": "Recent-window metadata path with optional content fields when provided.",
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
        auth_type = auth_type or "api_key"
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
