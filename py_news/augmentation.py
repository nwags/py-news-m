"""Wave 4 augmentation metadata + producer protocol helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from py_news.config import AppConfig
from py_news.m_cache_shared_shim import get_shared_symbol
from py_news.models import (
    ARTICLES_COLUMNS,
    AUGMENTATION_ARTIFACT_COLUMNS,
    AUGMENTATION_EVENT_COLUMNS,
    AUGMENTATION_PRODUCER_KIND_VALUES,
    AUGMENTATION_RUN_COLUMNS,
    AUGMENTATION_STATUS_VALUES,
    AUGMENTATION_TYPE_VALUES,
)
from py_news.storage.paths import normalized_artifact_path, slugify
from py_news.storage.writes import upsert_parquet_rows, write_text

_INLINE_PAYLOAD_MAX_BYTES_DEFAULT = 64 * 1024

ApiAugmentationMeta = get_shared_symbol("ApiAugmentationMeta")
ProducerTargetDescriptor = get_shared_symbol("ProducerTargetDescriptor")
shared_load_json_schema = get_shared_symbol("load_json_schema")


@dataclass(frozen=True, slots=True)
class AugmentationTargetInspection:
    domain: str
    resource_family: str
    canonical_key: str
    article_id: str
    text_bearing: bool
    augmentation_applicable: bool
    text_source: str
    source_text_version: str | None
    text_present: bool
    text_length: int
    reason: str
    inspect_runs_path: str
    inspect_artifacts_path: str
    language: str | None = None
    document_time_reference: str | None = None
    producer_hints: dict[str, Any] | None = None


def list_augmentation_types() -> list[str]:
    return list(AUGMENTATION_TYPE_VALUES)


def list_producer_kinds() -> list[str]:
    return list(AUGMENTATION_PRODUCER_KIND_VALUES)


def list_status_values() -> list[str]:
    return list(AUGMENTATION_STATUS_VALUES)


def canonical_article_key(article_id: str) -> str:
    return f"article:{article_id}"


def load_augmentation_runs(config: AppConfig) -> pd.DataFrame:
    return _load_parquet_with_columns(normalized_artifact_path(config, "augmentation_runs"), AUGMENTATION_RUN_COLUMNS)


def load_augmentation_events(config: AppConfig) -> pd.DataFrame:
    return _load_parquet_with_columns(normalized_artifact_path(config, "augmentation_events"), AUGMENTATION_EVENT_COLUMNS)


def load_augmentation_artifacts(config: AppConfig) -> pd.DataFrame:
    return _load_parquet_with_columns(normalized_artifact_path(config, "augmentation_artifacts"), AUGMENTATION_ARTIFACT_COLUMNS)


def inspect_article_target(
    config: AppConfig,
    *,
    article_id: str,
    text_source: str = "auto",
) -> AugmentationTargetInspection:
    articles = _load_parquet_with_columns(normalized_artifact_path(config, "articles"), ARTICLES_COLUMNS)
    matched = articles[articles["article_id"] == article_id]
    if matched.empty:
        return AugmentationTargetInspection(
            domain="news",
            resource_family="articles",
            canonical_key=canonical_article_key(article_id),
            article_id=article_id,
            text_bearing=True,
            augmentation_applicable=True,
            text_source=_normalize_text_source(text_source),
            source_text_version=None,
            text_present=False,
            text_length=0,
            reason="article_not_found",
            inspect_runs_path=f"/articles/{article_id}/augmentations",
            inspect_artifacts_path=f"/articles/{article_id}/augmentations",
            language=None,
            document_time_reference=None,
            producer_hints={"article_id": article_id},
        )

    row = matched.iloc[0].to_dict()
    normalized_source = _normalize_text_source(text_source)
    effective_source = normalized_source
    if normalized_source == "auto":
        effective_source = "content" if _article_content_text(row) else "metadata"

    selected_text = _select_text_for_source(config, row, article_id=article_id, text_source=effective_source)
    if selected_text:
        source_text_version = _sha256_text(selected_text)
        text_present = True
        text_length = len(selected_text)
        reason = "eligible_text_available"
    else:
        source_text_version = None
        text_present = False
        text_length = 0
        reason = "eligible_text_missing"

    return AugmentationTargetInspection(
        domain="news",
        resource_family="articles",
        canonical_key=canonical_article_key(article_id),
        article_id=article_id,
        text_bearing=True,
        augmentation_applicable=True,
        text_source=effective_source,
        source_text_version=source_text_version,
        text_present=text_present,
        text_length=text_length,
        reason=reason,
        inspect_runs_path=f"/articles/{article_id}/augmentations",
        inspect_artifacts_path=f"/articles/{article_id}/augmentations",
        language=_clean_optional_text(row.get("language")),
        document_time_reference=_clean_optional_text(row.get("published_at")),
        producer_hints={
            "provider": _clean_optional_text(row.get("provider")),
            "source_domain": _clean_optional_text(row.get("source_domain")),
        },
    )


def producer_target_descriptor(
    config: AppConfig,
    *,
    article_id: str,
    text_source: str = "auto",
) -> ProducerTargetDescriptor | None:
    inspected = inspect_article_target(config, article_id=article_id, text_source=text_source)
    if not inspected.source_text_version:
        return None
    source_uri = f"api:/articles/{article_id}" if inspected.text_source == "metadata" else f"api:/articles/{article_id}/content"
    return ProducerTargetDescriptor(
        domain=inspected.domain,
        resource_family=inspected.resource_family,
        canonical_key=inspected.canonical_key,
        text_source=source_uri,
        source_text_version=inspected.source_text_version,
        language=inspected.language,
        document_time_reference=inspected.document_time_reference,
        producer_hints=inspected.producer_hints or {},
    )


def build_producer_target_descriptor(
    config: AppConfig,
    *,
    article_id: str,
    text_source: str = "auto",
) -> ProducerTargetDescriptor | None:
    """Canonical Wave 4.1 seam for producer target-descriptor construction."""
    return producer_target_descriptor(config, article_id=article_id, text_source=text_source)


def submit_producer_run(config: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    now = _utc_now_iso()
    row = {
        "run_id": str(payload.get("run_id") or "").strip(),
        "event_at": now,
        "domain": str(payload.get("domain") or "").strip(),
        "resource_family": str(payload.get("resource_family") or "").strip(),
        "canonical_key": str(payload.get("canonical_key") or "").strip(),
        "augmentation_type": str(payload.get("augmentation_type") or "").strip(),
        "source_text_version": str(payload.get("source_text_version") or "").strip(),
        "producer_kind": str(payload.get("producer_kind") or "").strip(),
        "producer_name": str(payload.get("producer_name") or "").strip(),
        "producer_version": str(payload.get("producer_version") or "").strip(),
        "payload_schema_name": str(payload.get("payload_schema_name") or "").strip(),
        "payload_schema_version": str(payload.get("payload_schema_version") or "").strip(),
        "status": str(payload.get("status") or "").strip(),
        "success": bool(payload.get("success")),
        "reason_code": str(payload.get("reason_code") or "").strip(),
        "message": _clean_optional_text(payload.get("message")),
        "persisted_locally": bool(payload.get("persisted_locally")),
        "latency_ms": _safe_int(payload.get("latency_ms")),
        "rate_limited": bool(payload.get("rate_limited", False)),
        "retry_count": max(0, _safe_int(payload.get("retry_count"), default=0) or 0),
        "deferred_until": _clean_optional_text(payload.get("deferred_until")),
    }
    row["idempotency_key"] = _run_idempotency_key(row)
    _validate_domain_resource(row["domain"], row["resource_family"])

    details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "augmentation_runs"),
        rows=[row],
        dedupe_keys=["idempotency_key"],
        column_order=AUGMENTATION_RUN_COLUMNS,
    )
    event_details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "augmentation_events"),
        rows=[row],
        dedupe_keys=["idempotency_key", "status", "reason_code"],
        column_order=AUGMENTATION_EVENT_COLUMNS,
    )
    return {
        "status": "ok",
        "action": "submit_run",
        "idempotency_key": row["idempotency_key"],
        "run_id": row["run_id"],
        "deduped": int(details.get("incoming_count", 0)) == 1 and int(details.get("deduped_count", 0)) > 0,
        "runs_after_count": int(details.get("after_count", 0)),
        "events_after_count": int(event_details.get("after_count", 0)),
    }


def submit_run_envelope(config: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Canonical Wave 4.1 seam for producer run-submission envelopes."""
    return submit_producer_run(config, payload)


def submit_producer_artifact(
    config: AppConfig,
    payload: dict[str, Any],
    *,
    inline_payload_max_bytes: int = _INLINE_PAYLOAD_MAX_BYTES_DEFAULT,
) -> dict[str, Any]:
    now = _utc_now_iso()
    domain = str(payload.get("domain") or "").strip()
    resource_family = str(payload.get("resource_family") or "").strip()
    canonical_key = str(payload.get("canonical_key") or "").strip()
    augmentation_type = str(payload.get("augmentation_type") or "").strip()
    source_text_version = str(payload.get("source_text_version") or "").strip()
    producer_name = str(payload.get("producer_name") or "").strip()
    producer_version = str(payload.get("producer_version") or "").strip()
    payload_schema_name = str(payload.get("payload_schema_name") or "").strip()
    payload_schema_version = str(payload.get("payload_schema_version") or "").strip()
    success = bool(payload.get("success"))
    run_id = _clean_optional_text(payload.get("run_id"))
    explicit_locator = _clean_optional_text(payload.get("artifact_locator"))
    payload_body = payload.get("payload")

    _validate_domain_resource(domain, resource_family)
    row_for_key = {
        "domain": domain,
        "resource_family": resource_family,
        "canonical_key": canonical_key,
        "augmentation_type": augmentation_type,
        "source_text_version": source_text_version,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "payload_schema_name": payload_schema_name,
        "payload_schema_version": payload_schema_version,
    }
    idempotency_key = _artifact_idempotency_key(row_for_key)

    payload_inline_json = None
    payload_size_bytes = 0
    payload_truncated = False
    artifact_locator = explicit_locator

    if isinstance(payload_body, dict):
        serialized = json.dumps(payload_body, sort_keys=True, separators=(",", ":"))
        payload_size_bytes = len(serialized.encode("utf-8"))
        if payload_size_bytes <= max(0, int(inline_payload_max_bytes)):
            payload_inline_json = serialized
        else:
            payload_truncated = True
            if artifact_locator is None:
                artifact_locator = _write_payload_sidecar(
                    config=config,
                    canonical_key=canonical_key,
                    augmentation_type=augmentation_type,
                    idempotency_key=idempotency_key,
                    serialized_payload=serialized,
                )

    row = {
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "domain": domain,
        "resource_family": resource_family,
        "canonical_key": canonical_key,
        "augmentation_type": augmentation_type,
        "artifact_locator": artifact_locator,
        "source_text_version": source_text_version,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "payload_schema_name": payload_schema_name,
        "payload_schema_version": payload_schema_version,
        "payload_inline_json": payload_inline_json,
        "payload_size_bytes": payload_size_bytes,
        "payload_truncated": payload_truncated,
        "event_at": now,
        "success": success,
    }

    details = upsert_parquet_rows(
        path=normalized_artifact_path(config, "augmentation_artifacts"),
        rows=[row],
        dedupe_keys=["idempotency_key"],
        column_order=AUGMENTATION_ARTIFACT_COLUMNS,
    )
    return {
        "status": "ok",
        "action": "submit_artifact",
        "idempotency_key": idempotency_key,
        "deduped": int(details.get("incoming_count", 0)) == 1 and int(details.get("deduped_count", 0)) > 0,
        "artifacts_after_count": int(details.get("after_count", 0)),
        "payload_size_bytes": payload_size_bytes,
        "payload_truncated": payload_truncated,
        "artifact_locator": artifact_locator,
        "stored_inline": payload_inline_json is not None,
        "inline_payload_max_bytes": max(0, int(inline_payload_max_bytes)),
    }


def submit_artifact_envelope(
    config: AppConfig,
    payload: dict[str, Any],
    *,
    inline_payload_max_bytes: int = _INLINE_PAYLOAD_MAX_BYTES_DEFAULT,
) -> dict[str, Any]:
    """Canonical Wave 4.1 seam for producer artifact-submission envelopes."""
    return submit_producer_artifact(config, payload, inline_payload_max_bytes=inline_payload_max_bytes)


def api_augmentation_meta_for_article(
    config: AppConfig,
    *,
    article_id: str,
    text_source: str,
) -> ApiAugmentationMeta:
    target = inspect_article_target(config, article_id=article_id, text_source=text_source)
    artifact_rows = _artifact_rows_for_article(config, article_id=article_id)
    successful = artifact_rows[artifact_rows["success"] == True] if not artifact_rows.empty else artifact_rows
    types_present = sorted([str(value) for value in successful.get("augmentation_type", []).dropna().unique()]) if not successful.empty else []

    last_augmented_at = None
    if not successful.empty and "event_at" in successful.columns:
        times = pd.to_datetime(successful["event_at"], errors="coerce", utc=True).dropna()
        if not times.empty:
            last_augmented_at = times.max().isoformat().replace("+00:00", "Z")

    augmentation_stale: bool | None = None
    if target.source_text_version and not successful.empty:
        latest = successful.sort_values(by=["event_at"], ascending=False, na_position="last").iloc[0].to_dict()
        latest_version = _clean_optional_text(latest.get("source_text_version"))
        augmentation_stale = latest_version != target.source_text_version
    elif successful.empty:
        augmentation_stale = None

    return _pack_additive_augmentation_meta(
        augmentation_available=not successful.empty,
        augmentation_types_present=types_present,
        last_augmented_at=last_augmented_at,
        augmentation_stale=augmentation_stale,
        inspect_path=f"/articles/{article_id}/augmentations",
    )


def read_wave_schema(schema_name: str, *, project_root: Path, wave_version: str) -> dict[str, Any]:
    return shared_load_json_schema(schema_name, project_root=project_root, wave_version=wave_version)


def validate_outer_metadata_shape(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return _validate_outer_metadata_shape_local(payload, schema=schema)


def _load_parquet_with_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_parquet(path)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    return frame[columns].copy()


def _pack_additive_augmentation_meta(
    *,
    augmentation_available: bool,
    augmentation_types_present: list[str],
    last_augmented_at: str | None,
    augmentation_stale: bool | None,
    inspect_path: str | None,
) -> ApiAugmentationMeta:
    return ApiAugmentationMeta(
        augmentation_available=bool(augmentation_available),
        augmentation_types_present=list(augmentation_types_present),
        last_augmented_at=last_augmented_at,
        augmentation_stale=augmentation_stale,
        inspect_path=inspect_path,
    )


def _validate_outer_metadata_shape_local(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = schema.get("required", [])
    for key in required:
        if key not in payload:
            errors.append(f"missing_required:{key}")
    properties = schema.get("properties", {})
    for key, spec in properties.items():
        if key not in payload:
            continue
        enum_values = spec.get("enum")
        if enum_values is not None and payload[key] is not None and payload[key] not in enum_values:
            errors.append(f"invalid_enum:{key}")
    return errors


def _artifact_rows_for_article(config: AppConfig, *, article_id: str) -> pd.DataFrame:
    artifacts = load_augmentation_artifacts(config)
    if artifacts.empty:
        return artifacts
    return artifacts[artifacts["canonical_key"] == canonical_article_key(article_id)]


def _normalize_text_source(text_source: str) -> str:
    value = str(text_source or "auto").strip().lower()
    if value not in {"auto", "metadata", "content"}:
        return "auto"
    return value


def _article_metadata_text(row: dict[str, Any]) -> str | None:
    parts = [
        _clean_optional_text(row.get("title")),
        _clean_optional_text(row.get("summary_text")),
        _clean_optional_text(row.get("snippet")),
        _clean_optional_text(row.get("byline")),
    ]
    text = "\n".join([part for part in parts if part])
    return text or None


def _article_content_text(row: dict[str, Any]) -> str | None:
    return _clean_optional_text(row.get("article_text"))


def _select_text_for_source(config: AppConfig, row: dict[str, Any], *, article_id: str, text_source: str) -> str | None:
    if text_source == "content":
        direct = _article_content_text(row)
        if direct:
            return direct
        return _artifact_content_text(config, article_id=article_id)
    return _article_metadata_text(row)


def _artifact_content_text(config: AppConfig, *, article_id: str) -> str | None:
    path = normalized_artifact_path(config, "article_artifacts")
    if not path.exists():
        return None
    frame = pd.read_parquet(path)
    if frame.empty:
        return None
    if "artifact_type" not in frame.columns or "artifact_path" not in frame.columns:
        return None
    if "exists_locally" not in frame.columns:
        frame["exists_locally"] = False
    matched = frame[
        (frame["article_id"] == article_id)
        & (frame["artifact_type"] == "article_text")
        & (frame["exists_locally"] == True)
    ]
    if matched.empty:
        return None
    for record in matched.to_dict(orient="records"):
        raw_path = _clean_optional_text(record.get("artifact_path"))
        if not raw_path:
            continue
        file_path = Path(raw_path)
        if not file_path.exists():
            continue
        try:
            return file_path.read_text(encoding="utf-8")
        except OSError:
            continue
    return None


def _sha256_text(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _run_idempotency_key(row: dict[str, Any]) -> str:
    material = "|".join(
        [
            str(row.get("producer_name") or ""),
            str(row.get("producer_version") or ""),
            str(row.get("augmentation_type") or ""),
            str(row.get("canonical_key") or ""),
            str(row.get("source_text_version") or ""),
            str(row.get("run_id") or ""),
        ]
    )
    return f"run_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:24]}"


def _artifact_idempotency_key(row: dict[str, Any]) -> str:
    material = "|".join(
        [
            str(row.get("producer_name") or ""),
            str(row.get("producer_version") or ""),
            str(row.get("augmentation_type") or ""),
            str(row.get("canonical_key") or ""),
            str(row.get("source_text_version") or ""),
            str(row.get("payload_schema_name") or ""),
            str(row.get("payload_schema_version") or ""),
        ]
    )
    return f"art_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:24]}"


def _write_payload_sidecar(
    *,
    config: AppConfig,
    canonical_key: str,
    augmentation_type: str,
    idempotency_key: str,
    serialized_payload: str,
) -> str:
    safe_key = slugify(canonical_key)
    safe_type = slugify(augmentation_type)
    path = config.cache_root / "augmentations" / safe_key / safe_type / f"{idempotency_key}.json"
    write_text(path, serialized_payload + "\n")
    return str(path.resolve())


def _validate_domain_resource(domain: str, resource_family: str) -> None:
    if domain != "news":
        raise ValueError("Wave 4 producer protocol in this repo only supports domain=news")
    if resource_family != "articles":
        raise ValueError("Wave 4 producer protocol in this repo only supports resource_family=articles")


def _safe_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "nat", "none"}:
        return None
    return text
