"""Canonical shared pure packers/builders."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from m_cache_shared.augmentation.models import ApiAugmentationMeta, EventsViewRow, RunStatusView


def pack_additive_augmentation_meta(
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


def pack_run_status_view(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    packed: list[dict[str, Any]] = []
    for row in rows:
        view = RunStatusView(
            run_id=str(row.get("run_id") or ""),
            augmentation_type=str(row.get("augmentation_type") or ""),
            canonical_key=str(row.get("canonical_key") or ""),
            source_text_version=str(row.get("source_text_version") or ""),
            producer_name=str(row.get("producer_name") or ""),
            producer_version=str(row.get("producer_version") or ""),
            status=str(row.get("status") or ""),
            success=bool(row.get("success")),
            reason_code=str(row.get("reason_code") or ""),
            persisted_locally=bool(row.get("persisted_locally")),
            idempotency_key=_optional_text(row.get("idempotency_key")),
            augmentation_stale=_optional_bool(row.get("augmentation_stale")),
            last_updated_at=_optional_text(row.get("last_updated_at")),
        )
        merged = dict(row)
        for key, value in asdict(view).items():
            merged.setdefault(key, value)
        packed.append(merged)
    return packed


def pack_events_view(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    packed: list[dict[str, Any]] = []
    for row in rows:
        event_code = _optional_text(row.get("event_code")) or str(row.get("status") or row.get("reason_code") or "event")
        view = EventsViewRow(
            event_at=str(row.get("event_at") or ""),
            event_code=event_code,
            canonical_key=str(row.get("canonical_key") or ""),
            augmentation_type=_optional_text(row.get("augmentation_type")),
            run_id=_optional_text(row.get("run_id")),
            producer_name=_optional_text(row.get("producer_name")),
            producer_version=_optional_text(row.get("producer_version")),
            reason_code=_optional_text(row.get("reason_code")),
            success=_optional_bool(row.get("success")),
        )
        merged = dict(row)
        for key, value in asdict(view).items():
            merged.setdefault(key, value)
        packed.append(merged)
    return packed


# Compatibility aliases retained during Wave 5.1 normalization.
def pack_api_augmentation_meta(
    *,
    augmentation_available: bool,
    augmentation_types_present: list[str],
    last_augmented_at: str | None,
    augmentation_stale: bool | None,
    inspect_path: str | None,
) -> ApiAugmentationMeta:
    return pack_additive_augmentation_meta(
        augmentation_available=augmentation_available,
        augmentation_types_present=augmentation_types_present,
        last_augmented_at=last_augmented_at,
        augmentation_stale=augmentation_stale,
        inspect_path=inspect_path,
    )


def pack_run_status_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return pack_run_status_view(rows)


def pack_events_view_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return pack_events_view(rows)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "nat", "none"}:
        return None
    return text


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return bool(value)

