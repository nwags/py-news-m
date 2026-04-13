"""Canonical shared augmentation models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from m_cache_shared.augmentation.enums import AugmentationType, ProducerKind, RunStatus


@dataclass(frozen=True, slots=True)
class ProducerTargetDescriptor:
    domain: str
    resource_family: str
    canonical_key: str
    text_source: str
    source_text_version: str
    language: str | None = None
    document_time_reference: str | None = None
    producer_hints: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ProducerRunSubmission:
    run_id: str
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    source_text_version: str
    producer_kind: ProducerKind
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    status: RunStatus
    success: bool
    reason_code: str
    persisted_locally: bool = False


@dataclass(frozen=True, slots=True)
class ProducerArtifactSubmission:
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    source_text_version: str
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    artifact_locator: str | None = None
    payload: dict[str, Any] | None = None
    success: bool = True


@dataclass(frozen=True, slots=True)
class ApiAugmentationMeta:
    augmentation_available: bool
    augmentation_types_present: list[str]
    last_augmented_at: str | None
    augmentation_stale: bool | None
    inspect_path: str | None


@dataclass(frozen=True, slots=True)
class RunStatusView:
    run_id: str
    augmentation_type: str
    canonical_key: str
    source_text_version: str
    producer_name: str
    producer_version: str
    status: str
    success: bool
    reason_code: str
    persisted_locally: bool
    idempotency_key: str | None = None
    augmentation_stale: bool | None = None
    last_updated_at: str | None = None


@dataclass(frozen=True, slots=True)
class EventsViewRow:
    event_at: str
    event_code: str
    canonical_key: str
    augmentation_type: str | None = None
    run_id: str | None = None
    producer_name: str | None = None
    producer_version: str | None = None
    reason_code: str | None = None
    success: bool | None = None

