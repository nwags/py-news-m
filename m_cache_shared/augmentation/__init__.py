"""Canonical shared augmentation export surface (Wave 5.1)."""

from m_cache_shared.augmentation.cli_helpers import parse_json_input_payload
from m_cache_shared.augmentation.enums import (
    AUGMENTATION_PRODUCER_KIND_VALUES,
    AUGMENTATION_STATUS_VALUES,
    AUGMENTATION_TYPE_VALUES,
    AugmentationType,
    ProducerKind,
    RunStatus,
)
from m_cache_shared.augmentation.models import (
    ApiAugmentationMeta,
    EventsViewRow,
    ProducerArtifactSubmission,
    ProducerRunSubmission,
    ProducerTargetDescriptor,
    RunStatusView,
)
from m_cache_shared.augmentation.packers import (
    pack_additive_augmentation_meta,
    pack_api_augmentation_meta,
    pack_events_view,
    pack_events_view_rows,
    pack_run_status_items,
    pack_run_status_view,
)
from m_cache_shared.augmentation.schema_loaders import load_json_schema, read_wave_schema
from m_cache_shared.augmentation.validators import (
    validate_artifact_submission_envelope,
    validate_outer_metadata_shape,
    validate_producer_artifact_submission,
    validate_producer_run_submission,
    validate_producer_target_descriptor,
    validate_run_submission_envelope,
)

__all__ = [
    "ApiAugmentationMeta",
    "AugmentationType",
    "AUGMENTATION_TYPE_VALUES",
    "ProducerKind",
    "AUGMENTATION_PRODUCER_KIND_VALUES",
    "RunStatus",
    "AUGMENTATION_STATUS_VALUES",
    "ProducerTargetDescriptor",
    "ProducerRunSubmission",
    "ProducerArtifactSubmission",
    "RunStatusView",
    "EventsViewRow",
    "validate_producer_target_descriptor",
    "validate_producer_run_submission",
    "validate_producer_artifact_submission",
    "validate_run_submission_envelope",
    "validate_artifact_submission_envelope",
    "load_json_schema",
    "pack_run_status_view",
    "pack_events_view",
    "pack_additive_augmentation_meta",
    "parse_json_input_payload",
    # Compatibility aliases kept for one normalization cycle.
    "read_wave_schema",
    "validate_outer_metadata_shape",
    "pack_run_status_items",
    "pack_events_view_rows",
    "pack_api_augmentation_meta",
]

