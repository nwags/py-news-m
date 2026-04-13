"""Compatibility re-exports for pre-Wave-5.1 flat imports."""

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
    "load_json_schema",
    "read_wave_schema",
    "validate_artifact_submission_envelope",
    "validate_outer_metadata_shape",
    "validate_producer_artifact_submission",
    "validate_producer_run_submission",
    "validate_producer_target_descriptor",
    "validate_run_submission_envelope",
]
