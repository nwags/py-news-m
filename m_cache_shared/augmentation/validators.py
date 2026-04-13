"""Canonical shared validator helpers."""

from __future__ import annotations

from typing import Any


def _validate_payload_against_schema(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
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


def validate_producer_target_descriptor(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return _validate_payload_against_schema(payload, schema=schema)


def validate_producer_run_submission(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return _validate_payload_against_schema(payload, schema=schema)


def validate_producer_artifact_submission(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return _validate_payload_against_schema(payload, schema=schema)


def validate_run_submission_envelope(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return validate_producer_run_submission(payload, schema=schema)


def validate_artifact_submission_envelope(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return validate_producer_artifact_submission(payload, schema=schema)


# Compatibility alias retained during Wave 5.1 normalization.
def validate_outer_metadata_shape(payload: dict[str, Any], *, schema: dict[str, Any]) -> list[str]:
    return _validate_payload_against_schema(payload, schema=schema)

