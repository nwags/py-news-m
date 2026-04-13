import json
from pathlib import Path

import pytest

from m_cache_shared.augmentation import (
    ApiAugmentationMeta,
    load_json_schema,
    pack_additive_augmentation_meta,
    pack_events_view,
    pack_run_status_view,
    parse_json_input_payload,
    validate_outer_metadata_shape,
)


def test_parse_json_input_payload_supports_inline_and_file(tmp_path: Path):
    inline = parse_json_input_payload(None, '{"a": 1}')
    assert inline == {"a": 1}

    payload_path = tmp_path / "payload.json"
    payload_path.write_text('{"b": 2}', encoding="utf-8")
    from_file = parse_json_input_payload(payload_path, None)
    assert from_file == {"b": 2}


def test_parse_json_input_payload_rejects_missing_input():
    with pytest.raises(Exception):
        parse_json_input_payload(None, None)


def test_wave4_schema_validation_still_works_through_shared_helper():
    root = Path(__file__).resolve().parent.parent
    payload = json.loads(
        (root / "docs" / "standardization" / "m_cache_reference_pack_v4" / "examples" / "producer_run_submission.sample.json").read_text(
            encoding="utf-8"
        )
    )
    schema = load_json_schema("producer-run-submission.schema.json", project_root=root, wave_version="v4")
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_pack_api_augmentation_meta_is_pure_shared_model():
    packed = pack_additive_augmentation_meta(
        augmentation_available=True,
        augmentation_types_present=["entity_tagging"],
        last_augmented_at="2026-04-08T17:04:00Z",
        augmentation_stale=False,
        inspect_path="/articles/a/augmentations",
    )
    assert isinstance(packed, ApiAugmentationMeta)
    assert packed.augmentation_available is True
    assert packed.augmentation_types_present == ["entity_tagging"]


def test_status_and_events_packers_add_view_keys_without_removing_existing_fields():
    status_rows = [
        {
            "run_id": "r1",
            "augmentation_type": "entity_tagging",
            "canonical_key": "article:a",
            "source_text_version": "sha256:x",
            "producer_name": "p",
            "producer_version": "1",
            "status": "completed",
            "success": True,
            "reason_code": "completed",
            "persisted_locally": True,
            "domain": "news",
        }
    ]
    packed_status = pack_run_status_view(status_rows)
    assert packed_status[0]["domain"] == "news"
    assert packed_status[0]["run_id"] == "r1"

    event_rows = [
        {
            "event_at": "2026-04-08T17:04:00Z",
            "canonical_key": "article:a",
            "status": "completed",
            "producer_name": "p",
        }
    ]
    packed_events = pack_events_view(event_rows)
    assert packed_events[0]["event_code"] == "completed"
    assert packed_events[0]["canonical_key"] == "article:a"


def test_flat_module_shims_still_resolve_for_one_normalization_cycle():
    from m_cache_shared.cli_helpers import parse_json_input_payload as flat_parse
    from m_cache_shared.packers import pack_run_status_items as flat_pack_status
    from m_cache_shared.schemas import read_wave_schema as flat_read_schema

    assert flat_parse is not None
    assert flat_pack_status is not None
    assert flat_read_schema is not None
