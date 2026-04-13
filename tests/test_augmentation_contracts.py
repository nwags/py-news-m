import json
from pathlib import Path

from py_news.augmentation import read_wave_schema, validate_outer_metadata_shape


def _load_example(name: str) -> dict:
    root = Path(__file__).resolve().parent.parent
    path = root / "docs" / "standardization" / "m_cache_reference_pack_v3" / "examples" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_wave3_augmentation_run_meta_example_matches_outer_contract():
    payload = _load_example("augmentation_run_meta.sample.json")
    schema = read_wave_schema(
        "augmentation-run-meta.schema.json",
        project_root=Path(__file__).resolve().parent.parent,
        wave_version="v3",
    )
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_wave3_augmentation_artifact_meta_example_matches_outer_contract():
    payload = _load_example("augmentation_artifact_meta.sample.json")
    schema = read_wave_schema(
        "augmentation-artifact-meta.schema.json",
        project_root=Path(__file__).resolve().parent.parent,
        wave_version="v3",
    )
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_wave3_api_augmentation_meta_example_matches_outer_contract():
    payload = _load_example("api_augmentation_meta.sample.json")
    schema = read_wave_schema(
        "api-augmentation-meta.schema.json",
        project_root=Path(__file__).resolve().parent.parent,
        wave_version="v3",
    )
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_wave4_producer_target_descriptor_example_matches_outer_contract():
    root = Path(__file__).resolve().parent.parent
    payload = json.loads(
        (root / "docs" / "standardization" / "m_cache_reference_pack_v4" / "examples" / "producer_target_descriptor.sample.json").read_text(
            encoding="utf-8"
        )
    )
    schema = read_wave_schema("producer-target-descriptor.schema.json", project_root=root, wave_version="v4")
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_wave4_producer_run_submission_example_matches_outer_contract():
    root = Path(__file__).resolve().parent.parent
    payload = json.loads(
        (root / "docs" / "standardization" / "m_cache_reference_pack_v4" / "examples" / "producer_run_submission.sample.json").read_text(
            encoding="utf-8"
        )
    )
    schema = read_wave_schema("producer-run-submission.schema.json", project_root=root, wave_version="v4")
    assert validate_outer_metadata_shape(payload, schema=schema) == []


def test_wave4_producer_artifact_submission_example_matches_outer_contract():
    root = Path(__file__).resolve().parent.parent
    payload = json.loads(
        (root / "docs" / "standardization" / "m_cache_reference_pack_v4" / "examples" / "producer_artifact_submission.sample.json").read_text(
            encoding="utf-8"
        )
    )
    schema = read_wave_schema("producer-artifact-submission.schema.json", project_root=root, wave_version="v4")
    assert validate_outer_metadata_shape(payload, schema=schema) == []
