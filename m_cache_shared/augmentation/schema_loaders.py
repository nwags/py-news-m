"""Canonical shared schema-loader helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_SCHEMA_ROOT = "docs/standardization"


def load_json_schema(schema_name: str, *, project_root: Path, wave_version: str) -> dict[str, Any]:
    schema_path = (project_root / _SCHEMA_ROOT / f"m_cache_reference_pack_{wave_version}" / "schemas" / schema_name).resolve()
    if not schema_path.exists():
        repo_root = Path(__file__).resolve().parent.parent.parent
        schema_path = (repo_root / _SCHEMA_ROOT / f"m_cache_reference_pack_{wave_version}" / "schemas" / schema_name).resolve()
    return json.loads(schema_path.read_text(encoding="utf-8"))


# Compatibility alias retained during Wave 5.1 normalization.
def read_wave_schema(schema_name: str, *, project_root: Path, wave_version: str) -> dict[str, Any]:
    return load_json_schema(schema_name, project_root=project_root, wave_version=wave_version)

