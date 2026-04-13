"""Canonical shared thin CLI helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click


def parse_json_input_payload(input_json: Path | None, json_payload: str | None) -> dict[str, Any]:
    if input_json is None and (json_payload is None or not json_payload.strip()):
        raise click.UsageError("Provide --input-json or --json-payload.")
    if input_json is not None and json_payload is not None and json_payload.strip():
        raise click.UsageError("Provide only one of --input-json or --json-payload.")
    if input_json is not None:
        return json.loads(input_json.read_text(encoding="utf-8"))
    return json.loads(str(json_payload))

