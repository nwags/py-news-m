"""Operator-facing runtime summary rendering."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def render_summary_block(title: str, values: Mapping[str, Any]) -> str:
    lines = [title]
    lines.append("=" * len(title))
    for key in sorted(values.keys()):
        lines.append(f"{key}: {values[key]}")
    return "\n".join(lines)
