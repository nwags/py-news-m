"""Utilities for local tabular bulk article normalization."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

import pandas as pd

_WS_RE = re.compile(r"\s+")


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return _WS_RE.sub(" ", text)


def normalize_provider(value: Any, default: str = "local_tabular") -> str:
    normalized = normalize_text(value)
    if not normalized:
        return default
    return normalized.lower()


def normalize_url(value: Any) -> str | None:
    url = normalize_text(value)
    if not url:
        return None
    return url


def normalize_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = normalize_text(value)
    if text is None:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    dt = parsed.to_pydatetime().astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def coerce_date_from_iso(value: str | None) -> str | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def pick_column(columns: list[str], candidates: list[str]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = lowered.get(candidate.lower())
        if match:
            return match
    return None


def extract_first(row: dict[str, Any], columns: list[str], candidates: list[str]) -> Any:
    column = pick_column(columns, candidates)
    if column is None:
        return None
    return row.get(column)


def row_has_meaningful_metadata(values: dict[str, Any]) -> bool:
    keys = [
        "provider_document_id",
        "source_name",
        "source_domain",
        "url",
        "canonical_url",
        "title",
        "published_at",
        "language",
        "section",
        "byline",
        "article_text",
        "summary_text",
        "snippet",
    ]
    return any(values.get(key) for key in keys)


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
