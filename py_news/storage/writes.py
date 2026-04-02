"""Simple file/parquet write helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import json

import pandas as pd



def write_text(path: Path | str, text: str) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path



def write_json(path: Path | str, payload: dict) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path



def _enforce_columns(df: pd.DataFrame, columns: list[str] | None, drop_extra: bool) -> pd.DataFrame:
    if not columns:
        return df
    working = df.copy()
    for column in columns:
        if column not in working.columns:
            working[column] = None
    if drop_extra:
        return working[columns]
    extra_columns = [column for column in working.columns if column not in columns]
    return working[columns + extra_columns]



def upsert_parquet_rows(
    path: Path | str,
    rows: Iterable[dict],
    dedupe_keys: list[str],
    column_order: list[str] | None = None,
    drop_extra_columns: bool = True,
) -> dict[str, int | Path]:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    incoming_df = pd.DataFrame(list(rows))
    incoming_df = _enforce_columns(incoming_df, column_order, drop_extra_columns)

    if incoming_df.empty and not output_path.exists() and column_order:
        incoming_df = pd.DataFrame(columns=column_order)

    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        existing_df = _enforce_columns(existing_df, column_order, drop_extra_columns)
    else:
        existing_df = pd.DataFrame(columns=column_order or [])

    before_count = len(existing_df)
    incoming_count = len(incoming_df)

    merged_df = pd.concat([existing_df, incoming_df], ignore_index=True)

    if dedupe_keys and not merged_df.empty:
        merged_df = merged_df.drop_duplicates(subset=dedupe_keys, keep="last")

    merged_df = _enforce_columns(merged_df, column_order, drop_extra_columns)
    after_count = len(merged_df)

    merged_df.to_parquet(output_path, index=False)

    return {
        "path": output_path,
        "before_count": before_count,
        "incoming_count": incoming_count,
        "after_count": after_count,
        "deduped_count": max(0, before_count + incoming_count - after_count),
    }



def upsert_parquet(
    path: Path | str,
    rows: Iterable[dict],
    dedupe_keys: list[str],
) -> Path:
    details = upsert_parquet_rows(path=path, rows=rows, dedupe_keys=dedupe_keys)
    return details["path"]  # type: ignore[return-value]


def append_parquet_rows(
    path: Path | str,
    rows: Iterable[dict],
    column_order: list[str] | None = None,
    drop_extra_columns: bool = True,
) -> dict[str, int | Path]:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    incoming_df = pd.DataFrame(list(rows))
    incoming_df = _enforce_columns(incoming_df, column_order, drop_extra_columns)

    if incoming_df.empty and not output_path.exists() and column_order:
        incoming_df = pd.DataFrame(columns=column_order)

    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        existing_df = _enforce_columns(existing_df, column_order, drop_extra_columns)
    else:
        existing_df = pd.DataFrame(columns=column_order or [])

    before_count = len(existing_df)
    incoming_count = len(incoming_df)
    merged_df = pd.concat([existing_df, incoming_df], ignore_index=True)
    merged_df = _enforce_columns(merged_df, column_order, drop_extra_columns)
    after_count = len(merged_df)
    merged_df.to_parquet(output_path, index=False)

    return {
        "path": output_path,
        "before_count": before_count,
        "incoming_count": incoming_count,
        "after_count": after_count,
        "deduped_count": 0,
    }
