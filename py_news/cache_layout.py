"""Canonical physical storage mapping and cache-layout rebuild helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import ast
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from py_news.config import AppConfig
from py_news.models import (
    ARTICLE_ARTIFACT_COLUMNS,
    ARTICLES_COLUMNS,
    ARTICLE_STORAGE_MAP_COLUMNS,
    STORAGE_ARTICLES_COLUMNS,
)
from py_news.storage.paths import (
    derive_publisher_slug,
    normalized_artifact_path,
    provider_full_index_dir_path,
    publisher_article_artifact_path,
    publisher_article_meta_path,
    slugify,
)
from py_news.storage.writes import upsert_parquet_rows, write_json


@dataclass(frozen=True, slots=True)
class StorageIdentity:
    storage_article_id: str
    mapping_basis: str
    equivalence_basis: str
    equivalence_value: str


def load_articles(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "articles")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLES_COLUMNS)
    df = pd.read_parquet(path)
    for col in ARTICLES_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[ARTICLES_COLUMNS].copy()


def load_article_artifacts(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "article_artifacts")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLE_ARTIFACT_COLUMNS)
    df = pd.read_parquet(path)
    for col in ARTICLE_ARTIFACT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[ARTICLE_ARTIFACT_COLUMNS].copy()


def load_storage_articles(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "storage_articles")
    if not path.exists():
        return pd.DataFrame(columns=STORAGE_ARTICLES_COLUMNS)
    df = pd.read_parquet(path)
    for col in STORAGE_ARTICLES_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[STORAGE_ARTICLES_COLUMNS].copy()


def load_article_storage_map(config: AppConfig) -> pd.DataFrame:
    path = normalized_artifact_path(config, "article_storage_map")
    if not path.exists():
        return pd.DataFrame(columns=ARTICLE_STORAGE_MAP_COLUMNS)
    df = pd.read_parquet(path)
    for col in ARTICLE_STORAGE_MAP_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[ARTICLE_STORAGE_MAP_COLUMNS].copy()


def derive_storage_identity(row: dict[str, Any]) -> StorageIdentity:
    canonical_url = normalize_url_identity(row.get("canonical_url")) or normalize_url_identity(row.get("url"))
    provider = clean_text(row.get("provider")) or "unknown"
    provider_document_id = clean_text(row.get("provider_document_id"))
    article_id = clean_text(row.get("article_id")) or "unknown"
    if canonical_url:
        material = f"canonical_url|{canonical_url}"
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
        return StorageIdentity(
            storage_article_id=f"sto_{digest}",
            mapping_basis="canonical_url",
            equivalence_basis="canonical_url",
            equivalence_value=canonical_url,
        )
    if provider_document_id:
        material = f"provider_doc|{provider}|{provider_document_id}"
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
        return StorageIdentity(
            storage_article_id=f"sto_{digest}",
            mapping_basis="provider_document_id_within_provider",
            equivalence_basis="provider_document_id_within_provider",
            equivalence_value=f"{provider}|{provider_document_id}",
        )
    material = f"fallback_article_id|{article_id}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return StorageIdentity(
        storage_article_id=f"sto_{digest}",
        mapping_basis="fallback_article_id",
        equivalence_basis="fallback_article_id",
        equivalence_value=article_id,
    )


def ensure_storage_mapping(config: AppConfig, row: dict[str, Any]) -> dict[str, Any]:
    identity = derive_storage_identity(row)
    provider = clean_text(row.get("provider")) or ""
    article_id = clean_text(row.get("article_id")) or ""
    source_domain = repair_domain(row.get("source_domain")) or ""
    source_name = clean_text(row.get("source_name")) or ""
    published_at = clean_text(row.get("published_at")) or "1970-01-01"
    publisher_slug = derive_publisher_slug(source_domain=source_domain, source_name=source_name, provider=provider)
    folder = publisher_article_meta_path(
        config,
        publisher_slug=publisher_slug,
        published_at=published_at,
        article_id=identity.storage_article_id,
    ).parent
    now = utc_now_iso()
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "storage_articles"),
        rows=[
            {
                "storage_article_id": identity.storage_article_id,
                "publisher_slug": publisher_slug,
                "storage_anchor_date": _date_part(published_at),
                "storage_folder_path": str(folder),
                "equivalence_basis": identity.equivalence_basis,
                "equivalence_value": identity.equivalence_value,
                "created_at": now,
                "updated_at": now,
            }
        ],
        dedupe_keys=["storage_article_id"],
        column_order=STORAGE_ARTICLES_COLUMNS,
    )
    upsert_parquet_rows(
        path=normalized_artifact_path(config, "article_storage_map"),
        rows=[
            {
                "article_id": article_id,
                "provider": provider,
                "resolved_document_identity": clean_text(row.get("resolved_document_identity")) or "",
                "storage_article_id": identity.storage_article_id,
                "mapping_basis": identity.mapping_basis,
                "mapped_at": now,
            }
        ],
        dedupe_keys=["article_id"],
        column_order=ARTICLE_STORAGE_MAP_COLUMNS,
    )
    return {
        "storage_article_id": identity.storage_article_id,
        "publisher_slug": publisher_slug,
        "published_at": published_at,
        "storage_folder_path": str(folder),
        "mapping_basis": identity.mapping_basis,
    }


def mapped_storage_id_for_article(config: AppConfig, article_id: str) -> str | None:
    mapping = load_article_storage_map(config)
    if mapping.empty:
        return None
    matched = mapping[mapping["article_id"] == article_id]
    if matched.empty:
        return None
    return clean_text(matched.iloc[0].get("storage_article_id"))


def mapped_article_ids_for_storage(config: AppConfig, storage_article_id: str) -> list[str]:
    mapping = load_article_storage_map(config)
    if mapping.empty:
        return []
    matched = mapping[mapping["storage_article_id"] == storage_article_id]
    values = [clean_text(v) for v in matched.get("article_id", [])]
    return sorted({v for v in values if v})


def storage_folder_path_for_storage(config: AppConfig, storage_article_id: str) -> str | None:
    storage = load_storage_articles(config)
    if storage.empty:
        return None
    matched = storage[storage["storage_article_id"] == storage_article_id]
    if matched.empty:
        return None
    return clean_text(matched.iloc[0].get("storage_folder_path"))


def build_provider_full_index(config: AppConfig) -> dict[str, int]:
    articles = load_articles(config)
    artifacts = load_article_artifacts(config)
    mapping = load_article_storage_map(config)
    if mapping.empty:
        return {}
    joined = mapping.merge(
        articles[["article_id", "provider", "canonical_url", "url", "source_domain", "title", "published_at"]],
        on=["article_id", "provider"],
        how="left",
    )
    counts: dict[str, int] = {}
    providers = sorted({clean_text(v) for v in joined["provider"].tolist() if clean_text(v)})
    for provider in providers:
        provider_rows = joined[joined["provider"] == provider].copy()
        index_dir = provider_full_index_dir_path(config, provider_id=provider)
        index_dir.mkdir(parents=True, exist_ok=True)
        provider_rows = provider_rows.sort_values(by=["article_id", "storage_article_id"])
        provider_rows.to_parquet(index_dir / "article_map.parquet", index=False)
        provider_artifacts = artifacts.merge(
            provider_rows[["article_id", "storage_article_id"]],
            on=["article_id", "storage_article_id"],
            how="inner",
        )
        provider_artifacts = provider_artifacts.sort_values(by=["article_id", "artifact_type", "artifact_path"])
        provider_artifacts.to_parquet(index_dir / "artifact_index.parquet", index=False)
        counts[provider] = len(provider_rows)
    return counts


def rebuild_cache_layout(config: AppConfig, *, cleanup_legacy: bool = False) -> dict[str, Any]:
    return rebuild_cache_layout_with_options(config, cleanup_legacy=cleanup_legacy, repair_metadata=False)


def rebuild_cache_layout_with_options(
    config: AppConfig,
    *,
    cleanup_legacy: bool = False,
    repair_metadata: bool = False,
) -> dict[str, Any]:
    articles = load_articles(config)
    artifacts = load_article_artifacts(config)
    repair_summary = None
    if repair_metadata and not articles.empty:
        repair_summary, articles = _repair_articles_metadata(articles)
        if repair_summary["rows_repaired_source_domain"] or repair_summary["rows_repaired_section"] or repair_summary["rows_repaired_byline"]:
            _write_parquet(
                normalized_artifact_path(config, "articles"),
                articles.to_dict(orient="records"),
                ARTICLES_COLUMNS,
            )
    if articles.empty:
        return {
            "stage": "cache_rebuild_layout",
            "status": "ok",
            "mapped_rows": 0,
            "storage_articles_rows": 0,
            "artifact_linkage_rows": 0,
            "legacy_cleanup_performed": False,
            "metadata_repair": repair_summary,
        }

    rows = [row for row in articles.to_dict(orient="records")]
    rows.sort(key=lambda r: (clean_text(r.get("article_id")) or "", clean_text(r.get("provider")) or ""))
    mapped: list[dict[str, Any]] = []
    for row in rows:
        mapped.append(ensure_storage_mapping(config, row))

    # Build canonical artifact folders with deterministic merge policy.
    artifacts_by_storage: dict[str, list[dict[str, Any]]] = {}
    map_df = load_article_storage_map(config)
    if not map_df.empty:
        merge_source = artifacts.drop(columns=["storage_article_id"], errors="ignore")
        merged = merge_source.merge(map_df[["article_id", "storage_article_id"]], on="article_id", how="left")
        for rec in merged.to_dict(orient="records"):
            sid = clean_text(rec.get("storage_article_id"))
            if sid:
                artifacts_by_storage.setdefault(sid, []).append(rec)

    linkage_rows: list[dict[str, Any]] = []
    now = utc_now_iso()
    for entry in mapped:
        sid = entry["storage_article_id"]
        pub = entry["publisher_slug"]
        published_at = entry["published_at"]
        meta_path = publisher_article_meta_path(
            config,
            publisher_slug=pub,
            published_at=published_at,
            article_id=sid,
        )
        folder = meta_path.parent
        folder.mkdir(parents=True, exist_ok=True)
        merged_files = _merge_storage_files(
            folder=folder,
            candidates=artifacts_by_storage.get(sid, []),
        )
        mapped_ids = mapped_article_ids_for_storage(config, sid)
        write_json(
            meta_path,
            {
                "storage_article_id": sid,
                "publisher_slug": pub,
                "mapped_article_ids": mapped_ids,
                "rebuilt_at": now,
                "artifacts_present": merged_files,
            },
        )
        if meta_path.exists():
            merged_files["meta"] = str(meta_path)
        for aid in mapped_ids:
            if "txt" in merged_files:
                linkage_rows.append(_artifact_row(aid, sid, "article_text", merged_files["txt"], now))
            if "json" in merged_files:
                linkage_rows.append(_artifact_row(aid, sid, "article_json", merged_files["json"], now))
            if "html" in merged_files:
                linkage_rows.append(_artifact_row(aid, sid, "article_html", merged_files["html"], now))

    # Canonical current artifact index is rewritten from canonical storage only.
    canonical_rows = _canonicalize_artifact_rows(config, linkage_rows)
    _write_parquet(normalized_artifact_path(config, "article_artifacts"), canonical_rows, ARTICLE_ARTIFACT_COLUMNS)

    provider_counts = build_provider_full_index(config)

    verification = _verify_rebuild_counts(config)
    safety = _canonical_artifact_safety_check(config)
    if not safety["ok"]:
        verification["ok"] = False
        verification["canonical_artifact_safety"] = safety
    if cleanup_legacy:
        if not verification["ok"]:
            raise ValueError("Refusing cleanup: rebuild verification failed")
        _cleanup_legacy_cache(config)

    return {
        "stage": "cache_rebuild_layout",
        "status": "ok",
        "mapped_rows": len(load_article_storage_map(config)),
        "storage_articles_rows": len(load_storage_articles(config)),
        "artifact_linkage_rows": len(canonical_rows),
        "provider_full_index_counts": provider_counts,
        "verification_ok": verification["ok"],
        "verification": verification,
        "canonical_artifact_safety": safety,
        "legacy_cleanup_performed": bool(cleanup_legacy and verification["ok"]),
        "metadata_repair": repair_summary,
    }


def _artifact_row(article_id: str, storage_article_id: str, artifact_type: str, path: str, now: str) -> dict[str, Any]:
    _ = now
    return {
        "article_id": article_id,
        "storage_article_id": storage_article_id,
        "artifact_type": artifact_type,
        "artifact_path": path,
        "provider": None,
        "source_domain": None,
        "published_date": None,
        "exists_locally": Path(path).exists(),
    }


def _merge_storage_files(*, folder: Path, candidates: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    targets = {
        "article_text": ("article.txt", "txt"),
        "article_json": ("article.json", "json"),
        "article_html": ("article.html", "html"),
    }
    for artifact_type, (filename, key) in targets.items():
        canonical_path = folder / filename
        if _is_nonempty(canonical_path):
            out[key] = str(canonical_path)
            continue
        pool = [
            rec
            for rec in candidates
            if clean_text(rec.get("artifact_type")) == artifact_type and _is_nonempty(Path(str(rec.get("artifact_path") or "")))
        ]
        pool.sort(
            key=lambda r: (
                clean_text(r.get("storage_article_id")) != clean_text(folder.name),
                clean_text(r.get("article_id")) or "",
                clean_text(r.get("artifact_path")) or "",
            )
        )
        if pool:
            src = Path(str(pool[0]["artifact_path"]))
            canonical_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, canonical_path)
            out[key] = str(canonical_path)
    return out


def _is_nonempty(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _verify_rebuild_counts(config: AppConfig) -> dict[str, Any]:
    mapping = load_article_storage_map(config)
    storage = load_storage_articles(config)
    artifacts = load_article_artifacts(config)
    artifact_linked = artifacts[artifacts["storage_article_id"].notna()] if not artifacts.empty else artifacts
    ok = len(mapping) > 0 and len(storage) > 0
    return {
        "ok": bool(ok),
        "mapping_rows": len(mapping),
        "storage_rows": len(storage),
        "artifact_linked_rows": len(artifact_linked),
    }


def _canonical_artifact_safety_check(config: AppConfig) -> dict[str, Any]:
    artifacts = load_article_artifacts(config)
    if artifacts.empty:
        return {
            "ok": True,
            "rows_checked": 0,
            "null_storage_article_id_rows": 0,
            "outside_canonical_cache_rows": 0,
        }
    rows_checked = len(artifacts)
    null_sid = int(artifacts["storage_article_id"].isna().sum()) if "storage_article_id" in artifacts.columns else rows_checked
    expected_prefix = str((config.cache_root / "publisher" / "data").resolve())
    paths = artifacts["artifact_path"].fillna("").astype(str)
    outside = int((~paths.map(lambda p: str(Path(p).resolve()).startswith(expected_prefix) if p else False)).sum())
    ok = (null_sid == 0) and (outside == 0)
    return {
        "ok": ok,
        "rows_checked": rows_checked,
        "null_storage_article_id_rows": null_sid,
        "outside_canonical_cache_rows": outside,
    }


def _cleanup_legacy_cache(config: AppConfig) -> None:
    publisher_root = config.cache_root / "publisher"
    if publisher_root.exists():
        for child in publisher_root.iterdir():
            if child.name == "data":
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)


def _canonicalize_artifact_rows(config: AppConfig, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_prefix = str((config.cache_root / "publisher" / "data").resolve())
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        sid = clean_text(row.get("storage_article_id"))
        path_text = clean_text(row.get("artifact_path"))
        aid = clean_text(row.get("article_id"))
        atype = clean_text(row.get("artifact_type"))
        if not sid or not path_text or not aid or not atype:
            continue
        path_obj = Path(path_text)
        if not path_obj.exists():
            continue
        resolved = str(path_obj.resolve())
        if not resolved.startswith(expected_prefix):
            continue
        key = (aid, sid, atype)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "article_id": aid,
                "storage_article_id": sid,
                "artifact_type": atype,
                "artifact_path": resolved,
                "provider": row.get("provider"),
                "source_domain": row.get("source_domain"),
                "published_date": row.get("published_date"),
                "exists_locally": True,
            }
        )
    out.sort(key=lambda r: (str(r["article_id"]), str(r["storage_article_id"]), str(r["artifact_type"])))
    return out


def _write_parquet(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    df = pd.DataFrame(rows)
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _repair_articles_metadata(articles: pd.DataFrame) -> tuple[dict[str, int], pd.DataFrame]:
    df = articles.copy()
    rows_scanned = len(df)
    repaired_domain = 0
    repaired_section = 0
    repaired_byline = 0
    rows_unchanged = 0
    rows_skipped = 0

    for idx, row in df.iterrows():
        changed_any = False
        unrepairable_any = False

        domain_before = clean_text(row.get("source_domain"))
        if domain_before and ("://" in domain_before or "/" in domain_before):
            repaired = repair_domain(domain_before)
            if repaired:
                if repaired != domain_before:
                    df.at[idx, "source_domain"] = repaired
                    repaired_domain += 1
                    changed_any = True
            else:
                unrepairable_any = True

        section_before = row.get("section")
        section_repaired = _repair_listlike_scalar(section_before, mode="first")
        if section_repaired["attempted"]:
            if section_repaired["value"] is not None:
                if clean_text(section_repaired["value"]) != clean_text(section_before):
                    df.at[idx, "section"] = section_repaired["value"]
                    repaired_section += 1
                    changed_any = True
            else:
                unrepairable_any = True

        byline_before = row.get("byline")
        byline_repaired = _repair_listlike_scalar(byline_before, mode="join")
        if byline_repaired["attempted"]:
            if byline_repaired["value"] is not None:
                if clean_text(byline_repaired["value"]) != clean_text(byline_before):
                    df.at[idx, "byline"] = byline_repaired["value"]
                    repaired_byline += 1
                    changed_any = True
            else:
                unrepairable_any = True

        if not changed_any and not unrepairable_any:
            rows_unchanged += 1
        if unrepairable_any:
            rows_skipped += 1

    summary = {
        "rows_scanned": rows_scanned,
        "rows_repaired_source_domain": repaired_domain,
        "rows_repaired_section": repaired_section,
        "rows_repaired_byline": repaired_byline,
        "rows_unchanged": rows_unchanged,
        "rows_skipped_unrepairable": rows_skipped,
    }
    return summary, df


def _repair_listlike_scalar(value: Any, *, mode: str) -> dict[str, Any]:
    text = clean_text(value)
    if not text:
        return {"attempted": False, "value": value}
    if not (text.startswith("[") and text.endswith("]")):
        return {"attempted": False, "value": value}
    parsed: Any = None
    try:
        parsed = json.loads(text.replace("'", '"'))
    except ValueError:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            parsed = None
    if not isinstance(parsed, list):
        return {"attempted": True, "value": None}
    cleaned = [clean_text(v) for v in parsed]
    cleaned = [v for v in cleaned if v]
    if not cleaned:
        return {"attempted": True, "value": None}
    if mode == "first":
        return {"attempted": True, "value": cleaned[0]}
    return {"attempted": True, "value": ", ".join(cleaned)}


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_url_identity(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    parsed = urlparse(text)
    if not parsed.scheme:
        parsed = urlparse(f"https://{text}")
    host = parsed.netloc.lower().strip()
    if not host:
        return None
    path = parsed.path.rstrip("/")
    return f"{host}{path}"


def repair_domain(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    if parsed.netloc:
        return parsed.netloc.lower()
    return text.split("/", 1)[0].lower()


def _date_part(value: str) -> str:
    return value[:10] if len(value) >= 10 else "1970-01-01"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
