"""Read-only reconciliation/audit helpers for canonical authorities and cache/index state."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

import pandas as pd

from py_news.config import AppConfig
from py_news.models import (
    ARTICLE_ARTIFACT_COLUMNS,
    ARTICLES_COLUMNS,
    ARTICLE_STORAGE_MAP_COLUMNS,
    LOOKUP_ARTICLE_COLUMNS,
    RESOLUTION_EVENT_COLUMNS,
    STORAGE_ARTICLES_COLUMNS,
)
from py_news.providers import load_provider_registry
from py_news.storage.paths import normalized_artifact_path, provider_full_index_dir_path

_MAX_SAMPLE = 20
_CANONICAL_ARTICLE_FILES = ("article.html", "article.txt", "article.json")


@dataclass(slots=True)
class _Issue:
    count: int = 0
    sample_ids: set[str] = field(default_factory=set)
    sample_paths: set[str] = field(default_factory=set)


class _IssueBucket:
    def __init__(self, max_sample: int) -> None:
        self._max_sample = max(1, max_sample)
        self._issues: dict[str, _Issue] = {}

    def add(self, issue_code: str, *, sample_id: str | None = None, sample_path: str | None = None, count: int = 1) -> None:
        if not issue_code:
            return
        issue = self._issues.setdefault(issue_code, _Issue())
        issue.count += max(1, int(count))
        if sample_id and len(issue.sample_ids) < self._max_sample:
            issue.sample_ids.add(str(sample_id))
        if sample_path and len(issue.sample_paths) < self._max_sample:
            issue.sample_paths.add(str(sample_path))

    def extend(self, issue_code: str, sample_ids: list[str] | None = None, sample_paths: list[str] | None = None) -> None:
        ids = sample_ids or []
        paths = sample_paths or []
        base_count = max(len(ids), len(paths), 1)
        issue = self._issues.setdefault(issue_code, _Issue())
        issue.count += base_count
        for sample_id in ids:
            if len(issue.sample_ids) >= self._max_sample:
                break
            issue.sample_ids.add(str(sample_id))
        for sample_path in paths:
            if len(issue.sample_paths) >= self._max_sample:
                break
            issue.sample_paths.add(str(sample_path))

    def as_list(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for issue_code in sorted(self._issues):
            issue = self._issues[issue_code]
            out.append(
                {
                    "issue_code": issue_code,
                    "count": int(issue.count),
                    "sample_ids": sorted(issue.sample_ids)[: self._max_sample],
                    "sample_paths": sorted(issue.sample_paths)[: self._max_sample],
                }
            )
        return out

    def total_count(self) -> int:
        return sum(issue.count for issue in self._issues.values())

    def counts_by_issue(self) -> dict[str, int]:
        return {code: int(issue.count) for code, issue in sorted(self._issues.items())}


def run_audit_summary(config: AppConfig, *, max_sample: int = _MAX_SAMPLE) -> dict[str, Any]:
    return _run_audit(config, stage="audit_summary", max_sample=max_sample)


def run_audit_cache(config: AppConfig, *, max_sample: int = _MAX_SAMPLE) -> dict[str, Any]:
    return _run_audit(config, stage="audit_cache", max_sample=max_sample)


def run_audit_article(config: AppConfig, *, article_id: str, max_sample: int = _MAX_SAMPLE) -> dict[str, Any]:
    return _run_audit(config, stage="audit_article", article_id=article_id, max_sample=max_sample)


def run_audit_provider(config: AppConfig, *, provider_id: str, max_sample: int = _MAX_SAMPLE) -> dict[str, Any]:
    return _run_audit(config, stage="audit_provider", provider_id=provider_id, max_sample=max_sample)


def _run_audit(
    config: AppConfig,
    *,
    stage: str,
    article_id: str | None = None,
    provider_id: str | None = None,
    max_sample: int = _MAX_SAMPLE,
) -> dict[str, Any]:
    hard = _IssueBucket(max_sample=max_sample)
    warn = _IssueBucket(max_sample=max_sample)
    obs = _IssueBucket(max_sample=max_sample)

    loaded = _load_authorities(config, hard=hard)
    articles = loaded["articles"]
    artifacts = loaded["article_artifacts"]
    storage = loaded["storage_articles"]
    mapping = loaded["article_storage_map"]
    lookup = loaded["local_lookup_articles"]
    events = loaded["resolution_events"]
    providers = loaded["provider_registry"]

    scope_article = (article_id or "").strip() or None
    scope_provider = (provider_id or "").strip() or None

    if scope_provider:
        known_provider_ids = set(providers["provider_id"].astype(str).tolist()) if not providers.empty else set()
        if known_provider_ids and scope_provider not in known_provider_ids:
            hard.add("unknown_provider", sample_id=scope_provider)

    _check_normalized_coherence(
        articles=articles,
        mapping=mapping,
        storage=storage,
        lookup=lookup,
        hard=hard,
        warn=warn,
        article_id=scope_article,
        provider_id=scope_provider,
    )
    _check_artifact_coherence(
        config=config,
        artifacts=artifacts,
        mapping=mapping,
        storage=storage,
        hard=hard,
        article_id=scope_article,
        provider_id=scope_provider,
    )

    folder_scan = _scan_storage_folders(config)
    _check_storage_and_sidecar_coherence(
        folder_scan=folder_scan,
        storage=storage,
        mapping=mapping,
        hard=hard,
        warn=warn,
        obs=obs,
        article_id=scope_article,
        provider_id=scope_provider,
    )

    provider_index_counts = _check_provider_index_coherence(
        config=config,
        providers=providers,
        articles=articles,
        mapping=mapping,
        artifacts=artifacts,
        hard=hard,
        provider_id=scope_provider,
    )

    _check_provenance_observations(config=config, events=events, obs=obs, article_id=scope_article, provider_id=scope_provider)

    payload: dict[str, Any] = {
        "stage": stage,
        "ok": hard.total_count() == 0,
        "articles_rows": int(len(articles)),
        "lookup_rows": int(len(lookup)),
        "mapping_rows": int(len(mapping)),
        "storage_rows": int(len(storage)),
        "artifact_rows": int(len(artifacts)),
        "resolution_events_rows": int(len(events)),
        "provider_index_counts": provider_index_counts,
        "hard_failures": hard.as_list(),
        "warnings": warn.as_list(),
        "observations": obs.as_list(),
    }
    if scope_article:
        payload["article_id"] = scope_article
        payload["article_exists"] = bool(not articles.empty and (articles["article_id"] == scope_article).any())
        payload["lookup_present"] = bool(not lookup.empty and (lookup["article_id"] == scope_article).any())
        payload["storage_article_ids"] = sorted(
            {
                str(v)
                for v in mapping[mapping["article_id"] == scope_article].get("storage_article_id", []).tolist()
                if str(v).strip() and str(v).strip().lower() not in {"nan", "none"}
            }
        )
    if scope_provider:
        payload["provider"] = scope_provider

    counts = defaultdict(int)
    for bucket in (hard, warn, obs):
        for code, value in bucket.counts_by_issue().items():
            counts[code] += int(value)
    payload["counts_by_issue"] = dict(sorted(counts.items()))

    has_hard = bool(payload["hard_failures"])
    has_warn = bool(payload["warnings"])
    payload["status"] = "fail" if has_hard else ("warn" if has_warn else "ok")
    return payload


def _load_authorities(config: AppConfig, *, hard: _IssueBucket) -> dict[str, pd.DataFrame]:
    required = {
        "articles": ARTICLES_COLUMNS,
        "article_artifacts": ARTICLE_ARTIFACT_COLUMNS,
        "storage_articles": STORAGE_ARTICLES_COLUMNS,
        "article_storage_map": ARTICLE_STORAGE_MAP_COLUMNS,
        "local_lookup_articles": LOOKUP_ARTICLE_COLUMNS,
        "resolution_events": RESOLUTION_EVENT_COLUMNS,
    }
    loaded: dict[str, pd.DataFrame] = {}
    for artifact, columns in required.items():
        path = normalized_artifact_path(config, artifact)
        if not path.exists():
            hard.add("missing_canonical_authority", sample_id=artifact, sample_path=str(path))
            loaded[artifact] = pd.DataFrame(columns=columns)
            continue
        df = pd.read_parquet(path)
        for column in columns:
            if column not in df.columns:
                df[column] = None
        loaded[artifact] = df[columns].copy()

    provider_path = normalized_artifact_path(config, "provider_registry")
    if not provider_path.exists():
        hard.add("missing_canonical_authority", sample_id="provider_registry", sample_path=str(provider_path))
        loaded["provider_registry"] = pd.DataFrame(columns=["provider_id"])
    else:
        loaded["provider_registry"] = load_provider_registry(config)
    return loaded


def _check_normalized_coherence(
    *,
    articles: pd.DataFrame,
    mapping: pd.DataFrame,
    storage: pd.DataFrame,
    lookup: pd.DataFrame,
    hard: _IssueBucket,
    warn: _IssueBucket,
    article_id: str | None,
    provider_id: str | None,
) -> None:
    scoped_articles = articles
    if provider_id and not scoped_articles.empty:
        scoped_articles = scoped_articles[scoped_articles["provider"] == provider_id]
    if article_id and not scoped_articles.empty:
        scoped_articles = scoped_articles[scoped_articles["article_id"] == article_id]

    scoped_mapping = mapping
    if provider_id and not scoped_mapping.empty:
        scoped_mapping = scoped_mapping[scoped_mapping["provider"] == provider_id]
    if article_id and not scoped_mapping.empty:
        scoped_mapping = scoped_mapping[scoped_mapping["article_id"] == article_id]

    article_ids = set(scoped_articles.get("article_id", []).astype(str).tolist()) if not scoped_articles.empty else set()
    storage_ids = set(storage.get("storage_article_id", []).astype(str).tolist()) if not storage.empty else set()

    if article_ids:
        grouped = scoped_mapping.groupby("article_id").size().to_dict() if not scoped_mapping.empty else {}
        missing_ids = sorted([aid for aid in article_ids if grouped.get(aid, 0) == 0])
        duplicate_ids = sorted([aid for aid in article_ids if grouped.get(aid, 0) > 1])
        if missing_ids:
            hard.extend("missing_article_storage_mapping", sample_ids=missing_ids[:_MAX_SAMPLE])
        if duplicate_ids:
            hard.extend("duplicate_article_storage_mapping", sample_ids=duplicate_ids[:_MAX_SAMPLE])

    if not scoped_mapping.empty:
        for row in scoped_mapping.to_dict(orient="records"):
            sid = _clean_text(row.get("storage_article_id"))
            if not sid or sid not in storage_ids:
                hard.add(
                    "mapping_storage_missing",
                    sample_id=_clean_text(row.get("article_id")),
                )

    scoped_lookup = lookup
    if article_id and not scoped_lookup.empty:
        scoped_lookup = scoped_lookup[scoped_lookup["article_id"] == article_id]
    if provider_id and not scoped_lookup.empty:
        scoped_lookup = scoped_lookup[scoped_lookup["provider"] == provider_id]

    if not scoped_lookup.empty:
        for aid in scoped_lookup["article_id"].astype(str).tolist():
            if aid not in article_ids:
                hard.add("lookup_missing_article", sample_id=aid)

    # Drift warning uses global parity to keep signal stable for operators.
    if len(lookup) != len(articles):
        warn.add("lookup_row_count_drift")

    if article_id and article_ids and not scoped_lookup.empty:
        pass
    if article_id and article_ids and scoped_lookup.empty:
        hard.add("lookup_article_missing", sample_id=article_id)


def _check_artifact_coherence(
    *,
    config: AppConfig,
    artifacts: pd.DataFrame,
    mapping: pd.DataFrame,
    storage: pd.DataFrame,
    hard: _IssueBucket,
    article_id: str | None,
    provider_id: str | None,
) -> None:
    if artifacts.empty:
        return

    scoped = artifacts
    if article_id:
        scoped = scoped[scoped["article_id"] == article_id]
    if provider_id and not scoped.empty:
        provider_article_ids = set(
            mapping[mapping["provider"] == provider_id].get("article_id", []).astype(str).tolist()
            if not mapping.empty
            else []
        )
        scoped = scoped[scoped["article_id"].isin(provider_article_ids)]

    storage_ids = set(storage.get("storage_article_id", []).astype(str).tolist()) if not storage.empty else set()
    valid_pairs = set()
    if not mapping.empty:
        for row in mapping.to_dict(orient="records"):
            aid = _clean_text(row.get("article_id"))
            sid = _clean_text(row.get("storage_article_id"))
            if aid and sid:
                valid_pairs.add((aid, sid))

    storage_folder_map = {}
    if not storage.empty:
        for row in storage.to_dict(orient="records"):
            sid = _clean_text(row.get("storage_article_id"))
            folder = _clean_text(row.get("storage_folder_path"))
            if sid and folder:
                storage_folder_map[sid] = str(Path(folder).resolve())

    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    for row in scoped.to_dict(orient="records"):
        aid = _clean_text(row.get("article_id"))
        sid = _clean_text(row.get("storage_article_id"))
        path_text = _clean_text(row.get("artifact_path"))
        if not sid:
            hard.add("null_storage_article_id", sample_id=aid, sample_path=path_text)
            continue
        if not path_text:
            hard.add("artifact_file_missing", sample_id=aid)
            continue

        resolved_path = str(Path(path_text).resolve())
        if not resolved_path.startswith(canonical_prefix):
            hard.add("outside_canonical_cache_path", sample_id=aid, sample_path=resolved_path)
            continue
        if not Path(resolved_path).exists():
            hard.add("artifact_file_missing", sample_id=aid, sample_path=resolved_path)
            continue
        if sid not in storage_ids:
            hard.add("unknown_storage_article_id", sample_id=aid, sample_path=resolved_path)
            continue
        if (aid or "", sid) not in valid_pairs:
            hard.add("invalid_article_storage_pairing", sample_id=aid, sample_path=resolved_path)
        folder = storage_folder_map.get(sid)
        if folder and not Path(folder).exists():
            hard.add("artifact_storage_folder_missing", sample_id=aid, sample_path=folder)


def _scan_storage_folders(config: AppConfig) -> dict[str, Any]:
    base = config.cache_root / "publisher" / "data"
    folders: list[Path] = []
    if base.exists():
        for publisher_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
            for year_dir in sorted([p for p in publisher_dir.iterdir() if p.is_dir()]):
                for month_dir in sorted([p for p in year_dir.iterdir() if p.is_dir()]):
                    for article_dir in sorted([p for p in month_dir.iterdir() if p.is_dir()]):
                        folders.append(article_dir.resolve())
    files_by_folder: dict[str, dict[str, str]] = {}
    for folder in folders:
        current: dict[str, str] = {}
        for filename in ("meta.json", *_CANONICAL_ARTICLE_FILES):
            p = folder / filename
            if p.exists():
                current[filename] = str(p.resolve())
        files_by_folder[str(folder)] = current

    stray_meta: list[str] = []
    if config.cache_root.exists():
        for path in sorted(config.cache_root.rglob("meta.json")):
            resolved = str(path.resolve())
            if not resolved.startswith(str(base.resolve())):
                stray_meta.append(resolved)
    return {
        "base": str(base.resolve()),
        "folders": [str(p) for p in folders],
        "files_by_folder": files_by_folder,
        "stray_meta": stray_meta,
    }


def _check_storage_and_sidecar_coherence(
    *,
    folder_scan: dict[str, Any],
    storage: pd.DataFrame,
    mapping: pd.DataFrame,
    hard: _IssueBucket,
    warn: _IssueBucket,
    obs: _IssueBucket,
    article_id: str | None,
    provider_id: str | None,
) -> None:
    storage_by_folder: dict[str, dict[str, Any]] = {}
    storage_rows = storage.to_dict(orient="records") if not storage.empty else []
    for row in storage_rows:
        sid = _clean_text(row.get("storage_article_id"))
        folder = _clean_text(row.get("storage_folder_path"))
        if not sid or not folder:
            continue
        storage_by_folder[str(Path(folder).resolve())] = row
        if not Path(folder).exists():
            hard.add("storage_folder_missing_for_storage_row", sample_id=sid, sample_path=str(Path(folder).resolve()))

    mapping_by_storage: dict[str, set[str]] = defaultdict(set)
    mapping_rows = mapping.to_dict(orient="records") if not mapping.empty else []
    for row in mapping_rows:
        sid = _clean_text(row.get("storage_article_id"))
        aid = _clean_text(row.get("article_id"))
        provider = _clean_text(row.get("provider"))
        if not sid or not aid:
            continue
        if provider_id and provider != provider_id:
            continue
        if article_id and aid != article_id:
            continue
        mapping_by_storage[sid].add(aid)

    for meta_path in folder_scan["stray_meta"]:
        warn.add("orphan_meta_sidecar", sample_path=meta_path)

    for folder in folder_scan["folders"]:
        files = folder_scan["files_by_folder"].get(folder, {})
        row = storage_by_folder.get(folder)
        if row is None:
            warn.add("orphan_storage_folder", sample_path=folder)
            continue

        sid = _clean_text(row.get("storage_article_id")) or ""
        if article_id or provider_id:
            mapped_ids = mapping_by_storage.get(sid, set())
            if not mapped_ids:
                continue

        has_content = any(name in files for name in _CANONICAL_ARTICLE_FILES)
        meta_path = files.get("meta.json")
        if meta_path and not has_content:
            obs.add("metadata_only_folder", sample_id=sid, sample_path=meta_path)

        if not meta_path:
            continue
        try:
            payload = json.loads(Path(meta_path).read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("sidecar_not_object")
        except (OSError, ValueError, json.JSONDecodeError):
            warn.add("invalid_meta_sidecar", sample_id=sid, sample_path=meta_path)
            continue

        sidecar_sid = _clean_text(payload.get("storage_article_id"))
        if sidecar_sid and sidecar_sid != sid:
            warn.add("sidecar_mapping_conflict", sample_id=sid, sample_path=meta_path)
        sidecar_ids = payload.get("mapped_article_ids")
        if isinstance(sidecar_ids, list):
            expected = sorted(mapping_by_storage.get(sid, set()))
            current = sorted({_clean_text(v) for v in sidecar_ids if _clean_text(v)})
            if expected and current != expected:
                warn.add("sidecar_mapping_conflict", sample_id=sid, sample_path=meta_path)


def _check_provider_index_coherence(
    *,
    config: AppConfig,
    providers: pd.DataFrame,
    articles: pd.DataFrame,
    mapping: pd.DataFrame,
    artifacts: pd.DataFrame,
    hard: _IssueBucket,
    provider_id: str | None,
) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    canonical_provider_ids = sorted({str(v) for v in mapping.get("provider", []).tolist() if str(v).strip()}) if not mapping.empty else []
    index_provider_ids: list[str] = []
    base = config.cache_root / "provider" / "full-index"
    if base.exists():
        by_slug: dict[str, str] = {}
        if not providers.empty:
            for pid in providers["provider_id"].astype(str).tolist():
                by_slug[(provider_full_index_dir_path(config, provider_id=pid)).name] = pid
        for entry in sorted([p for p in base.iterdir() if p.is_dir()]):
            mapped = by_slug.get(entry.name)
            if mapped:
                index_provider_ids.append(mapped)
    if provider_id:
        provider_ids = [provider_id]
    else:
        provider_ids = sorted(set(canonical_provider_ids) | set(index_provider_ids))
    if not provider_ids:
        return counts

    article_cols = [
        "article_id",
        "provider",
        "resolved_document_identity",
        "storage_article_id",
        "mapping_basis",
        "mapped_at",
        "canonical_url",
        "url",
        "source_domain",
        "title",
        "published_at",
    ]

    for pid in provider_ids:
        map_rows = mapping[mapping["provider"] == pid] if not mapping.empty else pd.DataFrame(columns=ARTICLE_STORAGE_MAP_COLUMNS)
        art_rows = articles[articles["provider"] == pid] if not articles.empty else pd.DataFrame(columns=ARTICLES_COLUMNS)
        expected_map = map_rows.merge(
            art_rows[["article_id", "provider", "canonical_url", "url", "source_domain", "title", "published_at"]],
            on=["article_id", "provider"],
            how="left",
        )
        for col in article_cols:
            if col not in expected_map.columns:
                expected_map[col] = None
        expected_map = expected_map[article_cols].copy().fillna("")

        expected_artifacts = artifacts.merge(
            map_rows[["article_id", "storage_article_id"]],
            on=["article_id", "storage_article_id"],
            how="inner",
        ) if not artifacts.empty else pd.DataFrame(columns=ARTICLE_ARTIFACT_COLUMNS)
        for col in ARTICLE_ARTIFACT_COLUMNS:
            if col not in expected_artifacts.columns:
                expected_artifacts[col] = None
        expected_artifacts = expected_artifacts[ARTICLE_ARTIFACT_COLUMNS].copy().fillna("")

        idx_dir = provider_full_index_dir_path(config, provider_id=pid)
        map_path = idx_dir / "article_map.parquet"
        art_path = idx_dir / "artifact_index.parquet"

        if not map_path.exists():
            hard.add("missing_provider_index_file", sample_id=pid, sample_path=str(map_path.resolve()))
            actual_map = pd.DataFrame(columns=article_cols)
        else:
            actual_map = pd.read_parquet(map_path)
            for col in article_cols:
                if col not in actual_map.columns:
                    actual_map[col] = None
            actual_map = actual_map[article_cols].copy().fillna("")

        if not art_path.exists():
            hard.add("missing_provider_index_file", sample_id=pid, sample_path=str(art_path.resolve()))
            actual_artifacts = pd.DataFrame(columns=ARTICLE_ARTIFACT_COLUMNS)
        else:
            actual_artifacts = pd.read_parquet(art_path)
            for col in ARTICLE_ARTIFACT_COLUMNS:
                if col not in actual_artifacts.columns:
                    actual_artifacts[col] = None
            actual_artifacts = actual_artifacts[ARTICLE_ARTIFACT_COLUMNS].copy().fillna("")

        counts[pid] = {
            "article_map_rows": int(len(actual_map)),
            "artifact_index_rows": int(len(actual_artifacts)),
            "expected_article_map_rows": int(len(expected_map)),
            "expected_artifact_index_rows": int(len(expected_artifacts)),
        }

        expected_map_set = _row_set(expected_map, article_cols)
        actual_map_set = _row_set(actual_map, article_cols)
        missing_map = sorted(expected_map_set - actual_map_set)
        extra_map = sorted(actual_map_set - expected_map_set)
        if missing_map or extra_map:
            hard.add("provider_article_map_mismatch", sample_id=pid)

        expected_art_set = _row_set(expected_artifacts, ARTICLE_ARTIFACT_COLUMNS)
        actual_art_set = _row_set(actual_artifacts, ARTICLE_ARTIFACT_COLUMNS)
        missing_art = sorted(expected_art_set - actual_art_set)
        extra_art = sorted(actual_art_set - expected_art_set)
        if missing_art or extra_art:
            hard.add("provider_artifact_index_mismatch", sample_id=pid)

        canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
        if not actual_artifacts.empty:
            for path_value in actual_artifacts["artifact_path"].astype(str).tolist():
                if path_value and not str(Path(path_value).resolve()).startswith(canonical_prefix):
                    hard.add("provider_index_path_outside_canonical_cache", sample_id=pid, sample_path=str(Path(path_value).resolve()))

    return counts


def _check_provenance_observations(
    *,
    config: AppConfig,
    events: pd.DataFrame,
    obs: _IssueBucket,
    article_id: str | None,
    provider_id: str | None,
) -> None:
    if events.empty:
        return
    scoped = events
    if article_id:
        scoped = scoped[scoped["article_id"] == article_id]
    if provider_id:
        scoped = scoped[scoped["provider"] == provider_id]
    if scoped.empty:
        return

    canonical_prefix = str((config.cache_root / "publisher" / "data").resolve())
    for row in scoped.to_dict(orient="records"):
        for field in ("artifact_path", "meta_sidecar_path"):
            path_value = _clean_text(row.get(field))
            if not path_value:
                continue
            resolved = str(Path(path_value).resolve())
            if "/tmp/" in resolved or "pytest" in resolved:
                obs.add("historical_tmp_or_pytest_path", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)
            if ".news_cache" in resolved and not resolved.startswith(canonical_prefix):
                obs.add("historical_legacy_cache_path", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)
            if field == "meta_sidecar_path":
                if "/meta.json" not in resolved or not resolved.startswith(canonical_prefix):
                    obs.add("historical_sidecar_outside_canonical_layout", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)
            if not Path(resolved).exists():
                obs.add("historical_path_missing_now", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)

        provenance = row.get("provenance_json")
        parsed = None
        if isinstance(provenance, str) and provenance.strip():
            try:
                loaded = json.loads(provenance)
                if isinstance(loaded, dict):
                    parsed = loaded
            except ValueError:
                parsed = None
        if parsed:
            for value in _iter_path_like_strings(parsed):
                resolved = str(Path(value).resolve())
                if "/tmp/" in resolved or "pytest" in resolved:
                    obs.add("historical_tmp_or_pytest_path", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)
                if ".news_cache" in resolved and not resolved.startswith(canonical_prefix):
                    obs.add("historical_legacy_cache_path", sample_id=_clean_text(row.get("event_id")), sample_path=resolved)


def _iter_path_like_strings(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for value in current.values():
                stack.append(value)
            continue
        if isinstance(current, list):
            stack.extend(current)
            continue
        if isinstance(current, str):
            text = current.strip()
            if "/" in text or ".news_cache" in text:
                out.append(text)
    return out


def _row_set(df: pd.DataFrame, columns: list[str]) -> set[tuple[str, ...]]:
    if df.empty:
        return set()
    tuples = []
    for row in df[columns].to_dict(orient="records"):
        tuples.append(tuple(str(row.get(col) or "").strip() for col in columns))
    return set(tuples)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    return text
