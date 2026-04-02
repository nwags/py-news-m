# STORAGE_LAYOUT.md

## Purpose

Define canonical local storage contracts for normalized authority and relocatable cache artifacts.
This document describes the current stable storage contract. The current phase does not change storage architecture.

## Root Rules

- Canonical normalized authority is fixed under repo root:
  - `refdata/normalized/...`
- Only cache may relocate:
  - default: `<PROJECT_ROOT>/.news_cache`
  - override: `PY_NEWS_CACHE_ROOT`
- `PY_NEWS_PROJECT_ROOT` is deprecated for relocation.

## Cache Layout (Publisher-Centered Article Folders)

```text
.news_cache/
  publisher/
    data/
      <publisher_slug>/
        YYYY/
          MM/
            <storage_article_id>/
              article.html
              article.txt
              article.json
              meta.json
  provider/
    full-index/
      <provider_id>/
        article_map.parquet
        artifact_index.parquet
```

Rules:
- physical article files are colocated in one per-article folder,
- `meta.json` lives inside the article folder,
- provider full-index is read/index only and references canonical storage (does not duplicate article files),
- acquisition/window/query provenance remains in normalized `resolution_events.parquet` and article-folder meta state.
- `article_artifacts.parquet` is canonical current local artifact index and is rebuilt from current canonical storage state.
- legacy dead artifact rows are cleaned from canonical current artifact state during rebuild/repair.
- metadata-only folders (only `meta.json`) are expected for metadata-only articles.

`publisher_slug` derivation order:
1. `source_domain`
2. `source_name`
3. `provider` fallback

## Canonical Normalized Artifacts

- `refdata/normalized/source_catalog.parquet`
- `refdata/normalized/provider_registry.parquet`
- `refdata/normalized/articles.parquet`
- `refdata/normalized/article_artifacts.parquet`
- `refdata/normalized/storage_articles.parquet`
- `refdata/normalized/article_storage_map.parquet`
- `refdata/normalized/local_lookup_articles.parquet`
- `refdata/normalized/resolution_events.parquet` (append-only)

## Design Rules

- `articles.parquet` is canonical metadata authority.
- `article_artifacts.parquet` is canonical local artifact index authority.
- `storage_articles.parquet` is canonical storage-folder authority.
- `article_storage_map.parquet` is canonical provider/local-article to storage mapping.
- `resolution_events.parquet` is canonical append-only resolution provenance authority.
- Per-article `meta.json` sidecars are local cache state, not the sole provenance authority.

## Future Direction (Not Yet Implemented)

- Future augmentation data will be submitted from an external augmentation producer through authenticated augmentation submission in a later phase.
- Canonical augmentation storage/query behavior is not implemented yet and does not alter the current storage contract in this phase.

## Audit Semantics

- `py-news audit ...` commands are read-only inspections of current canonical authorities/cache/index state.
- Audit does not run rebuild, cleanup, or repair actions.
- Historical provenance path references may include legacy/tmp paths and are reported as observations.
