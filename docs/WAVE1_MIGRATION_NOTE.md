# Wave 1 Migration Note (`py-news-m`)

This note records the Wave 1 parallel standardization outcome for `py-news-m`.
Wave 1 is additive and compatibility-first.

## What Became Canonical

- Canonical shared command surface added: `m-cache news ...`.
- Canonical config loader added: `m-cache.toml` with Wave 1 precedence (`--config`, `M_CACHE_CONFIG`, repo-local file, legacy env mapping, defaults).
- Canonical provider registry materialization aligned in `refdata/normalized/provider_registry.parquet` with additive shared fields and deterministic local override support.
- Canonical resolution provenance alignment added in `refdata/normalized/resolution_events.parquet` with additive shared columns.
- Canonical reconciliation artifacts are materialized/reserved:
  - `refdata/normalized/reconciliation_events.parquet`
  - `refdata/normalized/reconciliation_discrepancies.parquet`

## What Remains Aliased / Preserved

- `py-news ...` remains the operator compatibility surface.
- Legacy `py-news` default output behavior remains unchanged.
- `m-cache news ...` is additive and provides the canonical Wave 1 shared output surface.
- Provider-aware article storage, provider/content fetch, and provenance behavior remain news-domain specific.
- Audit/reconciliation commands remain read-only.

## Reserved for Later Waves

- Monitor/reconcile operational redesign (beyond reserved shared command-family names).
- Augmentation ingestion/submission and entity-aware query behavior.
- In-process heavy NLP/extraction changes in `py-news-m`.
- Cross-repo shared package extraction.
