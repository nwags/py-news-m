# DATA_SOURCES.md

## Operating assumption

News providers vary by metadata/text availability and API policy.
The system is metadata-first and provider-aware.

## Implemented providers in current phase

### Historical import

- `local_tabular`
- `nyt_archive`

### Recent-window adapters

- `gdelt_recent`
- `newsdata`

## Current source boundaries

Current provider/source inputs in `py-news-m` are article metadata/content retrieval inputs only.

This repo does not currently implement entity/temporal extraction adapters.

Entity/temporal tagging is expected to be produced by an external augmentation producer service in a future augmentation flow, then submitted back through future authenticated augmentation submission surfaces.
That augmentation flow is expected to follow the accepted `py-sec-edgar-m` pattern after transfer/design sync.

## Provider capability modeling

Capability/rule truth is stored in `provider_registry.parquet` and used at runtime.

## Resolution behavior

For a requested article:
1. local metadata check,
2. local artifact check,
3. provider-rule strategy chain,
4. truthful persistence on success,
5. truthful failure provenance on miss/failure.

Direct URL fetch is rule-controlled fallback, not default for all providers.

## Near-term direction

Next implementation direction is audit/reconciliation/operator hardening.
Augmentation ingestion transfer/design sync from accepted `py-sec-edgar-m` follows that.
