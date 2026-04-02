# TARGET_ARCHITECTURE.md

## Goal

Provider-aware, local-first article ingestion/retrieval with canonical normalized authority and truthful local artifact/provenance state.

## Architecture Planes

1. Canonical Article Plane
- Canonical normalized article metadata in `refdata/normalized/articles.parquet`.
- Canonical provider/article identity and stable local article lookup surfaces.

2. Cache/Storage Plane
- Canonical physical article storage under `.news_cache/publisher/data/<publisher_slug>/YYYY/MM/<storage_article_id>/`.
- Canonical storage mapping authorities in `storage_articles.parquet` and `article_storage_map.parquet`.
- Rebuildable provider read/index views under `.news_cache/provider/full-index/<provider_id>/...`.

3. Provider-Aware Resolution Plane
- Local-first article/content resolution with explicit remote gating.
- Provider registry rule authority in `provider_registry.parquet`.

4. Provenance/Reconciliation Plane
- Append-only normalized resolution provenance in `resolution_events.parquet`.
- Read-only reconciliation/audit tooling to inspect canonical authorities and cache/index state consistency.
- Rebuild/cleanup/repair remain explicit separate operator commands, not part of audit commands.

5. Future Augmentation Plane (Not Yet Implemented)
- External augmentation producer reads `py-news-m` read APIs.
- External producer performs entity and temporal-expression extraction.
- Future authenticated augmentation submission into `py-news-m`.
- Canonical augmentation storage inside `py-news-m`.
- Future entity-aware query behavior using stored augmentations.
- Expected design transfer from accepted `py-sec-edgar-m` augmentation pattern after transfer/design sync.

See [`docs/AUGMENTATION_DIRECTION.md`](AUGMENTATION_DIRECTION.md).

## API Behavior (Current)

- `GET /articles` remains bounded local-only.
- `GET /articles/{article_id}` and `GET /articles/{article_id}/content` remain local-first with explicit `resolve_remote=true` gating.
- Operator read/inspection surfaces are focused on storage/resolution/provenance truth.

## Not In Scope Now

- No in-process extraction/NLP inside `py-news-m`.
- No authenticated augmentation write API in this phase.
- No entity-aware query implementation in this phase.
- No monitor/reconciliation redesign in this phase (next work is bounded audit/reconciliation/operator hardening).
