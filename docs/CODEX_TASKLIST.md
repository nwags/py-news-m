# CODEX_TASKLIST.md

## Current objective

Operator Maintenance / Reporting Hardening on top of implemented read-only audit/reconciliation, including deterministic operator audit bundle export.

Wave 1 reference:
- [`WAVE1_MIGRATION_NOTE.md`](WAVE1_MIGRATION_NOTE.md)
Wave 2 reference:
- [`WAVE2_MIGRATION_NOTE.md`](WAVE2_MIGRATION_NOTE.md)
Wave 3 reference:
- [`WAVE3_MIGRATION_NOTE.md`](WAVE3_MIGRATION_NOTE.md)
Wave 4 reference:
- [`WAVE4_MIGRATION_NOTE.md`](WAVE4_MIGRATION_NOTE.md)

## Current implemented foundation (unchanged by this phase)

- Canonical normalized authorities under repo-root `refdata/normalized`.
- Provider-aware local-first article/content resolution with explicit remote gating.
- Canonical storage mapping and canonical current artifact index.
- Append-only normalized resolution provenance.
- Operator inspection surfaces for resolution/provenance/cache state.
- Read-only audit/reconciliation CLI for canonical authorities vs cache/index state.

## Next implementation phases (ordered)

1. Operator maintenance/reporting hardening (active).
2. Augmentation-ingestion transfer/design sync from accepted `py-sec-edgar-m`.
3. Authenticated augmentation submission/storage.
4. Entity-aware query/read behavior using stored augmentations.

## Superseded direction

- In-process extraction inside `py-news-m` is superseded by the external augmentation producer model.
- Augmentation implementation direction in this repo follows transfer/design sync from accepted `py-sec-edgar-m` once approved.

## Deferred

- Authenticated augmentation submission API (future phase).
- Canonical augmentation storage/query surfaces (future phase).
- Entity-aware query behavior using stored augmentations (future phase).
- Monitor/reconciliation runtime redesign.
