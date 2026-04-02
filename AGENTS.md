# AGENTS.md

This repository is a local-first news article ingestion and retrieval system.

The system should prioritize:

1. correctness and repeatability,
2. bounded and polite HTTP behavior,
3. explicit source-adapter boundaries,
4. canonical local storage contracts,
5. parquet-first normalized outputs,
6. reproducible local development,
7. machine-readable operator summaries.

## Non-negotiables

- Do **not** hard-code business logic to one news provider.
- Do **not** assume historical news import and incremental freshness come from the same source.
- Do **not** assume full article text is always available.
- Do **not** mix raw source payloads with normalized parquet outputs.
- Do **not** allow unbounded parallelism.
- Do **not** make the core ingestion path depend on downstream NLP or LLM extraction.
- Prefer typed, testable modules over hidden global state.
- Prefer additive adapters over source-specific hacks in the CLI layer.

## Source of truth hierarchy

If there is conflict, use this order:

1. `docs/TARGET_ARCHITECTURE.md`
2. `docs/STORAGE_LAYOUT.md`
3. `docs/REFDATA_SCHEMA.md`
4. `docs/DATA_SOURCES.md`
5. tests
6. legacy placeholders

## Required architecture direction

Implement a staged pipeline:

1. refresh or load reference data and taxonomies,
2. bulk-import article history,
3. backfill or refresh article metadata/content through adapters,
4. build deterministic lookup artifacts,
5. expose a small local-first API surface,
6. add monitoring / reconciliation after ingestion foundations are reliable,
7. keep downstream extraction additive.

## Expected new repo capabilities

- import article history from local bulk datasets without code edits,
- fetch and persist recent article metadata from explicit adapters,
- fetch and persist article text when available,
- maintain deterministic lookup artifacts for local retrieval,
- preserve provider-native taxonomic/event annotations when available,
- support future event/entity extraction workflows downstream.

## Delivery standard

Every meaningful change should include:

- code,
- tests,
- doc updates,
- a short migration note when behavior changes.
