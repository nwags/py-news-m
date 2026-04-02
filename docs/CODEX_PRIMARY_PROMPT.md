You are refactoring a Python repo named `py-news-m`.

Align implementation to canonical docs and AGENTS.md with priority on:
- fixed canonical normalized storage contracts,
- provider-aware local-first resolution,
- truthful artifact/provenance persistence,
- auditability/reconciliation readiness,
- operator-safe read/query surfaces.

Architectural guardrails:
- `py-news-m` should own ingestion/storage/overlay/query responsibilities.
- Heavy NLP extraction (entity/temporal tagging) belongs to an external augmentation producer unless explicitly directed otherwise.
- Do not drift into in-process extraction implementations by default.

Current-phase constraints:
- `refdata/` authority remains canonical under repo root,
- only `.news_cache/` may relocate,
- provider-aware resolution and canonical cache/index integrity are in scope,
- audit/reconciliation/operator hardening is next-direction work,
- authenticated augmentation submission/storage is future work,
- entity-aware query behavior using stored augmentations is future work,
- monitor/reconciliation runtime redesign is deferred.

Delivery standard:
- code + tests + docs in the same patch when behavior changes,
- machine-readable summaries preserved,
- offline deterministic tests.
