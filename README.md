# py-news-m

`py-news-m` is a local-first news article ingestion, storage, and retrieval system.

Current responsibilities:
- canonical normalized article metadata authority under `refdata/normalized/`,
- canonical cache/storage mapping and artifact indexing,
- provider-aware local-first resolution,
- append-only provenance and operator inspection,
- read API surfaces for local-first article access.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest
py-news --help
```

## Canonical Storage Contract

- Canonical normalized authority (fixed under repo root):
  - `refdata/normalized/articles.parquet`
  - `refdata/normalized/article_artifacts.parquet`
  - `refdata/normalized/storage_articles.parquet`
  - `refdata/normalized/article_storage_map.parquet`
  - `refdata/normalized/provider_registry.parquet`
  - `refdata/normalized/source_catalog.parquet`
  - `refdata/normalized/local_lookup_articles.parquet`
  - `refdata/normalized/resolution_events.parquet` (append-only provenance)
- Relocatable cache root:
  - default: `<PROJECT_ROOT>/.news_cache`
  - override: `PY_NEWS_CACHE_ROOT=/abs/path`

Publisher-centered cache layout:

```text
.news_cache/
  publisher/
    data/
      <publisher_slug>/YYYY/MM/<storage_article_id>/
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

`publisher_slug` derivation is deterministic:
1. `source_domain`
2. `source_name`
3. `provider` fallback

`storage_article_id` is a deterministic canonical physical-storage identity and is distinct from provider/local `article_id`.

## Implemented Providers

- Historical import adapters:
  - `local_tabular`
  - `nyt_archive`
- Recent-window metadata adapters:
  - `gdelt_recent`
  - `newsdata`

Recent-window semantics:
- `--date`: end-of-day boundary
- `--window-key`: trailing duration ending at boundary (`1d`, `1h`, `15m`)

## Provider-Aware Resolution

Resolution is local-first and `article_id`-based:
1. local metadata (`articles.parquet`)
2. local artifacts (`article_artifacts.parquet`)
3. provider rule-driven strategies (`provider_registry.parquet`)
4. truthful persistence + provenance

Direct URL fetch is rule-controlled provider fallback, not implicit default.

## API

- `GET /health`
- `GET /articles` (bounded local-only)
- `GET /articles/{article_id}`
- `GET /articles/{article_id}/content`

Detail/content endpoints support explicit remote resolution:
- `?resolve_remote=true`
- default is local-only

## CLI

- `py-news refdata refresh`
- `py-news providers refresh [--summary-json]`
- `py-news providers list [--json]`
- `py-news articles import-history --dataset ... --adapter ...`
- `py-news articles backfill --provider gdelt_recent|newsdata --date YYYY-MM-DD --window-key ...`
- `py-news articles fetch-content ...`
- `py-news articles resolve --article-id ... --representation content|metadata`
- `py-news lookup refresh`
- `py-news lookup query --scope articles`
- `py-news resolution events [filters...]`
- `py-news audit summary [--summary-json]`
- `py-news audit cache [--summary-json]`
- `py-news audit article --article-id ... [--json]`
- `py-news audit provider --provider ... [--json]`
- `py-news audit report [--json] [--ndjson] [--output <path>]`
- `py-news audit compare --left <report.json> --right <report.json> [--json]`
- `py-news audit bundle --output-dir <dir> [--overwrite] [--include-ndjson] [--article-id <id>]... [--provider <provider_id>]...`
- `py-news articles inspect --article-id ... [--json]`
- `py-news cache rebuild-layout [--summary-json] [--cleanup-legacy] [--repair-metadata]`
- `py-news api serve`

See [`docs/OPERATOR_VALIDATION.md`](docs/OPERATOR_VALIDATION.md) for current operator validation workflows.

## Wave 1 Canonical Surface (`m-cache news`)

Wave 1 adds an additive canonical shared surface while preserving `py-news ...` for operators:

- `m-cache news refdata refresh`
- `m-cache news providers refresh`
- `m-cache news providers list`
- `m-cache news articles import-history ...`
- `m-cache news articles backfill ...`
- `m-cache news articles fetch-content ...`
- `m-cache news lookup refresh`
- `m-cache news lookup query ...`
- `m-cache news resolve article ...`
- `m-cache news resolution events ...`
- `m-cache news audit ...`
- `m-cache news cache rebuild-layout ...`
- `m-cache news api serve`

Reserved canonical families for later waves:

- `m-cache news monitor ...`
- `m-cache news reconcile ...`
- `m-cache news storage ...`

Canonical runtime flags are available on `m-cache`:

- `--summary-json`
- `--progress-json`
- `--progress-heartbeat-seconds`
- `--verbose`
- `--quiet`
- `--log-level`
- `--log-file`

`py-news` output defaults remain unchanged for compatibility.

## Canonical Config (`m-cache.toml`)

Wave 1 adds a canonical loader precedence for `m-cache`:

1. `--config PATH`
2. `M_CACHE_CONFIG`
3. `./m-cache.toml`
4. legacy `PY_NEWS_*` env mapping
5. built-in defaults

Canonical effective config is emitted in `m-cache ... --summary-json` runtime summaries.
Legacy `py-news` config/env behavior remains intact.

Wave 1 closure summary:
- [`docs/WAVE1_MIGRATION_NOTE.md`](docs/WAVE1_MIGRATION_NOTE.md)

## Wave 2 Canonical Semantics

Wave 2 keeps behavior compatibility-first while aligning provider/rate-limit/resolve/API transparency semantics on the additive canonical surface:

- canonical provider read surfaces on `m-cache news`:
  - `providers list`
  - `providers show --provider <provider_id>`
- optional read-only helper:
  - `providers explain-resolution ...` (repo-specific, additive)
- explicit resolve modes on `m-cache news resolve article ...`:
  - `local_only`
  - `resolve_if_missing`
  - `refresh_if_stale` (transparent `mode_unsupported` when not supported on this path)
- canonical rate-limit/degradation transparency surfaced in:
  - `--summary-json`
  - `--progress-json`
  - append-only `refdata/normalized/resolution_events.parquet`
- additive API resolution transparency fields on remote-capable detail/content endpoints (while preserving existing endpoint paths and status behavior).

Compatibility remains unchanged:
- `py-news ...` stays the operator compatibility surface with existing defaults preserved.
- `m-cache news ...` remains additive/canonical.

Reserved for later waves:
- monitor/reconcile operational redesign,
- augmentation/in-process extraction expansion,
- cross-repo shared package extraction.

Wave 2 closure summary:
- [`docs/WAVE2_MIGRATION_NOTE.md`](docs/WAVE2_MIGRATION_NOTE.md)

## Wave 3 Augmentation Metadata Plane

Wave 3 adds an additive, read-only augmentation metadata plane for text-bearing article resources:

- augmentation-eligible resources:
  - article metadata text
  - full article content text
- explicitly non-augmentation resources:
  - provider registry
  - lookup artifacts
  - audit/reconciliation artifacts
  - operational metadata artifacts

Canonical additive `m-cache news aug` surfaces in Wave 3:

- `m-cache news aug list-types`
- `m-cache news aug inspect-target --article-id ...`
- `m-cache news aug inspect-runs ...`
- `m-cache news aug inspect-artifacts ...`
- `m-cache news aug submit ...` (reserved placeholder)
- `m-cache news aug status ...` (reserved placeholder)
- `m-cache news aug events ...` (reserved runtime behavior; metadata inspection only)

Canonical Wave 3 outer metadata artifacts:
- `refdata/normalized/augmentation_runs.parquet`
- `refdata/normalized/augmentation_events.parquet`
- `refdata/normalized/augmentation_artifacts.parquet`

Wave 3 also adds additive `augmentation_meta` on existing:
- `GET /articles/{article_id}`
- `GET /articles/{article_id}/content`

This repo standardizes only the outer metadata contract in Wave 3; augmentation payload body schemas remain producer/service-owned.

Wave 3 closure summary:
- [`docs/WAVE3_MIGRATION_NOTE.md`](docs/WAVE3_MIGRATION_NOTE.md)

## Wave 4 Producer-Protocol Pilot

`py-news-m` is treated as a Wave 4 producer-protocol pilot repo.

Wave 4 keeps the repo standalone and compatibility-first while adding bounded producer protocol support on the additive canonical surface:

- canonical augmentation command family:
  - `m-cache news aug inspect-target --article-id ...`
  - `m-cache news aug submit-run --input-json <file.json>`
  - `m-cache news aug submit-artifact --input-json <file.json>`
  - `m-cache news aug status --run-id ...`
  - `m-cache news aug events ...`
- compatibility aliases (preserved):
  - `m-cache news aug submit --kind run --input-json <file.json>`
  - `m-cache news aug submit --kind artifact --input-json <file.json>`
  - `m-cache news aug target-descriptor --article-id ... --text-source metadata|content|auto`
- idempotent status/read-back:
  - `m-cache news aug inspect-runs ...`
  - `m-cache news aug inspect-artifacts ...`

Payload-schema ownership remains producer/service-owned:
- `py-news-m` validates only outer metadata envelopes.
- annotation payload body remains opaque/producer-owned.

Bounded payload policy:
- inline payloads are accepted up to a bounded size,
- larger payloads are locator-backed via sidecar artifact storage.

Wave 4 preserves additive API behavior:
- existing `/articles/{article_id}` and `/articles/{article_id}/content` remain the primary text retrieval and `augmentation_meta` surfaces,
- no broad new augmentation endpoint family is introduced in this phase.

Wave 4 closure summary:
- [`docs/WAVE4_MIGRATION_NOTE.md`](docs/WAVE4_MIGRATION_NOTE.md)

## Wave 5 Shared Package Extraction (Minimal First Cut)

Wave 5 introduces a minimal in-repo `m_cache_shared` package for shared outer protocol/helper code only:

- shared protocol/view models,
- shared augmentation vocabularies,
- shared schema loader/validator helpers,
- pure metadata/view packers,
- thin CLI JSON-input parsing helpers.

Wave 5 preserves current runtime behavior and boundaries:

- live pilot `submit-run` / `submit-artifact` persistence remains local in `py-news-m`,
- article target/text selection/retrieval/storage placement remains local,
- applicability remains article-only for augmentation surfaces,
- operational metadata families remain non-augmentation,
- existing article detail/content API surfaces remain primary.

Wave 5.1 normalization update:

- `m_cache_shared` now exposes the canonical nested augmentation layout under `m_cache_shared/augmentation/...`,
- canonical shared exports are provided via `m_cache_shared.augmentation`,
- flat `m_cache_shared/*` modules are retained as temporary compatibility re-export shims,
- runtime behavior, pilot role, applicability, and authority semantics remain unchanged.

Wave 6 shim-first externalization prep:

- external shared-package Git-tag pin is centralized in `requirements/m_cache_shared_external.txt`,
- shim-first resolution is implemented in `py_news/m_cache_shared_shim.py`,
- external imports use explicit `m_cache_shared_ext.augmentation` (no ambiguous local-vs-external `m_cache_shared` shadowing),
- canonical shim source-mode contract is:
  - `M_CACHE_SHARED_SOURCE={auto|external|local}`
  - `M_CACHE_SHARED_EXTERNAL_ROOT` (default `m_cache_shared_ext.augmentation`)
- current shared RC tag pin in this repo is `v0.1.0-rc9` (Wave 6.1 initially converged on `v0.1.0-rc1`),
- local fallback remains available via shim for one stabilization cycle (`auto` fallback or forced `local` mode),
- strict-common subset only is used for first external public API adoption.

Wave 5 closure summary:
- [`docs/WAVE5_MIGRATION_NOTE.md`](docs/WAVE5_MIGRATION_NOTE.md)

## Wave 7 Lifecycle Hardening (Repo-Side Obligations Only)

Wave 7 is lifecycle hardening only in `py-news-m`:

- no extraction-scope expansion,
- no runtime behavior changes,
- no CLI/API semantic changes,
- no shared public API broadening.

`py-news-m` role in Wave 7:
- pilot article consumer-validator/signoff participant for RC/stable candidates,
- validates compatibility and pilot article workflow behavior for candidate pins,
- does not redefine external package ownership/governance policy.

Wave 7 preserves frozen behavior:
- `py-news ...` remains compatibility surface,
- `m-cache news ...` remains additive canonical surface,
- `py_news/m_cache_shared_shim.py` remains first-hop facade (no direct-import cleanup wave),
- article-only applicability and local pilot write-path ownership remain unchanged.

Wave 7 policy and obligations:
- [`docs/WAVE7_LIFECYCLE_POLICY.md`](docs/WAVE7_LIFECYCLE_POLICY.md)

Wave 7 migration summary:
- [`docs/WAVE7_MIGRATION_NOTE.md`](docs/WAVE7_MIGRATION_NOTE.md)

## Wave 7.1 Release Execution Hardening (Participation-Focused)

Wave 7.1 in `py-news-m` is package-side release-execution hardening only:

- no runtime behavior changes,
- no CLI/API semantic changes,
- no shared public API broadening,
- no cleanup/removal execution in this pass.

`py-news-m` role in Wave 7.1:
- pilot article consumer-validator/signoff participant in shared RC/stable execution,
- validates article workflow and pilot safety behavior for candidate pins,
- provides repo-side signoff/evidence into the central release bundle,
- does not define external package ownership or package-wide governance policy.

Wave 7.1 preserves frozen behavior:
- `py-news ...` remains compatibility surface,
- `m-cache news ...` remains additive canonical surface,
- `py_news/m_cache_shared_shim.py` remains first-hop facade,
- article-only applicability and local article target/text/storage/idempotency/live write-path behavior remain unchanged.

Wave 7.1 policy and obligations:
- [`docs/WAVE7_1_RELEASE_EXECUTION_POLICY.md`](docs/WAVE7_1_RELEASE_EXECUTION_POLICY.md)

Wave 7.1 migration summary:
- [`docs/WAVE7_1_MIGRATION_NOTE.md`](docs/WAVE7_1_MIGRATION_NOTE.md)

## Wave 7.2 Companion RC Participation (Minimal, Participation-Only)

Wave 7.2 in `py-news-m` is a lightweight consumer companion pass for the first real shared RC cycle:

- no runtime behavior changes,
- no CLI/API semantic changes,
- no shared public API broadening,
- no cleanup/removal work.

Companion focus in this repo:
- pin shared RC,
- consume shared RC in active virtualenv via portable local sibling path,
- run required validations,
- emit canonical machine-readable signoff inputs into central bundle,
- confirm rollback readiness.

Wave 7.2 preserves frozen behavior and boundaries:
- `py_news/m_cache_shared_shim.py` remains first-hop facade,
- article-only applicability remains unchanged,
- pilot article behavior remains unchanged,
- local article target/text/storage/idempotency/live write-path ownership remains unchanged,
- `py-news ...` compatibility and `m-cache news ...` additive canonical surfaces remain unchanged.

Wave 7.2 companion policy:
- [`docs/WAVE7_2_COMPANION_POLICY.md`](docs/WAVE7_2_COMPANION_POLICY.md)

Wave 7.2 companion migration note:
- [`docs/WAVE7_2_MIGRATION_NOTE.md`](docs/WAVE7_2_MIGRATION_NOTE.md)

## Read-Only Audit/Reconciliation

Audit commands inspect current canonical authorities and cache/index state as-is:
- normalized coherence across `articles`, `article_storage_map`, `storage_articles`, and `local_lookup_articles`,
- canonical artifact/index coherence in `article_artifacts`,
- storage folder/sidecar coherence under `.news_cache/publisher/data/...`,
- provider full-index coherence under `.news_cache/provider/full-index/...`,
- historical path observations from append-only `resolution_events`.

Audit is read-only and does not trigger rebuild/cleanup/repair.

## Future Augmentation Model

1. An external augmentation producer service reads article data from `py-news-m` APIs.
2. That external service performs entity and temporal-expression extraction/tagging.
3. A future authenticated augmentation submission surface in `py-news-m` will accept augmentation overlays.
4. `py-news-m` will store and serve those overlays for later entity-aware query behavior using stored augmentations.

Direction transfer note: augmentation ingestion/storage/query behavior is expected to transfer from the accepted `py-sec-edgar-m` augmentation design once that transfer is approved for `py-news-m`.

See [`docs/AUGMENTATION_DIRECTION.md`](docs/AUGMENTATION_DIRECTION.md).

## What This Repo Does Not Do Today

- No in-process extraction/NLP tagging in `py-news-m`.
- No authenticated augmentation submission API yet.
- No entity-aware query behavior using stored augmentations yet.

## Near-Term Direction

Next implementation direction is:
1. operator hardening/maintenance reporting surfaces,
2. augmentation-ingestion transfer/design sync from accepted `py-sec-edgar-m`,
3. authenticated augmentation submission/storage,
4. entity-aware query/read behavior using stored augmentations.

Implemented now:
- read-only reconciliation/audit tooling for canonical authorities vs cache/index state.

Not in this phase:
- no in-process extraction, augmentation submission endpoints, or entity-aware query behavior.
- no storage/provider/resolution contract redesign.
- no rebuild/cleanup/repair side effects from audit report/compare/bundle commands.

---

# m-cache Reference Pack v1

This bundle is the **first canonical Codex reference pack** for parallel standardization across:

- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

It is intentionally focused on the first shared slice:

1. command model
2. configuration model
3. provider registry model
4. runtime summary / progress / resolution / reconciliation event models

## Intended use

Use this pack as the **normative source of truth** when implementing the next wave in each repo **in parallel**.

Do not treat this pack as an instruction to immediately merge the codebases.
The intended sequence is:

1. standardize contracts in each repo
2. add compatibility shims
3. compare behavior across repos
4. only then decide which pieces become a true shared package
5. only after that, decide whether to formally merge repos

## Deliverables in this pack

- `CANONICAL_COMMAND_MODEL.md`
- `CANONICAL_CONFIG_SCHEMA.md`
- `CANONICAL_PROVIDER_REGISTRY.md`
- `CANONICAL_RUNTIME_OUTPUT_AND_EVENTS.md`
- `MIGRATION_CHECKLIST.md`
- `CODEX_REFERENCE_PROMPT.md`
- `REFERENCE_MODELS.py`
- `schemas/*.json`
- `examples/*`

## Scope boundaries

This pack standardizes **shared orchestration and contracts**.
It does **not** flatten domain-specific identities or storage semantics.

That means:

- SEC accession semantics remain SEC-specific
- earnings transcript/forecast identities remain earnings-specific
- Fed document/release/series identities remain Fed-specific
- news article storage and provider/content fetch behavior remain news-specific

The shared layer standardizes how those domain-specific behaviors are configured, surfaced, reported, and orchestrated.
