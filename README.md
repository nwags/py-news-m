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
- `py-news articles inspect --article-id ... [--json]`
- `py-news cache rebuild-layout [--summary-json] [--cleanup-legacy] [--repair-metadata]`
- `py-news api serve`

See [`docs/OPERATOR_VALIDATION.md`](docs/OPERATOR_VALIDATION.md) for current operator validation workflows.

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
1. reconciliation/audit tooling,
2. operator hardening/maintenance surfaces,
3. augmentation-ingestion transfer/design sync from accepted `py-sec-edgar-m`,
4. authenticated augmentation submission/storage,
5. entity-aware query/read behavior using stored augmentations.
