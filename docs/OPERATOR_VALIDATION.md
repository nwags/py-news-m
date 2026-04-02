# OPERATOR_VALIDATION.md

This runbook validates the current provider-aware local-first resolution behavior with explicit remote gating.
It focuses on storage/resolution/provenance correctness and reconciliation readiness in the current phase.

Storage contract reminder:
- canonical normalized authority is fixed at `refdata/normalized/` under repo root,
- only `.news_cache/` may relocate via `PY_NEWS_CACHE_ROOT`.
- cache article files are now stored in publisher/data article folders keyed by `storage_article_id`.

## 1) Environment setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Optional cache relocation:

```bash
export PY_NEWS_CACHE_ROOT=/abs/path/py-news-cache
```

Optional NewsData key for real provider testing:

```bash
export NEWSDATA_API_KEY=your_key_here
```

## 2) Refresh canonical provider/reference data

```bash
py-news refdata refresh
py-news providers refresh --summary-json
py-news providers list --json
```

## 3) Historical import (`nyt_archive`)

```bash
py-news articles import-history \
  --adapter nyt_archive \
  --dataset data/news/history/nyt_archive/nyt_archive_sample.json
```

## 4) Recent-window backfill (`newsdata`)

```bash
py-news articles backfill \
  --provider newsdata \
  --date 2026-03-10 \
  --window-key 1d \
  --max-records 50 \
  --summary-json
```

Notes:
- NewsData requests are bounded to a safe max of `10`; the summary shows requested vs effective values and whether clamping occurred.
- If auth is missing/invalid, operator diagnostics surface `NEWSDATA_API_KEY` as the expected env var name (never the value).

## 5) Refresh lookup projection

```bash
py-news lookup refresh
py-news lookup query --scope articles --limit 5 --json
```

## 5a) Read-only audit/reconciliation checks

```bash
py-news audit summary --summary-json
py-news audit cache --summary-json
py-news audit article --article-id <article_id> --json
py-news audit provider --provider newsdata --json
```

Audit commands are read-only and inspect current state only; they do not rebuild, clean up, or repair canonical authorities/cache.

## 5b) Rebuild cache layout/index (deterministic)

```bash
py-news cache rebuild-layout --summary-json
```

Optional bounded metadata repair during rebuild:

```bash
py-news cache rebuild-layout --repair-metadata --summary-json
```

`--repair-metadata` emits machine-readable counts:
- `rows_scanned`
- `rows_repaired_source_domain`
- `rows_repaired_section`
- `rows_repaired_byline`
- `rows_unchanged`
- `rows_skipped_unrepairable`

Optional cleanup after successful rebuild verification:

```bash
py-news cache rebuild-layout --cleanup-legacy --summary-json
```

Cleanup safety includes canonical artifact checks (no null `storage_article_id`, no artifact paths outside `.news_cache/publisher/data/...`).
Rebuild/cleanup remains separate from audit commands.

## 6) Serve API locally

```bash
py-news api serve --host 127.0.0.1 --port 8000
```

## 7) Local-only article/detail fetch

```bash
curl -s "http://127.0.0.1:8000/articles?limit=5" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>/content" | jq
```

## 8) Explicit remote-assisted fetch

```bash
curl -s "http://127.0.0.1:8000/articles/<article_id>?resolve_remote=true" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>/content?resolve_remote=true" | jq
```

Detail/content responses include explicit resolution reason/strategy/source plus bounded auth diagnostics (`resolution_auth_env_var`, `resolution_auth_configured`) when applicable.

## 9) Inspect canonical resolution provenance

```bash
py-news resolution events --limit 20 --json
py-news resolution events --article-id <article_id> --representation content --json
py-news resolution events --provider newsdata --reason-code metadata_refreshed --json
```

## 10) Inspect single article state

```bash
py-news articles inspect --article-id <article_id> --json
```

Inspection output includes metadata presence, provider/source fields, local artifact state, sidecar path, provider rule summary, and latest resolution events.

## Future augmentation note (not yet implemented)

- Entity/temporal extraction is expected from an external augmentation producer, not from in-process extraction in `py-news-m`.
- Authenticated augmentation submission and stored-overlay entity-aware query behavior are future phases after transfer/design sync from accepted `py-sec-edgar-m`.

## Provenance audit note

`resolution_events.parquet` is append-only historical truth. Audit may report legacy/tmp/missing historical paths as observations, not canonical-state failures by themselves.
