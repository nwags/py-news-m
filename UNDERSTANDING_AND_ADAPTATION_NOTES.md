# Understanding and adaptation notes

## What `py-sec-edgar-m` contributes

`py-sec-edgar-m` is the reusable ingestion backbone.

Its core value is not SEC-specific parsing. Its core value is:

- a staged ingestion pipeline,
- local-first storage and retrieval,
- deterministic storage contracts,
- derived lookup artifacts,
- bounded monitoring,
- one-shot reconciliation,
- operator-friendly CLI workflows,
- machine-readable summaries and observability.

That project is explicitly not trying to be a general research platform or a full semantic reasoning engine. It is an ingestion-first, retrieval-first substrate that makes downstream work possible.

## What `py-earnings-calls-m` demonstrates

`py-earnings-calls-m` is the proof that the architecture can be adapted without changing the basic operating model.

It preserved the same general shape:

1. refresh reference data,
2. import historical bulk data,
3. backfill freshness through explicit adapters,
4. rebuild deterministic lookup tables,
5. serve a small local API,
6. add monitor/reconcile on top.

What changed was the domain contract:

- SEC filings became transcript history + forecast snapshots,
- SEC paths became earnings-specific cache paths,
- accession-based identities became transcript/snapshot identities,
- filing lookup and filing APIs became transcript/forecast lookup and APIs.

That is the main lesson for the news project:

> preserve the ingestion spine, swap the adapters, schemas, path rules, and query surfaces.

## How the news version should differ

News is not a single-source corpus like SEC filings.

It has uneven provider guarantees:

- some sources provide metadata only,
- some provide snippets,
- some provide URLs to original pages,
- some provide full text for only a recent window,
- some provide taxonomic/event labels but not the full article body.

So the news version should be stricter than the prior projects about separating these layers:

1. **article records**
   - canonical metadata row for the article/unit of coverage
2. **article artifacts**
   - raw payloads, fetched HTML, parsed text, JSON sidecars
3. **taxonomy/reference authority**
   - sources, event taxonomies, topic taxonomies, entity aliases
4. **optional downstream extractions**
   - entity mentions, event mentions, summaries, embeddings, forecasts

## Most important design choice for the news project

The normalized `articles.parquet` table should not require full text.

That is the key adaptation required by the source landscape. A news article row may be valid with:

- provider metadata,
- URL,
- title,
- publication timestamp,
- source information,
- language/country/section,
- snippet/summary,
- optional provider event/taxonomy annotations,
- and **no full text yet**.

Full text should be a separate best-effort artifact and may arrive:

- at import time,
- through an explicit content fetch stage,
- or never.

## Narrow initial scope

To keep the first build practical, this guidance assumes the initial repo is a **news article ingestion and retrieval system**, not yet a full event-understanding platform.

That means the initial priority is:

- ingest article metadata and optional text,
- normalize into deterministic parquet,
- support local query/service,
- preserve source-specific annotations when available,
- leave deeper event/entity reasoning additive and downstream.

## Historical bootstrap and freshness split

The first two prior projects suggest the right pattern:

- **history first** through bulk import,
- **freshness second** through explicit adapters.

For news this becomes:

- historical bootstrap: bulk datasets / archive APIs / local tabular inputs,
- incremental freshness: recent-window adapters and bounded polling,
- reconciliation: check expected windows or universe targets against local presence.

## Working initial source strategy

For an initial build, the cleanest structure is:

- bulk/history:
  - GDELT bulk / BigQuery exports
  - NYT archive or Kaggle-style tabular imports
  - local tabular imports
- freshness:
  - GDELT DOC / recent query adapters
  - NYT Article Search / Archive windows
  - NewsData.io or similar recent full-text provider
- later optional enrichers:
  - entity extraction,
  - event extraction,
  - link canonicalization,
  - dedup across sources,
  - embeddings/summaries.

## Practical summary

A good one-line description is:

`py-news-m` should be a local-first, resumable ingestion engine for news article metadata and optional full-text artifacts, built around deterministic parquet outputs, explicit source adapters, lookup-backed API serving, and bounded monitoring/reconciliation.
