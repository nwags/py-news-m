#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${NEWSDATA_API_KEY:-}" ]]; then
  echo "NEWSDATA_API_KEY is not set. Continuing with local-only and mocked/manual steps."
fi

echo "[1/7] Refreshing canonical refdata/provider registry"
py-news refdata refresh
py-news providers refresh --summary-json

echo "[2/7] Importing historical NYT sample"
py-news articles import-history \
  --adapter nyt_archive \
  --dataset data/news/history/nyt_archive/nyt_archive_sample.json

echo "[3/7] Running newsdata backfill"
py-news articles backfill \
  --provider newsdata \
  --date "$(date +%F)" \
  --window-key 1d \
  --max-records 25 \
  --summary-json || true

echo "[4/7] Refreshing lookup projection"
py-news lookup refresh
ARTICLE_ID=$(py-news lookup query --scope articles --limit 1 --json | python -c 'import json,sys; data=json.load(sys.stdin); print(data[0]["article_id"] if data else "")')

if [[ -z "${ARTICLE_ID}" ]]; then
  echo "No article available to inspect."
  exit 0
fi

echo "[5/7] Resolving content for ${ARTICLE_ID}"
py-news articles resolve --article-id "${ARTICLE_ID}" --representation content --summary-json

echo "[6/7] Inspecting normalized provenance"
py-news resolution events --article-id "${ARTICLE_ID}" --limit 5 --json

echo "[7/7] Inspecting single article state"
py-news articles inspect --article-id "${ARTICLE_ID}" --json
