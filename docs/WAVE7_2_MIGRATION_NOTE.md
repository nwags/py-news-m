# Wave 7.2 Companion Migration Note (`py-news-m`)

This note records the lightweight Wave 7.2 companion outcome for `py-news-m`.

## Scope Implemented

Wave 7.2 companion updates for this repo define:
- minimum first real shared RC participation obligations,
- portable local RC-consumption step for active virtualenv validation,
- exact validation sequence,
- canonical `SIGNOFF.json` contract fields and `pass|warn|block` signoff state,
- central bundle input mapping at `evidence/candidates/<candidate_tag>/consumer/py-news-m/SIGNOFF.json` and `evidence/candidates/<candidate_tag>/consumer/py-news-m/SIGNOFF.md`,
- rollback-readiness field requirements.

## Scope Explicitly Not Implemented

This pass does not implement:
- runtime behavior changes,
- CLI/API semantic changes,
- shared public API broadening,
- cleanup/removal work.

## Role and Freeze Confirmation

`py-news-m` remains a pilot article consumer-validator/signoff contributor.
It does not define package governance ownership.

The following remain unchanged:
- first-hop facade `py_news/m_cache_shared_shim.py`,
- article-only applicability,
- pilot workflow behavior,
- local article target/text/storage/idempotency/live write-path ownership,
- compatibility/canonical command semantics.

## Wave 7.2 Artifact

Detailed companion obligations are defined in:
- [`docs/WAVE7_2_COMPANION_POLICY.md`](WAVE7_2_COMPANION_POLICY.md)
