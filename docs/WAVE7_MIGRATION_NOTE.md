# Wave 7 Migration Note (`py-news-m`)

This note records the Wave 7 lifecycle-hardening outcome for `py-news-m`.

## Scope Implemented

Wave 7 in this repo is lifecycle hardening only:
- governance participation contract clarified,
- RC/stable evidence obligations clarified,
- rollback/incident handling clarified,
- user-testing gate policy clarified,
- shim-retirement criteria clarified.

## Scope Explicitly Not Implemented

Wave 7 does not implement:
- extraction-scope expansion,
- runtime behavior changes,
- CLI/API semantic changes,
- shared public API broadening,
- shim/fallback removals,
- compatibility-layer pruning,
- domain-logic externalization.

## Repo Role Clarification

`py-news-m` remains a pilot article consumer-validator/signoff participant for external package RC/stable candidates.

This repo does not redefine ownership of `m-cache-shared-ext` governance policy.

## Frozen Behavior Confirmed

Wave 7 preserves:
- `py-news ...` compatibility surface,
- `m-cache news ...` additive canonical surface,
- first-hop facade at `py_news/m_cache_shared_shim.py`,
- article-only applicability,
- local ownership of article target/text/storage/idempotency/live write-path behavior.

## Wave 7 Artifact

Detailed lifecycle obligations are defined in:
- [`docs/WAVE7_LIFECYCLE_POLICY.md`](WAVE7_LIFECYCLE_POLICY.md)
