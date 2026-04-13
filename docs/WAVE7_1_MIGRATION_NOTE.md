# Wave 7.1 Migration Note (`py-news-m`)

This note records the Wave 7.1 package-side release-execution hardening outcome for `py-news-m`.

## Scope Implemented

Wave 7.1 in this repo clarifies:
- release execution participation role for RC/stable candidates,
- required validation/signoff obligations,
- central evidence-bundle field mapping,
- blocker vs warning classification,
- rollback/incident participation rules,
- comprehensive user-testing start-gate prerequisites.

## Scope Explicitly Not Implemented

Wave 7.1 in this repo does not implement:
- runtime behavior changes,
- CLI/API semantic changes,
- shared public API broadening,
- cleanup/removal execution.

## Role Clarification

`py-news-m` remains a pilot article consumer-validator/signoff participant.
It does not define external package ownership or package-wide governance policy.

## Frozen Behavior Confirmed

Wave 7.1 preserves:
- `py-news ...` compatibility surface,
- `m-cache news ...` additive canonical surface,
- first-hop facade at `py_news/m_cache_shared_shim.py`,
- article-only applicability and local pilot behavior ownership.

## Wave 7.1 Artifact

Detailed Wave 7.1 participation policy is defined in:
- [`docs/WAVE7_1_RELEASE_EXECUTION_POLICY.md`](WAVE7_1_RELEASE_EXECUTION_POLICY.md)
