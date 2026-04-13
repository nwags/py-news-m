# Wave 7 Lifecycle Hardening Policy (`py-news-m`)

This document defines **repo-side lifecycle obligations** for Wave 7.

Wave 7 in `py-news-m` is lifecycle hardening only:
- no new extraction scope,
- no runtime behavior changes,
- no CLI/API semantic changes,
- no shared public API broadening.

`py-news-m` remains standalone and backward compatible.

## Role in External Package Lifecycle

`py-news-m` participates as a **pilot article consumer-validator / signoff repo** for `m-cache-shared-ext` release candidates and stable promotions.

`py-news-m` is **not** the owner of external package governance policy.
This repo does not redefine package ownership, approval rights, or external maintainer policy.

Repo role in lifecycle decisions:
- validate pinned RC/stable candidates as a consumer,
- provide repo-side signoff evidence,
- block promotion when repo-side validation fails,
- provide rollback and incident evidence when failures occur.

## Frozen Behavior and Boundaries

Wave 7 keeps the following unchanged:
- `py-news ...` compatibility surface,
- `m-cache news ...` additive canonical surface,
- `py_news/m_cache_shared_shim.py` as first-hop facade,
- article-only applicability (`resource_family=articles`),
- pilot live write-path behavior,
- local ownership of:
  - article target building,
  - text selection and retrieval,
  - storage placement,
  - idempotency logic,
  - live write-path persistence.

## RC and Stable Validation Obligations

For each RC or stable candidate pin, `py-news-m` must validate:
- exact candidate pin in `requirements/m_cache_shared_external.txt`,
- strict-v1 shared API consumption through shim/facade,
- shim/facade behavior in all source modes:
  - `M_CACHE_SHARED_SOURCE=local`
  - `M_CACHE_SHARED_SOURCE=auto`
  - `M_CACHE_SHARED_SOURCE=external`
- unchanged pilot article workflows,
- unchanged CLI/API semantics (`py-news` + `m-cache news`),
- unchanged article-only applicability and authority boundaries.

## Required Evidence for Repo Signoff

A candidate is signoff-eligible only with all of the following evidence:

1. Candidate identity
- candidate tag (`vX.Y.Z-rcN` or stable tag),
- pinned spec string from `requirements/m_cache_shared_external.txt`.

2. Validation command evidence
- test command(s) run,
- pass/fail status,
- date/time and operator.

3. Facade-mode evidence
- result in `local`, `auto`, and `external` modes,
- explicit note that `py_news/m_cache_shared_shim.py` remained first-hop facade.

4. Behavior-freeze evidence
- confirmation of unchanged pilot write-path behavior,
- confirmation of unchanged article-only applicability,
- confirmation of unchanged CLI/API semantics.

5. Decision outcome
- `signoff=pass` or `signoff=blocked`,
- blocker list (if blocked),
- rollback action (if invoked).

## Blocker Conditions

This repo must block RC/stable promotion when any of the following is observed:
- pin mismatch with intended RC/stable candidate,
- missing strict-v1 shared symbols or shim contract break,
- regression in any required shim source mode,
- pilot article workflow regression,
- article-only applicability drift,
- CLI/API semantic drift,
- unresolved incident affecting repo validation reliability.

## Rollback and Incident Handling

Primary rollback steps:
1. repin `requirements/m_cache_shared_external.txt` to prior known-good tag,
2. rerun repo validation against the repinned candidate.

Secondary safety step (when needed):
3. force local facade mode via `M_CACHE_SHARED_SOURCE=local` for controlled recovery,
4. rerun validation to confirm compatibility behavior remains intact.

Incident note requirements:
- failing candidate tag,
- failure symptoms and impacted validation scope,
- blocker classification,
- rollback action taken,
- recovery validation outcome,
- required follow-up before next promotion attempt.

## User-Testing Gate Policy

Policy for Wave 7 and post-Wave-7 stabilization:
- maintainer/developer validation is mandatory for every candidate,
- RC matrix validation is mandatory for every candidate,
- cross-application user testing is mandatory only for **compatibility-impacting** releases,
- cross-application user testing is not mandatory for every routine stable release,
- cross-application user testing never replaces maintainer/developer or RC matrix validation.

Compatibility-impacting includes changes likely to affect:
- facade/import behavior,
- pinning/compatibility semantics,
- shim/fallback behavior,
- upgrade/rollback operator experience,
- cross-repo consumption contracts.

## Deferred Cleanup and Shim Retirement Criteria

Wave 7 defines criteria only; no cleanup executes in this pass.

Explicitly deferred in Wave 7:
- public API broadening,
- immediate shim/fallback removal,
- compatibility-layer pruning,
- domain-logic externalization,
- direct-import cleanup replacing shim-first facade.

Earliest cleanup consideration requires all of:
- multiple successful stable cycles,
- consistently green cross-repo RC/stable validation,
- successful compatibility-impact user testing cycles,
- high rollback confidence demonstrated in practice,
- no violation of repo safety constraints above.
