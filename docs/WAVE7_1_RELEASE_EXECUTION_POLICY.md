# Wave 7.1 Release Execution Participation Policy (`py-news-m`)

This document defines `py-news-m` participation in Wave 7.1 package-side RC/stable release execution for `m-cache-shared-ext`.

## Wave 7.1 Intent and Non-Goals

Wave 7.1 is package-side release-execution hardening only for this repo:
- no runtime behavior changes,
- no CLI/API semantic changes,
- no shared public API broadening,
- no cleanup/removal execution in this pass.

`py-news-m` remains standalone and backward compatible.

## Repo Role in Shared Release Execution

`py-news-m` is a pilot article consumer-validator/signoff participant.

This repo:
- validates article workflow and pilot safety behavior against the same RC/stable candidate pin used by all repos,
- emits repo-side signoff evidence into the central release evidence bundle,
- can block promotion when required validation fails.

This repo does not:
- define external package ownership,
- define package-wide governance policy,
- replace package-side release manager decisions.

## Frozen Behavior and Local Ownership

Wave 7.1 preserves:
- `py-news ...` as compatibility surface,
- `m-cache news ...` as additive canonical surface,
- first-hop facade at `py_news/m_cache_shared_shim.py`,
- article-only applicability (`resource_family=articles`),
- local ownership of article target/text/storage/idempotency/live write-path behavior.

## Required Repo Validation and Signoff Obligations

For each RC/stable candidate:
- validate exact candidate pin in `requirements/m_cache_shared_external.txt`,
- validate strict-v1 shared API consumption through shim facade,
- validate facade modes:
  - `M_CACHE_SHARED_SOURCE=local`
  - `M_CACHE_SHARED_SOURCE=auto`
  - `M_CACHE_SHARED_SOURCE=external`
- validate unchanged pilot article workflow behavior,
- validate unchanged article-only applicability boundaries,
- validate unchanged compatibility/canonical command semantics.

Signoff output must include:
- signoff decision (`pass` or `blocked`),
- blocker and warning codes observed,
- incident linkage when applicable.

## Central Evidence-Bundle Integration (Required Fields)

Repo-side evidence must map into the central candidate bundle with these required fields:
- `candidate_identity`: tag + pin spec string,
- `commands_outcomes`: commands run + pass/fail status,
- `facade_mode_evidence`: `local|auto|external` results,
- `invariance_evidence`: pilot workflow + article-only + CLI/API semantic freeze checks,
- `signoff_decision`: `pass|blocked`,
- `blocker_codes`: list (empty if none),
- `warning_codes`: list (empty if none),
- `incident_links`: incident IDs/paths when applicable.

Bundle quality requirement:
- evidence must be reproducible and sufficient for package-side promotion/rejection decisions without ad hoc reinterpretation.

## Blocker vs Warning Classification

Promotion blockers for `py-news-m`:
- `B001_PIN_MISMATCH`: candidate pin does not match intended candidate tag,
- `B002_VALIDATION_FAILURE`: required validation command fails,
- `B003_FACADE_MODE_REGRESSION`: shim/facade fails in `local|auto|external`,
- `B004_ARTICLE_APPLICABILITY_DRIFT`: applicability no longer strict article-only,
- `B005_PILOT_WORKFLOW_DRIFT`: pilot article workflow behavior regresses,
- `B006_CLI_API_SEMANTIC_DRIFT`: compatibility/canonical command semantics drift,
- `B007_EVIDENCE_INCOMPLETE`: required signoff/evidence fields missing,
- `B008_BLOCKING_INCIDENT_OPEN`: unresolved blocking lifecycle incident involving this repo.

Non-blocking warnings for this repo:
- `W001_RETRY_RECOVERED`: transient issue recovered with successful rerun,
- `W002_OPTIONAL_DIAGNOSTIC_MISSING`: non-required diagnostic attachment missing,
- `W003_NON_MATERIAL_DOC_GAP`: wording/doc issue with no validation or behavior impact.

Warnings must still be recorded in signoff evidence but do not block by themselves.

## Rollback and Incident Participation Rules

Repo rollback participation path:
1. repin `requirements/m_cache_shared_external.txt` to prior known-good stable tag,
2. rerun required repo validation/signoff checks,
3. if needed for controlled recovery, force `M_CACHE_SHARED_SOURCE=local`,
4. rerun validation and attach recovery evidence.

Incident participation requirements for this repo:
- record incident severity (`blocking` or `non_blocking`),
- record affected candidate and impact scope,
- record blocker/warning classification,
- record mitigation and recovery evidence,
- record closure criteria for promotion retry.

## Comprehensive User-Testing Start Gate Prerequisites

Comprehensive cross-application user testing begins only after:
- Wave 7.1 implementation is complete,
- one shared RC has executed through the full package-side lifecycle,
- central evidence-bundle flow is usable in practice,
- rollback path has been verified,
- no open blocking lifecycle incident involving this repo remains.

User testing remains:
- a post-Wave-7.1 stabilization gate,
- a pre-cleanup/pre-shim-retirement gate,
- not a replacement for package/repo validation.

## Explicitly Deferred in Wave 7.1

- no shared public API broadening,
- no shim/fallback removal,
- no compatibility-layer pruning,
- no import-root collapse,
- no domain-local logic externalization.

Wave 7.1 defines cleanup entry criteria only; cleanup execution is deferred to a later wave.
