# Wave 7.2 Companion RC Participation Policy (`py-news-m`)

This document defines the **minimum** Wave 7.2 companion obligations for `py-news-m` in the first real shared RC cycle.

## Companion Scope (Minimal, Participation-Only)

Wave 7.2 companion scope for this repo is limited to:
- pinning the shared RC,
- consuming that RC in the active virtualenv,
- running required validations,
- emitting signoff/evidence inputs to the central bundle,
- confirming rollback readiness.

Out of scope in this pass:
- runtime behavior changes,
- CLI/API semantic changes,
- shared public API broadening,
- cleanup/removal work.

## Repo Role and Frozen Boundaries

`py-news-m` remains a pilot consumer-validator for article workflows only.

The following stay unchanged:
- `py_news/m_cache_shared_shim.py` remains first-hop facade,
- article-only applicability remains unchanged,
- pilot workflow behavior remains unchanged,
- local article targeting/text/storage/idempotency/live write-path ownership remains local,
- `py-news ...` compatibility and `m-cache news ...` additive canonical surfaces remain unchanged.

This repo does not define package governance ownership.

## Exact First Real Local RC-Consumption Step (Portable Default)

Use a repo-relative/configurable local path for the sibling shared package repo.

```bash
export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"
.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"
```

Pin confirmation remains required from this repo:

```bash
cat requirements/m_cache_shared_external.txt
```

## Exact Validation Sequence

Run the following sequence in order:

1. Confirm candidate pin metadata:
```bash
cat requirements/m_cache_shared_external.txt
```
2. Validate shim external consumption path:
```bash
M_CACHE_SHARED_SOURCE=external pytest -q \
  tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present \
  tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing \
  tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used
```
3. Validate full shim source-mode contract:
```bash
M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py
```
4. Validate shared-surface and canonical CLI behavior:
```bash
pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py
```
5. Validate repo baseline safety:
```bash
pytest -q
```

## Canonical Machine-Readable Signoff Output (`SIGNOFF.json`)

`py-news-m` must emit machine-readable signoff output aligned to the package-side contract with **exactly** these fields:
- `candidate_tag`
- `repo`
- `release_role`
- `pin_confirmed`
- `validation_status`
- `signoff_state`
- `blockers`
- `warnings`
- `rollback_ready`

Required value conventions:
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: boolean
- `validation_status`: `pass | fail`
- `signoff_state`: `pass | warn | block`
- `blockers`: array of blocker codes (empty when none)
- `warnings`: array of warning codes (empty when none)
- `rollback_ready`: boolean

Example payload shape:

```json
{
  "candidate_tag": "vX.Y.Z-rcN",
  "repo": "py-news-m",
  "release_role": "pilot_consumer_validator",
  "pin_confirmed": true,
  "validation_status": "pass",
  "signoff_state": "warn",
  "blockers": [],
  "warnings": ["W001_RETRY_RECOVERED"],
  "rollback_ready": true
}
```

## Exact Signoff / Blocker / Warning Mapping

Decision mapping:
- `signoff_state=pass`: `validation_status=pass` and no blockers and no warnings.
- `signoff_state=warn`: `validation_status=pass`, no blockers, warnings present.
- `signoff_state=block`: `validation_status=fail` or any blocker present.

Blocker taxonomy:
- `B001_PIN_MISMATCH`
- `B002_VALIDATION_FAILURE`
- `B003_FACADE_MODE_REGRESSION`
- `B004_ARTICLE_APPLICABILITY_DRIFT`
- `B005_PILOT_WORKFLOW_DRIFT`
- `B006_CLI_API_SEMANTIC_DRIFT`
- `B007_EVIDENCE_INCOMPLETE`
- `B008_BLOCKING_INCIDENT_OPEN`

Warning taxonomy:
- `W001_RETRY_RECOVERED`
- `W002_OPTIONAL_DIAGNOSTIC_MISSING`
- `W003_NON_MATERIAL_DOC_GAP`

## Exact Rollback-Readiness Obligations

`rollback_ready=true` requires confirmation that:
1. repin path is available to prior known-good stable tag (`requirements/m_cache_shared_external.txt`),
2. rerun path for required validations is available after repin,
3. controlled fallback path is available via first-hop facade:
   - `M_CACHE_SHARED_SOURCE=local`
4. no unresolved blocking incident prevents executing 1-3.

If any of the above is missing, set `rollback_ready=false`.

## Exact Central Bundle Input Mapping

Evidence is central-bundle-oriented only; no parallel repo-local release process is introduced.

Default consumer location for this repo input:
- `evidence/candidates/<candidate_tag>/consumer/py-news-m/`

Required files for this repo contribution:
- `SIGNOFF.json` (canonical contract fields above),
- `SIGNOFF.md` (human-readable validation/signoff notes),
- optional incident reference link/file when applicable.

Required field-to-bundle mapping:
- `candidate_tag` -> central candidate metadata linkage,
- `repo` + `release_role` -> consumer identity/role linkage,
- `pin_confirmed` + `validation_status` -> validation ingestion summary,
- `signoff_state` + `blockers` + `warnings` -> decision ingestion inputs,
- `rollback_ready` -> rollback readiness input for promotion/rejection decision.

## User-Testing Handoff Prerequisites (Unchanged Placement)

Comprehensive cross-application user testing begins only after:
- Wave 7.2 implementation is complete,
- one real shared RC cycle has executed,
- central evidence/signoff ingestion has worked end-to-end,
- rollback path has been verified,
- no blocking release-lifecycle incident remains open.

User testing remains post-Wave-7.2 stabilization and pre-cleanup/pre-shim-retirement.
