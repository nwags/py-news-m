# SIGNOFF (`py-news-m`, `v0.1.0-rc9`)

## Summary
- `candidate_tag`: `v0.1.0-rc9`
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: `true`
- `validation_status`: `pass`
- `signoff_state`: `pass`
- `rollback_ready`: `true`

## RC9 Pin and Local Consumption
- Pin file confirms RC9: `requirements/m_cache_shared_external.txt`
- Local RC consumption command executed in active repo venv:
  - `export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"`
  - `.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"`

## Validation Commands and Outcomes
1. `cat requirements/m_cache_shared_external.txt` -> pass (`v0.1.0-rc9` confirmed)
2. `M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used` -> pass (3/3)
3. `M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
4. `M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
5. `pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py` -> pass (22/22)
6. `pytest -q tests/test_augmentation_contracts.py` -> pass (6/6)
7. `pytest -q tests/test_m_cache_cli.py::test_m_cache_aug_submit_run_and_status_idempotent tests/test_m_cache_cli.py::test_m_cache_aug_submit_artifact_large_payload_uses_locator tests/test_m_cache_cli.py::test_m_cache_aug_submit_run_schema_validation_error` -> pass (3/3)
8. `pytest -q` -> pass (157 passed, 1 warning)

## Compatibility Distinction
Previously resolved issues remain resolved:
- schema-loader compatibility (`project_root`, `wave_version` keyword shape, positional schema-name call form, missing `schema_filename`, and `v3`/`v4` wave-string normalization),
- schema-loader signature compatibility,
- packer positional-call compatibility (`pack_run_status_view`, `pack_events_view`),
- CLI validation error-signaling semantics.

RC9 shim/facade correction in this repo:
- shim mode behavior itself was already consistent,
- the remaining local-mode blocker was a mode-agnostic test expectation; `test_shim_uses_external_when_strict_subset_present` now explicitly scopes itself to `external` mode so it no longer conflicts with process-level `M_CACHE_SHARED_SOURCE=local` validation runs.

## Blockers and Warnings
- `blockers`: none
- `warnings`: none

## Rollback Readiness
`rollback_ready=true` is supported by:
1. repin path to prior known-good stable via `requirements/m_cache_shared_external.txt`,
2. rerun path for required validations,
3. controlled fallback path via `M_CACHE_SHARED_SOURCE=local` with first-hop shim unchanged,
4. no open blocking incident preventing these actions.
