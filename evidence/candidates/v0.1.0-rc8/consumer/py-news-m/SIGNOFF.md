# SIGNOFF (`py-news-m`, `v0.1.0-rc8`)

## Summary
- `candidate_tag`: `v0.1.0-rc8`
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: `true`
- `validation_status`: `fail`
- `signoff_state`: `block`
- `rollback_ready`: `true`

## RC8 Pin and Local Consumption
- Pin file confirms RC8: `requirements/m_cache_shared_external.txt`
- Local RC consumption command executed in active repo venv:
  - `export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"`
  - `.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"`

## Validation Commands and Outcomes
1. `cat requirements/m_cache_shared_external.txt` -> pass (`v0.1.0-rc8` confirmed)
2. `M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used` -> pass (3/3)
3. `M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py` -> fail (1 failed, 7 passed)
4. `M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
5. `pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py` -> pass (22/22)
6. `pytest -q tests/test_augmentation_contracts.py` -> pass (6/6)
7. `pytest -q tests/test_m_cache_cli.py::test_m_cache_aug_submit_run_and_status_idempotent tests/test_m_cache_cli.py::test_m_cache_aug_submit_artifact_large_payload_uses_locator tests/test_m_cache_cli.py::test_m_cache_aug_submit_run_schema_validation_error` -> pass (3/3)
8. `pytest -q` -> pass (157 passed, 1 warning)

## Compatibility Distinction
Previously resolved schema-loader and signature issues remain resolved:
- `project_root` mismatch form,
- `wave_version` keyword-shape mismatch form,
- positional schema-name call form,
- missing-`schema_filename` form,
- `v3`/`v4` wave-string normalization form.

Prior remaining packer/CLI drift issues are resolved:
- `pack_run_status_view` positional-call issue resolved,
- `pack_events_view` positional-call issue resolved,
- CLI schema-validation non-zero exit/error-signaling issue resolved.

Remaining blocker in this exact RC8 sequence is isolated to the shim local-mode full-suite run:
- with `M_CACHE_SHARED_SOURCE=local`, `tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present` fails because the suite includes an external-path assertion while local mode is forced.

## Blockers and Warnings
- `blockers`:
  - `B002_VALIDATION_FAILURE`
  - `B003_FACADE_MODE_REGRESSION`
- `warnings`: none

## Rollback Readiness
`rollback_ready=true` is supported by:
1. repin path to prior known-good stable via `requirements/m_cache_shared_external.txt`,
2. rerun path for required validations,
3. controlled fallback path via `M_CACHE_SHARED_SOURCE=local` with first-hop shim unchanged,
4. no open blocking incident preventing these actions.
