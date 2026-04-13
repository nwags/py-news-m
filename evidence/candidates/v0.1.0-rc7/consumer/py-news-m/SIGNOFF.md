# SIGNOFF (`py-news-m`, `v0.1.0-rc7`)

## Summary
- `candidate_tag`: `v0.1.0-rc7`
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: `true`
- `validation_status`: `fail`
- `signoff_state`: `block`
- `rollback_ready`: `true`

## RC7 Pin and Local Consumption
- Pin file confirms RC7: `requirements/m_cache_shared_external.txt`
- Local RC consumption command executed in active repo venv:
  - `export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"`
  - `.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"`

## Validation Commands and Outcomes
1. `cat requirements/m_cache_shared_external.txt` -> pass (`v0.1.0-rc7` confirmed)
2. `M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used` -> pass (3/3)
3. `M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py` -> fail (1 failed, 7 passed)
4. `M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
5. `pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py` -> fail (3 failures)
6. `pytest -q tests/test_augmentation_contracts.py` -> pass (6/6)
7. `pytest -q` -> fail (3 failures)

## Compatibility Distinction
- previously resolved `project_root`: still resolved,
- previously resolved `wave_version` keyword-shape issue: still resolved,
- previously resolved positional-call issue (`takes 0 positional arguments` form): still resolved,
- previously resolved missing-`schema_filename` issue: still resolved,
- previously remaining `v3`/`v4` wave-string normalization issue: resolved (`tests/test_augmentation_contracts.py` now passes).

Remaining RC7 news-local blockers are now distinct from schema-loader loading:
- `TypeError: pack_run_status_view() takes 0 positional arguments but 1 was given`
- `TypeError: pack_events_view() takes 0 positional arguments but 1 was given`
- CLI schema-validation behavior drift in `test_m_cache_aug_submit_run_schema_validation_error` (command now exits `0` instead of non-zero).

## Blockers and Warnings
- `blockers`:
  - `B002_VALIDATION_FAILURE`
  - `B006_CLI_API_SEMANTIC_DRIFT`
- `warnings`: none

## Rollback Readiness
`rollback_ready=true` is supported by:
1. repin path to prior known-good stable via `requirements/m_cache_shared_external.txt`,
2. rerun path for required validations,
3. controlled fallback path via `M_CACHE_SHARED_SOURCE=local` with first-hop shim unchanged,
4. no open blocking incident preventing these actions.
