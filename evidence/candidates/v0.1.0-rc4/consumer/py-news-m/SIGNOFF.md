# SIGNOFF (`py-news-m`, `v0.1.0-rc4`)

## Summary
- `candidate_tag`: `v0.1.0-rc4`
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: `true`
- `validation_status`: `fail`
- `signoff_state`: `block`
- `rollback_ready`: `true`

## RC4 Pin and Local Consumption
- Pin file confirms RC4: `requirements/m_cache_shared_external.txt`
- Local RC consumption command executed in active repo venv:
  - `export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"`
  - `.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"`

## Validation Commands and Outcomes
1. `cat requirements/m_cache_shared_external.txt` -> pass (`v0.1.0-rc4` confirmed)
2. `M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used` -> pass (3/3)
3. `M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py` -> fail (1 failed, 7 passed)
4. `M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
5. `pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py` -> fail (5 failures)
6. `pytest -q tests/test_augmentation_contracts.py` -> fail (6 failures)
7. `pytest -q` -> fail (11 failures)

## Compatibility Distinction
- Previously resolved (`RC2` era) `project_root` issue remains resolved in this rerun (no `unexpected keyword argument 'project_root'` observed).
- Prior `wave_version` blocker is resolved as the active error class (no `unexpected keyword argument 'wave_version'` observed).
- Remaining blocker after the shared-package fix is a schema-loader call-signature mismatch:
  - `TypeError: load_json_schema() takes 0 positional arguments but 1 positional argument (and 2 keyword-only arguments) were given`
  - This impacts augmentation schema-loader contract tests and augmentation CLI submit/status tests.

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
