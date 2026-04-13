# SIGNOFF (`py-news-m`, `v0.1.0-rc5`)

## Summary
- `candidate_tag`: `v0.1.0-rc5`
- `repo`: `py-news-m`
- `release_role`: `pilot_consumer_validator`
- `pin_confirmed`: `true`
- `validation_status`: `fail`
- `signoff_state`: `block`
- `rollback_ready`: `true`

## RC5 Pin and Local Consumption
- Pin file confirms RC5: `requirements/m_cache_shared_external.txt`
- Local RC consumption command executed in active repo venv:
  - `export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"`
  - `.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"`

## Validation Commands and Outcomes
1. `cat requirements/m_cache_shared_external.txt` -> pass (`v0.1.0-rc5` confirmed)
2. `M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used` -> pass (3/3)
3. `M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py` -> fail (1 failed, 7 passed)
4. `M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py` -> pass (8/8)
5. `pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py` -> fail (5 failures)
6. `pytest -q tests/test_augmentation_contracts.py` -> fail (6 failures)
7. `pytest -q` -> fail (11 failures)

## Compatibility Distinction
- Previously resolved `project_root` issue remains resolved (no `unexpected keyword argument 'project_root'` observed).
- Previously resolved `wave_version` issue remains resolved (no `unexpected keyword argument 'wave_version'` observed).
- Prior positional-call compatibility issue is **not resolved** in this rerun.
- Current failure signature:
  - `TypeError: load_json_schema() missing 1 required keyword-only argument: 'schema_filename'`
- The installed shared function shape shows the mismatch source:
  - `load_json_schema(wave=None, *, schema_filename, project_root=None, wave_version=None)`
  - consumer calls are still positional-first schema-name style (`load_json_schema(schema_name, ...)`) and thus do not satisfy `schema_filename`.

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
