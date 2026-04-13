# RC9 User Testing Report (`py-news-m`)

## Scope and Context
- Candidate baseline: `v0.1.0-rc9`
- Testing mode: comprehensive operator/user-testing pass (read-mostly + evidence generation)
- Project docs reviewed before testing:
  - `AGENTS.md`
  - `docs/TARGET_ARCHITECTURE.md`
  - `docs/STORAGE_LAYOUT.md`
  - `docs/REFDATA_SCHEMA.md`
  - `docs/DATA_SOURCES.md`
  - `docs/WAVE7_2_COMPANION_POLICY.md`
  - `docs/WAVE7_1_RELEASE_EXECUTION_POLICY.md`

## Environment
- Python: `3.13.12`
- CLI entrypoint: `.venv/bin/py-news`
- User-test project root: `/tmp/pynews_rc9_usertest`
- Shared package pin: `m-cache-shared-ext @ git+https://github.com/m-cache/m_cache_shared_ext.git@v0.1.0-rc9`

## Workflows Tested
1. Environment/setup sanity
2. Shared-package consumption sanity (RC9)
3. Article import and lookup refresh/query workflow
4. Article inspect/resolve/fetch-content and resolution-events workflow
5. Audit workflow (pre and post cache layout rebuild)
6. API smoke
7. Shim/facade mode stability checks (`external`, `local`, `auto`)

## Commands Run
```bash
# Environment and pin sanity
.venv/bin/python --version
.venv/bin/py-news --help
cat requirements/m_cache_shared_external.txt

# Shared package install in active venv
export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"
.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"

# Shared shim import sanity
.venv/bin/python - <<'PY'
import inspect
import py_news.m_cache_shared_shim as shim
m = shim.load_shared_augmentation_module()
print(m.__name__)
print(inspect.signature(getattr(m, 'load_json_schema')))
PY

# Refdata/providers
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest refdata refresh
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest providers refresh --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest providers list --json

# Article import + lookup
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest articles import-history --dataset tests/fixtures/articles_sample.csv --adapter local_tabular
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest lookup refresh
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest lookup query --scope articles --limit 5 --json

# Article workflows
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest articles inspect --article-id <ART_ID> --json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest articles resolve --article-id <ART_ID> --representation metadata --local-only --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest articles resolve --article-id <ART_ID> --representation content --local-only --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest articles fetch-content --article-id <ART_ID> --limit 1 --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest resolution events --article-id <ART_ID> --limit 10 --json

# Audit workflows
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit summary --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit cache --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit article --article-id <ART_ID> --json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit provider --provider local_tabular --json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit report --json

# Rebuild + audit rerun
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest cache rebuild-layout --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit summary --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit cache --summary-json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit article --article-id <ART_ID> --json
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest audit provider --provider local_tabular --json

# API serve attempt (socket bind failed in sandbox)
.venv/bin/py-news --project-root /tmp/pynews_rc9_usertest api serve --host 127.0.0.1 --port 8031

# API smoke via tests
pytest -q tests/test_api.py

# Shim/facade stability checks
M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used
M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py
```

## Outcomes
### Passed
- Refdata refresh and provider refresh/list completed successfully.
- Article history import (`local_tabular`) succeeded:
  - loaded rows: 4
  - imported rows: 3
  - skipped rows: 1
- Lookup refresh/query succeeded (3 rows present).
- Article inspect/resolve/content workflow behaved as expected for metadata-only imported data:
  - metadata resolve: `status=ok`, `resolution_source=local_metadata`
  - content resolve: `status=miss`, `reason=content_missing_local`
- Shim/facade validation in all modes passed:
  - external targeted: `3 passed`
  - local full suite: `8 passed`
  - auto full suite: `8 passed`
- API behavior checks passed via repo smoke tests:
  - `pytest -q tests/test_api.py` -> `14 passed`

### Failed / Fragile / Confusing
1. API serve command in this sandbox environment could not bind localhost socket
- Evidence: uvicorn error `could not bind on any address out of [('127.0.0.1', 8031)]`
- Severity: Low
- Likely ownership: test harness/environment
- Impact: prevented direct `curl` live-server smoke; mitigated with in-repo API smoke tests (`tests/test_api.py`).

2. Audit commands immediately after import reported hard failures until cache layout rebuild was run
- Initial hard failures:
  - `missing_article_storage_mapping`
  - `missing_provider_index_file` (provider index parquet files)
- After `cache rebuild-layout`, all audit checks passed (`status=ok` with zero hard failures).
- Severity: Medium (operator ergonomics/document clarity)
- Likely ownership: docs/operator ergonomics (sequence expectation) + workflow clarity
- Impact: new operator may interpret early audit as data corruption rather than incomplete post-import normalization/rebuild step.

## Issue Classification Summary
- Repo-local defects: none confirmed during this pass.
- Shared-package defects: none confirmed during this pass.
- Docs/operator ergonomics: one medium observation (import -> audit sequencing clarity).
- Test harness/environment: one low issue (localhost bind blocked in sandbox).

## Final Status (RC9 User Testing)
- Overall status: **PASS with minor observations**
- Practical operator workflows for article-oriented behavior validated successfully.
- No blocking defects found for promoted RC9 baseline in this pass.
