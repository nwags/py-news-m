# OPERATOR_VALIDATION.md

This runbook validates the current provider-aware local-first resolution behavior with explicit remote gating.
It focuses on storage/resolution/provenance correctness and reconciliation readiness in the current phase.

Storage contract reminder:
- canonical normalized authority is fixed at `refdata/normalized/` under repo root,
- only `.news_cache/` may relocate via `PY_NEWS_CACHE_ROOT`.
- cache article files are now stored in publisher/data article folders keyed by `storage_article_id`.

## 1) Environment setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Optional cache relocation:

```bash
export PY_NEWS_CACHE_ROOT=/abs/path/py-news-cache
```

Optional NewsData key for real provider testing:

```bash
export NEWSDATA_API_KEY=your_key_here
```

## 2) Refresh canonical provider/reference data

```bash
py-news refdata refresh
py-news providers refresh --summary-json
py-news providers list --json
```

Canonical Wave 1 equivalents:

```bash
m-cache --summary-json news refdata refresh
m-cache --summary-json news providers refresh
m-cache news providers list --json
m-cache news providers show --provider newsdata --json
```

## 3) Historical import (`nyt_archive`)

```bash
py-news articles import-history \
  --adapter nyt_archive \
  --dataset data/news/history/nyt_archive/nyt_archive_sample.json
```

## 4) Recent-window backfill (`newsdata`)

```bash
py-news articles backfill \
  --provider newsdata \
  --date 2026-03-10 \
  --window-key 1d \
  --max-records 50 \
  --summary-json
```

Notes:
- NewsData requests are bounded to a safe max of `10`; the summary shows requested vs effective values and whether clamping occurred.
- If auth is missing/invalid, operator diagnostics surface `NEWSDATA_API_KEY` as the expected env var name (never the value).

## 5) Refresh lookup projection

```bash
py-news lookup refresh
py-news lookup query --scope articles --limit 5 --json
```

## 5a) Read-only audit/reconciliation checks

```bash
py-news audit summary --summary-json
py-news audit cache --summary-json
py-news audit article --article-id <article_id> --json
py-news audit provider --provider newsdata --json
py-news audit report --json
py-news audit report --ndjson
```

Audit commands are read-only and inspect current state only; they do not rebuild, clean up, or repair canonical authorities/cache.

## 5a.1) Persist and compare audit reports (read-only)

```bash
py-news audit report --output /tmp/audit_now.json
py-news audit compare --left /tmp/audit_prev.json --right /tmp/audit_now.json --json
```

`audit report` and `audit compare` are read-only reporting tools. They do not trigger rebuild/cleanup/repair or mutate canonical authorities/cache.

## 5a.2) Capture an operator audit bundle (read-only)

```bash
py-news audit bundle --output-dir /tmp/py-news-audit-bundle --include-ndjson
```

Optional targeted bundle:

```bash
py-news audit bundle \
  --output-dir /tmp/py-news-audit-bundle-targeted \
  --provider newsdata \
  --article-id <article_id> \
  --overwrite
```

`audit bundle` is a read-only convenience/export command that orchestrates existing audit/report builders into a deterministic directory. It does not run rebuild/cleanup/repair and does not mutate canonical authorities or cache state.

## 5b) Rebuild cache layout/index (deterministic)

```bash
py-news cache rebuild-layout --summary-json
```

Optional bounded metadata repair during rebuild:

```bash
py-news cache rebuild-layout --repair-metadata --summary-json
```

`--repair-metadata` emits machine-readable counts:
- `rows_scanned`
- `rows_repaired_source_domain`
- `rows_repaired_section`
- `rows_repaired_byline`
- `rows_unchanged`
- `rows_skipped_unrepairable`

Optional cleanup after successful rebuild verification:

```bash
py-news cache rebuild-layout --cleanup-legacy --summary-json
```

Cleanup safety includes canonical artifact checks (no null `storage_article_id`, no artifact paths outside `.news_cache/publisher/data/...`).
Rebuild/cleanup remains separate from audit commands.

## 6) Serve API locally

```bash
py-news api serve --host 127.0.0.1 --port 8000
```

## 7) Local-only article/detail fetch

```bash
curl -s "http://127.0.0.1:8000/articles?limit=5" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>/content" | jq
```

## 8) Explicit remote-assisted fetch

```bash
curl -s "http://127.0.0.1:8000/articles/<article_id>?resolve_remote=true" | jq
curl -s "http://127.0.0.1:8000/articles/<article_id>/content?resolve_remote=true" | jq
```

Detail/content responses include explicit resolution reason/strategy/source plus bounded auth diagnostics (`resolution_auth_env_var`, `resolution_auth_configured`) when applicable.
Wave 2 also adds canonical additive transparency fields on these remote-capable endpoints:
- `resolution_mode`
- `resolution_provider_requested`
- `resolution_provider_used`
- `resolution_served_from`
- `resolution_persisted_locally`
- `resolution_rate_limited`
- `resolution_retry_count`
- `resolution_deferred_until`

## 9) Inspect canonical resolution provenance

```bash
py-news resolution events --limit 20 --json
py-news resolution events --article-id <article_id> --representation content --json
py-news resolution events --provider newsdata --reason-code metadata_refreshed --json
```

## 10) Inspect single article state

```bash
py-news articles inspect --article-id <article_id> --json
```

Inspection output includes metadata presence, provider/source fields, local artifact state, sidecar path, provider rule summary, and latest resolution events.

## Future augmentation note (not yet implemented)

- Entity/temporal extraction is expected from an external augmentation producer, not from in-process extraction in `py-news-m`.
- Authenticated augmentation submission and stored-overlay entity-aware query behavior are future phases after transfer/design sync from accepted `py-sec-edgar-m`.

## Provenance audit note

`resolution_events.parquet` is append-only historical truth. Audit may report legacy/tmp/missing historical paths as observations, not canonical-state failures by themselves.

## Wave 1 reservation note

`m-cache news monitor ...` and `m-cache news reconcile ...` are reserved shared command families in Wave 1.
Operational monitor/reconcile redesign is explicitly deferred to later waves.

Wave 1 migration summary:
- [`WAVE1_MIGRATION_NOTE.md`](WAVE1_MIGRATION_NOTE.md)

Wave 2 migration summary:
- [`WAVE2_MIGRATION_NOTE.md`](WAVE2_MIGRATION_NOTE.md)

## Wave 3 augmentation metadata checks (read-only/planning-first)

```bash
m-cache news aug list-types --json
m-cache news aug inspect-target --article-id <article_id> --text-source auto --json
m-cache news aug inspect-runs --article-id <article_id> --json
m-cache news aug inspect-artifacts --article-id <article_id> --json
m-cache news aug events --article-id <article_id> --json
```

Notes:
- Wave 3 augmentation scope is text-bearing `articles` resources only.
- `m-cache news aug submit` and `m-cache news aug status` remain reserved placeholders.
- `/articles/{article_id}` and `/articles/{article_id}/content` include additive `augmentation_meta`.

Wave 3 migration summary:
- [`WAVE3_MIGRATION_NOTE.md`](WAVE3_MIGRATION_NOTE.md)

## Wave 4 producer-protocol pilot checks

```bash
m-cache news aug inspect-target --article-id <article_id> --text-source auto --json
m-cache news aug submit-run --input-json /tmp/producer_run_submission.json --json
m-cache news aug submit-artifact --input-json /tmp/producer_artifact_submission.json --json
m-cache news aug status --run-id <run_id> --json
m-cache news aug events --article-id <article_id> --json
m-cache news aug inspect-runs --article-id <article_id> --json
m-cache news aug inspect-artifacts --article-id <article_id> --json
```

Compatibility aliases (preserved):

```bash
m-cache news aug target-descriptor --article-id <article_id> --text-source auto --json
m-cache news aug submit --kind run --input-json /tmp/producer_run_submission.json --json
m-cache news aug submit --kind artifact --input-json /tmp/producer_artifact_submission.json --json
```

Notes:
- Wave 4 pilot producer protocol remains bounded to text-bearing `articles` resources.
- Outer metadata envelopes are validated in-repo; payload body schema remains producer/service-owned.
- Large payloads should use locator-backed behavior via bounded payload-size policy.
- Existing article detail/content API surfaces remain the text retrieval path.

Wave 4 migration summary:
- [`WAVE4_MIGRATION_NOTE.md`](WAVE4_MIGRATION_NOTE.md)

## Wave 5 shared-package extraction note

Wave 5 introduces internal `m_cache_shared` extraction for outer protocol/helpers only.
Operator command behavior and surfaces in this document remain unchanged.

Wave 5.1 normalizes shared-package layout/exports to canonical `m_cache_shared.augmentation` while preserving runtime behavior exactly.

Wave 5 migration summary:
- [`WAVE5_MIGRATION_NOTE.md`](WAVE5_MIGRATION_NOTE.md)

## Wave 7 lifecycle validation and signoff checks (lifecycle hardening only)

Wave 7 for `py-news-m` does not change runtime behavior. It hardens release lifecycle obligations for external RC/stable candidates only.

Core policy reference:
- [`WAVE7_LIFECYCLE_POLICY.md`](WAVE7_LIFECYCLE_POLICY.md)

### Candidate pin evidence

Record candidate identity for each validation run:

```bash
cat requirements/m_cache_shared_external.txt
```

### Required repo validation command set

Run repo validation against the candidate pin:

```bash
pytest -q tests/test_m_cache_shared_shim.py tests/test_m_cache_shared.py tests/test_m_cache_cli.py
pytest -q
```

### Required facade-mode evidence (`local|auto|external`)

Validate shim source-mode contract behavior:

```bash
M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py
```

### Required signoff evidence fields

For each RC/stable candidate, capture:
- candidate tag and pinned spec,
- commands executed and pass/fail results,
- facade-mode results (`local`, `auto`, `external`),
- confirmation that pilot article behavior is unchanged,
- confirmation that article-only applicability is unchanged,
- final decision (`signoff=pass` or `signoff=blocked`) and blocker list if blocked.

### Blocker conditions

Block promotion for this repo when any of the following occurs:
- pin mismatch for intended candidate,
- strict-v1 symbol/facade contract failure,
- shim-mode regression in `local`, `auto`, or `external`,
- pilot article workflow regression,
- article-only applicability drift,
- CLI/API semantic drift,
- unresolved incident affecting validation confidence.

### Rollback steps

Use pinning/facade controls first:
1. repin `requirements/m_cache_shared_external.txt` to prior known-good tag,
2. rerun required repo validation command set,
3. if needed for controlled recovery, force `M_CACHE_SHARED_SOURCE=local`,
4. rerun validation and record recovery evidence.

### User-testing gate policy for releases

- Maintainer/developer validation and RC matrix validation remain mandatory for every candidate.
- Cross-application user testing is mandatory only for compatibility-impacting releases.
- Cross-application user testing is not mandatory for every routine stable release.
- Cross-application user testing never replaces maintainer/developer or RC matrix validation.

### Deferred cleanup criteria (explicitly deferred in Wave 7)

Deferred in this pass:
- no public API broadening,
- no immediate shim/fallback removal,
- no compatibility-layer pruning,
- no domain-logic externalization.

Earliest cleanup consideration requires:
- multiple successful stable cycles,
- consistently green cross-repo validation,
- successful compatibility-impact user testing cycles,
- high rollback confidence.

## Wave 7.1 release-execution participation checks (package-side lifecycle)

Wave 7.1 keeps repo behavior frozen and hardens this repo's participation in shared RC/stable release execution.

Core policy reference:
- [`WAVE7_1_RELEASE_EXECUTION_POLICY.md`](WAVE7_1_RELEASE_EXECUTION_POLICY.md)

### Candidate identity capture (required)

```bash
cat requirements/m_cache_shared_external.txt
```

### Required validation commands and outcomes (required)

```bash
pytest -q tests/test_m_cache_shared_shim.py tests/test_m_cache_shared.py tests/test_m_cache_cli.py
pytest -q
```

### Required facade-mode evidence (required)

```bash
M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=external pytest -q tests/test_m_cache_shared_shim.py
```

### Central evidence-bundle field mapping (required)

Record repo-side evidence in the central candidate bundle with fields:
- `candidate_identity` (tag + pin),
- `commands_outcomes`,
- `facade_mode_evidence`,
- `invariance_evidence` (pilot/article-only/semantic freeze),
- `signoff_decision` (`pass|blocked`),
- `blocker_codes`,
- `warning_codes`,
- `incident_links` (when applicable).

### Blocker vs warning classification (required)

Blockers:
- `B001_PIN_MISMATCH`
- `B002_VALIDATION_FAILURE`
- `B003_FACADE_MODE_REGRESSION`
- `B004_ARTICLE_APPLICABILITY_DRIFT`
- `B005_PILOT_WORKFLOW_DRIFT`
- `B006_CLI_API_SEMANTIC_DRIFT`
- `B007_EVIDENCE_INCOMPLETE`
- `B008_BLOCKING_INCIDENT_OPEN`

Warnings:
- `W001_RETRY_RECOVERED`
- `W002_OPTIONAL_DIAGNOSTIC_MISSING`
- `W003_NON_MATERIAL_DOC_GAP`

Warnings are recorded but do not block by themselves.

### Rollback and incident participation rules

1. repin `requirements/m_cache_shared_external.txt` to prior known-good stable tag,
2. rerun required validation/signoff checks,
3. if needed, force `M_CACHE_SHARED_SOURCE=local` for controlled recovery,
4. rerun and attach recovery evidence.

Incident records must include:
- severity (`blocking|non_blocking`),
- affected candidate + impact scope,
- blocker/warning classification,
- mitigation and recovery evidence,
- closure criteria for retrying promotion.

### Comprehensive user-testing start gate prerequisites

Comprehensive cross-application user testing may begin only after:
- Wave 7.1 implementation is complete,
- one shared RC has executed through full package-side lifecycle,
- central evidence-bundle flow is usable,
- rollback path is verified,
- no open blocking lifecycle incident involving this repo remains.

### Explicit deferred cleanup items (Wave 7.1)

- no shared public API broadening,
- no shim/fallback removal,
- no compatibility-layer pruning,
- no import-root collapse,
- no domain-local logic externalization.

## Wave 7.2 companion checks (first real shared RC cycle, minimal)

Wave 7.2 for `py-news-m` is participation-only and keeps behavior frozen.

Core policy reference:
- [`WAVE7_2_COMPANION_POLICY.md`](WAVE7_2_COMPANION_POLICY.md)

### Exact first-real-local-RC consumption step (portable default)

```bash
export M_CACHE_SHARED_EXT_LOCAL_PATH="${M_CACHE_SHARED_EXT_LOCAL_PATH:-../m-cache-shared-ext}"
.venv/bin/python -m pip install --no-build-isolation --editable "$M_CACHE_SHARED_EXT_LOCAL_PATH"
```

### Exact validation sequence

1. Confirm candidate pin metadata:
```bash
cat requirements/m_cache_shared_external.txt
```
2. Validate shim external-consumption path:
```bash
M_CACHE_SHARED_SOURCE=external pytest -q \
  tests/test_m_cache_shared_shim.py::test_shim_uses_external_when_strict_subset_present \
  tests/test_m_cache_shared_shim.py::test_shim_external_mode_fails_loudly_when_external_missing \
  tests/test_m_cache_shared_shim.py::test_shim_external_root_override_is_used
```
3. Validate remaining shim source modes:
```bash
M_CACHE_SHARED_SOURCE=local pytest -q tests/test_m_cache_shared_shim.py
M_CACHE_SHARED_SOURCE=auto pytest -q tests/test_m_cache_shared_shim.py
```
4. Validate shared/CLI surfaces:
```bash
pytest -q tests/test_m_cache_shared.py tests/test_m_cache_cli.py
```
5. Validate repo baseline:
```bash
pytest -q
```

### Exact machine-readable signoff contract (`SIGNOFF.json`)

Emit `SIGNOFF.json` with exactly:
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
- `validation_status`: `pass|fail`
- `signoff_state`: `pass|warn|block`

### Exact signoff / blocker / warning mapping

`signoff_state` mapping:
- `pass`: validations pass, no blockers, no warnings,
- `warn`: validations pass, no blockers, warnings present,
- `block`: any blocker present or validation failure.

Blockers:
- `B001_PIN_MISMATCH`
- `B002_VALIDATION_FAILURE`
- `B003_FACADE_MODE_REGRESSION`
- `B004_ARTICLE_APPLICABILITY_DRIFT`
- `B005_PILOT_WORKFLOW_DRIFT`
- `B006_CLI_API_SEMANTIC_DRIFT`
- `B007_EVIDENCE_INCOMPLETE`
- `B008_BLOCKING_INCIDENT_OPEN`

Warnings:
- `W001_RETRY_RECOVERED`
- `W002_OPTIONAL_DIAGNOSTIC_MISSING`
- `W003_NON_MATERIAL_DOC_GAP`

### Exact rollback-readiness fields and criteria

`rollback_ready=true` only when:
- repin path to prior known-good stable is available,
- rerun validation path exists after repin,
- local fallback path exists via `M_CACHE_SHARED_SOURCE=local`,
- no unresolved blocking incident prevents those actions.

Otherwise set `rollback_ready=false`.

### Exact central bundle input mapping

Consumer evidence is central-bundle-oriented only (no parallel repo-local release process).

Default news consumer location:
- `evidence/candidates/<candidate_tag>/consumer/py-news-m/`

Required repo inputs:
- `SIGNOFF.json`,
- `SIGNOFF.md`,
- incident reference link/file when applicable.

Required mapping:
- `candidate_tag` -> candidate linkage,
- `repo`/`release_role` -> consumer role linkage,
- `pin_confirmed`/`validation_status` -> validation ingestion,
- `signoff_state`/`blockers`/`warnings` -> decision ingestion,
- `rollback_ready` -> rollback readiness ingestion.

### User-testing handoff prerequisites (Wave 7.2)

Comprehensive cross-application user testing starts only after:
- Wave 7.2 implementation is complete,
- one real shared RC cycle has executed,
- evidence/signoff ingestion is operational end-to-end,
- rollback path is verified,
- no blocking release-lifecycle incident remains open.
