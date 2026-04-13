# Wave 3 Migration Note (`py-news-m`)

This note records the Wave 3 compatibility-first standardization outcome for `py-news-m`.

## What Became Canonical in Wave 3

- Canonical additive augmentation command family reserved under `m-cache news aug ...`.
- Implemented read-only/planning-first `aug` surfaces:
  - `list-types`
  - `inspect-target`
  - `inspect-runs`
  - `inspect-artifacts`
- Reserved/non-executing placeholders:
  - `submit`
  - `status`
  - `events` runtime behavior (metadata inspection only in Wave 3)
- Canonical outer augmentation metadata artifacts materialized:
  - `refdata/normalized/augmentation_runs.parquet`
  - `refdata/normalized/augmentation_events.parquet`
  - `refdata/normalized/augmentation_artifacts.parquet`
- Additive API augmentation metadata (`augmentation_meta`) added to existing:
  - `/articles/{article_id}`
  - `/articles/{article_id}/content`

## Text-Bearing Applicability

Augmentation applies to text-bearing article resources only:
- article metadata text
- full article content text

Non-augmentation in Wave 3:
- provider registry
- lookup artifacts
- audit/reporting artifacts
- reconciliation artifacts
- other operational metadata artifacts

## What Remained Repo-Local / Compatibility-Preserved

- `py-news ...` remains the operator compatibility surface with unchanged defaults.
- `m-cache news ...` remains additive canonical shared surface.
- Article identity derivation, storage mapping/layout, provider fetch logic, parsing/extraction details, and article-specific strategy ordering remain repo-local.
- Augmentation payload body schema remains producer/service-owned; only outer metadata contract is standardized.

## Reserved for Later Waves

- Augmentation execution orchestration (`submit/status/events` runtime workflows).
- Monitor/reconcile redesign.
- Heavy in-process extraction redesign.
- Broad endpoint-family redesign for augmentation.
- Cross-repo shared package extraction/publication.
