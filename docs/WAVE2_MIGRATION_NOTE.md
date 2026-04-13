# Wave 2 Migration Note (`py-news-m`)

This note records the Wave 2 parallel standardization outcome for `py-news-m`.
Wave 2 remains additive and compatibility-first.

## What Became Canonical in Wave 2

- Canonical provider inspection surfaces on `m-cache news`:
  - `providers list`
  - `providers show --provider <provider_id>`
- Canonical resolve-mode vocabulary surfaced on `m-cache news resolve article ...`:
  - `local_only`
  - `resolve_if_missing`
  - `refresh_if_stale` (transparent unsupported-mode outcome where not implemented on this path)
- Canonical rate-limit/degradation visibility added to runtime outputs and resolution provenance:
  - `rate_limited`
  - `retry_count`
  - `deferred_until` (when applicable)
- Canonical additive API transparency fields exposed on remote-capable detail/content responses:
  - `resolution_mode`
  - `resolution_provider_requested`
  - `resolution_provider_used`
  - `resolution_served_from`
  - `resolution_persisted_locally`
  - `resolution_rate_limited`
  - `resolution_retry_count`
  - `resolution_deferred_until`
- `refdata/normalized/resolution_events.parquet` remains append-only with additive canonical provider/rate-limit fields.

## What Remains Aliased / Preserved

- `py-news ...` remains the operator compatibility surface.
- Legacy `py-news` default output behavior remains unchanged.
- `m-cache news ...` remains additive as the canonical shared cross-repo surface.
- Existing provider-aware local-first behavior and domain-specific article/content storage/fetch semantics are preserved.
- Audit/reporting behavior remains read-only.

## Additive Wave 2 Notes

- `providers explain-resolution ...` is kept as an optional repo-specific read-only helper; canonical shared provider read surface is `providers list/show`.
- Existing API endpoint paths and status-code behavior are preserved; Wave 2 standardizes metadata semantics additively.

## Reserved for Later Waves

- Monitor/reconcile operational redesign.
- Broad reconciliation workflow expansion beyond reserved/materialized artifacts.
- Augmentation and in-process heavy NLP extraction expansion.
- Cross-repo shared package extraction.
