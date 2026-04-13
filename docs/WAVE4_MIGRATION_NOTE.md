# Wave 4 Migration Note (`py-news-m`)

This note records the Wave 4 compatibility-first producer-protocol pilot outcome for `py-news-m`.

## Wave 4 Pilot Scope Implemented

`py-news-m` is treated as one of the designated Wave 4 producer-protocol pilot repos.

Implemented producer protocol surfaces on additive `m-cache news aug ...`:
- canonical command family:
  - `inspect-target`
  - `submit-run`
  - `submit-artifact`
- `status` for idempotent run read-back.
- `events` for augmentation event read-back.
- enriched `inspect-runs` and `inspect-artifacts` filtering/read-back for producer metadata.

Compatibility aliases preserved:
- `target-descriptor`
- `submit --kind run`
- `submit --kind artifact`

## Applicability and Non-Applicability

Augmentation remains scoped to text-bearing article resources only:
- article metadata text
- article content text

Operational metadata families remain non-augmentation:
- provider registry
- lookup artifacts
- audit/reporting artifacts
- reconciliation artifacts
- other operational metadata tables

## Payload Ownership and Validation Boundary

- The repo validates only outer producer metadata envelopes.
- Annotation payload schema/body ownership remains external/service-owned.
- Payload body is stored opaquely (inline when small, locator-backed when large).

## Idempotency and Replay-Safe Behavior

Wave 4 pilot submission handling adds idempotency keys for run/artifact envelopes and upsert-based dedupe.
Replay of the same producer/version/type/key/source-version envelope does not create duplicate canonical rows.

## Bounded Payload-Size Policy

- Inline artifact payload persistence is bounded by a size limit.
- Larger payloads are persisted by locator-backed sidecar storage.
- Locator-backed mode is not optional-only; it is actively supported by policy.

## Compatibility Preserved

- `py-news ...` remains the operator compatibility surface with unchanged defaults.
- `m-cache news ...` remains the additive canonical shared surface.
- Existing article detail/content API surfaces remain primary for text retrieval + additive `augmentation_meta`.

## Reserved for Later Waves

- Broad augmentation endpoint-family expansion.
- In-repo heavy extraction redesign.
- Adapter extraction, article parsing strategy extraction, storage/path extraction, execution engine extraction.
- Monitor/reconcile redesign.
- Cross-repo shared package extraction/publishing rollout.
