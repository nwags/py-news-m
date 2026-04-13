# REFDATA_SCHEMA.md

## Canonical normalized tables (implemented now)

### provider_registry

Runtime provider/source rule authority.

### source_catalog

Lightweight source/provider catalog mirror for reference workflows.

### articles

Canonical article metadata authority.

### article_artifacts

Canonical current local artifact presence/index authority.

### storage_articles

Canonical physical storage article-folder authority.

### article_storage_map

Canonical mapping from provider/local article identity to physical storage identity.

### local_lookup_articles

Deterministic, rebuildable article lookup projection.

### resolution_events

Append-only resolution provenance authority.

### augmentation_runs

Wave 3 canonical outer augmentation run metadata authority (read-only/planning-first in this phase).
Wave 4 pilot adds producer run submission envelope persistence/idempotent replay handling.

### augmentation_events

Wave 3 canonical outer augmentation event metadata authority (read-only/planning-first in this phase).
Wave 4 pilot adds producer event/read-back enrichment for run submission outcomes.

### augmentation_artifacts

Wave 3 canonical outer augmentation artifact metadata authority for article-linked text-bearing augmentation outputs.
Payload body schema remains producer/service-owned.
Wave 4 pilot adds producer artifact submission envelope handling with bounded inline payload policy and locator-backed persistence support.

## Planned / Not Yet Implemented

Future augmentation execution orchestration and authenticated submission workflows are not implemented in this repo today.

When augmentation ingestion is transferred, the augmentation storage pattern is expected to follow the accepted `py-sec-edgar-m` augmentation design (external augmentation producer -> authenticated augmentation submission -> canonical augmentation storage -> entity-aware query behavior using stored augmentations).

Do not treat Wave 3 metadata reservations as full augmentation runtime execution.

## Output format preference

Use parquet for canonical normalized tables.
CSV exports remain optional convenience outputs only.
