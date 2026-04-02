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

## Planned / Not Yet Implemented

Future augmentation overlay schema/authorities are not implemented in this repo today.

When augmentation ingestion is transferred, the augmentation storage pattern is expected to follow the accepted `py-sec-edgar-m` augmentation design (external augmentation producer -> authenticated augmentation submission -> canonical augmentation storage -> entity-aware query behavior using stored augmentations).

Do not treat any augmentation overlay table as implemented until that transfer phase lands.

## Output format preference

Use parquet for canonical normalized tables.
CSV exports remain optional convenience outputs only.
