# Wave 5 Migration Note (`py-news-m`)

This note records the first Wave 5 shared-package extraction cut for `py-news-m`.

## Wave 5 Scope Implemented

Wave 5 introduces an in-repo `m_cache_shared` package for the minimal shared outer protocol/helper layer:

- protocol/view models,
- shared augmentation vocabularies,
- outer-envelope schema loader and validator helpers,
- pure metadata/view packers,
- thin CLI JSON-input parsing helper.

## Preserved Local Ownership

The following remain repo-local and unchanged:

- article target building and text/source selection,
- article text retrieval and storage placement behavior,
- idempotency key decisions and live run/artifact persistence,
- article-specific applicability and authority enforcement,
- CLI alias/help behavior and command wiring.

## Compatibility and Runtime Role

- `py-news ...` remains the compatibility surface.
- `m-cache news ...` remains the additive canonical surface.
- Pilot runtime behavior is preserved: live `submit-run` and `submit-artifact` write-path handling remains local in `py-news-m`.
- Applicability remains article-only (`resource_family=articles`).
- Operational metadata families remain non-augmentation.

## Wave 5.1 Normalization (Layout and Exports Only)

Wave 5.1 normalizes `m_cache_shared` shape and exports without changing extraction scope or runtime behavior:

- canonical nested layout is now used:
  - `m_cache_shared/augmentation/{enums,models,validators,schema_loaders,packers,cli_helpers}.py`
- canonical shared export surface is provided from:
  - `m_cache_shared.augmentation`
- flat `m_cache_shared/*` modules are retained as thin compatibility re-export shims for one normalization cycle.

Behavior freeze retained in Wave 5.1:

- pilot live write-path behavior remains unchanged,
- article-only applicability remains unchanged,
- local authority/storage/idempotency/text-selection logic remains local and unchanged,
- CLI/API/operator semantics remain unchanged.

## Wave 6 Externalization Prep (Shim-First, Behavior-Frozen)

Wave 6 prep introduces explicit shim-first adoption mechanics for an external `m_cache_shared` source of truth:

- Git-tag pin is centralized in:
  - `requirements/m_cache_shared_external.txt`
- external import root is explicit and distinct:
  - `m_cache_shared_ext.augmentation`
- local shim resolver:
  - `py_news/m_cache_shared_shim.py`
- centralized pin metadata:
  - `py_news/m_cache_shared_pin.py`
- fixed shared RC tag for initial Wave 6.1 convergence:
  - `v0.1.0-rc1` (historical initial convergence tag; current repo pin may advance per active RC cycle)

Shadowing-safe import strategy:

- the shim attempts external `m_cache_shared_ext.augmentation` first,
- validates strict-common required symbols,
- falls back to local `m_cache_shared.augmentation` when external package is absent or incomplete,
- does not depend on ambiguous module-precedence between local and external `m_cache_shared`.

Wave 6.1 canonical shim contract:

- `M_CACHE_SHARED_SOURCE={auto|external|local}`
  - `auto`: external first, strict-subset verification, local fallback
  - `external`: fail loudly when external package/root/symbols are unavailable
  - `local`: bypass external and use local in-repo shared module
- `M_CACHE_SHARED_EXTERNAL_ROOT` defaults to `m_cache_shared_ext.augmentation`
- shim resolves once per process and does not mix local/external symbol sources in one runtime process

Rollback strategy for first externalization cycle:

- repin `requirements/m_cache_shared_external.txt` to earlier Git tag, or
- allow shim fallback to local `m_cache_shared.augmentation` (no CLI/API redesign required).

Scope/behavior freeze retained:

- strict-common subset only for first external public API consumption,
- `pack_additive_augmentation_meta` remains local in `py_news`,
- compatibility alias behavior remains local,
- pilot live write-path behavior remains unchanged and local.
