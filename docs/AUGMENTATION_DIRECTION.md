# AUGMENTATION_DIRECTION.md

## Direction

`py-news-m` will become entity-aware by storing and serving augmentation overlays, not by running extraction in-process.

## External Producer Model

1. An external augmentation producer reads article data from `py-news-m` read APIs.
2. That external producer performs entity and temporal-expression extraction.
3. `py-news-m` will later expose an authenticated augmentation submission surface.
4. Submitted overlays will be stored canonically in `py-news-m`.
5. Entity-aware lookup/query behavior will later be driven by those stored augmentations.

## Transfer Source

The augmentation ingestion/storage/query pattern is expected to transfer from the accepted `py-sec-edgar-m` augmentation design once transfer/design sync is approved for `py-news-m`.

## Current Status

Not implemented yet in `py-news-m`:
- authenticated augmentation submission,
- canonical augmentation overlay storage,
- entity-aware query behavior using stored augmentations.
