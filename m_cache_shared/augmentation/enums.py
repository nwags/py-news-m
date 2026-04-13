"""Canonical shared augmentation enums and vocabularies."""

from typing import Literal

AugmentationType = Literal["entity_tagging", "temporal_expression_tagging"]
ProducerKind = Literal["llm", "rules", "hybrid", "manual"]
RunStatus = Literal["queued", "running", "completed", "failed", "deferred", "skipped"]

# Compatibility convenience value lists (secondary to typed aliases).
AUGMENTATION_TYPE_VALUES = [
    "entity_tagging",
    "temporal_expression_tagging",
]

AUGMENTATION_PRODUCER_KIND_VALUES = [
    "llm",
    "rules",
    "hybrid",
    "manual",
]

AUGMENTATION_STATUS_VALUES = [
    "queued",
    "running",
    "completed",
    "failed",
    "deferred",
    "skipped",
]

