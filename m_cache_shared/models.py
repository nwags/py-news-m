"""Compatibility re-exports for pre-Wave-5.1 flat imports."""

from m_cache_shared.augmentation.models import (
    ApiAugmentationMeta,
    EventsViewRow,
    ProducerArtifactSubmission,
    ProducerRunSubmission,
    ProducerTargetDescriptor,
    RunStatusView,
)

__all__ = [
    "ApiAugmentationMeta",
    "EventsViewRow",
    "ProducerArtifactSubmission",
    "ProducerRunSubmission",
    "ProducerTargetDescriptor",
    "RunStatusView",
]
