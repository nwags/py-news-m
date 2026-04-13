"""Wave 6.1 shadowing-safe shim for shared augmentation symbols.

This shim provides the canonical source-mode contract:

- ``M_CACHE_SHARED_SOURCE={auto|external|local}``
- ``M_CACHE_SHARED_EXTERNAL_ROOT`` (default ``m_cache_shared_ext.augmentation``)
"""

from __future__ import annotations

import importlib
import os
from types import ModuleType

from py_news.m_cache_shared_pin import EXTERNAL_IMPORT_ROOT

M_CACHE_SHARED_SOURCE_ENV = "M_CACHE_SHARED_SOURCE"
M_CACHE_SHARED_EXTERNAL_ROOT_ENV = "M_CACHE_SHARED_EXTERNAL_ROOT"
_DEFAULT_SOURCE_MODE = "auto"
_SOURCE_MODES = {"auto", "external", "local"}
_LOCAL_IMPORT_ROOT = "m_cache_shared.augmentation"

# Strict proven common subset for first external public API adoption.
STRICT_PUBLIC_API_SYMBOLS = (
    "ProducerTargetDescriptor",
    "ProducerRunSubmission",
    "ProducerArtifactSubmission",
    "RunStatusView",
    "EventsViewRow",
    "ApiAugmentationMeta",
    "AugmentationType",
    "ProducerKind",
    "RunStatus",
    "validate_producer_target_descriptor",
    "validate_producer_run_submission",
    "validate_producer_artifact_submission",
    "validate_run_submission_envelope",
    "validate_artifact_submission_envelope",
    "load_json_schema",
    "pack_run_status_view",
    "pack_events_view",
    "parse_json_input_payload",
)

_RESOLVED_MODULE: ModuleType | None = None


def _source_mode() -> str:
    mode = os.getenv(M_CACHE_SHARED_SOURCE_ENV, _DEFAULT_SOURCE_MODE).strip().lower()
    if mode not in _SOURCE_MODES:
        raise RuntimeError(
            f"Invalid {M_CACHE_SHARED_SOURCE_ENV}={mode!r}. Expected one of: auto, external, local."
        )
    return mode


def _external_import_root() -> str:
    root = os.getenv(M_CACHE_SHARED_EXTERNAL_ROOT_ENV, EXTERNAL_IMPORT_ROOT).strip()
    if not root:
        raise RuntimeError(
            f"{M_CACHE_SHARED_EXTERNAL_ROOT_ENV} resolved to an empty import root."
        )
    return root


def _load_external_module(root: str) -> ModuleType:
    return importlib.import_module(root)


def _load_local_module() -> ModuleType:
    return importlib.import_module(_LOCAL_IMPORT_ROOT)


def _has_strict_subset(module: ModuleType) -> bool:
    return all(hasattr(module, symbol) for symbol in STRICT_PUBLIC_API_SYMBOLS)


def _strict_subset_missing(module: ModuleType) -> list[str]:
    return [name for name in STRICT_PUBLIC_API_SYMBOLS if not hasattr(module, name)]


def load_shared_augmentation_module() -> ModuleType:
    """Resolve shared augmentation module under canonical Wave 6.1 source-mode semantics."""
    global _RESOLVED_MODULE
    if _RESOLVED_MODULE is not None:
        return _RESOLVED_MODULE

    mode = _source_mode()
    external_root = _external_import_root()

    if mode == "local":
        local = _load_local_module()
        missing = _strict_subset_missing(local)
        if missing:
            raise RuntimeError(f"Local shared fallback missing strict public API symbols: {missing}")
        _RESOLVED_MODULE = local
        return _RESOLVED_MODULE

    if mode == "external":
        try:
            external = _load_external_module(external_root)
        except Exception as exc:
            raise RuntimeError(
                f"External shared module import failed for root {external_root!r} in external mode."
            ) from exc
        missing = _strict_subset_missing(external)
        if missing:
            raise RuntimeError(
                f"External shared module at {external_root!r} missing strict public API symbols: {missing}"
            )
        _RESOLVED_MODULE = external
        return _RESOLVED_MODULE

    try:
        external = _load_external_module(external_root)
        if _has_strict_subset(external):
            _RESOLVED_MODULE = external
            return _RESOLVED_MODULE
    except Exception:
        pass

    local = _load_local_module()
    missing = _strict_subset_missing(local)
    if missing:
        raise RuntimeError(f"Local shared fallback missing strict public API symbols: {missing}")
    _RESOLVED_MODULE = local
    return _RESOLVED_MODULE


def get_shared_symbol(name: str):
    module = load_shared_augmentation_module()
    return getattr(module, name)
