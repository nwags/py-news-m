"""Centralized Wave 6 external m_cache_shared pin metadata."""

from __future__ import annotations

from pathlib import Path

EXTERNAL_PACKAGE_NAME = "m-cache-shared-ext"
EXTERNAL_IMPORT_ROOT = "m_cache_shared_ext.augmentation"
EXTERNAL_GIT_URL = "https://github.com/m-cache/m_cache_shared_ext.git"
EXTERNAL_GIT_TAG = "v0.1.0-rc9"
EXTERNAL_GIT_SPEC = f"{EXTERNAL_PACKAGE_NAME} @ git+{EXTERNAL_GIT_URL}@{EXTERNAL_GIT_TAG}"

# Single local file containing the explicit pin for first external adoption cycle.
EXTERNAL_PIN_FILE = Path("requirements/m_cache_shared_external.txt")
