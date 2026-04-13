from types import ModuleType

import importlib
import py_news.m_cache_shared_shim as shared_shim


def _fake_module_with_strict_subset() -> ModuleType:
    module = ModuleType("fake_external_aug")
    for symbol in shared_shim.STRICT_PUBLIC_API_SYMBOLS:
        setattr(module, symbol, object())
    return module


def _reload_shim():
    return importlib.reload(shared_shim)


def test_shim_falls_back_to_local_when_external_missing(monkeypatch):
    shim = _reload_shim()
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            raise ImportError("missing external package")
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    module = shim.load_shared_augmentation_module()
    assert module.__name__ == "m_cache_shared.augmentation"


def test_shim_uses_external_when_strict_subset_present(monkeypatch):
    shim = _reload_shim()
    # Keep this assertion mode-scoped and independent from process-level test env.
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "external")
    real_import = importlib.import_module
    fake_external = _fake_module_with_strict_subset()

    def fake_import(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            return fake_external
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    module = shim.load_shared_augmentation_module()
    assert module is fake_external


def test_shim_external_mode_fails_loudly_when_external_missing(monkeypatch):
    shim = _reload_shim()
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "external")
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            raise ImportError("no external package")
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    try:
        shim.load_shared_augmentation_module()
    except RuntimeError as exc:
        assert "external mode" in str(exc)
        return
    raise AssertionError("Expected RuntimeError in external mode when external package is missing")


def test_shim_local_mode_bypasses_external_import(monkeypatch):
    shim = _reload_shim()
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "local")
    real_import = importlib.import_module
    attempted_external = {"value": False}

    def fake_import(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            attempted_external["value"] = True
            raise AssertionError("external import should not be attempted in local mode")
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    module = shim.load_shared_augmentation_module()
    assert module.__name__ == "m_cache_shared.augmentation"
    assert attempted_external["value"] is False


def test_shim_external_root_override_is_used(monkeypatch):
    shim = _reload_shim()
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "external")
    monkeypatch.setenv(shim.M_CACHE_SHARED_EXTERNAL_ROOT_ENV, "custom.external.root")
    real_import = importlib.import_module
    fake_external = _fake_module_with_strict_subset()
    observed_names: list[str] = []

    def fake_import(name: str, package: str | None = None):
        observed_names.append(name)
        if name == "custom.external.root":
            return fake_external
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    module = shim.load_shared_augmentation_module()
    assert module is fake_external
    assert "custom.external.root" in observed_names


def test_shim_invalid_source_mode_raises(monkeypatch):
    shim = _reload_shim()
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "bogus")
    try:
        shim.load_shared_augmentation_module()
    except RuntimeError as exc:
        assert "Expected one of: auto, external, local" in str(exc)
        return
    raise AssertionError("Expected RuntimeError for invalid source mode")


def test_shim_auto_mode_falls_back_to_local_when_external_missing_symbols(monkeypatch):
    shim = _reload_shim()
    real_import = importlib.import_module
    incomplete_external = ModuleType("incomplete_external")
    setattr(incomplete_external, "ProducerTargetDescriptor", object())

    def fake_import(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            return incomplete_external
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    module = shim.load_shared_augmentation_module()
    assert module.__name__ == "m_cache_shared.augmentation"


def test_shim_does_not_mix_sources_once_resolved(monkeypatch):
    shim = _reload_shim()
    real_import = importlib.import_module

    def fake_import_local_first(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            raise ImportError("missing external package")
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import_local_first)
    local_module = shim.load_shared_augmentation_module()
    assert local_module.__name__ == "m_cache_shared.augmentation"

    fake_external = _fake_module_with_strict_subset()

    def fake_import_external_now_available(name: str, package: str | None = None):
        if name == "m_cache_shared_ext.augmentation":
            return fake_external
        return real_import(name, package=package)

    monkeypatch.setattr(importlib, "import_module", fake_import_external_now_available)
    monkeypatch.setenv(shim.M_CACHE_SHARED_SOURCE_ENV, "external")
    second = shim.load_shared_augmentation_module()
    assert second is local_module
