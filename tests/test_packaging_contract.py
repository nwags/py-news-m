import tomllib
from pathlib import Path


def test_build_system_requires_modern_setuptools_floor():
    data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    requires = data["build-system"]["requires"]
    assert "setuptools>=68" in requires
