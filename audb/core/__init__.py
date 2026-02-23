# Lazy loading for audb.core submodules
import importlib


_SUBMODULES = {
    "api",
    "cache",
    "config",
    "conftest",
    "define",
    "dependencies",
    "flavor",
    "info",
    "load",
    "load_to",
    "lock",
    "publish",
    "repository",
    "stream",
    "utils",
}

_loaded = {}


def __getattr__(name: str):
    """Lazily import submodules on first access."""
    if name in _SUBMODULES:
        if name not in _loaded:
            _loaded[name] = importlib.import_module(f"audb.core.{name}")
        return _loaded[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return list(_SUBMODULES)
