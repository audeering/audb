# Lazy imports for faster import time.
# Heavy dependencies (pandas, audbackend, audformat, etc.) are only loaded
# when the corresponding functions/classes are actually accessed.

import importlib


# Single source of truth for all lazy-loaded names.
# Submodules map to their fully-qualified module names.
# Symbols map to the module that defines them.
_LAZY_SUBMODULES = {"info", "core"}

_LAZY_TARGETS = {
    # Submodules
    "info": "audb.info",
    "core": "audb.core",
    # From audb.core.api
    "available": "audb.core.api",
    "cached": "audb.core.api",
    "dependencies": "audb.core.api",
    "exists": "audb.core.api",
    "flavor_path": "audb.core.api",
    "latest_version": "audb.core.api",
    "remove_media": "audb.core.api",
    "repository": "audb.core.api",
    "versions": "audb.core.api",
    # From audb.core.cache
    "default_cache_root": "audb.core.cache",
    # From audb.core.config
    "config": "audb.core.config",
    # From audb.core.dependencies
    "Dependencies": "audb.core.dependencies",
    # From audb.core.flavor
    "Flavor": "audb.core.flavor",
    # From audb.core.load
    "load": "audb.core.load",
    "load_attachment": "audb.core.load",
    "load_media": "audb.core.load",
    "load_table": "audb.core.load",
    # From audb.core.load_to
    "load_to": "audb.core.load_to",
    # From audb.core.publish
    "publish": "audb.core.publish",
    # From audb.core.repository
    "Repository": "audb.core.repository",
    # From audb.core.stream
    "DatabaseIterator": "audb.core.stream",
    "stream": "audb.core.stream",
}

_loaded = {}


def __getattr__(name: str):
    """Lazily import attributes and submodules on first access."""
    if name not in _LAZY_TARGETS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    if name in _loaded:
        return _loaded[name]

    module = importlib.import_module(_LAZY_TARGETS[name])
    # For submodules, return the module itself
    if name in _LAZY_SUBMODULES:
        value = module
    else:
        value = getattr(module, name)

    _loaded[name] = value
    return value


def __dir__():
    """List available attributes for autocomplete."""
    # Standard module attributes (exclude private implementation details)
    standard = [k for k in globals().keys() if k.startswith("__")]
    return standard + list(_LAZY_TARGETS)


__all__ = []


# Dynamically get the version of the installed module
try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"
