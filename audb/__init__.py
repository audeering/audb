# Lazy imports for faster import time.
# Heavy dependencies (pandas, audbackend, audformat, etc.) are only loaded
# when the corresponding functions/classes are actually accessed.

import importlib


# Submodules that should be lazily imported
_LAZY_SUBMODULES = {"info", "core"}

# Define what should be lazily imported and from where
_LAZY_IMPORTS = {
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

# Cache for lazily loaded attributes
_loaded = {}


def __getattr__(name: str):
    """Lazily import attributes on first access."""
    # Handle submodules
    if name in _LAZY_SUBMODULES:
        if name not in _loaded:
            _loaded[name] = importlib.import_module(f"audb.{name}")
        return _loaded[name]

    # Handle attributes from modules
    if name in _LAZY_IMPORTS:
        if name not in _loaded:
            module_path = _LAZY_IMPORTS[name]
            module = importlib.import_module(module_path)
            _loaded[name] = getattr(module, name)
        return _loaded[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """List available attributes for autocomplete."""
    return list(_LAZY_SUBMODULES) + list(_LAZY_IMPORTS.keys()) + ["__version__"]


__all__ = list(_LAZY_SUBMODULES) + list(_LAZY_IMPORTS.keys())


# Dynamically get the version of the installed module
try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"
