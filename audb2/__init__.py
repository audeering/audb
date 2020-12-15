from audb2 import backend
from audb2 import define
from audb2.core.api import (
    default_cache_root,
)
from audb2.core.config import config
from audb2.core.flavor import Flavor


__all__ = []


# Dynamically get the version of the installed module
try:
    import pkg_resources
    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    pkg_resources = None  # pragma: no cover
finally:
    del pkg_resources
