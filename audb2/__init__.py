from audb2 import define
from audb2 import info
from audb2.core.api import (
    available,
    cached,
    default_cache_root,
    dependencies,
    exists,
    flavor_path,
    latest_version,
    lookup_repository,
    remove_media,
    versions,
)
from audb2.core.config import config
from audb2.core.dependencies import Dependencies
from audb2.core.flavor import Flavor
from audb2.core.load import (
    load,
    load_original_to,
)
from audb2.core.publish import publish
from audb2.core.repository import Repository


__all__ = []


# Dynamically get the version of the installed module
try:
    import pkg_resources
    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    pkg_resources = None  # pragma: no cover
finally:
    del pkg_resources
