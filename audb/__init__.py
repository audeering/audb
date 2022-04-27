from audb import info
from audb.core.api import (
    available,
    cached,
    dependencies,
    exists,
    flavor_path,
    latest_version,
    remove_media,
    repository,
    versions,
)
from audb.core.cache import default_cache_root
from audb.core.config import config
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.load import (
    load,
    load_media,
    load_table,
)
from audb.core.load_to import load_to
from audb.core.publish import publish
from audb.core.repository import Repository


__all__ = []


# Dynamically get the version of the installed module
try:
    import pkg_resources
    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    pkg_resources = None  # pragma: no cover
finally:
    del pkg_resources
