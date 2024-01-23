from audb import info
from audb.core.api import available
from audb.core.api import cached
from audb.core.api import dependencies
from audb.core.api import exists
from audb.core.api import flavor_path
from audb.core.api import latest_version
from audb.core.api import remove_media
from audb.core.api import repository
from audb.core.api import versions
from audb.core.cache import default_cache_root
from audb.core.config import config
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.load import load
from audb.core.load import load_attachment
from audb.core.load import load_media
from audb.core.load import load_table
from audb.core.load_to import load_to
from audb.core.publish import publish
from audb.core.repository import Repository


__all__ = []


# Dynamically get the version of the installed module
try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__name__)
except Exception:  # pragma: no cover
    importlib = None  # pragma: no cover
finally:
    del importlib
