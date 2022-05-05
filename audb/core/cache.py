import os

import audeer

from audb.core import define
from audb.core.config import config
from audb.core.flavor import Flavor


def database_cache_root(
        name: str,
        version: str,
        cache_root: str = None,
        flavor: Flavor = None,
) -> str:
    r"""Create and return database cache folder.

    Args:
        name: name of database
        version: version of database
        cache_root: path to cache folder
        flavor: flavor of database

    Returns:
        path to cache folder

    """
    if cache_root is None:
        cache_roots = [
            default_cache_root(True),  # check shared cache first
            default_cache_root(False),
        ]
    else:
        cache_roots = [cache_root]
    for cache_root in cache_roots:
        if flavor is None:
            db_root = audeer.path(
                cache_root,
                name,
                version,
            )
        else:
            db_root = audeer.path(
                cache_root,
                flavor.path(name, version),
            )
        if os.path.exists(db_root):
            break

    audeer.mkdir(db_root)
    return db_root


def database_tmp_root(
        db_root: str,
) -> str:
    r"""Create and return temporary database cache folder.

    The temporary cache folder is created under ``db_root + '~'``.

    Args:
        db_root: path to database cache folder

    Returns:
        path to temporary cache folder

    """
    tmp_root = db_root + '~'
    tmp_root = audeer.mkdir(tmp_root)
    return tmp_root


def default_cache_root(
        shared=False,
) -> str:
    r"""Default cache folder.

    If ``shared`` is ``True``,
    returns the path specified
    by the environment variable
    ``AUDB_SHARED_CACHE_ROOT``
    or
    ``audb.config.SHARED_CACHE_ROOT``.
    If ``shared`` is ``False``,
    returns the path specified
    by the environment variable
    ``AUDB_CACHE_ROOT``
    or
    ``audb.config.CACHE_ROOT``.

    Args:
        shared: if ``True`` returns path to shared cache folder

    Returns:
        path normalized by :func:`audeer.path`

    """
    if shared:
        cache = (
            os.environ.get('AUDB_SHARED_CACHE_ROOT')
            or config.SHARED_CACHE_ROOT
        )
    else:
        cache = (
            os.environ.get('AUDB_CACHE_ROOT')
            or config.CACHE_ROOT
        )
    return audeer.path(cache)
