import os
import tempfile
import typing

import pandas as pd

import audformat

from audb.core import define
from audb.core.api import (
    dependencies,
    latest_version,
)
from audb.core.utils import lookup_backend


def author(
        name: str,
        *,
        version: str = None,
) -> str:
    """Author(s) of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        author(s) of database

    """
    db = header(name, version=version)
    return db.author


def bit_depths(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Set[int]:
    """Media bit depth.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        bit depths

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    return set(
        [
            deps.bit_depth(file) for file in deps.media
        ]
    )


def channels(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Set[int]:
    """Media channels.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        channel numbers

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    return set(
        [
            deps.channels(file) for file in deps.media
        ]
    )


def description(
        name: str,
        *,
        version: str = None,
) -> str:
    """Description of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        description of database

    """
    db = header(name, version=version)
    return db.description


def duration(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> pd.Timedelta:
    """Total media duration.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        duration

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    return pd.to_timedelta(
        sum([deps.duration(file) for file in deps.media]),
        unit='s',
    )


def formats(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Set[str]:
    """Media formats.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        format

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    return set(
        [
            deps.format(file) for file in deps.media
        ]
    )


def header(
        name: str,
        *,
        version: str = None,
) -> audformat.Database:
    r"""Load header of database.

    Downloads the :file:`db.yaml` to a temporal directory,
    loads the database header and returns it.
    Does not write to the :mod:`audb` cache folders.

    Args:
        name: name of database
        version: version of database

    Returns:
        database object without table data

    """
    if version is None:
        version = latest_version(name)
    backend = lookup_backend(name, version)

    with tempfile.TemporaryDirectory() as root:
        remote_header = backend.join(name, define.HEADER_FILE)
        local_header = os.path.join(root, define.HEADER_FILE)
        backend.get_file(
            remote_header,
            local_header,
            version,
        )
        db = audformat.Database.load(root, load_data=False)

    return db


def languages(
        name: str,
        *,
        version: str = None,
) -> typing.List[str]:
    """Languages of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        languages of database

    """
    db = header(name, version=version)
    return db.languages


def license(
        name: str,
        *,
        version: str = None,
) -> str:
    """License of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        license of database

    """
    db = header(name, version=version)
    return db.license


def license_url(
        name: str,
        *,
        version: str = None,
) -> str:
    """License URL of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        license URL of database

    """
    db = header(name, version=version)
    return db.license_url


def media(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Audio and video media of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        media of database

    """
    db = header(name, version=version)
    return db.media


def meta(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Meta information of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        meta information of database

    """
    db = header(name, version=version)
    return db.meta


def organization(
        name: str,
        *,
        version: str = None,
) -> str:
    """Organization responsible for database.

    Args:
        name: name of database
        version: version of database

    Returns:
        organization responsible for database

    """
    db = header(name, version=version)
    return db.organization


def raters(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Raters contributed to database.

    Args:
        name: name of database
        version: version of database

    Returns:
        raters of database

    """
    db = header(name, version=version)
    return db.raters


def sampling_rates(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Set[int]:
    """Media sampling rates.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        sampling rates

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    return set(
        [
            deps.sampling_rate(file) for file in deps.media
        ]
    )


def schemes(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Schemes of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        schemes of database

    """
    db = header(name, version=version)
    return db.schemes


def source(
        name: str,
        *,
        version: str = None,
) -> str:
    """Source of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        source of database

    """
    db = header(name, version=version)
    return db.source


def splits(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Splits of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        splits of database

    """
    db = header(name, version=version)
    return db.splits


def tables(
        name: str,
        *,
        version: str = None,
) -> typing.Dict:
    """Tables of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        tables of database

    """
    db = header(name, version=version)
    return db.tables


def usage(
        name: str,
        *,
        version: str = None,
) -> str:
    """Usage of database.

    Args:
        name: name of database
        version: version of database

    Returns:
        usage of database

    """
    db = header(name, version=version)
    return db.usage
