import typing

import pandas as pd

import audformat

from audb.core.api import (
    dependencies,
    latest_version,
)
from audb.core.load import (
    database_cache_folder,
    load_header,
)


def author(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """Author(s) of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        author(s) of database

    """
    db = header(name, version=version, cache_root=cache_root)
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
    df = deps()
    return set(df[df.type == define.DependType.MEDIA].bit_depth)


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
    df = deps()
    return set(df[df.type == define.DependType.MEDIA].channels)


def description(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """Description of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        description of database

    """
    db = header(name, version=version, cache_root=cache_root)
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
    df = deps()
    return pd.to_timedelta(
        df[df.type == define.DependType.MEDIA].duration.sum(),
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
    df = deps()
    return set(df[df.type == define.DependType.MEDIA].format)


def header(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> audformat.Database:
    r"""Load header of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        database object without table data

    """
    if version is None:
        version = latest_version(name)
    db_root = database_cache_folder(name, version, cache_root)
    db, _ = load_header(db_root, name, version)
    return db


def languages(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.List[str]:
    """Languages of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        languages of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.languages


def license(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """License of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        license of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.license


def license_url(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """License URL of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        license URL of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.license_url


def media(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Audio and video media of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        media of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.media


def meta(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Meta information of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        meta information of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.meta


def organization(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """Organization responsible for database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        organization responsible for database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.organization


def raters(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Raters contributed to database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        raters of database

    """
    db = header(name, version=version, cache_root=cache_root)
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
    df = deps()
    return set(df[df.type == define.DependType.MEDIA].sampling_rate)


def schemes(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Schemes of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        schemes of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.schemes


def source(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """Source of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        source of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.source


def splits(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Splits of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        splits of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.splits


def tables(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> typing.Dict:
    """Tables of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        tables of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.tables


def usage(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> str:
    """Usage of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        usage of database

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.usage
