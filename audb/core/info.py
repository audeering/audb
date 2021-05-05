import typing

import pandas as pd

import audformat

from audb.core import define
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

    Example:
        >>> author('emodb', version='1.1.1')
        'Felix Burkhardt, Astrid Paeschke, Miriam Rolfes, Walter Sendlmeier, Benjamin Weiss'

    """  # noqa: E501
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

    Example:
        >>> bit_depths('emodb', version='1.1.1')
        {16}

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

    Example:
        >>> channels('emodb', version='1.1.1')
        {1}

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

    Example:
        >>> desc = description('emodb', version='1.1.1')
        >>> desc.split('.')[0]  # show first sentence
        'Berlin Database of Emotional Speech'

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

    Example:
        >>> duration('emodb', version='1.1.1')
        Timedelta('0 days 00:24:47.092187500')

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

    Example:
        >>> formats('emodb', version='1.1.1')
        {'wav'}

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

    Example:
        >>> db = header('emodb', version='1.1.1')
        >>> db.name
        'emodb'

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

    Example:
        >>> languages('emodb', version='1.1.1')
        ['deu']

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

    Example:
        >>> license('emodb', version='1.1.1')
        'CC0-1.0'

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

    Example:
        >>> license_url('emodb', version='1.1.1')
        'https://creativecommons.org/publicdomain/zero/1.0/'

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

    Example:
        >>> media('emodb', version='1.1.1')
        microphone:
            {type: other, format: wav, channels: 1, sampling_rate: 16000}

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

    Example:
        >>> meta('emodb', version='1.1.1')
        pdf:
          http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.130.8506&rep=rep1&type=pdf

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

    Example:
        >>> organization('emodb', version='1.1.1')
        'audEERING'

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

    Example:
        >>> raters('emodb', version='1.1.1')
        gold:
            {type: human}

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

    Example:
        >>> sampling_rates('emodb', version='1.1.1')
        {16000}

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

    Example:
        >>> list(schemes('emodb', version='1.1.1'))
        ['confidence', 'duration', 'emotion', 'speaker', 'transcription']

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

    Example:
        >>> source('emodb', version='1.1.1')
        'http://emodb.bilderbar.info/download/download.zip'

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

    Example:
        >>> splits('emodb', version='1.1.1')


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

    Example:
        >>> list(tables('emodb', version='1.1.1'))
        ['emotion', 'files']

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

    Example:
        >>> usage('emodb', version='1.1.1')
        'unrestricted'

    """
    db = header(name, version=version, cache_root=cache_root)
    return db.usage
