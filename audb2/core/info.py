import os
import tempfile
import typing

import pandas as pd

import audformat

from audb2.core import define
from audb2.core.api import (
    default_backend,
    dependencies,
    repository_and_version,
)
from audb2.core.backend import Backend
from audb2.core.config import config


def bit_depths(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Set[int]:
    """Media bit depth.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        bit depths

    """
    deps = dependencies(name, version=version, backend=backend)
    return set(
        [
            deps.bit_depth(file) for file in deps.media
            if deps.bit_depth(file)
        ]
    )


def channels(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Set[int]:
    """Media channels.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        channel numbers

    """
    deps = dependencies(name, version=version, backend=backend)
    return set(
        [
            deps.channels(file) for file in deps.media
            if deps.channels(file)
        ]
    )


def description(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> str:
    """Description of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        description of database

    """
    db = header(name, version=version, backend=backend)
    return db.description


def duration(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> pd.Timedelta:
    """Total media duration.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        duration

    """
    deps = dependencies(name, version=version, backend=backend)
    return pd.to_timedelta(
        sum([deps.duration(file) for file in deps.media]),
        unit='s',
    )


def formats(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Set[str]:
    """Media formats.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        format

    """
    deps = dependencies(name, version=version, backend=backend)
    return set(
        [
            deps.format(file) for file in deps.media
        ]
    )


def header(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> audformat.Database:
    r"""Load header of database.

    Downloads the :file:`db.yaml` to a temporal directory,
    loads the database header and returns it.
    Does not write to the :mod:`audb2` cache folders.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        database object without table data

    """
    backend = default_backend(backend)
    repository, version = repository_and_version(
        name, version, backend=backend,
    )

    with tempfile.TemporaryDirectory() as root:
        remote_header = backend.join(name, define.HEADER_FILE)
        local_header = os.path.join(root, define.HEADER_FILE)
        backend.get_file(remote_header, local_header, version, repository)
        db = audformat.Database.load(root, load_data=False)

    return db


def languages(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.List[str]:
    """Languages of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        languages of database

    """
    db = header(name, version=version, backend=backend)
    return db.languages


def media(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Audio and video media of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        media of database

    """
    db = header(name, version=version, backend=backend)
    return db.media


def meta(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Meta information of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        meta information of database

    """
    db = header(name, version=version, backend=backend)
    return db.meta


def raters(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Raters contributed to database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        raters of database

    """
    db = header(name, version=version, backend=backend)
    return db.raters


def sampling_rates(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Set[int]:
    """Media sampling rates.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        sampling rates

    """
    deps = dependencies(name, version=version, backend=backend)
    return set(
        [
            deps.sampling_rate(file) for file in deps.media
            if deps.sampling_rate(file)
        ]
    )


def schemes(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Schemes of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        schemes of database

    """
    db = header(name, version=version, backend=backend)
    return db.schemes


def source(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> str:
    """Source of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        source of database

    """
    db = header(name, version=version, backend=backend)
    return db.source


def splits(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Splits of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        splits of database

    """
    db = header(name, version=version, backend=backend)
    return db.splits


def tables(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> typing.Dict:
    """Tables of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        tables of database

    """
    db = header(name, version=version, backend=backend)
    return db.tables


def usage(
        name: str,
        *,
        version: str = None,
        backend: Backend = None,
) -> str:
    """Usage of database.

    Args:
        name: name of database
        version: version of database
        backend: backend object

    Returns:
        usage of database

    """
    db = header(name, version=version, backend=backend)
    return db.usage
