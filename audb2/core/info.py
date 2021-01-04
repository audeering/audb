import tempfile
import typing

import audformat

from audb2.core import define
from audb2.core.api import (
    default_backend,
    repository_and_version,
)
from audb2.core.backend import Backend
from audb2.core.config import config


def description(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> str:
    """Description of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        description of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.description


def header(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> audformat.Database:
    r"""Load header of database.

    Downloads the :file:`db.yaml` to a temporal directory,
    loads the database header and returns it.
    Does not write to the :mod:`audb2` cache folders.

    Args:
        name: name of database
        version: version of database
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        database object without table data

    """
    backend = default_backend(backend, verbose=verbose)
    repository, version = repository_and_version(
        name, version, group_id=group_id, backend=backend,
    )

    with tempfile.TemporaryDirectory() as root:
        backend.get_file(
            root, define.DB_HEADER, version, repository, f'{group_id}.{name}',
        )
        db = audformat.Database.load(root, load_data=False)

    return db


def languages(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.List[str]:
    """Languages of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        languages of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.languages


def media(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Audio and video media of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        media of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.media


def meta(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Meta information of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        meta information of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.meta


def raters(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Raters contributed to database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        raters of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.raters


def schemes(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Schemes of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        schemes of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.schemes


def source(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> str:
    """Source of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        source of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.source


def splits(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Splits of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        splits of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.splits


def tables(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.Dict:
    """Tables of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        tables of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.tables


def usage(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> str:
    """Usage of database.

    Args:
        name: name of database
        group_id: group ID of database
        version: version of database
        backend: backend object
        verbose: show debug messages

    Returns:
        usage of database

    """
    db = header(
        name, group_id=group_id, version=version,
        backend=backend, verbose=verbose,
    )
    return db.usage
