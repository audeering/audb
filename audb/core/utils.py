import os
import typing
import warnings

import audbackend
import audeer

from audb.core import define
from audb.core.config import config
from audb.core.repository import Repository


def lookup_backend(
        name: str,
        version: str,
) -> audbackend.Backend:
    r"""Return backend of requested database.

    If the database is stored in several repositories,
    only the first one is considered.
    The order of the repositories to look for the database
    is given by :attr:`config.REPOSITORIES`.

    Args:
        name: database name
        version: version string

    Returns:
        backend


    Raises:
        RuntimeError: if database is not found

    """
    return _lookup(name, version)[1]


def mkdir_tree(
        files: typing.Sequence[str],
        root: str,
):
    r"""Helper function to create folder tree."""
    folders = set()
    for file in files:
        folders.add(os.path.dirname(file))
    for folder in folders:
        audeer.mkdir(os.path.join(root, folder))


def _lookup(
        name: str,
        version: str,
) -> typing.Tuple[Repository, audbackend.Backend]:
    r"""Helper function to look up database in all repositories.

    Returns repository, version and backend object.

    """
    for repository in config.REPOSITORIES:

        backend = audbackend.create(
            repository.backend,
            repository.host,
            repository.name,
        )
        header = backend.join(name, 'db.yaml')

        if backend.exists(header, version):
            return repository, backend

    raise RuntimeError(
        'Cannot find version '
        f'{version} '
        f'for database '
        f"'{name}'."
    )


def timeout_warning():
    warnings.warn(
        define.TIMEOUT_MSG,
        category=UserWarning,
    )
