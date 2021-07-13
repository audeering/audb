import typing
import warnings

import audbackend

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


def repository(
        name: str,
        version: str,
) -> Repository:
    r"""Return repository that stores the requested database.

    If the database is stored in several repositories,
    only the first one is returned.
    The order of the repositories to look for the database
    is given by :attr:`config.REPOSITORIES`.

    Args:
        name: database name
        version: version string

    Returns:
        repository that contains the database

    Raises:
        RuntimeError: if database is not found

    """
    return _lookup(name, version)[0]


def mix_mapping(
        mix: str,
        warn: bool = True,
) -> typing.Tuple[typing.Optional[typing.List[int]], bool]:
    r"""Argument mapping for deprecated mix argument.

    Args:
        mix: old mix argument from audb,
            can be ``'mono'``, ``'stereo'``, ``'left'``, ``'right'``
        warn: if ``True`` it shows a deprecation warning

    Returns:
        channels and mixdown arguments

    """
    if warn:
        warnings.warn(
            "Argument 'mix' is deprecated "
            "and will be removed with version '1.2.0'. "
            "Use 'channels' and 'mixdown' instead.",
            category=UserWarning,
            stacklevel=2,
        )
    if mix == 'mono':
        channels = None
        mixdown = True
    elif mix == 'stereo':
        channels = [0, 1]
        mixdown = False
    elif mix == 'left':
        channels = [0]
        mixdown = False
    elif mix == 'right':
        channels = [1]
        mixdown = False
    elif mix is None:
        channels = None
        mixdown = False
    else:
        raise ValueError(
            f"Using deprecated argument 'mix' with value '{mix}' "
            "is no longer supported."
        )
    return channels, mixdown


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
