import os
import tempfile
import typing

import pandas as pd

import audeer

from audb2.core import define
from audb2.core.backend import (
    Artifactory,
    Backend,
)
from audb2.core.config import config
from audb2.core.depend import Dependencies


def default_cache_root(
        shared=False,
) -> str:
    r"""Return default cache folder.

    If ``shared`` is ``True``,
    returns the path specified
    by the environment variable
    ``AUDB2_SHARED_CACHE_ROOT``
    or :attr:`audb2.config.SHARED_CACHE_ROOT`.
    If ``shared`` is ``False``,
    returns the path specified
    by the environment variable
    ``AUDB2_CACHE_ROOT``
    or :attr:`audb2.config.CACHE_ROOT`.

    The returned path is normalized by :func:`audeer.safe_path`.

    Args:
        shared: if ``True`` returns path to shared cache folder

    """
    if shared:
        cache = (
            os.environ.get('AUDB2_SHARED_CACHE_ROOT')
            or config.SHARED_CACHE_ROOT
        )
    else:
        cache = (
            os.environ.get('AUDB2_CACHE_ROOT')
            or config.CACHE_ROOT
        )
    return audeer.safe_path(cache)


def latest_version(
        name,
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
) -> str:
    r"""Latest version of database.

    Args:
        name: name of database
        group_id: group ID
        backend: backend object

    Returns:
        version string

    """
    backend = backend or Artifactory(name)
    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id: str = f'{group_id}.{name}'
    return backend.latest_version(define.DB_HEADER, repository, group_id)


def remove_media(
        name: str,
        files: typing.Union[str, typing.Sequence[str]],
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
):
    r"""Remove media from all versions.

    Args:
        name: name of database
        files: list of files that should be removed
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    """
    backend = backend or Artifactory(name, verbose=verbose)

    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id = f'{group_id}.{name}'

    if isinstance(files, str):
        files = [files]

    for version in backend.versions(
        define.DB_HEADER, repository, group_id,
    ):
        with tempfile.TemporaryDirectory() as db_root:

            # download dependencies
            dep_path = backend.get_archive(
                db_root, audeer.basename_wo_ext(define.DB_DEPEND),
                version, repository, group_id,
            )[0]
            dep_path = os.path.join(db_root, dep_path)
            upload = False

            depend = Dependencies()
            depend.from_file(dep_path)
            for file in files:
                if file in depend.files:
                    archive = depend.archive(file)

                    # remove file from archive
                    files = backend.get_archive(
                        db_root, archive, version, repository,
                        f'{group_id}.{define.TYPE_NAMES[define.Type.MEDIA]}',
                    )
                    files.remove(file)
                    os.remove(os.path.join(db_root, file))
                    backend.put_archive(
                        db_root, files, archive, version, repository,
                        f'{group_id}.{define.TYPE_NAMES[define.Type.MEDIA]}',
                        force=True,
                    )

                    # update dependency
                    depend.remove(file)
                    upload = True

            # upload dependencies
            if upload:
                depend.to_file(dep_path)
                backend.put_archive(
                    db_root, define.DB_DEPEND,
                    audeer.basename_wo_ext(define.DB_DEPEND),
                    version, repository, group_id, force=True,
                )


def versions(
        name: str,
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
) -> typing.List[str]:
    r"""Available versions of database.

    Args:
        name: name of database
        group_id: group ID
        backend: backend object

    Returns:
        list of versions

    """
    backend = backend or Artifactory(name)

    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id = f'{group_id}.{name}'

    return backend.versions(define.DB_HEADER, repository, group_id)
