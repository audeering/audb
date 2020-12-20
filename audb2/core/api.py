import os
import tempfile
import typing

import pandas as pd

import audeer
import audformat

from audb2.core import define
from audb2.core import utils
from audb2.core.backend import (
    Artifactory,
    Backend,
)
from audb2.core.config import config
from audb2.core.depend import Depend


def available(
        group_id: str = config.GROUP_ID,
        *,
        backend: Backend = None,
        verbose: bool = False,
) -> pd.DataFrame:
    r"""List all databases that are available to the user.

    Args:
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        table with name, version and private flag

    """
    backend = default_backend(backend, verbose=verbose)

    data = {}
    for repository in (
            config.REPOSITORY_PUBLIC,
            # config.REPOSITORY_PRIVATE, TODO: catch errors
    ):
        for p in backend.glob('**/*.yaml', repository, group_id):
            name, _, version, _ = p.split('/')[-4:]
            data[name] = {
                'version': version,
                'private': repository == config.REPOSITORY_PRIVATE,
            }

    return pd.DataFrame.from_dict(data, orient='index')


def cached(
        cache_root: str = None,
        *,
        shared: bool = False,
) -> pd.DataFrame:
    r"""List available databases in the cache.

    Args:
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb2.default_cache_root` is used
        shared: list shared databases

    Returns:
        cached databases

    """
    cache_root = audeer.safe_path(
        cache_root or default_cache_root(shared=shared)
    )

    data = {}
    for root, dirs, files in os.walk(cache_root):
        if define.DB_HEADER in files:
            name, flavor_id, version = root.split(os.path.sep)[-3:]
            db = audformat.Database.load(root, load_data=False)
            flavor = db.meta['audb']['flavor']
            data[root] = {
                'name': name,
                'flavor_id': flavor_id,
                'version': version,
            }
            data[root].update(flavor)

    return pd.DataFrame.from_dict(data, orient='index')


def default_backend(
        backend: Backend = None,
        *,
        verbose: bool = False,
):
    r"""Default backend."""
    return backend or Artifactory(verbose=verbose)


def default_cache_root(
        shared=False,
) -> str:
    r"""Default cache folder.

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


def dependencies(
        name: str,
        *,
        version: str = None,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> Depend:
    r"""Database dependencies.

    Args:
        name: name of database
        version: version string
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        dependency object

    """
    backend = default_backend(backend, verbose=verbose)
    repository, version = repository_and_version(
        name, version, group_id=group_id, backend=backend,
    )
    group_id: str = f'{group_id}.{name}'

    with tempfile.TemporaryDirectory() as root:
        dep_path = backend.get_archive(
            root, audeer.basename_wo_ext(define.DB_DEPEND),
            version, repository, group_id,
        )[0]
        dep_path = os.path.join(root, dep_path)
        depend = Depend()
        depend.from_file(dep_path)

    return depend


def latest_version(
        name,
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> str:
    r"""Latest version of database.

    Args:
        name: name of database
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        version string

    """
    backend = default_backend(backend, verbose=verbose)

    vs = versions(name, group_id=group_id, backend=backend)
    if not vs:
        raise RuntimeError(
            f"Cannot find a version for database '{name}'.",
        )
    utils.sort_versions(vs)
    return vs[-1]


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
    backend = default_backend(backend, verbose=verbose)

    if isinstance(files, str):
        files = [files]

    for version in versions(name, group_id=group_id, backend=backend):
        repository, version = repository_and_version(
            name, version, group_id=group_id, backend=backend,
        )

        with tempfile.TemporaryDirectory() as db_root:

            # download dependencies
            dep_path = backend.get_archive(
                db_root, audeer.basename_wo_ext(define.DB_DEPEND),
                version, repository, f'{group_id}.{name}',
            )[0]
            dep_path = os.path.join(db_root, dep_path)
            upload = False

            depend = Depend()
            depend.from_file(dep_path)
            for file in files:
                if file in depend.files:
                    archive = depend.archive(file)

                    # remove file from archive
                    files = backend.get_archive(
                        db_root, archive, version, repository,
                        f'{group_id}.{name}.'
                        f'{define.DEPEND_TYPE_NAMES[define.DependType.MEDIA]}',
                    )
                    files.remove(file)
                    os.remove(os.path.join(db_root, file))
                    backend.put_archive(
                        db_root, files, archive, version, repository,
                        f'{group_id}.{name}.'
                        f'{define.DEPEND_TYPE_NAMES[define.DependType.MEDIA]}',
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
                    version, repository, f'{group_id}.{name}', force=True,
                )


def repository_and_version(
        name,
        version: typing.Optional[str],
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
) -> (str, str):
    r"""Resolve version of database."""

    if version is None:
        version = latest_version(name, group_id=group_id, backend=backend)
    else:
        if version not in versions(name, group_id=group_id, backend=backend):
            raise RuntimeError(
                f"A version '{version}' does not exist for database '{name}'."
            )

    for repository in (
        config.REPOSITORY_PUBLIC,  # check public repository first
        config.REPOSITORY_PRIVATE,
    ):
        if backend.exists(
            define.DB_HEADER, version, repository, f'{group_id}.{name}',
        ):
            break

    return repository, version


def versions(
        name: str,
        *,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> typing.List[str]:
    r"""Available versions of database.

    Args:
        name: name of database
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        list of versions

    """
    backend = default_backend(backend, verbose=verbose)

    vs = []
    for repository in [
        config.REPOSITORY_PUBLIC,
        config.REPOSITORY_PRIVATE,
    ]:
        vs.extend(
            backend.versions(
                define.DB_HEADER, repository, f'{group_id}.{name}',
            )
        )

    return vs
