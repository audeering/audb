import os
import tempfile
import typing
import warnings

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
from audb2.core.flavor import Flavor


def available(
        group_id: str = config.GROUP_ID,
        *,
        latest_only: bool = False,
        backend: Backend = None,
        verbose: bool = False,
) -> pd.DataFrame:
    r"""List all databases that are available to the user.

    Args:
        group_id: group ID
        latest_only: keep only latest version
        backend: backend object
        verbose: show debug messages

    Returns:
        table with name, version and private flag

    """
    backend = default_backend(backend, verbose=verbose)

    match = {}
    for repository in (
            config.REPOSITORY_PUBLIC,
            config.REPOSITORY_PRIVATE,
    ):
        for p in backend.glob('**/*.yaml', repository, group_id):
            name, _, version, _ = p.split('/')[-4:]
            if name not in match:
                match[name] = {
                    'version': [],
                    'private': repository == config.REPOSITORY_PRIVATE,
                }
            match[name]['version'].append(version)

    for name in match:
        utils.sort_versions(match[name]['version'])
        if latest_only:
            match[name]['version'] = [match[name]['version'][-1]]

    data = []
    for name in match:
        for v in match[name]['version']:
            data.append(
                [name, v, match[name]['private']]
            )
    return pd.DataFrame.from_records(
        data, columns=['name', 'version', 'private']
    ).set_index('name')


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

    Args:
        shared: if ``True`` returns path to shared cache folder

    Returns:
        path normalized by :func:`audeer.safe_path`

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


def exists(
    name: str,
    *,
    version: str = None,
    only_metadata: bool = False,
    bit_depth: int = None,
    channels: typing.Union[int, typing.Sequence[int]] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    tables: typing.Union[str, typing.Sequence[str]] = None,
    include: typing.Union[str, typing.Sequence[str]] = None,
    exclude: typing.Union[str, typing.Sequence[str]] = None,
    cache_root: str = None,
    group_id: str = config.GROUP_ID,
    backend: Backend = None,
    verbose: bool = False,
) -> typing.Optional[str]:
    r"""Check if specified database flavor exists in local cache folder.

    Does not yet return ``True`` or ``False``,
    but ``None`` or path to flavor.
    Nonetheless,
    it can still be used with an if-statement:

    .. code-block::

        if audb2.exists('emodb', version='1.0.1', mixdown=True):
            print('emodb v1.0.1 {mono} cached')

    Note that the return value will change to ``bool`` with version 1.0.0.

    Does not check for any flavor of the requested database in the cache,
    but only for a particular flavor.
    Note, that using only the name, e.g. ``audb.exists('emodb')``
    is also a single flavor.

    To list all available flavors of a particular database, use:

    .. code-block::

        audb2.cached().query('name == "emodb"')

    Args:
        name: name of database
        version: version string, latest if ``None``
        only_metadata: only metadata is stored
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        tables: include only tables matching the regular expression or
            provided in the list
        include: include only media from archives matching the regular
            expression or provided in the list
        exclude: don't include media from archives matching the regular
            expression or provided in the list. This filter is applied
            after ``include``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb2.default_cache_root` is used
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        ``None`` or path to flavor

    """
    warnings.warn(
        "The return value of 'exists' will "
        "change from 'str' to 'bool' "
        "with version '1.0.0'.",
        category=UserWarning,
        stacklevel=2,
    )

    backend = default_backend(backend, verbose=verbose)
    repository, version = repository_and_version(
        name, version, group_id=group_id, backend=backend,
    )

    relative_flavor_path = flavor_path(
        name,
        version,
        repository,
        group_id,
        only_metadata=only_metadata,
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
        tables=tables,
        include=include,
        exclude=exclude,
    )

    cache_roots = [
        default_cache_root(True),  # check shared cache first
        default_cache_root(False),
    ] if cache_root is None else [cache_root]
    for cache_root in cache_roots:
        db_root = audeer.safe_path(
            os.path.join(cache_root, relative_flavor_path)
        )
        if os.path.exists(db_root):
            return db_root  # True

    return None  # False


def flavor_path(
    name: str,
    version: str,
    repository: str,
    group_id: str,
    *,
    only_metadata: bool = False,
    bit_depth: int = None,
    channels: typing.Union[int, typing.Sequence[int]] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    tables: typing.Union[str, typing.Sequence[str]] = None,
    include: typing.Union[str, typing.Sequence[str]] = None,
    exclude: typing.Union[str, typing.Sequence[str]] = None,
) -> str:
    r"""Flavor cache path.

    Returns the path under which :func:`audb2.load` stores a specific
    flavor of a database in the cache folder, that is:

     ``<repository>/<group-id>/<name>/<flavor-id>/<version>/``

    Note that the returned path is relative.
    To get the absolute path, do:

    .. code-block::

        os.path.join(
            audb2.default_cache_root(...),
            audb2.flavor_path(...),
        )

    Args:
        name: name of database
        version: version string
        repository: repository
        group_id: group ID
        only_metadata: only metadata is stored
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        tables: include only tables matching the regular expression or
            provided in the list
        include: include only media from archives matching the regular
            expression or provided in the list
        exclude: don't include media from archives matching the regular
            expression or provided in the list. This filter is applied
            after ``include``

    Returns:
        flavor path relative to cache folder

    """
    flavor = Flavor(
        only_metadata=only_metadata,
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
        tables=tables,
        include=include,
        exclude=exclude,
    )

    return flavor.path(name, version, repository, group_id)


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
