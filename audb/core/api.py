import os
import shutil
import tempfile
import typing

import pandas as pd

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.cache import (
    database_cache_root,
    default_cache_root,
)
from audb.core.config import config
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.lock import FolderLock
from audb.core.repository import Repository
from audb.core.utils import (
    lookup_backend,
    _lookup,
)


def available(
        *,
        only_latest: bool = False,
) -> pd.DataFrame:
    r"""List all databases that are available to the user.

    Args:
        only_latest: include only latest version of database

    Returns:
        table with database name as index,
        and backend, host, repository, version as columns

    Example:
        >>> audb.available(only_latest=True)
                   backend                                    host   repository version
        name
        emodb  artifactory  https://audeering.jfrog.io/artifactory  data-public   1.3.0

    """  # noqa: E501
    databases = []
    for repository in config.REPOSITORIES:
        backend = audbackend.create(
            repository.backend,
            repository.host,
            repository.name,
        )
        try:
            names = backend.ls('')
        except FileNotFoundError:
            # Handle missing repos
            continue
        for name in names:
            try:
                versions = backend.ls(f'{name}/{define.DB}')
                for version in versions:
                    databases.append(
                        [
                            name,
                            repository.backend,
                            repository.host,
                            repository.name,
                            version,
                        ]
                    )
            except FileNotFoundError:
                # Handle broken databases
                continue

    df = pd.DataFrame.from_records(
        databases,
        columns=['name', 'backend', 'host', 'repository', 'version'],
    )
    if only_latest:
        # Pick latest version for every database, see
        # https://stackoverflow.com/a/53842408
        df = df[
            df['version'] == df.groupby('name')['version'].transform(
                lambda x: audeer.sort_versions(x)[-1]
            )
        ]
    else:
        # Sort by version
        df = df.sort_values(by=['version'], key=audeer.sort_versions)
    df = df.sort_values(by=['name'])
    return df.set_index('name')


def cached(
        cache_root: str = None,
        *,
        name: str = None,
        shared: bool = False,
) -> pd.DataFrame:
    r"""List available databases and flavors in the cache.

    Args:
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        name: name of database.
            If provided,
            it will show only cached versions of that database
        shared: include databases from shared cache

    Returns:
        cached databases
        with cache path as index,
        and name,
        flavor_id,
        version,
        complete,
        bit_depth,
        channels,
        format,
        mixdown,
        sampling_rate
        as columns

    Example:
        >>> db = audb.load(
        ...     'emodb',
        ...     version='1.3.0',
        ...     only_metadata=True,
        ...     full_path=False,
        ...     verbose=False,
        ... )
        >>> df = cached()
        >>> print(df.iloc[0].to_string())
        name                emodb
        flavor_id        d3b62a9b
        version             1.3.0
        complete            False
        bit_depth            None
        channels             None
        format               None
        mixdown             False
        sampling_rate        None

    """  # noqa: E501
    cache_root = audeer.path(
        cache_root or default_cache_root(shared=shared)
    )

    columns = [
        'name',
        'flavor_id',
        'version',
        'complete',
        'bit_depth',
        'channels',
        'format',
        'mixdown',
        'sampling_rate',
    ]
    df = pd.DataFrame([], columns=columns)

    if not os.path.exists(cache_root):
        return df

    database_paths = audeer.list_dir_names(cache_root)
    for database_path in database_paths:
        database = os.path.basename(database_path)

        # Limit to databases of given name
        if name is not None and database != name:
            continue

        version_paths = audeer.list_dir_names(database_path)
        for version_path in version_paths:
            version = os.path.basename(version_path)

            # Skip tmp folder (e.g. 1.0.0~)
            if version.endswith('~'):  # pragma: no cover
                continue

            flavor_id_paths = audeer.list_dir_names(version_path)

            # Skip old audb cache (e.g. 1 as flavor)
            files = audeer.list_file_names(version_path)
            deps_path = os.path.join(version_path, define.DEPENDENCIES_FILE)
            deps_path_cached = os.path.join(
                version_path,
                define.CACHED_DEPENDENCIES_FILE,
            )
            if deps_path not in files and deps_path_cached not in files:
                # Skip all cache entries
                # that don't contain a db.csv or db.pkl file
                # as those stem from audb<1.0.0.
                # We only look for db.csv
                # as we switched to db.pkl with audb>=1.0.5
                continue  # pragma: no cover

            for flavor_id_path in flavor_id_paths:
                flavor_id = os.path.basename(flavor_id_path)
                files = audeer.list_file_names(flavor_id_path)
                files = [os.path.basename(f) for f in files]

                if define.HEADER_FILE in files:
                    db = audformat.Database.load(
                        flavor_id_path,
                        load_data=False,
                    )
                    flavor = db.meta['audb']['flavor']
                    complete = db.meta['audb']['complete']
                    df.loc[flavor_id_path] = [
                        database,
                        flavor_id,
                        version,
                        complete,
                        flavor['bit_depth'],
                        flavor['channels'],
                        flavor['format'],
                        flavor['mixdown'],
                        flavor['sampling_rate'],
                    ]

    # Replace NaN with None
    return df.where(pd.notnull(df), None)


def dependencies(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
        verbose: bool = False,
) -> Dependencies:
    r"""Database dependencies.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        verbose: show debug messages

    Returns:
        dependency object

    Example:
        >>> deps = dependencies('emodb', version='1.3.0')
        >>> deps.version('db.emotion.csv')
        '1.1.0'

    """
    if version is None:
        version = latest_version(name)

    db_root = database_cache_root(
        name,
        version,
        cache_root=cache_root,
    )
    deps_path = os.path.join(db_root, define.CACHED_DEPENDENCIES_FILE)

    deps = Dependencies()

    with FolderLock(db_root):
        try:
            deps.load(deps_path)
        except (AttributeError, FileNotFoundError, ValueError, EOFError):
            # If loading pickled cached file fails, load again from backend
            backend = lookup_backend(name, version)
            with tempfile.TemporaryDirectory() as tmp_root:
                archive = backend.join(name, define.DB)
                backend.get_archive(
                    archive,
                    tmp_root,
                    version,
                    verbose=verbose,
                )
                deps.load(os.path.join(tmp_root, define.DEPENDENCIES_FILE))
                deps.save(deps_path)

    return deps


def exists(
    name: str,
    *,
    version: str = None,
    bit_depth: int = None,
    channels: typing.Union[int, typing.Sequence[int]] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    cache_root: str = None,
) -> bool:
    r"""Check if specified database flavor exists in local cache folder.

    Does not check for any flavor of the requested database in the cache,
    but only for a particular flavor.
    Note, that using only the name, e.g. ``audb.exists('emodb')``
    is also a single flavor.

    To list all available flavors of a particular database, use:

    .. code-block::

        audb.cached(name='emodb')

    Args:
        name: name of database
        version: version string, latest if ``None``
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        ``True`` if database flavor exists

    Example:
        >>> db = audb.load(
        ...     'emodb',
        ...     version='1.3.0',
        ...     only_metadata=True,
        ...     verbose=False,
        ... )
        >>> audb.exists('emodb', version='1.3.0')
        True
        >>> audb.exists('emodb', version='1.3.0', format='wav')
        False

    """
    if version is None:
        version = latest_version(name)

    relative_flavor_path = flavor_path(
        name,
        version,
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )

    cache_roots = [
        default_cache_root(True),  # check shared cache first
        default_cache_root(False),
    ] if cache_root is None else [cache_root]
    for cache_root in cache_roots:
        db_root = audeer.path(cache_root, relative_flavor_path)
        if os.path.exists(db_root):
            return True

    return False


def flavor_path(
    name: str,
    version: str,
    *,
    bit_depth: int = None,
    channels: typing.Union[int, typing.Sequence[int]] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
) -> str:
    r"""Flavor cache path.

    Returns the path under which :func:`audb.load` stores a specific
    flavor of a database in the cache folder, that is:

     ``<name>/<version>/<flavor-id>/``

    Note that the returned path is relative.
    To get the absolute path, do:

    .. code-block::

        os.path.join(
            audb.default_cache_root(...),
            audb.flavor_path(...),
        )

    Args:
        name: name of database
        version: version string
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``

    Returns:
        flavor path relative to cache folder

    Raises:
        ValueError: if a non-supported ``bit_depth``,
            ``format``,
            or ``sampling_rate``
            is requested

    Example:
        >>> flavor_path('emodb', version='1.3.0').split(os.path.sep)
        ['emodb', '1.3.0', 'd3b62a9b']

    """
    flavor = Flavor(
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )

    return flavor.path(name, version)


def latest_version(
        name,
) -> str:
    r"""Latest version of database.

    Args:
        name: name of database

    Returns:
        version string

    Raises:
        RuntimeError: if no version exists for the requested database

    Example:
        >>> latest_version('emodb')
        '1.3.0'

    """
    vs = versions(name)
    if not vs:
        raise RuntimeError(
            f"Cannot find a version for database '{name}'.",
        )
    return vs[-1]


def remove_media(
        name: str,
        files: typing.Union[str, typing.Sequence[str]],
        *,
        verbose: bool = False,
):
    r"""Remove media from all versions.

    Be careful,
    this removes files from all published versions
    on all backends.
    Those files cannot be restored afterwards.

    Args:
        name: name of database
        files: list of files that should be removed
        verbose: show debug messages

    """
    if isinstance(files, str):
        files = [files]

    for version in versions(name):

        backend = lookup_backend(name, version)

        with tempfile.TemporaryDirectory() as db_root:

            # download dependencies
            archive = backend.join(name, define.DB)
            deps_path = backend.get_archive(
                archive,
                db_root,
                version,
                verbose=verbose,
            )[0]
            deps_path = os.path.join(db_root, deps_path)
            deps = Dependencies()
            deps.load(deps_path)
            upload = False

            for file in audeer.progress_bar(
                files,
                disable=not verbose,
                desc=f'Remove media from v{version}',
            ):
                if file in deps.media:
                    archive = deps.archive(file)

                    # if archive exists in this version,
                    # remove file from it and re-publish
                    remote_archive = backend.join(
                        name,
                        define.DEPEND_TYPE_NAMES[define.DependType.MEDIA],
                        archive,
                    )
                    if backend.exists(
                            f'{remote_archive}.zip',
                            version,
                    ):

                        files_in_archive = backend.get_archive(
                            remote_archive,
                            db_root,
                            version,
                        )
                        # skip if file was already deleted
                        if file in files_in_archive:
                            os.remove(os.path.join(db_root, file))
                            files_in_archive.remove(file)
                            backend.put_archive(
                                db_root,
                                files_in_archive,
                                remote_archive,
                                version,
                            )

                    # mark file as removed
                    deps._remove(file)
                    upload = True

            # upload dependencies
            if upload:
                deps.save(deps_path)
                remote_archive = backend.join(name, define.DB)
                backend.put_archive(
                    db_root,
                    define.DEPENDENCIES_FILE,
                    remote_archive,
                    version,
                    verbose=verbose,
                )


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

    Example:
        >>> audb.repository('emodb', '1.3.0')
        Repository('data-public', 'https://audeering.jfrog.io/artifactory', 'artifactory')

    """  # noqa: E501
    return _lookup(name, version)[0]


def versions(
        name: str,
) -> typing.List[str]:
    r"""Available versions of database.

    Args:
        name: name of database

    Returns:
        list of versions

    Example:
        >>> versions('emodb')
        ['1.1.0', '1.1.1', '1.2.0', '1.3.0']

    """
    vs = []
    for repository in config.REPOSITORIES:
        backend = audbackend.create(
            repository.backend,
            repository.host,
            repository.name,
        )
        header = backend.join(name, 'db.yaml')
        vs.extend(backend.versions(header))
    return audeer.sort_versions(vs)
