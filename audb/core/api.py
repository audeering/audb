import os
import shutil
import tempfile
import typing

import pandas as pd

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.config import config
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.utils import (
    lookup_backend,
    mix_mapping,
)


def available(
        *,
        only_latest: bool = False,
) -> pd.DataFrame:
    r"""List all databases that are available to the user.

    Args:
        only_latest: keep only latest version

    Returns:
        table with name, version and private flag

    """
    databases = []
    for repository in config.REPOSITORIES:
        pattern = f'*/{define.DB}/*/{define.DB}-*.yaml'
        backend = audbackend.create(
            repository.backend,
            repository.host,
            repository.name,
        )
        for p in backend.glob(pattern):
            name, _, version, _ = p.split('/')[-4:]
            databases.append(
                [
                    name,
                    repository.backend,
                    repository.host,
                    repository.name,
                    version,
                ]
            )

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
        shared: bool = False,
) -> pd.DataFrame:
    r"""List available databases in the cache.

    Args:
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        shared: list shared databases

    Returns:
        cached databases

    """
    cache_root = audeer.safe_path(
        cache_root or default_cache_root(shared=shared)
    )

    data = {}

    database_paths = audeer.list_dir_names(cache_root)
    for database_path in database_paths:
        database = os.path.basename(database_path)
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
                    data[flavor_id_path] = {
                        'name': database,
                        'flavor_id': flavor_id,
                        'version': version,
                        'complete': complete,
                    }
                    data[flavor_id_path].update(flavor)

    df = pd.DataFrame.from_dict(data, orient='index', dtype='object')
    # Replace NaN with None
    return df.where(pd.notnull(df), None)


def default_cache_root(
        shared=False,
) -> str:
    r"""Default cache folder.

    If ``shared`` is ``True``,
    returns the path specified
    by the environment variable
    ``AUDB_SHARED_CACHE_ROOT``
    or
    ``audb.config.SHARED_CACHE_ROOT``.
    If ``shared`` is ``False``,
    returns the path specified
    by the environment variable
    ``AUDB_CACHE_ROOT``
    or
    ``audb.config.CACHE_ROOT``.

    Args:
        shared: if ``True`` returns path to shared cache folder

    Returns:
        path normalized by :func:`audeer.safe_path`

    """
    if shared:
        cache = (
            os.environ.get('AUDB_SHARED_CACHE_ROOT')
            or config.SHARED_CACHE_ROOT
        )
    else:
        cache = (
            os.environ.get('AUDB_CACHE_ROOT')
            or config.CACHE_ROOT
        )
    return audeer.safe_path(cache)


def dependencies(
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
) -> Dependencies:
    r"""Database dependencies.

    Args:
        name: name of database
        version: version string
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        dependency object

    """
    if version is None:
        version = latest_version(name)

    cache_roots = [
        default_cache_root(True),  # check shared cache first
        default_cache_root(False),
    ] if cache_root is None else [cache_root]
    for cache_root in cache_roots:
        deps_root = audeer.safe_path(
            os.path.join(
                cache_root,
                name,
                version,
            )
        )
        if os.path.exists(deps_root):
            break

    audeer.mkdir(deps_root)
    deps_path = os.path.join(deps_root, define.CACHED_DEPENDENCIES_FILE)

    deps = Dependencies()
    if not os.path.exists(deps_path):
        backend = lookup_backend(name, version)
        with tempfile.TemporaryDirectory() as tmp_root:
            archive = backend.join(name, define.DB)
            backend.get_archive(
                archive,
                tmp_root,
                version,
            )
            deps.load(os.path.join(tmp_root, define.DEPENDENCIES_FILE))
            deps.save(deps_path)
    else:
        deps.load(deps_path)

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
    **kwargs,
) -> bool:
    r"""Check if specified database flavor exists in local cache folder.

    Does not check for any flavor of the requested database in the cache,
    but only for a particular flavor.
    Note, that using only the name, e.g. ``audb.exists('emodb')``
    is also a single flavor.

    To list all available flavors of a particular database, use:

    .. code-block::

        audb.cached().query('name == "emodb"')

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

    """
    # Map mix to channels and mixdown
    if (
            channels is None
            and not mixdown
            and 'mix' in kwargs
    ):  # pragma: no cover
        mix = kwargs['mix']
        channels, mixdown = mix_mapping(mix)

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
        db_root = audeer.safe_path(
            os.path.join(cache_root, relative_flavor_path)
        )
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
            )[0]
            deps_path = os.path.join(db_root, deps_path)
            deps = Dependencies()
            deps.load(deps_path)
            upload = False

            for file in files:
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
                )


def versions(
        name: str,
) -> typing.List[str]:
    r"""Available versions of database.

    Args:
        name: name of database

    Returns:
        list of versions

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
