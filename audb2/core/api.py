import os
import tempfile
import typing
import warnings

import pandas as pd

import audbackend
import audeer
import audformat

from audb2.core import define
from audb2.core.config import config
from audb2.core.dependencies import Dependencies
from audb2.core.flavor import Flavor


def available(
        *,
        latest_only: bool = False,
) -> pd.DataFrame:
    r"""List all databases that are available to the user.

    Args:
        latest_only: keep only latest version

    Returns:
        table with name, version and private flag

    """

    match = {}
    for repository in config.REPOSITORIES:
        pattern = f'*/{define.DB}/*/{define.DB}-*.yaml'
        backend = audbackend.create(
            repository.backend,
            repository.host,
            repository.name,
        )
        for p in backend.glob(pattern):
            name, _, version, _ = p.split('/')[-4:]
            if name not in match:
                match[name] = {
                    'backend': repository.backend,
                    'host': repository.host,
                    'repository': repository.name,
                    'version': [],
                }
            match[name]['version'].append(version)

    for name in match:
        match[name]['version'] = audeer.sort_versions(
            match[name]['version']
        )
        if latest_only:
            match[name]['version'] = [match[name]['version'][-1]]

    data = []
    for name in match:
        for v in match[name]['version']:
            data.append(
                [
                    name,
                    match[name]['backend'],
                    match[name]['host'],
                    match[name]['repository'],
                    v,
                ]
            )
    return pd.DataFrame.from_records(
        data,
        columns=['name', 'backend', 'host', 'repository', 'version'],
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
        if define.HEADER_FILE in files:
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


def default_cache_root(
        shared=False,
) -> str:
    r"""Default cache folder.

    If ``shared`` is ``True``,
    returns the path specified
    by the environment variable
    ``AUDB2_SHARED_CACHE_ROOT``
    or
    ``audb2.config.SHARED_CACHE_ROOT``.
    If ``shared`` is ``False``,
    returns the path specified
    by the environment variable
    ``AUDB2_CACHE_ROOT``
    or
    ``audb2.config.CACHE_ROOT``.

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
) -> Dependencies:
    r"""Database dependencies.

    Args:
        name: name of database
        version: version string

    Returns:
        dependency object

    """
    _, version, backend = _lookup(name, version)

    with tempfile.TemporaryDirectory() as root:
        archive = backend.join(name, define.DB)
        deps_path = backend.get_archive(
            archive,
            root,
            version,
        )[0]
        deps_path = os.path.join(root, deps_path)
        deps = Dependencies()
        deps.load(deps_path)

    return deps


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

    Returns:
        ``None`` or path to flavor.
        Note that the return value will change
        to ``False`` or ``True`` with version 1.0.0.

    """
    warnings.warn(
        "The return value of 'exists' will "
        "change from 'str' to 'bool' "
        "with version '1.0.0'.",
        category=UserWarning,
        stacklevel=2,
    )

    repository, version, backend = _lookup(name, version)

    relative_flavor_path = flavor_path(
        name,
        version,
        repository.name,
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
        repository: repository name
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

    return flavor.path(name, version, repository)


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
    vs = audeer.sort_versions(vs)
    return vs[-1]


def _lookup(
        name: str,
        version: str = None,
) -> (typing.Dict[str, str], str, audbackend.Backend):
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

        if version is None:
            try:
                version = backend.latest_version(header)
            except RuntimeError:
                continue

        if backend.exists(header, version):
            return repository, version, backend

    if version is None:
        raise RuntimeError(
            'Cannot find database '
            f"'{name}'."
        )
    else:
        raise RuntimeError(
            'Cannot find version '
            f'{version} '
            f'for database '
            f"'{name}'."
        )


def lookup_repository(
        name: str,
        version: str = None,
) -> typing.Dict[str, str]:
    r"""Look for database in all repositories.

    Args:
        name: database name
        version: version string, if `None` look for latest

    Returns:
        repository name, version string, backend object

    Raises:
        RuntimeError: if database and/or version is not found

    """
    return _lookup(name, version)[0]


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

        _, version, backend = _lookup(name, version)

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
                        os.remove(os.path.join(db_root, file))
                        files_in_archive.remove(file)
                        backend.put_archive(
                            db_root,
                            files_in_archive,
                            remote_archive,
                            version,
                        )

                    # mark file as removed
                    deps.remove(file)
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
