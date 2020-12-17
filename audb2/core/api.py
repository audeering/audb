import os
import tempfile
import typing

import pandas as pd

import audata
import audeer

from audb2.core import define
from audb2.core.backend import (
    Artifactory,
    Backend,
)
from audb2.core.config import config
from audb2.core.depend import Dependencies
from audb2.core.flavor import Flavor


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


def dependencies(
        dep_path: str,
) -> pd.DataFrame:
    r"""Read dependency table.

    Args:
        dep_path: file path

    Returns:
        table

    """
    return pd.read_csv(dep_path, index_col=0, na_filter=False)


def latest_version(
        name,
        *,
        backend: Backend = None,
) -> str:
    r"""Latest version of database.

    Args:
        name: name of database
        backend: backend object

    Returns:
        version string

    """
    backend = backend or Artifactory(name)
    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id: str = f'{config.GROUP_ID}.{name}'
    return backend.latest_version(define.DB_HEADER, repository, group_id)


def _load(
        name: str,
        db_root: str,
        version: str,
        flavor: Flavor = None,
        removed_media: bool = False,
        full_path: bool = True,
        backend: Backend = None,
        verbose: bool = False,
) -> audata.Database:
    r"""Helper function for load()."""

    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id: str = f'{config.GROUP_ID}.{name}'

    backend.get_file(
        db_root, define.DB_HEADER, version, repository, group_id,
    )

    dep_path = backend.get_archive(
        db_root, audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )[0]
    dep_path = os.path.join(db_root, dep_path)

    with Dependencies(
            dep_path, db_root, repository, group_id, backend,
            verbose=verbose
    ) as depend:

        db = depend.load(flavor)

        # filter rows referencing removed media
        if not removed_media:
            db.filter_files(lambda x: not depend.removed(x))

        # fix file extension in tables
        if flavor is not None and flavor.format is not None:
            # Faster solution then using db.map_files()
            cur_ext = r'\.[a-zA-Z0-9]+$'  # match file extension
            new_ext = f'.{flavor.format}'
            for table in db.tables.values():
                if table.is_filewise:
                    table.df.index = table.df.index.str.replace(
                        cur_ext, new_ext,
                    )
                else:
                    table.df.index.set_levels(
                        table.df.index.levels[0].str.replace(
                            cur_ext, new_ext),
                        'file',
                        inplace=True,
                    )

        # store root and flavor
        if flavor is not None:
            db.meta['audb'] = {
                'root': db_root,
                'version': version,
                'flavor': flavor.arguments,
            }

    if full_path:
        # Faster solution then using db.map_files()
        root = db_root + os.path.sep
        for table in db.tables.values():
            if table.is_filewise:
                table.df.index = root + table.df.index
                table.df.index.name = 'file'
            elif len(table.df.index) > 0:
                table.df.index.set_levels(
                    root + table.df.index.levels[0], 'file', inplace=True,
                )

    return db


def load(
        name: str,
        version: str = None,
        *,
        only_metadata: bool = False,
        bit_depth: int = None,
        format: str = None,
        mix: str = None,
        sampling_rate: int = None,
        tables: typing.Union[str, typing.Sequence[str]] = None,
        include: typing.Union[str, typing.Sequence[str]] = None,
        exclude: typing.Union[str, typing.Sequence[str]] = None,
        removed_media: bool = False,
        full_path: bool = True,
        backend: Backend = None,
        verbose: bool = False,
) -> audata.Database:
    r"""Load database from Artifactory.

    Args:
        name: name of database
        version: version string, latest if ``None``
        only_metadata: only metadata is stored
        format: file format, one of ``'flac'``, ``'wav'``
        mix: mixing strategy, one of
            ``'left'``, ``'right'``, ``'mono'``, ``'stereo'``
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        tables: include only tables matching the regular expression or
            provided in the list
        include: include only media from archives matching the regular
            expression or provided in the list
        exclude: don't include media from archives matching the regular
            expression or provided in the list. This filter is applied
            after ``include``
        removed_media: keep rows that reference removed media
        full_path: replace relative with absolute file paths
        backend: backend object
        verbose: show debug messages

    Returns:
        database object

    """
    backend = backend or Artifactory(name, verbose=verbose)

    if version is None:
        version = latest_version(name, backend=backend)

    if version not in versions(name, backend=backend):
        raise RuntimeError(
            f"A version '{version}' does not exist for database '{name}'."
        )

    flavor = Flavor(
        only_metadata=only_metadata,
        format=format,
        mix=mix,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
        tables=tables,
        include=include,
        exclude=exclude,
    )
    db_root = os.path.join(config.CACHE_ROOT, name, flavor.id, version)
    return _load(
        name, db_root, version, flavor, removed_media,
        full_path, backend, verbose,
    )


def load_raw(
        root: str,
        name: str,
        version: str = None,
        *,
        backend: Backend = None,
        verbose: bool = False,
) -> audata.Database:
    r"""Load database from Artifactory.

    Args:
        root: target directory
        name: name of database
        version: version string, latest if ``None``
        backend: backend object
        verbose: show debug messages

    Returns:
        database object

    """
    backend = backend or Artifactory(name, verbose=verbose)

    if version is None:
        version = latest_version(name, backend=backend)

    if version not in versions(name, backend=backend):
        raise RuntimeError(
            f"A version '{version}' does not exist for database '{name}'.")

    return _load(name, root, version, None, True, False, backend, verbose)


def publish(
        db_root: str,
        version: str,
        *,
        archives: typing.Mapping[str, str] = None,
        private: bool = False,
        backend: Backend = None,
        verbose: bool = False,
) -> pd.DataFrame:
    r"""Publish database to Artifactory.

    Args:
        db_root: root directory of database
        version: version string
        archives: map files to archives
        private: publish as private
        backend: backend object
        verbose: show debug messages

    Returns:
        dependency object

    Raises:
        RuntimeError: if version already exists

    """
    db = audata.Database.load(db_root, load_data=False)

    backend = backend or Artifactory(db.name, verbose=verbose)

    repository = config.REPOSITORY_PRIVATE if private else \
        config.REPOSITORY_PUBLIC
    group_id: str = f'{config.GROUP_ID}.{db.name}'

    if version in backend.versions(
        define.DB_HEADER,
        repository,
        group_id,
    ):
        raise RuntimeError(
            f"A version '{version}' already exists for "
            f"database '{db.name}'."
        )

    dep_path = os.path.join(db_root, define.DB_DEPEND)
    with Dependencies(
            dep_path, db_root, repository, group_id, backend, verbose=verbose
    ) as depend:
        depend.publish(version, archives=archives)
        df = depend()

    backend.put_file(
        db_root, define.DB_HEADER, version, repository, group_id,
    )

    backend.put_archive(
        db_root, define.DB_DEPEND, audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )

    return df


def remove_media(
        name: str,
        files: typing.Union[str, typing.Sequence[str]],
        *,
        backend: Backend = None,
        verbose: bool = False,
):
    r"""Remove media from all versions.

    Args:
        name: name of database
        files: list of files that should be removed
        backend: backend object
        verbose: show debug messages

    """
    backend = backend or Artifactory(name, verbose=verbose)

    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id = f'{config.GROUP_ID}.{name}'

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

            with Dependencies(
                    dep_path, db_root, repository, group_id, backend,
                    verbose=verbose,
            ) as depend:
                for file in files:
                    if file in depend.files:
                        archive = depend.archive(file)

                        # remove file from archive
                        files = backend.get_archive(
                            db_root, archive, version, repository,
                            f'{group_id}.'
                            f'{define.TYPE_NAMES[define.Type.MEDIA]}',
                        )
                        files.remove(file)
                        os.remove(os.path.join(db_root, file))
                        backend.put_archive(
                            db_root, files, archive, version, repository,
                            f'{group_id}.'
                            f'{define.TYPE_NAMES[define.Type.MEDIA]}',
                            force=True,
                        )

                        # update dependency
                        depend.remove(file)
                        upload = True

            # upload dependencies
            if upload:
                backend.put_archive(
                    db_root, define.DB_DEPEND,
                    audeer.basename_wo_ext(define.DB_DEPEND),
                    version, repository, group_id, force=True,
                )


def versions(
        name: str,
        *,
        backend: Backend = None,
) -> typing.List[str]:
    r"""Available versions of database.

    Args:
        name: name of database
        backend: backend object

    Returns:
        list of versions

    """
    backend = backend or Artifactory(name)

    repository = config.REPOSITORY_PUBLIC  # TODO: figure out
    group_id: str = f'{config.GROUP_ID}.{name}'

    return backend.versions(define.DB_HEADER, repository, group_id)
