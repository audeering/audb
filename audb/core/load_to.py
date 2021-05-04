import glob
import os
import typing

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.api import (
    dependencies,
    latest_version,
)
from audb.core.dependencies import Dependencies
from audb.core.load import (
    database_tmp_folder,
    load_header,
)


def _find_media(
        db: audformat.Database,
        db_root: str,
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Find altered and new media."""

    media = []

    def job(file: str):
        if not deps.removed(file):
            full_file = os.path.join(db_root, file)
            if not os.path.exists(full_file):
                media.append(file)

    audeer.run_tasks(
        job,
        params=[([file], {}) for file in db.files],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Find media',
    )

    return media


def _find_tables(
        db_header: audformat.Database,
        db_root: str,
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:

    tables = []

    def job(table: str):
        file = f'db.{table}.csv'
        full_file = os.path.join(db_root, file)
        if not os.path.exists(full_file):
            tables.append(file)
        else:
            checksum = audbackend.md5(full_file)
            # if the table already exists
            # we have to compare checksum
            # in case it was altered by flavor
            if checksum != deps.checksum(file):  # pragma: no cover
                tables.append(file)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in db_header.tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Find tables',
    )

    return tables


def _get_media(
        media: typing.List[str],
        db_root: str,
        db_root_tmp: str,
        db_name: str,
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):

    # create folder tree to avoid race condition
    # in os.makedirs when files are unpacked
    for file in media:
        audeer.mkdir(os.path.dirname(os.path.join(db_root, file)))
        audeer.mkdir(os.path.dirname(os.path.join(db_root_tmp, file)))

    # figure out archives
    archives = set()
    for file in media:
        archives.add(
            (deps.archive(file), deps.version(file))
        )

    def job(archive: str, version: str):
        archive = backend.join(
            db_name,
            define.DEPEND_TYPE_NAMES[define.DependType.MEDIA],
            archive,
        )
        files = backend.get_archive(archive, db_root_tmp, version)
        for file in files:
            _move_file(db_root_tmp, db_root, file)

    audeer.run_tasks(
        job,
        params=[([archive, version], {}) for archive, version in archives],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Get media',
    )


def _get_tables(
        tables: typing.List[str],
        db_root: str,
        db_root_tmp: str,
        db_name: str,
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):

    def job(table: str):
        # If a pickled version of the table exists,
        # we have to remove it to make sure that
        # later on the new CSV tables are loaded.
        # This can happen if we upgrading an existing
        # database to a different version.
        path_pkl = os.path.join(
            db_root, table
        )[:-3] + audformat.define.TableStorageFormat.PICKLE
        if os.path.exists(path_pkl):
            os.remove(path_pkl)
        archive = backend.join(
            db_name,
            define.DEPEND_TYPE_NAMES[define.DependType.META],
            deps.archive(table),
        )
        backend.get_archive(archive, db_root_tmp, deps.version(table))
        _move_file(db_root_tmp, db_root, table)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Get tables',
    )


def _move_file(
        root_src: str,
        root_dst: str,
        file: str,
):
    r"""Move file to another directory."""

    os.rename(
        os.path.join(root_src, file),
        os.path.join(root_dst, file),
    )


def _remove_empty_dirs(root):
    r"""Remove directories, fails if it contains non-empty sub-folders."""

    files = os.listdir(root)
    if len(files):
        for file in files:
            full_file = os.path.join(root, file)
            if os.path.isdir(full_file):
                _remove_empty_dirs(full_file)

    os.rmdir(root)


def _save_database(
        db: audformat.Database,
        db_root: str,
        db_root_tmp: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):

    for storage_format in [
        audformat.define.TableStorageFormat.CSV,
        audformat.define.TableStorageFormat.PICKLE,
    ]:
        db.save(
            db_root_tmp, storage_format=storage_format,
            num_workers=num_workers, verbose=verbose,
        )
        _move_file(db_root_tmp, db_root, define.HEADER_FILE)
        for path in glob.glob(
                os.path.join(db_root_tmp, f'*.{storage_format}')
        ):
            file = os.path.relpath(path, db_root_tmp)
            _move_file(db_root_tmp, db_root, file)


def load_to(
        root: str,
        name: str,
        *,
        version: str = None,
        cache_root: str = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = True,
) -> audformat.Database:
    r"""Load database to directory.

    Loads the original state of the database
    to a custom directory.
    No conversion or filtering will be applied.
    If the target folder already contains
    some version of the database,
    it will upgrade to the requested version.
    Unchanged files will be skipped.

    Args:
        root: target directory
        name: name of database
        version: version string, latest if ``None``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used.
            Only used to read the dependencies of the requested version
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        database object

    """
    if version is None:
        version = latest_version(name)

    db_root = audeer.safe_path(root)
    db_root_tmp = database_tmp_folder(db_root)

    # remove files with a wrong checksum
    # to ensure we load correct version
    update = os.path.exists(db_root) and os.listdir(db_root)
    audeer.mkdir(db_root)
    deps = dependencies(name, version=version, cache_root=cache_root)
    if update:
        for file in deps.files:
            full_file = os.path.join(db_root, file)
            if os.path.exists(full_file):
                checksum = audbackend.md5(full_file)
                if checksum != deps.checksum(file):
                    os.remove(full_file)

    # load database header without tables from backend

    db_header, backend = load_header(
        db_root_tmp,
        name,
        version,
        overwrite=True,
    )

    # get altered and new tables

    db_header.save(db_root_tmp, header_only=True)
    tables = _find_tables(db_header, db_root, deps, num_workers, verbose)
    _get_tables(tables, db_root, db_root_tmp, name, deps, backend,
                num_workers, verbose)

    # load database

    # move header to root and load database ...
    _move_file(db_root_tmp, db_root, define.HEADER_FILE)
    try:
        db = audformat.Database.load(
            db_root,
            num_workers=num_workers,
            verbose=verbose,
        )
    except (KeyboardInterrupt, Exception):  # pragma: no cover
        # make sure to remove header if user interrupts
        os.remove(os.path.join(db_root, define.HEADER_FILE))
        raise
    # afterwards remove header to avoid the database
    # can be loaded before download is complete
    os.remove(os.path.join(db_root, define.HEADER_FILE))

    # get altered and new media files

    media = _find_media(db, db_root, deps, num_workers, verbose)
    _get_media(media, db_root, db_root_tmp, name, deps, backend,
               num_workers, verbose)

    # save dependencies

    dep_path_tmp = os.path.join(db_root_tmp, define.DEPENDENCIES_FILE)
    deps.save(dep_path_tmp)
    _move_file(db_root_tmp, db_root, define.DEPENDENCIES_FILE)

    # save database and remove the temporal directory
    # to signal all files were correctly loaded

    _save_database(db, db_root, db_root_tmp, num_workers, verbose)
    try:
        _remove_empty_dirs(db_root_tmp)
    except OSError:  # pragma: no cover
        raise RuntimeError(
            'Could not remove temporary directory, '
            'probably there are some leftover files.'
            'This should not happen.'
        )

    return db
