import collections
import os
import typing

import audformat
import audeer
import audiofile

from audb2.core import define
from audb2.core import utils
from audb2.core.api import default_backend
from audb2.core.backend import Backend
from audb2.core.dependencies import Dependencies


def _find_tables(
        db: audformat.Database,
        db_root: str,
        version: str,
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Update tables."""

    # release dependencies to removed tables

    db_tables = [f'db.{table}.csv' for table in db.tables]
    for file in set(deps.tables) - set(db_tables):
        deps.data.pop(file)

    tables = []

    def job(table: str):

        file = f'db.{table}.csv'
        checksum = utils.md5(os.path.join(db_root, file))
        if file not in deps:
            deps.add_meta(file, table, checksum, version)
            tables.append(table)
        elif checksum != deps.checksum(file):
            deps.data[file][define.DependField.CHECKSUM] = checksum
            deps.data[file][define.DependField.VERSION] = version
            tables.append(table)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in db.tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Find tables',
    )

    return tables


def _find_media(
        db: audformat.Database,
        db_root: str,
        version: str,
        deps: Dependencies,
        archives: typing.Mapping[str, str],
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.Set[str]:

    # release dependencies to removed media
    # and select according archives for upload
    media = set()
    db_media = db.files
    for file in set(deps.media) - set(db_media):
        media.add(deps.archive(file))
        deps.data.pop(file)

    # update version of altered media and insert new ones

    def job(file):
        path = os.path.join(db_root, file)
        if file not in deps:
            checksum = utils.md5(path)
            if file in archives:
                archive = archives[file]
            else:
                archive = audeer.uid(from_string=file.replace('\\', '/'))
            deps.add_media(db_root, file, archive, checksum, version)
        elif not deps.is_removed(file):
            checksum = utils.md5(path)
            if checksum != deps.checksum(file):
                archive = deps.data[file][define.DependField.ARCHIVE]
                deps.add_media(db_root, file, archive, checksum, version)

    audeer.run_tasks(
        job,
        params=[([file], {}) for file in db_media],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Find media',
    )

    return media


def _put_media(
        media: typing.Set[str],
        db_root: str,
        db_name: str,
        version: str,
        deps: Dependencies,
        backend: Backend,
        repository: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    # create a mapping from archives to media and
    # select archives with new or altered files for upload
    map_media_to_files = collections.defaultdict(list)
    for file in deps.media:
        if not deps.is_removed(file):
            map_media_to_files[deps.archive(file)].append(file)
            if deps.version(file) == version:
                media.add(deps.archive(file))

    def job(archive):
        if archive in map_media_to_files:
            for file in map_media_to_files[archive]:
                deps.data[file][define.DependField.VERSION] = version
            archive_file = backend.join(
                db_name,
                define.DEPEND_TYPE_NAMES[define.DependType.MEDIA],
                archive,
            )
            backend.put_archive(
                db_root,
                map_media_to_files[archive],
                archive_file,
                version,
                repository,
            )

    # upload new and altered archives if it contains at least one file
    audeer.run_tasks(
        job,
        params=[([archive], {}) for archive in media],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Put media',
    )


def _put_tables(
        tables: typing.List[str],
        db_root: str,
        db_name: str,
        version: str,
        backend: Backend,
        repository: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    def job(table: str):
        file = f'db.{table}.csv'
        archive_file = backend.join(
            db_name,
            define.DEPEND_TYPE_NAMES[define.DependType.META],
            table,
        )
        backend.put_archive(db_root, file, archive_file, version, repository)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Put tables',
    )


def publish(
        db_root: str,
        version: str,
        repository: str,
        *,
        archives: typing.Mapping[str, str] = None,
        backend: Backend = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = True,
) -> Dependencies:
    r"""Publish database.

    Args:
        db_root: root directory of database
        version: version string
        repository: name of repository
        archives: map files to archives
        backend: backend object
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        dependency object

    Raises:
        RuntimeError: if version already exists

    """
    db = audformat.Database.load(db_root, load_data=False)

    backend = default_backend(backend)

    remote_header = backend.join(db.name, define.HEADER_FILE)
    if version in backend.versions(remote_header, repository):
        raise RuntimeError(
            f"A version '{version}' already exists for "
            f"database '{db.name}'."
        )

    # load database and dependencies
    db = audformat.Database.load(db_root)
    deps_path = os.path.join(db_root, define.DEPENDENCIES_FILE)
    deps = Dependencies()
    deps.load(deps_path)

    # make sure all tables are stored in CSV format
    for table_id, table in db.tables.items():
        table_path = os.path.join(db_root, f'db.{table_id}')
        table_ext = audformat.define.TableStorageFormat.CSV
        if not os.path.exists(table_path + f'.{table_ext}'):
            table.save(table_path, storage_format=table_ext)

    # check archives
    archives = archives or {}

    # publish tables
    tables = _find_tables(
        db, db_root, version, deps,
        num_workers, verbose,
    )
    _put_tables(
        tables, db_root, db.name, version, backend, repository,
        num_workers, verbose,
    )

    # publish media
    media = _find_media(
        db, db_root, version, deps, archives,
        num_workers, verbose,
    )
    _put_media(
        media, db_root, db.name, version, deps, backend, repository,
        num_workers, verbose,
    )

    # publish dependencies and header
    deps.save(deps_path)
    archive_file = backend.join(db.name, define.DB)
    backend.put_archive(
        db_root, define.DEPENDENCIES_FILE, archive_file, version, repository,
    )
    try:
        local_header = os.path.join(db_root, define.HEADER_FILE)
        remote_header = db.name + '/' + define.HEADER_FILE
        backend.put_file(local_header, remote_header, version, repository)
    except Exception:  # pragma: no cover
        # after the header is published
        # the new version becomes visible,
        # so if something goes wrong here
        # we better clean up
        if backend.exists(remote_header, version, repository):
            backend.remove_file(remote_header, version, repository)

    return deps
