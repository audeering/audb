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
from audb2.core.config import config
from audb2.core.depend import Depend


def _find_tables(
        db: audformat.Database,
        db_root: str,
        version: str,
        depend: Depend,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Update tables."""

    # release dependencies to removed tables

    db_tables = [f'db.{table}.csv' for table in db.tables]
    for file in set(depend.tables) - set(db_tables):
        depend.data.pop(file)

    tables = []

    def job(table: str):

        file = f'db.{table}.csv'
        checksum = utils.md5(os.path.join(db_root, file))
        if file not in depend:
            depend.data[file] = [
                table, 0, checksum, 0, 0, define.DependType.META, version,
            ]
            tables.append(table)
        elif checksum != depend.checksum(file):
            depend.data[file][define.DependField.CHANNELS] = 0
            depend.data[file][define.DependField.CHECKSUM] = checksum
            depend.data[file][define.DependField.DURATION] = 0
            depend.data[file][define.DependField.VERSION] = version
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
        depend: Depend,
        archives: typing.Mapping[str, str],
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.Set[str]:

    # release dependencies to removed media
    # and select according archives for upload
    media = set()
    db_media = db.files
    for file in set(depend.media) - set(db_media):
        media.add(depend.archive(file))
        depend.data.pop(file)

    # update version of altered media and insert new ones

    def job(file):
        path = os.path.join(db_root, file)
        if file not in depend:
            checksum = utils.md5(path)
            if file in archives:
                archive = archives[file]
            else:
                archive = audeer.uid(from_string=file)
            channels = audiofile.channels(path)
            duration = audiofile.duration(path)
            depend.data[file] = [
                archive, channels, checksum, duration, 0,
                define.DependType.MEDIA, version,
            ]
        elif not depend.removed(file):
            checksum = utils.md5(path)
            if checksum != depend.checksum(file):
                channels = audiofile.channels(path)
                duration = audiofile.duration(path)
                depend.data[file][define.DependField.CHANNELS] = channels
                depend.data[file][define.DependField.CHECKSUM] = checksum
                depend.data[file][define.DependField.DURATION] = duration
                depend.data[file][define.DependField.VERSION] = version

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
        version: str,
        depend: Depend,
        backend: Backend,
        repository: str,
        group_id: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    # create a mapping from archives to media and
    # select archives with new or altered files for upload
    map_media_to_files = collections.defaultdict(list)
    for file in depend.media:
        if not depend.removed(file):
            map_media_to_files[depend.archive(file)].append(file)
            if depend.version(file) == version:
                media.add(depend.archive(file))

    def job(archive):
        if archive in map_media_to_files:
            for file in map_media_to_files[archive]:
                depend.data[file][define.DependField.VERSION] = version
            backend.put_archive(
                db_root, map_media_to_files[archive],
                archive, version, repository,
                f'{group_id}.'
                f'{define.DEPEND_TYPE_NAMES[define.DependType.MEDIA]}',
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
        version: str,
        backend: Backend,
        repository: str,
        group_id: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    def job(table: str):
        file = f'db.{table}.csv'
        backend.put_archive(
            db_root, file, table, version, repository,
            f'{group_id}.'
            f'{define.DEPEND_TYPE_NAMES[define.DependType.META]}',
        )

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
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = False,
) -> Depend:
    r"""Publish database.

    Args:
        db_root: root directory of database
        version: version string
        repository: name of repository
        archives: map files to archives
        group_id: group ID
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
    group_id: str = f'{group_id}.{db.name}'

    if version in backend.versions(
        define.DB_HEADER, repository, group_id
    ):
        raise RuntimeError(
            f"A version '{version}' already exists for "
            f"database '{db.name}'."
        )

    # load database and dependencies
    db = audformat.Database.load(db_root)
    dep_path = os.path.join(db_root, define.DB_DEPEND)
    depend = Depend()
    depend.from_file(dep_path)

    # make sure all tables are stored in CSV format
    for table_id, table in db.tables.items():
        table_path = os.path.join(db_root, f'db.{table_id}')
        table_ext = audformat.define.TableStorageFormat.CSV
        if not os.path.exists(table_path + f'.{table_ext}'):
            table.save(table_path, storage_format=table_ext)

    # check archives
    archives = archives or {}
    for name in archives.values():
        if name and define.ARCHIVE_NAME_PATTERN.fullmatch(name) is None:
            raise ValueError(
                f"Invalid archive name '{name}', "
                "allowed characters are '[0-9][a-z][A-Z].-_'."
            )

    # publish tables
    tables = _find_tables(
        db, db_root, version, depend,
        num_workers, verbose,
    )
    _put_tables(
        tables, db_root, version, backend, repository, group_id,
        num_workers, verbose,
    )

    # publish media
    media = _find_media(
        db, db_root, version, depend, archives,
        num_workers, verbose,
    )
    _put_media(
        media, db_root, version, depend, backend, repository, group_id,
        num_workers, verbose,
    )

    # publish dependencies and header
    depend.to_file(dep_path)
    backend.put_archive(
        db_root, define.DB_DEPEND,
        audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )
    try:
        backend.put_file(
            db_root, define.DB_HEADER, version, repository, group_id,
        )
    except Exception:  # pragma: no cover
        # after the header is published
        # the new version becomes visible,
        # so if something goes wrong here
        # we better clean up
        if backend.exists(
                define.DB_HEADER, version, repository, group_id,
        ):
            backend.rem_file(
                define.DB_HEADER, version, repository, group_id,
            )

    return depend
