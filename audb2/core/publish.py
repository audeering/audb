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


def publish(
        db_root: str,
        version: str,
        *,
        archives: typing.Mapping[str, str] = None,
        private: bool = False,
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        verbose: bool = False,
) -> Depend:
    r"""Publish database.

    Args:
        db_root: root directory of database
        version: version string
        archives: map files to archives
        group_id: group ID
        private: publish as private
        backend: backend object
        verbose: show debug messages

    Returns:
        dependency object

    Raises:
        RuntimeError: if version already exists

    """
    db = audformat.Database.load(db_root, load_data=False)

    backend = default_backend(backend, verbose=verbose)

    repository = config.REPOSITORY_PRIVATE if private else \
        config.REPOSITORY_PUBLIC
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

    # check archives
    archives = archives or {}
    for name in archives.values():
        if name and define.ARCHIVE_NAME_PATTERN.fullmatch(name) is None:
            raise ValueError(
                f"Invalid archive name '{name}', "
                "allowed characters are '[0-9][a-z][A-Z].-_'."
            )

    # release dependencies to removed tables
    db_tables = [f'db.{table}.csv' for table in db.tables]
    for file in set(depend.tables) - set(db_tables):
        depend.data.pop(file)

    # update version of altered tables and insert new ones
    tables_to_upload = []
    for table in db.tables:
        file = f'db.{table}.csv'
        checksum = utils.md5(os.path.join(db_root, file))
        if file not in depend:
            depend.data[file] = [
                table, 0, checksum, 0, define.DependType.META, version,
            ]
            tables_to_upload.append(table)
        elif checksum != depend.checksum(file):
            depend.data[file][define.DependField.CHANNELS] = 0
            depend.data[file][define.DependField.CHECKSUM] = checksum
            depend.data[file][define.DependField.VERSION] = version
            tables_to_upload.append(table)

    # upload tables
    for table in tables_to_upload:
        file = f'db.{table}.csv'
        backend.put_archive(
            db_root, file, table, version, repository,
            f'{group_id}.'
            f'{define.DEPEND_TYPE_NAMES[define.DependType.META]}',
        )

    # release dependencies to removed media
    # and select according archives for upload
    media_to_upload = set()
    db_media = db.files
    for file in set(depend.media) - set(db_media):
        media_to_upload.add(depend.archive(file))
        depend.data.pop(file)

    # update version of altered media and insert new ones
    for file in db_media:
        path = os.path.join(db_root, file)
        if file not in depend:
            checksum = utils.md5(path)
            if file in archives:
                archive = archives[file]
            else:
                archive = audeer.uid(from_string=file)
            channels = audiofile.channels(path)
            depend.data[file] = [
                archive, channels, checksum, 0,
                define.DependType.MEDIA, version,
            ]
        elif not depend.removed(file):
            checksum = utils.md5(path)
            if checksum != depend.checksum(file):
                channels = audiofile.channels(path)
                depend.data[file][define.DependField.CHECKSUM] = channels
                depend.data[file][define.DependField.CHECKSUM] = checksum
                depend.data[file][define.DependField.VERSION] = version

    # create a mapping from archives to media and
    # select archives with new or altered files for upload
    map_media_to_files = collections.defaultdict(list)
    for file in depend.media:
        if not depend.removed(file):
            map_media_to_files[depend.archive(file)].append(file)
            if depend.version(file) == version:
                media_to_upload.add(depend.archive(file))

    # upload new and altered archives if it contains at least one file
    for archive in media_to_upload:
        if archive in map_media_to_files:
            for file in map_media_to_files[archive]:
                depend.data[file][define.DependField.VERSION] = version
            backend.put_archive(
                db_root, map_media_to_files[archive],
                archive, version, repository,
                f'{group_id}.'
                f'{define.DEPEND_TYPE_NAMES[define.DependType.MEDIA]}',
            )

    depend.to_file(dep_path)

    # upload header and dependencies
    backend.put_file(
        db_root, define.DB_HEADER, version, repository, group_id,
    )
    backend.put_archive(
        db_root, define.DB_DEPEND, audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )

    return depend
