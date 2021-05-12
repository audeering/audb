import collections
import os
import tempfile
import threading
import typing

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.api import dependencies
from audb.core.dependencies import Dependencies
from audb.core.repository import Repository


def _find_tables(
        db: audformat.Database,
        db_root: str,
        version: str,
        deps: Dependencies,
        verbose: bool,
) -> typing.List[str]:
    r"""Update tables."""

    # release dependencies to removed tables

    db_tables = [f'db.{table}.csv' for table in db.tables]
    for file in set(deps.tables) - set(db_tables):
        deps._drop(file)

    tables = []
    for table in audeer.progress_bar(
            db.tables,
            desc='Find tables',
            disable=not verbose,
    ):
        file = f'db.{table}.csv'
        checksum = audbackend.md5(os.path.join(db_root, file))
        if file not in deps or checksum != deps.checksum(file):
            deps._add_meta(file, version, table, checksum)
            tables.append(table)

    return tables


def _find_media(
        db: audformat.Database,
        db_root: str,
        version: str,
        deps: Dependencies,
        archives: typing.Mapping[str, str],
        verbose: bool,
) -> typing.Set[str]:

    # release dependencies to removed media
    # and select according archives for upload
    media = set()
    db_media = db.files
    for file in set(deps.media) - set(db_media):
        media.add(deps.archive(file))
        deps._drop(file)

    # update version of altered media and insert new ones

    for file in audeer.progress_bar(
            db_media,
            desc='Find media',
            disable=not verbose,
    ):
        path = os.path.join(db_root, file)
        if file not in deps:
            checksum = audbackend.md5(path)
            if file in archives:
                archive = archives[file]
            else:
                archive = audeer.uid(from_string=file.replace('\\', '/'))
            deps._add_media(db_root, file, version, archive, checksum)
        elif not deps.removed(file):
            checksum = audbackend.md5(path)
            if checksum != deps.checksum(file):
                archive = deps.archive(file)
                deps._add_media(db_root, file, version, archive, checksum)

    return media


def _put_media(
        media: typing.Set[str],
        db_root: str,
        db_name: str,
        version: str,
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    # create a mapping from archives to media and
    # select archives with new or altered files for upload
    map_media_to_files = collections.defaultdict(list)
    for file in deps.media:
        if not deps.removed(file):
            map_media_to_files[deps.archive(file)].append(file)
            if deps.version(file) == version:
                media.add(deps.archive(file))

    lock = threading.Lock()

    def job(archive):
        if archive in map_media_to_files:
            for file in map_media_to_files[archive]:
                with lock:
                    deps._add_media(db_root, file, version)
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
        backend: audbackend.Backend,
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
        backend.put_archive(db_root, file, archive_file, version)

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
        repository: Repository,
        *,
        archives: typing.Mapping[str, str] = None,
        previous_version: typing.Optional[str] = 'latest',
        cache_root: str = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = True,
) -> Dependencies:
    r"""Publish database.

    A database can have dependencies
    to files of an older version of itself.
    E.g. you might add a few new files to an existing database
    and publish as a new version.
    :func:`audb.publish` will upload then only the new files
    and store dependencies on the already published files.

    To allow for dependencies
    you first have to load the version of the database
    that the new version should depend on
    with :func:`audb.load_to` to ``db_root``.
    Afterwards you make your changes to that folder
    and run :func:`audb.publish`.
    :func:`audb.publish` will then check
    that the version of the files inside that folder
    match the version given by ``previous_version``.

    Setting ``previous_version=None`` allows you
    to start from scratch and upload all files
    even if an older versions exist.
    In this case you don't call :func:`audb.load_to`
    before running :func:`audb.publish`.

    Args:
        db_root: root directory of database
        version: version string
        repository: name of repository
        archives: dictionary mapping files to archive names.
            Can be used to bundle files into archives.
            Archive name must not include an extension
        previous_version: specifies the version
            this publication should be based on.
            If ``'latest'``
            it will use automatically the latest published version
            or ``None``
            if no version was published.
            If ``None`` it assumes you start from scratch.
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used.
            Only used to read the dependencies of the previous version
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        dependency object

    Raises:
        RuntimeError: if version already exists
        RuntimeError: if database tables reference non-existing files
        RuntimeError: if database in ``db_root`` depends on other version
            as indicated by ``previous_version``
        RuntimeError: if database is not portable,
            see :meth:`audformat.Database.is_portable`

    """
    db = audformat.Database.load(db_root, load_data=False)

    backend = audbackend.create(
        repository.backend,
        repository.host,
        repository.name,
    )

    remote_header = backend.join(db.name, define.HEADER_FILE)
    versions = backend.versions(remote_header)
    if version in versions:
        raise RuntimeError(
            'A version '
            f"'{version}' "
            'already exists for database '
            f"'{db.name}'."
        )
    if previous_version == 'latest':
        if len(versions) > 0:
            previous_version = versions[-1]
        else:
            previous_version = None

    # load database and dependencies
    deps_path = os.path.join(db_root, define.DEPENDENCIES_FILE)
    deps = Dependencies()
    if os.path.exists(deps_path):
        deps.load(deps_path)

    # check if database folder depends on the right version

    # dependencies shouldn't be there
    if previous_version is None and len(deps) > 0:
        raise RuntimeError(
            f"You did not set a dependency to a previous version, "
            f"but you have a '{define.DEPENDENCIES_FILE}' file present "
            f"in {db_root}."
        )

    # dependencies missing
    if previous_version is not None and len(deps) == 0:
        raise RuntimeError(
            f"You want to depend on '{previous_version}' "
            f"of {db.name}, "
            f"but you don't have a '{define.DEPENDENCIES_FILE}' file present "
            f"in {db_root}. "
            f"Did you forgot to call "
            f"'audb.load_to({db_root}, {db.name}, "
            f"version={previous_version}?"
        )

    # dependencies do not match version
    if previous_version is not None and len(deps) > 0:
        with tempfile.TemporaryDirectory() as tmp_dir:
            previous_deps_path = os.path.join(
                tmp_dir,
                define.DEPENDENCIES_FILE,
            )
            previous_deps = dependencies(
                db.name,
                version=previous_version,
                cache_root=cache_root,
            )
            previous_deps.save(previous_deps_path)
            if audbackend.md5(deps_path) != audbackend.md5(previous_deps_path):
                raise RuntimeError(
                    f"You want to depend on '{previous_version}' "
                    f"of {db.name}, "
                    f"but the MD5 sum of your "
                    f"'{define.DEPENDENCIES_FILE}' file "
                    f"in {db_root} "
                    f"does not match the MD5 sum of the corresponding file "
                    f"for the requested version in the repository. "
                    f"Did you forgot to call "
                    f"'audb.load_to({db_root}, {db.name}, "
                    f"version='{previous_version}') "
                    f"or modified the file manually?"
                )

    # load database from folder
    db = audformat.Database.load(db_root)

    if not db.is_portable:
        raise RuntimeError(
            "Some files in the tables have absolute paths "
            "or use '.' or '..' to address a folder. "
            "Please replace those paths by relative paths "
            "and use folder names instead of dots."
        )

    # check all files referenced in a table exists
    missing_files = [
        f for f in db.files
        if not os.path.exists(os.path.join(db_root, f))
    ]
    if len(missing_files) > 0:
        number_of_presented_files = 20
        error_msg = (
            f'{len(missing_files)} files are referenced in tables '
            'that cannot be found. '
            f"Missing files are: '{missing_files[:number_of_presented_files]}"
        )
        if len(missing_files) <= number_of_presented_files:
            error_msg += "'."
        else:
            error_msg += ", ...'."
        raise RuntimeError(error_msg)

    # make sure all tables are stored in CSV format
    for table_id, table in db.tables.items():
        table_path = os.path.join(db_root, f'db.{table_id}')
        table_ext = audformat.define.TableStorageFormat.CSV
        if not os.path.exists(table_path + f'.{table_ext}'):
            table.save(table_path, storage_format=table_ext)

    # check archives
    archives = archives or {}

    # publish tables
    tables = _find_tables(db, db_root, version, deps, verbose)
    _put_tables(tables, db_root, db.name, version, backend, num_workers,
                verbose)

    # publish media
    media = _find_media(db, db_root, version, deps, archives, verbose)
    _put_media(media, db_root, db.name, version, deps, backend, num_workers,
               verbose)

    # publish dependencies and header
    deps.save(deps_path)
    archive_file = backend.join(db.name, define.DB)
    backend.put_archive(
        db_root, define.DEPENDENCIES_FILE, archive_file, version,
    )
    try:
        local_header = os.path.join(db_root, define.HEADER_FILE)
        remote_header = db.name + '/' + define.HEADER_FILE
        backend.put_file(local_header, remote_header, version)
    except Exception:  # pragma: no cover
        # after the header is published
        # the new version becomes visible,
        # so if something goes wrong here
        # we better clean up
        if backend.exists(remote_header, version):
            backend.remove_file(remote_header, version)

    return deps
