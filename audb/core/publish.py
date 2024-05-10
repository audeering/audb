import collections
import os
import re
import shutil
import tempfile
import typing

import audbackend
import audeer
import audformat
import audiofile

from audb.core import define
from audb.core.api import dependencies
from audb.core.dependencies import Dependencies
from audb.core.dependencies import upload_dependencies
from audb.core.repository import Repository


def _check_for_duplicates(
    db: audformat.Database,
    num_workers: int,
    verbose: bool,
):
    r"""Ensures tables do not contain duplicated index entries."""

    def job(table_id):
        audformat.assert_no_duplicates(db[table_id]._df)

    table_ids = list(db)
    audeer.run_tasks(
        job,
        params=[([table_id], {}) for table_id in table_ids],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Check tables for duplicates",
    )


def _check_for_missing_media(
    db: audformat.Database,
    db_root: str,
    db_root_files: typing.Set[str],
    deps: Dependencies,
):
    r"""Check for media that is not in root and not in dependencies."""
    db_files = db.files
    deps_files = deps.media

    db_files_not_in_deps = set(db_files) - set(deps_files)
    missing_files = db_files_not_in_deps - db_root_files

    if len(missing_files) > 0:
        missing_files = sorted(list(missing_files))
        number_of_presented_files = 20
        error_msg = (
            f"The following "
            f"{len(missing_files)} "
            f"files are referenced in tables "
            f"that cannot be found on disk "
            f"and are not yet part of the database: "
            f"{missing_files[:number_of_presented_files]}"
        )
        if len(missing_files) <= number_of_presented_files:
            error_msg += "."
        else:
            error_msg = error_msg[:-1]
            error_msg += ", ...]."
        raise RuntimeError(error_msg)


def _find_attachments(
    db: audformat.Database,
    db_root: str,
    version: str,
    deps: Dependencies,
    verbose: bool,
) -> typing.List[str]:
    r"""Find altered, new or removed attachments and update 'deps'."""
    # drop removed attachments from dependency table
    removed_attachments = [
        deps._df.index[deps._df.archive == attachment_id][0]
        for attachment_id in deps.attachment_ids
        if attachment_id not in db.attachments
    ]
    deps._drop(removed_attachments)

    # check attachments are valid
    db_files = list(db.files) + [f"db.{t}.csv" for t in db.tables]
    for attachment_id in db.attachments:
        path = db.attachments[attachment_id].path
        for file in db_files:
            if file.startswith(path):
                raise RuntimeError(
                    "An attachment must not "
                    "overlap with media or tables. "
                    f"But attachment '{attachment_id}' "
                    f"contains '{file}'."
                )

        path = audeer.path(db.root, db.attachments[attachment_id].path)
        if os.path.exists(path):
            if os.path.isdir(path):
                folders = audeer.list_dir_names(
                    path,
                    recursive=True,
                    hidden=True,
                    basenames=True,
                )
                for folder in folders:
                    if not os.listdir(audeer.path(path, folder)):
                        raise RuntimeError(
                            "An attachment must not "
                            "contain empty sub-folders. "
                            f"But attachment '{attachment_id}' "
                            f"contains the empty sub-folder '{folder}'."
                        )

            files = db.attachments[attachment_id].files
            if len(files) == 0:
                raise RuntimeError(
                    "An attached folder must "
                    "contain at least one file. "
                    f"But attachment '{attachment_id}' "
                    "points to an empty folder."
                )

    # add dependencies to new or updated attachments
    attachment_ids = []
    for attachment_id in audeer.progress_bar(
        list(db.attachments),
        desc="Find attachments",
        disable=not verbose,
    ):
        # use one archive per attachment ID
        path = db.attachments[attachment_id].path
        if not os.path.exists(audeer.path(db_root, path)):
            if path not in deps:
                # Raise FileNotFoundError
                #
                # Attachment is not in deps,
                # but its path does not exist on disk either.
                # We call its `files` property
                # which raises a FileNotFoundError in this case
                db.attachments[attachment_id].files
        else:
            checksum = audeer.md5(audeer.path(db_root, path))
            if path not in deps or checksum != deps.checksum(path):
                deps._add_attachment(
                    file=path,
                    version=version,
                    archive=attachment_id,
                    checksum=checksum,
                )
                attachment_ids.append(attachment_id)

    return list(attachment_ids)


def _find_media(
    db: audformat.Database,
    db_root: str,
    db_root_files: typing.Set[str],
    version: str,
    deps: Dependencies,
    archives: typing.Mapping[str, str],
    num_workers: int,
    verbose: bool,
) -> typing.Set[str]:
    r"""Find archives with new, altered or removed media and update 'deps'."""
    media_archives = set()
    db_media = set(db.files)

    # release dependencies to removed media
    # and select according archives for upload
    removed_files = set(deps.media) - db_media
    for file in removed_files:
        media_archives.add(deps.archive(file))
    deps._drop(removed_files)

    # limit to relevant media
    db_media_in_root = db_media.intersection(db_root_files)

    # update version of altered media and insert new ones

    def job(file):
        path = os.path.join(db_root, file)
        if file not in deps:
            # assert lowercase file extensions
            ext = audeer.file_extension(file)
            if ext.lower() != ext:
                raise RuntimeError(
                    "The file extension of a media file must be lowercase, "
                    f"but '{file}' includes at least one uppercase letter."
                )
            checksum = audeer.md5(path)
            if file in archives:
                archive = archives[file]
            else:
                archive = audeer.uid(from_string=file.replace("\\", "/"))
            values = _media_values(
                db_root,
                file,
                version,
                archive,
                checksum,
            )
            add_media.append(values)
        elif not deps.removed(file):
            checksum = audeer.md5(path)
            if checksum != deps.checksum(file):
                archive = deps.archive(file)
                values = _media_values(
                    db_root,
                    file,
                    version,
                    archive,
                    checksum,
                )
                update_media.append(values)

    add_media = []
    update_media = []
    audeer.run_tasks(
        job,
        params=[([file], {}) for file in db_media_in_root],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Find media",
    )
    # Add updated and new media to dependencies
    # and sort them by paths
    # as they can be returned in random order
    # by the discovery job.
    # Sorting enforces determinism in the
    # generation of the dependencies table
    if update_media:
        update_media = sorted(update_media, key=lambda x: x[0])
        deps._update_media(update_media)
    if add_media:
        add_media = sorted(add_media, key=lambda x: x[0])
        deps._add_media(add_media)

    # select archives with new or altered files for upload
    for file in deps.media:
        if not deps.removed(file) and deps.version(file) == version:
            media_archives.add(deps.archive(file))

    return media_archives


def _find_tables(
    db: audformat.Database,
    db_root: str,
    version: str,
    deps: Dependencies,
    verbose: bool,
) -> typing.List[str]:
    r"""Find altered, new or removed tables and update 'deps'."""
    # release dependencies to removed tables

    db_tables = [f"db.{table}.csv" for table in list(db)]
    deps._drop(set(deps.tables) - set(db_tables))

    tables = []
    for table in audeer.progress_bar(
        list(db),
        desc="Find tables",
        disable=not verbose,
    ):
        file = f"db.{table}.csv"
        checksum = audeer.md5(os.path.join(db_root, file))
        if file not in deps or checksum != deps.checksum(file):
            deps._add_meta(file, version, table, checksum)
            tables.append(table)

    return tables


def _get_root_files(
    db_root: str,
) -> typing.Set[str]:
    r"""Return list of files in root directory."""
    db_root_files = audeer.list_file_names(
        db_root,
        basenames=True,
        recursive=True,
    )
    if os.name == "nt":  # pragma: no cover
        # convert '\\' to '/'
        db_root_files = [file.replace("\\", "/") for file in db_root_files]

    return set(db_root_files)


def _media_values(
    root: str,
    file: str,
    version: str,
    archive: str,
    checksum: str,
) -> typing.Tuple[str, str, int, int, str, float, str, int, float, int, str]:
    r"""Return values of a media entry in dependencies.

    The dependency table expects the following columns:

    * file
    * archive
    * bit depth
    * channels
    * checksum
    * duration
    * format
    * removed
    * sampling rate
    * dependency type
    * version

    Args:
        root: root of database
        file: relative media file path
        version: database version
        archive: archive the media file is stored in
        checksum: checksum of the media file

    Returns:
        row to be added to the dependency table as tuple

    """
    dependency_type = define.DependType.MEDIA
    format = audeer.file_extension(file).lower()
    removed = 0

    # Inspect media file to get audio/video metadata
    try:
        path = os.path.join(root, file)
        bit_depth = audiofile.bit_depth(path)
        if bit_depth is None:  # pragma: nocover (non SND files)
            bit_depth = 0
        channels = audiofile.channels(path)
        duration = audiofile.duration(path, sloppy=True)
        sampling_rate = audiofile.sampling_rate(path)
    except FileNotFoundError:  # pragma: nocover
        # If sox or mediafile are not installed
        # we get a FileNotFoundError error
        raise RuntimeError(
            f"sox and mediainfo have to be installed "
            f"to publish '{format}' media files."
        )
    except RuntimeError:
        # Skip audio/video metadata for media files,
        # that don't support them
        # (e.g. text files)
        bit_depth = 0
        channels = 0
        duration = 0.0
        sampling_rate = 0

    return (
        file,
        archive,
        bit_depth,
        channels,
        checksum,
        duration,
        format,
        removed,
        sampling_rate,
        dependency_type,
        version,
    )


def _put_attachments(
    attachments: typing.List[str],
    db_root: str,
    db: audformat.Database,
    version: str,
    backend_interface: typing.Type[audbackend.interface.Base],
    num_workers: typing.Optional[int],
    verbose: bool,
):
    def job(attachment_id: str):
        archive_file = backend_interface.join(
            "/",
            db.name,
            define.DEPEND_TYPE_NAMES[define.DependType.ATTACHMENT],
            attachment_id + ".zip",
        )
        files = db.attachments[attachment_id].files
        backend_interface.put_archive(db_root, archive_file, version, files=files)

    audeer.run_tasks(
        job,
        params=[([attachment_id], {}) for attachment_id in attachments],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Put attachments",
    )


def _put_media(
    media_archives: typing.Set[str],
    db_root: str,
    db_name: str,
    version: str,
    previous_version: typing.Optional[str],
    deps: Dependencies,
    backend_interface: typing.Type[audbackend.interface.Base],
    num_workers: typing.Optional[int],
    verbose: bool,
):
    r"""Upload archives with new, altered or removed media files."""
    if media_archives:
        # create a mapping from archives to media files
        map_archive_to_files = collections.defaultdict(list)
        for file in deps.media:
            if not deps.removed(file):
                map_archive_to_files[deps.archive(file)].append(file)

        def job(archive):
            if archive in map_archive_to_files:
                files = map_archive_to_files[archive]
                for file in files:
                    update_media.append(file)

                archive_file = backend_interface.join(
                    "/",
                    db_name,
                    define.DEPEND_TYPE_NAMES[define.DependType.MEDIA],
                    archive + ".zip",
                )

                if previous_version is not None:
                    # if only some files of the archive were altered
                    # it may happen that the others do not exist
                    # in the root folder,
                    # so we have to download the archive
                    # and copy the missing files first
                    missing_files = []
                    for file in files:
                        path = os.path.join(db_root, file)
                        if not os.path.exists(path):
                            missing_files.append(file)
                    if missing_files:
                        with tempfile.TemporaryDirectory() as tmp_root:
                            backend_interface.get_archive(
                                archive_file,
                                tmp_root,
                                deps.version(missing_files[0]),
                            )
                            for missing_file in missing_files:
                                src_path = os.path.join(tmp_root, missing_file)
                                dst_path = os.path.join(db_root, missing_file)
                                audeer.mkdir(os.path.dirname(dst_path))
                                shutil.copy(
                                    src_path,
                                    dst_path,
                                )

                backend_interface.put_archive(
                    db_root,
                    archive_file,
                    version,
                    files=files,
                )

        update_media = []
        audeer.run_tasks(
            job,
            params=[([archive], {}) for archive in media_archives],
            num_workers=num_workers,
            progress_bar=verbose,
            task_description="Put media",
        )
        deps._update_media_version(update_media, version)


def _put_tables(
    tables: typing.List[str],
    db_root: str,
    db_name: str,
    version: str,
    backend_interface: typing.Type[audbackend.interface.Base],
    num_workers: typing.Optional[int],
    verbose: bool,
):
    def job(table: str):
        file = f"db.{table}.csv"
        archive_file = backend_interface.join(
            "/",
            db_name,
            define.DEPEND_TYPE_NAMES[define.DependType.META],
            table + ".zip",
        )
        backend_interface.put_archive(db_root, archive_file, version, files=file)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Put tables",
    )


def publish(
    db_root: str,
    version: str,
    repository: Repository,
    *,
    archives: typing.Mapping[str, str] = None,
    previous_version: typing.Optional[str] = "latest",
    cache_root: str = None,
    num_workers: typing.Optional[int] = 1,
    verbose: bool = True,
) -> Dependencies:
    r"""Publish database.

    Publishes a database conform to audformat_,
    stored in the ``db_root`` folder.

    A database can have dependencies
    to media files and tables of an older version.
    E.g. you might alter an existing table
    by adding labels for new media files to it
    and publish it as a new version.
    :func:`audb.publish` will then upload
    new and altered files and update
    the dependencies accordingly.

    To update a database,
    you first have to load the version
    that the new version should depend on
    with :func:`audb.load_to` to ``db_root``.
    Media files that are not altered can be omitted,
    so it is recommended to set
    ``only_metadata=True`` in :func:`audb.load_to`.
    Afterwards you make your changes to that folder
    and run :func:`audb.publish`.
    To remove media files from a database,
    make sure they are no
    longer referenced in the tables.

    Setting ``previous_version=None`` allows you
    to start from scratch and upload all files
    even if an older versions exist.
    In this case you don't call :func:`audb.load_to`
    before running :func:`audb.publish`.

    Handling of audio formats
    is based on the file extension
    in :mod:`audb`.
    This means the file extension must be lowercase
    and should match the audio format of the file,
    e.g. ``.wav``.

    When canceling :func:`audb.publish`
    during publication
    you can restart it afterwards.
    It will continue from the current state,
    but you might need overwrite permissions
    in addition to write permissions
    on the backend.

    .. _audformat: https://audeering.github.io/audformat/data-introduction.html

    Args:
        db_root: root directory of database
        version: version string
        repository: name of repository
        archives: dictionary mapping files to archive names.
            Can be used to bundle files into archives,
            which will speed up communication with the server
            if the database contains many small files.
            Archive name must not include an extension
        previous_version: specifies the version
            this publication should be based on.
            If ``'latest'``
            it will use automatically the latest published version
            or ``None``
            if no version was published.
            If ``None`` it assumes you start from scratch
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
        RuntimeError: if database attachment path does not exist,
            is a symlink,
            is empty,
            or contains an empty sub-folder
        RuntimeError: if database in ``db_root`` depends on other version
            as indicated by ``previous_version``
        RuntimeError: if database is not portable,
            see :meth:`audformat.Database.is_portable`
        RuntimeError: if non-standard formats like MP3 and MP4 are published,
            but sox and/or mediafile is not installed
        RuntimeError: if the type of a database file changes,
            e.g. from media to attachment
        RuntimeError: if a new media file
            has an uppercase letter in its file extension
        RuntimeError: if database contains tables,
            misc tables, or attachments
            that are stored under an ID
            using a char not in ``'[A-Za-z0-9._-]'``
        ValueError: if ``version`` or ``previous_version``
            cannot be parsed by :class:`audeer.StrictVersion`
        ValueError: if ``previous_version`` >= ``version``

    """
    # Enforce error if version cannot be converted to audeer.StrictVersion
    audeer.StrictVersion(version)
    if (
        previous_version is not None
        and previous_version != "latest"
        and (audeer.StrictVersion(version) <= audeer.StrictVersion(previous_version))
    ):
        raise ValueError(
            "'previous_version' needs to be smaller than 'version', "
            f"but yours is {previous_version} >= {version}."
        )

    db = audformat.Database.load(
        db_root,
        load_data=False,
        num_workers=num_workers,
        verbose=verbose,
    )

    backend_interface = repository.create_backend_interface()

    with backend_interface.backend:
        remote_header = backend_interface.join("/", db.name, define.HEADER_FILE)
        versions = backend_interface.versions(
            remote_header,
            suppress_backend_errors=True,
        )

    if version in versions:
        raise RuntimeError(
            "A version " f"'{version}' " "already exists for database " f"'{db.name}'."
        )
    if previous_version == "latest":
        if len(versions) > 0:
            previous_version = versions[-1]
        else:
            previous_version = None

    # load database and dependencies
    deps = Dependencies()
    for deps_file in [define.DEPENDENCIES_FILE, define.LEGACY_DEPENDENCIES_FILE]:
        deps_path = os.path.join(db_root, deps_file)
        if os.path.exists(deps_path):
            deps.load(deps_path)
            break

    # check if database folder depends on the right version

    # dependencies shouldn't be there
    if previous_version is None and len(deps) > 0:
        raise RuntimeError(
            f"You did not set a dependency to a previous version, "
            f"but you have a '{deps_file}' file present "
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
        previous_deps = dependencies(
            db.name,
            version=previous_version,
            cache_root=cache_root,
            verbose=verbose,
        )
        if not deps().equals(previous_deps()):
            raise RuntimeError(
                f"You want to depend on '{previous_version}' "
                f"of {db.name}, "
                f"but the dependency file '{deps_file}' "
                f"in {db_root} "
                f"does not match the dependency file "
                f"for the requested version in the repository. "
                f"Did you forgot to call "
                f"'audb.load_to({db_root}, {db.name}, "
                f"version='{previous_version}') "
                f"or modified the file manually?"
            )

    # load database with table data
    db = audformat.Database.load(
        db_root,
        load_data=True,
        num_workers=num_workers,
        verbose=verbose,
    )

    # ensure table and attachment IDs
    # contain only chars allowed by the backend
    # as the IDs are included in the filenames of the archives
    # uploaded to the backend
    allowed_chars = audbackend.core.utils.BACKEND_ALLOWED_CHARS
    allowed_chars = allowed_chars.replace("/", "")
    allowed_chars_compiled = re.compile(allowed_chars)
    for table_id in list(db):
        if allowed_chars_compiled.fullmatch(table_id) is None:
            raise RuntimeError(
                "Table IDs must only contain chars from "
                f"{allowed_chars[:-1]}, "
                f"which is not the case for table '{table_id}'."
            )
    for attachment_id in list(db.attachments):
        if allowed_chars_compiled.fullmatch(attachment_id) is None:
            raise RuntimeError(
                "Attachment IDs must only contain chars from "
                f"{allowed_chars[:-1]}, "
                f"which is not the case for attachment '{attachment_id}'."
            )

    # check all tables are conform with audformat
    if not db.is_portable:
        raise RuntimeError(
            "Some files in the tables have absolute paths "
            "or use '\\', '.', '..' in its path. "
            "Please replace those paths by relative paths, "
            "use folder names instead of dots, "
            "and avoid Windows path notation."
        )
    _check_for_duplicates(db, num_workers, verbose)

    # check all media referenced in a table exist
    # on disk or are already part of the database
    db_root_files = _get_root_files(db_root)
    _check_for_missing_media(db, db_root, db_root_files, deps)

    # make sure all tables are stored in CSV format
    for table_id in list(db):
        table = db[table_id]
        table_path = os.path.join(db_root, f"db.{table_id}")
        table_ext = audformat.define.TableStorageFormat.CSV
        if not os.path.exists(table_path + f".{table_ext}"):
            table.save(table_path, storage_format=table_ext)

    # check archives
    archives = archives or {}

    with backend_interface.backend:
        # publish attachments
        attachments = _find_attachments(db, db_root, version, deps, verbose)
        _put_attachments(
            attachments, db_root, db, version, backend_interface, num_workers, verbose
        )

        # publish tables
        tables = _find_tables(db, db_root, version, deps, verbose)
        _put_tables(
            tables, db_root, db.name, version, backend_interface, num_workers, verbose
        )

        # publish media
        media_archives = _find_media(
            db, db_root, db_root_files, version, deps, archives, num_workers, verbose
        )
        _put_media(
            media_archives,
            db_root,
            db.name,
            version,
            previous_version,
            deps,
            backend_interface,
            num_workers,
            verbose,
        )

        # publish dependencies and header
        upload_dependencies(backend_interface, deps, db_root, db.name, version)
        try:
            local_header = os.path.join(db_root, define.HEADER_FILE)
            remote_header = backend_interface.join("/", db.name, define.HEADER_FILE)
            backend_interface.put_file(local_header, remote_header, version)
        except Exception:  # pragma: no cover
            # after the header is published
            # the new version becomes visible,
            # so if something goes wrong here
            # we better clean up
            if backend_interface.exists(remote_header, version):
                backend_interface.remove_file(remote_header, version)

    return deps
