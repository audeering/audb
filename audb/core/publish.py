from __future__ import annotations

import collections
from collections.abc import Mapping
import os
import re
import shutil
import tempfile

import audbackend
import audeer
import audformat
import audiofile

from audb.core import define
from audb.core import utils
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
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )


def _check_for_missing_media(
    db: audformat.Database,
    db_root: str,
    db_root_files: set[str],
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
) -> list[str]:
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
            checksum = utils.md5(audeer.path(db_root, path))
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
    db_root_files: set[str],
    version: str,
    deps: Dependencies,
    archives: Mapping[str, str],
    num_workers: int,
    verbose: bool,
) -> set[str]:
    """Find archives with new, altered or removed media and update 'deps'.

    The function alters the dependency table entries of ``deps`` in place
    by

    * adding entries for new media files
    * removing entries for removed media files
    * updating entries for altered media files

    It further collects all media archives,
    that are affected by those media files.

    Args:
        db: database
        db_root: path to root of database
        db_root_files: all files in root of database
        version: version of database
        deps: database dependency table
        archives: mapping of media files to archives
        num_workers: number of workers
        verbose: if ``True`` show progress bar

    Returns:
        media archives to be updated

    Raises:
        RuntimeError: if a newly added media file
            has any uppercase letter in its file extension

    """
    # Media archives to update
    media_archives = set()

    # Media files in database
    db_media = set(db.files)

    removed_media = set(deps.media) - db_media
    # Select media archives to update for removed media
    for file in removed_media:
        media_archives.add(deps.archive(file))
    # Remove rows in dependency table matching removed media
    deps._drop(removed_media)

    # Limit to relevant media
    db_media_in_root = db_media.intersection(db_root_files)

    # Prepare lists to store media updates
    add_media = []
    update_media = []

    def process_new_media(file: str):
        """Collect dependency values for new media files in 'add_media'."""
        ext = audeer.file_extension(file)
        if ext.lower() != ext:
            raise RuntimeError(
                "The file extension of a media file must be lowercase, "
                f"but '{file}' includes at least one uppercase letter."
            )
        checksum = audeer.md5(os.path.join(db_root, file))
        archive = archives.get(file) or audeer.uid(from_string=file.replace("\\", "/"))
        values = _media_values(db_root, file, version, archive, checksum)
        add_media.append(values)

    def process_existing_media(file: str):
        """Collect dependency values for updated media file in 'update_media'."""
        checksum = audeer.md5(os.path.join(db_root, file))
        if checksum != deps.checksum(file):
            archive = deps.archive(file)
            values = _media_values(db_root, file, version, archive, checksum)
            update_media.append(values)

    def job(file: str) -> None:
        if file not in deps:
            process_new_media(file)
        elif not deps.removed(file):
            process_existing_media(file)

    audeer.run_tasks(
        job,
        params=[([file], {}) for file in db_media_in_root],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Find media",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )

    # Add updated and new media to dependencies with sorting for consistency
    if update_media:
        deps._update_media(sorted(update_media, key=lambda x: x[0]))
    if add_media:
        deps._add_media(sorted(add_media, key=lambda x: x[0]))

    # Update media archives for new or altered media files
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
) -> list[str]:
    r"""Find altered, new or removed tables and update 'deps'."""
    table_ids = list(db)
    # PARQUET is default table,
    # CSV file is ignored
    # if it exists as well
    table_files = [
        f"db.{table}.parquet"
        if os.path.exists(os.path.join(db_root, f"db.{table}.parquet"))
        else f"db.{table}.csv"
        for table in table_ids
    ]

    # release dependencies to removed tables
    deps._drop(set(deps.tables) - set(table_files))

    # new database tables
    tables = []
    for table, file in audeer.progress_bar(
        zip(table_ids, table_files),
        desc="Find tables",
        disable=not verbose,
    ):
        checksum = utils.md5(os.path.join(db_root, file))
        if file not in deps or checksum != deps.checksum(file):
            deps._add_meta(file, version, checksum)
            tables.append(table)

    return tables


def _get_root_files(
    db_root: str,
) -> set[str]:
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
) -> tuple[str, str, int, int, str, float, str, int, float, int, str]:
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
    dependency_type = define.DEPENDENCY_TYPE["media"]
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
    attachments: list[str],
    db_root: str,
    db: audformat.Database,
    version: str,
    backend_interface: type[audbackend.interface.Base],
    num_workers: int | None,
    verbose: bool,
):
    def job(attachment_id: str):
        archive_file = backend_interface.join(
            "/", db.name, "attachment", attachment_id + ".zip"
        )
        files = db.attachments[attachment_id].files
        backend_interface.put_archive(db_root, archive_file, version, files=files)

    audeer.run_tasks(
        job,
        params=[([attachment_id], {}) for attachment_id in attachments],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Put attachments",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )


def _put_media(
    media_archives: set[str],
    db_root: str,
    db_name: str,
    version: str,
    previous_version: str | None,
    deps: Dependencies,
    backend_interface: type[audbackend.interface.Base],
    num_workers: int | None,
    verbose: bool,
):
    r"""Upload archives with new, altered, or removed media files.

    It uploads all media files
    and adjusts the version in ``deps`` in place.

    Args:
        media_archives: media archives to upload
        db_root: root directory of database
        db_name: name of database
        version: version of database
        previous_version: previous version of database, if any.
        deps: dependency table of database
        backend_interface: backend interface for file operations
        num_workers: number of parallel workers for processing
        verbose: whether to display progress information

    Raises:
        RuntimeError: if downloading missing media files fails

    """
    if not media_archives:
        return

    # Create a mapping from archives to media files
    #
    # `defaultdict(list)` eliminates the need for dictionary initialization check
    map_archive_to_files = collections.defaultdict(list)
    for media_file in [f for f in deps.media if not deps.removed(f)]:
        archive = deps.archive(media_file)
        map_archive_to_files[archive].append(media_file)

    # Collect media files from uploaded archives
    uploaded_media = []

    def upload_archive(archive: str):
        """Upload archive to backend."""
        # Media files stored in requested archive
        files = map_archive_to_files[archive]
        uploaded_media.extend(files)

        archive_file = backend_interface.join("/", db_name, "media", f"{archive}.zip")

        if previous_version:
            # An archive might include several media files.
            # If a previous version of a database exists,
            # not all media files of the requested archive
            # might be present in `db_root`,
            # e.g. if `only_metadata=True` was used
            # when downloading the previous version with `audb.load_to()`
            # and a media file is removed
            # by deleting it in all tables,
            # we need to download all other media files
            # also included in the archive
            # that stores the removed media file.
            # In this case,
            # we download the archive to a temporary folder,
            # and copy the missing files from there
            missing_files = [
                file
                for file in files
                if not os.path.exists(os.path.join(db_root, file))
            ]

            if missing_files:
                with tempfile.TemporaryDirectory() as tmp_root:
                    try:
                        backend_interface.get_archive(
                            archive_file,
                            tmp_root,
                            deps.version(missing_files[0]),
                        )
                        for missing_file in missing_files:
                            src_path = os.path.join(tmp_root, missing_file)
                            dst_path = os.path.join(db_root, missing_file)
                            audeer.mkdir(os.path.dirname(dst_path))
                            shutil.copy(src_path, dst_path)
                    except Exception as e:  # pragma: no cover
                        # Clean up any partially copied files
                        for file in missing_files:
                            dst_path = os.path.join(db_root, file)
                            if os.path.exists(dst_path):
                                try:
                                    os.remove(dst_path)
                                except OSError:
                                    pass  # Best effort cleanup
                        raise RuntimeError(
                            "Failed to download missing media files "
                            f"for archive {archive_file}: {e}"
                        )

        backend_interface.put_archive(
            db_root,
            archive_file,
            version,
            files=files,
        )

    audeer.run_tasks(
        upload_archive,
        params=[([archive], {}) for archive in media_archives],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Put media",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )
    # Adjust the version in the dependency table for all uploaded media files
    deps._update_media_version(uploaded_media, version)


def _put_tables(
    tables: list[str],
    db_root: str,
    db_name: str,
    version: str,
    backend_interface: type[audbackend.interface.Base],
    num_workers: int | None,
    verbose: bool,
):
    def job(table: str):
        if os.path.exists(os.path.join(db_root, f"db.{table}.parquet")):
            file = os.path.join(db_root, f"db.{table}.parquet")
            remote_file = backend_interface.join(
                "/", db_name, "meta", f"{table}.parquet"
            )
            backend_interface.put_file(file, remote_file, version)
        else:
            file = f"db.{table}.csv"
            archive_file = backend_interface.join("/", db_name, "meta", f"{table}.zip")
            backend_interface.put_archive(db_root, archive_file, version, files=file)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Put tables",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )


def publish(
    db_root: str,
    version: str,
    repository: Repository,
    *,
    archives: Mapping[str, str] = None,
    previous_version: str | None = "latest",
    cache_root: str = None,
    num_workers: int | None = 1,
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

    :mod:`audb` uses md5 hashes of the database files
    to check if they have changed.
    Be aware that for certain file formats,
    like parquet,
    md5 hashes might differ
    for files with identical content.
    Reasons include the library that wrote the file,
    involved compression codes,
    or additional metadata written by the library.
    For files stored in parquet format,
    :func:`audb.publish` will first look for a hash
    stored in its metadata
    under the ``b"hash"`` key.
    For parquet tables,
    this deterministic hash
    is automatically added by :mod:`audformat`.

    Tables stored only as pickle files,
    are converted to parquet files
    before publication.
    If a table is stored as a parquet and csv file,
    the csv file is ignored,
    and the parquet file is published.

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
        ValueError: if ``repository`` has artifactory as backend in Python>=3.13
        ValueError: if ``repository`` has a non-supported backend

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
    for deps_file in [define.DEPENDENCY_FILE, define.LEGACY_DEPENDENCY_FILE]:
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
            f"but you don't have a '{define.DEPENDENCY_FILE}' file present "
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

    # Make sure all tables are stored in CSV or PARQUET format.
    # If only a PKL is found,
    # the table is stored as PARQUET instead
    for table_id in list(db):
        table = db[table_id]
        table_path = os.path.join(db_root, f"db.{table_id}")
        if not os.path.exists(f"{table_path}.csv") and not os.path.exists(
            f"{table_path}.parquet"
        ):
            table.save(table_path, storage_format="parquet")

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
