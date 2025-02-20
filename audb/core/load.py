from __future__ import annotations

from collections.abc import Sequence
import os
import shutil

import filelock
import pandas as pd

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core import utils
from audb.core.api import cached
from audb.core.api import dependencies
from audb.core.api import latest_version
from audb.core.cache import database_cache_root
from audb.core.cache import database_tmp_root
from audb.core.cache import default_cache_root
from audb.core.dependencies import Dependencies
from audb.core.dependencies import error_message_missing_object
from audb.core.dependencies import filter_deps
from audb.core.flavor import Flavor
from audb.core.lock import FolderLock
from audb.core.utils import lookup_backend


CachedVersions = Sequence[tuple[audeer.StrictVersion, str, Dependencies]]


def _cached_versions(
    name: str,
    version: str,
    flavor: Flavor,
    cache_root: str | None,
) -> CachedVersions:
    r"""Find other cached versions of same flavor."""
    df = cached(cache_root=cache_root, name=name)
    # If no explicit cache root is given,
    # we look into the private and shared one.
    # This fixes https://github.com/audeering/audb/issues/101
    if cache_root is None and os.path.exists(default_cache_root(shared=True)):
        df = pd.concat((df, cached(name=name, shared=True)))
        # Ensure to remove duplicates,
        # which can occur if cache and shared cache
        # point to the same folder.
        # Compare https://github.com/audeering/audb/issues/314
        df = df[~df.index.duplicated(keep="first")]

    cached_versions = []
    for flavor_root, row in df.iterrows():
        if row["flavor_id"] == flavor.short_id:
            if row["version"] == version:
                continue
            deps = dependencies(
                name,
                version=row["version"],
                cache_root=cache_root,
            )
            # as it is more likely we find files
            # in newer versions, push them to front
            cached_versions.insert(
                0,
                (
                    audeer.StrictVersion(row["version"]),
                    str(flavor_root),
                    deps,
                ),
            )

    return cached_versions


def _cached_files(
    files: Sequence[str],
    deps: Dependencies,
    cached_versions: CachedVersions,
    flavor: Flavor | None,
    verbose: bool,
) -> tuple[list[str], list[str]]:
    r"""Find cached files.

    Args:
        files: media, attachment files, or table IDs
        deps: database dependencies
        cached_versions: information on cached versions of the database
        flavor: requested database flavor
        verbose: if ``True``, show progress bar

    Returns:
        ``([(<db_cache_root1>, <cached_file1>), ...)], [<missing_file1>, ...])``,
        where ``<db_cache_root1>`` is the absolute path
        to the database root,
        in which ``<cached_file1>`` is stored.
        ``<cached_file1>`` and ``<missing_file1>``
        represent the names of media, attachment files, or table IDs

    """
    cached_files = []
    missing_files = []

    for file in audeer.progress_bar(
        files,
        desc="Cached files",
        disable=not verbose,
    ):
        found = False
        if f"db.{file}.csv" in deps.tables:
            file_path = f"db.{file}.csv"
        elif f"db.{file}.parquet" in deps.tables:
            file_path = f"db.{file}.parquet"
        else:
            file_path = file
        file_version = audeer.StrictVersion(deps.version(file_path))
        for cache_version, cache_root, cache_deps in cached_versions:
            if cache_version >= file_version:
                if file_path in cache_deps:
                    if deps.checksum(file_path) == cache_deps.checksum(file_path):
                        path = os.path.join(cache_root, file_path)
                        if flavor and flavor.format is not None:
                            path = audeer.replace_file_extension(
                                path,
                                flavor.format,
                            )
                        if os.path.exists(path):
                            found = True
                            break
        if found:
            if flavor and flavor.format is not None:
                file = audeer.replace_file_extension(
                    file,
                    flavor.format,
                )
            cached_files.append((cache_root, file))
        else:
            missing_files.append(file)

    return cached_files, missing_files


def _copy_path(
    path: str,
    root_src: str,
    root_tmp: str,
    root_dst: str,
):
    r"""Copy file."""
    src_path = os.path.join(root_src, path)
    tmp_path = os.path.join(root_tmp, path)
    dst_path = os.path.join(root_dst, path)
    if os.path.isdir(src_path):
        shutil.copytree(src_path, tmp_path)
    else:
        audeer.mkdir(os.path.dirname(tmp_path))
        shutil.copy(src_path, tmp_path)
    audeer.mkdir(os.path.dirname(dst_path))
    audeer.move_file(tmp_path, dst_path)


def _database_check_complete(
    db: audformat.Database,
    db_root: str,
    flavor: Flavor,
    deps: Dependencies,
):
    def check() -> bool:
        complete = True
        for attachment in deps.attachments:
            if not os.path.exists(os.path.join(db_root, attachment)):
                return False
        for table in deps.tables:
            if not os.path.exists(os.path.join(db_root, table)):
                return False
        for media in deps.media:
            if not deps.removed(media):
                path = os.path.join(db_root, media)
                path = flavor.destination(path)
                if not os.path.exists(path):
                    return False
        return complete

    if check():
        db_root_tmp = database_tmp_root(db_root)
        db.meta["audb"]["complete"] = True
        db_original = audformat.Database.load(db_root, load_data=False)
        db_original.meta["audb"]["complete"] = True
        db_original.save(db_root_tmp, header_only=True)
        audeer.move_file(
            os.path.join(db_root_tmp, define.HEADER_FILE),
            os.path.join(db_root, define.HEADER_FILE),
        )
        audeer.rmdir(db_root_tmp)


def _database_is_complete(
    db: audformat.Database,
) -> bool:
    complete = False
    if "audb" in db.meta:
        if "complete" in db.meta["audb"]:
            complete = db.meta["audb"]["complete"]
    return complete


def _files_duration(
    db: audformat.Database,
    deps: Dependencies,
    files: Sequence[str],
    format: str | None,
):
    durs = deps().loc[files, "duration"]
    durs = durs[durs > 0]
    durs = pd.to_timedelta(durs, unit="s")
    durs.index.name = "file"
    if format is not None:
        durs.index = audformat.utils.replace_file_extension(durs.index, format)
    # Norm file path under Windows to include `\`
    if os.name == "nt":  # pragma: nocover as tested in Windows runner
        durs.index = audformat.utils.map_file_path(
            durs.index,
            os.path.normpath,
        )
    durs.index = audformat.utils.expand_file_path(durs.index, db.root)
    db._files_duration = durs.to_dict()


def _get_attachments_from_cache(
    attachments: Sequence[str],
    db_root: str,
    db: audformat.Database,
    deps: Dependencies,
    cached_versions: CachedVersions,
    flavor: Flavor,
    num_workers: int,
    verbose: bool,
) -> list[str]:
    r"""Copy files from cache.

    This function copies all files
    associated with the requested attachments
    from other cached versions
    to the new database folder.

    Args:
        attachments: sequence of attachment IDs
        db_root: database root
        db: database object
        deps: dependency object
        cached_versions: object containing information
            on existing cached versions of the database
        flavor: database flavor object
        num_workers: number of workers to use
        verbose: if ``True`` show progress bar

    Returns:
        list of attachment IDs that couldn't be found in cache

    """
    db_root_cached = [x[1] for x in cached_versions]

    paths = [db.attachments[attachment].path for attachment in attachments]

    with FolderLock(
        db_root_cached,
        timeout=define.CACHED_VERSIONS_TIMEOUT,
    ):
        cached_paths, missing_paths = _cached_files(
            paths,
            deps,
            cached_versions,
            flavor,
            verbose,
        )
        missing_attachments = [deps.archive(path) for path in missing_paths]
        db_root_tmp = database_tmp_root(db_root)

        def job(cache_root: str, file: str):
            _copy_path(file, cache_root, db_root_tmp, db_root)

        audeer.run_tasks(
            job,
            params=[([root, path], {}) for root, path in cached_paths],
            num_workers=num_workers,
            progress_bar=verbose,
            task_description="Copy attachments",
            maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
        )

        audeer.rmdir(db_root_tmp)

    return missing_attachments


def _get_files_from_cache(
    files: Sequence[str],
    files_type: str,
    db_root: str,
    deps: Dependencies,
    cached_versions: CachedVersions,
    flavor: Flavor,
    num_workers: int,
    verbose: bool,
) -> Sequence[str]:
    r"""Copy files from cache.

    This function copies requested media files
    or table files
    from other cached versions
    to the new database folder.

    Args:
        files: sequence of media files,
            attachment IDs,
            or table IDs
        files_type: ``'media'``,
            ``'table'``,
        db_root: database root
        deps: dependency object
        cached_versions: object containing information
            on existing cached versions of the database
        flavor: database flavor object
        num_workers: number of workers to use
        verbose: if ``True`` show progress bar

    Returns:
        list of files that couldn't be found in cache

    """
    db_root_cached = [x[1] for x in cached_versions]

    try:
        with FolderLock(
            db_root_cached,
            timeout=define.CACHED_VERSIONS_TIMEOUT,
        ):
            cached_files, missing_files = _cached_files(
                files,
                deps,
                cached_versions,
                flavor,
                verbose,
            )
            db_root_tmp = database_tmp_root(db_root)

            # Tables are stored as CSV or PARQUET files,
            # and are also cached as PKL files
            if files_type == "table":

                def job(cache_root: str, file: str):
                    for ext in ["csv", "parquet", "pkl"]:
                        table_file = f"db.{file}.{ext}"
                        if os.path.exists(os.path.join(cache_root, table_file)):
                            _copy_path(table_file, cache_root, db_root_tmp, db_root)

            else:

                def job(cache_root: str, file: str):
                    _copy_path(file, cache_root, db_root_tmp, db_root)

            audeer.run_tasks(
                job,
                params=[([root, file], {}) for root, file in cached_files],
                num_workers=num_workers,
                progress_bar=verbose,
                task_description=f"Copy {files_type}",
                maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
            )

            audeer.rmdir(db_root_tmp)

    except filelock.Timeout:
        missing_files = files

    return missing_files


def _get_attachments_from_backend(
    db: audformat.Database,
    attachments: Sequence[str],
    db_root: str,
    deps: Dependencies,
    backend_interface: type[audbackend.interface.Base],
    num_workers: int | None,
    verbose: bool,
):
    r"""Load attachments from backend."""
    db_root_tmp = database_tmp_root(db_root)

    paths = [db.attachments[attachment].path for attachment in attachments]

    # create folder tree to avoid race condition
    # in os.makedirs when files are unpacked
    utils.mkdir_tree(paths, db_root_tmp)

    def job(path: str):
        archive = deps.archive(path)
        version = deps.version(path)
        archive = backend_interface.join("/", db.name, "attachment", archive + ".zip")
        backend_interface.get_archive(
            archive,
            db_root_tmp,
            version,
            tmp_root=db_root_tmp,
        )
        src_path = audeer.path(db_root_tmp, path)
        dst_path = audeer.path(db_root, path)
        audeer.mkdir(os.path.dirname(dst_path))
        audeer.move_file(
            src_path,
            dst_path,
        )

    audeer.run_tasks(
        job,
        params=[([path], {}) for path in paths],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Load attachments",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )

    audeer.rmdir(db_root_tmp)


def _get_media_from_backend(
    name: str,
    media: Sequence[str],
    db_root: str,
    flavor: Flavor | None,
    deps: Dependencies,
    backend_interface: type[audbackend.interface.Base],
    num_workers: int | None,
    verbose: bool,
):
    r"""Load media from backend."""
    # figure out archives
    archives = set()
    archive_names = set()
    for file in media:
        archive_name = deps.archive(file)
        archive_version = deps.version(file)
        archives.add((archive_name, archive_version))
        archive_names.add(archive_name)
    # collect all files that will be extracted,
    # if we have more files than archives
    if len(deps.files) > len(deps.archives):
        files = list()
        for file in deps.media:
            archive = deps.archive(file)
            if archive in archive_names:
                files.append(file)
        media = files

    # create folder tree to avoid race condition
    # in os.makedirs when files are unpacked
    # using multi-processing
    db_root_tmp = database_tmp_root(db_root)
    utils.mkdir_tree(media, db_root)
    utils.mkdir_tree(media, db_root_tmp)

    def job(archive: str, version: str):
        archive = backend_interface.join("/", name, "media", archive + ".zip")
        # extract and move all files that are stored in the archive,
        # even if only a single file from the archive was requested
        files = backend_interface.get_archive(
            archive,
            db_root_tmp,
            version,
            tmp_root=db_root_tmp,
        )
        for file in files:
            if os.name == "nt":  # pragma: no cover
                file = file.replace(os.sep, "/")
            if flavor is not None:
                bit_depth = deps.bit_depth(file)
                channels = deps.channels(file)
                sampling_rate = deps.sampling_rate(file)
                src_path = os.path.join(db_root_tmp, file)
                file = flavor.destination(file)
                dst_path = os.path.join(db_root_tmp, file)
                try:
                    flavor(
                        src_path,
                        dst_path,
                        src_bit_depth=bit_depth,
                        src_channels=channels,
                        src_sampling_rate=sampling_rate,
                    )
                except RuntimeError:
                    raise RuntimeError(
                        f"Media file '{file}' does not support requesting a flavor."
                    )
                if src_path != dst_path:
                    os.remove(src_path)

            audeer.move_file(
                os.path.join(db_root_tmp, file),
                os.path.join(db_root, file),
            )

    audeer.run_tasks(
        job,
        params=[([archive, version], {}) for archive, version in archives],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Load media",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )

    audeer.rmdir(db_root_tmp)


def _get_tables_from_backend(
    db: audformat.Database,
    tables: Sequence[str],
    db_root: str,
    deps: Dependencies,
    backend_interface: type[audbackend.interface.Base],
    pickle_tables: bool,
    num_workers: int | None,
    verbose: bool,
):
    r"""Load tables from backend.

    Args:
        db: database
        tables: table IDs to load from backend
        db_root: database root
        deps: database dependencies
        backend_interface: backend interface
        pickle_tables: if ``True``,
            tables are cached locally
            in their original format
            and as pickle files.
            tables are stored in their original format,
            and as pickle files
            in the cache.
            This allows for faster loading,
            when loading from cache
        num_workers: number of workers
        verbose: if ``True``, show progress bar

    """
    db_root_tmp = database_tmp_root(db_root)

    def job(table: str):
        csv_file = f"db.{table}.csv"
        parquet_file = f"db.{table}.parquet"

        if csv_file in deps.tables:
            table_file = csv_file
            remote_file = backend_interface.join("/", db.name, "meta", f"{table}.zip")
            backend_interface.get_archive(
                remote_file,
                db_root_tmp,
                deps.version(table_file),
                tmp_root=db_root_tmp,
            )
        else:
            table_file = parquet_file
            remote_file = backend_interface.join(
                "/", db.name, "meta", f"{table}.parquet"
            )
            backend_interface.get_file(
                remote_file,
                os.path.join(db_root_tmp, table_file),
                deps.version(table_file),
            )

        table_files = [table_file]

        # Cache table as PKL file
        if pickle_tables:
            pickle_file = f"db.{table}.pkl"
            table_path = os.path.join(db_root_tmp, f"db.{table}")
            db[table].load(table_path)
            db[table].save(
                table_path,
                storage_format=audformat.define.TableStorageFormat.PICKLE,
            )
            table_files.append(pickle_file)

        # Move tables from tmp folder to database root
        for table_file in table_files:
            audeer.move_file(
                os.path.join(db_root_tmp, table_file),
                os.path.join(db_root, table_file),
            )

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Load tables",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )

    audeer.rmdir(db_root_tmp)


def _load_attachments(
    attachments: Sequence[str],
    backend_interface: type[audbackend.interface.Base],
    db_root: str,
    db: audformat.Database,
    version: str,
    cached_versions: CachedVersions | None,
    deps: Dependencies,
    flavor: Flavor,
    cache_root: str,
    num_workers: int,
    verbose: bool,
) -> CachedVersions | None:
    r"""Load attachments to cache.

    Args:
        attachments: list of attachment IDs
        backend_interface: backend object
        db_root: database root
        db: database object
        version: database version
        cached_versions: object representing cached versions
            of the database
        deps: database dependency object
        flavor: database flavor object
        cache_root: root path of cache
        num_workers: number of workers to use
        verbose: if ``True`` show progress bars
            for each step

    Returns:
        cached versions object
            if other versions of the database are found in cache

    """
    missing_attachments = []
    for attachment in attachments:
        path = db.attachments[attachment].path
        path = audeer.path(db_root, path)
        if not os.path.exists(path):
            missing_attachments.append(attachment)

    if missing_attachments:
        if cached_versions is None:
            cached_versions = _cached_versions(
                db.name,
                version,
                flavor,
                cache_root,
            )
        if cached_versions:
            missing_attachments = _get_attachments_from_cache(
                missing_attachments,
                db_root,
                db,
                deps,
                cached_versions,
                flavor,
                num_workers,
                verbose,
            )
        if missing_attachments:
            if backend_interface is None:
                backend_interface = lookup_backend(db.name, version)
            _get_attachments_from_backend(
                db,
                missing_attachments,
                db_root,
                deps,
                backend_interface,
                num_workers,
                verbose,
            )

    return cached_versions


def _load_files(
    files: Sequence[str],
    files_type: str,
    backend_interface: type[audbackend.interface.Base],
    db_root: str,
    db: audformat.Database,
    version: str,
    cached_versions: CachedVersions | None,
    deps: Dependencies,
    flavor: Flavor,
    cache_root: str,
    pickle_tables: bool,
    num_workers: int,
    verbose: bool,
) -> CachedVersions | None:
    r"""Load files to cache.

    Loads media files,
    attachment files,
    or table files to database root folder.

    All files not existing in cache yet
    are copied from the corresponding flavor cache
    folder of other versions of the database
    or are downloaded from the backend.

    Args:
        files: list of media files,
            attachment files,
            or table IDs
        files_type: ``'media'``,
            ``'table'``,
            or ``'attachment'``
        backend_interface: backend object
        db_root: database root
        db: database object
        version: database version
        cached_versions: object representing cached versions
            of the database
        deps: database dependency object
        flavor: database flavor object
        cache_root: root path of cache
        pickle_tables: if ``True``,
            tables are cached locally
            in their original format
            and as pickle files.
            This allows for faster loading,
            when loading from cache
        num_workers: number of workers to use
        verbose: if ``True`` show progress bars
            for each step

    Returns:
        cached versions object
            if other versions of the database are found in cache

    """
    missing_files = _missing_files(
        files,
        files_type,
        db_root,
        flavor,
        verbose,
    )
    if missing_files:
        if cached_versions is None:
            cached_versions = _cached_versions(
                db.name,
                version,
                flavor,
                cache_root,
            )
        if cached_versions:
            missing_files = _get_files_from_cache(
                missing_files,
                files_type,
                db_root,
                deps,
                cached_versions,
                flavor,
                num_workers,
                verbose,
            )
        if missing_files:
            if backend_interface is None:
                backend_interface = lookup_backend(db.name, version)
            if files_type == "media":
                _get_media_from_backend(
                    db.name,
                    missing_files,
                    db_root,
                    flavor,
                    deps,
                    backend_interface,
                    num_workers,
                    verbose,
                )
            elif files_type == "table":
                _get_tables_from_backend(
                    db,
                    missing_files,
                    db_root,
                    deps,
                    backend_interface,
                    pickle_tables,
                    num_workers,
                    verbose,
                )

    return cached_versions


def _misc_tables_used_in_scheme(
    db: audformat.Database,
) -> list[str]:
    r"""List of misc tables that are used inside a scheme.

    Args:
        db: database object

    Returns:
        unique list of misc tables used in schemes

    """
    misc_tables_used_in_scheme = []
    for scheme in db.schemes.values():
        if scheme.uses_table:
            misc_tables_used_in_scheme.append(scheme.labels)

    return audeer.unique(misc_tables_used_in_scheme)


def _misc_tables_used_in_table(
    table: audformat.Table,
) -> list[str]:
    r"""List of misc tables that are used inside schemes of a table.

    Args:
        table: table object

    Returns:
        unique list of misc tables used in schemes of the table

    """
    misc_tables_used_in_table = []
    for column_id, column in table.columns.items():
        if column.scheme_id is not None:
            scheme = table.db.schemes[column.scheme_id]
            if scheme.uses_table:
                misc_tables_used_in_table.append(scheme.labels)
    return audeer.unique(misc_tables_used_in_table)


def _missing_files(
    files: Sequence[str],
    files_type: str,
    db_root: str,
    flavor: Flavor,
    verbose: bool,
) -> list[str]:
    r"""List missing files.

    Checks for media files,
    attachment files,
    or table files
    if they exist already in database root.

    Args:
        db_root: database root
        files: list of media files,
            attachment files,
            or table IDs
        files_type: ``'media'``,
            ``'table'``,
            or ``'attachment'``
        flavor: requested database flavor
        verbose: if ``True`` show progress bar

    Returns:
        list of missing files or table IDs

    """

    def is_cached(file):
        if files_type == "table":
            path1 = os.path.join(db_root, f"db.{file}.csv")
            path2 = os.path.join(db_root, f"db.{file}.parquet")
            return os.path.exists(path1) or os.path.exists(path2)
        elif files_type == "media" and flavor.format is not None:
            # https://github.com/audeering/audb/issues/324
            cached_file = audeer.replace_file_extension(file, flavor.format)
            return os.path.exists(os.path.join(db_root, cached_file))
        else:
            return os.path.exists(os.path.join(db_root, file))

    pbar = audeer.progress_bar(files, desc=f"Missing {files_type}", disable=not verbose)
    return [file for file in pbar if not is_cached(file)]


def _remove_media(
    db: audformat.Database,
    deps: Dependencies,
    num_workers: int,
    verbose: bool,
):
    removed_files = deps.removed_media
    if removed_files:
        db.drop_files(
            removed_files,
            num_workers=num_workers,
            verbose=verbose,
        )


def _update_path(
    db: audformat.Database,
    root: str,
    full_path: bool,
    format: str | None,
    num_workers: int,
    verbose: bool,
):
    r"""Change the file path in all tables.

    Args:
        db: database object
        root: root to add to path
        full_path: if ``True`` expand file path with ``root``
        format: file extension to change to in path
        num_workers: number of workers to use
        verbose: if ``True`` show progress bar

    """
    if not full_path and format is None:
        return

    def job(table):
        if full_path:
            table._df.index = audformat.utils.expand_file_path(
                table._df.index,
                root,
            )
            # Norm file path under Windows to include `\`
            if os.name == "nt":  # pragma: nocover as tested in Windows runner
                table._df.index = audformat.utils.map_file_path(
                    table._df.index,
                    os.path.normpath,
                )
        if format is not None:
            table._df.index = audformat.utils.replace_file_extension(
                table._df.index,
                format,
            )

    tables = db.tables.values()
    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description="Update file path",
        maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
    )


def filtered_dependencies(
    name: str,
    version: str,
    media: str | Sequence[str],
    tables: str | Sequence[str],
    cache_root: str = None,
) -> pd.DataFrame:
    r"""Filter media by tables.

    Return all media files from ``media``
    that are referenced in at least one table
    from ``tables``.
    This will download all tables.

    Args:
        name: name of database
        version: version of database
        media: media files
        tables: table IDs
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        filtered dependencies

    """
    deps = dependencies(name, version=version, cache_root=cache_root)
    if tables is None and media is None:
        df = deps()
    else:
        # Load header to get list of tables
        db = load_header(name, version=version, cache_root=cache_root)
        tables = filter_deps(tables, list(db), "table")
        tables = [t for t in tables if t not in list(db.misc_tables)]
        # Gather media files from tables
        available_media = []
        for table in tables:
            df = load_table(
                name,
                table,
                version=version,
                cache_root=cache_root,
                verbose=False,
            )
            available_media += list(df.index.get_level_values("file").unique())

        if len(available_media) > 0:
            media = filter_deps(media, deps.media, "media", name, version)
            available_media = [m for m in media if m in list(set(available_media))]
        df = deps().loc[available_media]

    return df


def load(
    name: str,
    *,
    version: str = None,
    only_metadata: bool = False,
    bit_depth: int = None,
    channels: int | Sequence[int] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    attachments: str | Sequence[str] = None,
    tables: str | Sequence[str] = None,
    media: str | Sequence[str] = None,
    removed_media: bool = False,
    full_path: bool = True,
    pickle_tables: bool = True,
    cache_root: str = None,
    num_workers: int | None = 1,
    timeout: float = define.TIMEOUT,
    verbose: bool = True,
) -> audformat.Database | None:
    r"""Load database.

    Loads meta and media files of a database to the local cache and returns
    a :class:`audformat.Database` object.

    By setting
    ``bit_depth``,
    ``channels``,
    ``format``,
    ``mixdown``,
    and ``sampling_rate``
    we can request a specific flavor of the database.
    In that case media files are automatically converted to the desired
    properties (see also :class:`audb.Flavor`).

    It is possible to filter meta and media files with the arguments
    ``tables`` and ``media``.
    Only media files with at least one reference are loaded.
    I.e. filtering meta files, may also remove media files.
    Likewise, references to missing media files will be removed, too.
    I.e. filtering media files, may also remove entries from the meta files.

    Args:
        name: name of database
        version: version string, latest if ``None``
        only_metadata: load only header and tables of database
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`.
            Note that media files with too few channels
            will be first upsampled by repeating the existing channels.
            E.g. ``channels=[0, 1]`` upsamples all mono files to stereo,
            and ``channels=[1]`` returns the second channel
            of all multi-channel files
            and all mono files
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22050``, ``24000``, ``44100``, ``48000``
        attachments: load only attachment files
            for the attachments
            matching the regular expression
            or provided in the list.
            If set to ``[]`` no attachments are loaded
        tables: load only tables and misc tables
            matching the regular expression
            or provided in the list.
            Media files not referenced
            in the selected tables
            are automatically excluded, too.
            If set to ``[]``
            no tables and media files are loaded.
            Misc tables used in schemes are always loaded
        media: load only media files
            matching the regular expression
            or provided in the list.
            Excluded media files are
            automatically removed from the tables, too.
            This may result in empty tables.
            If set to ``[]``
            no media files are loaded
            and all tables except
            misc tables will be empty
        removed_media: keep rows that reference removed media
        full_path: replace relative with absolute file paths
        pickle_tables: if ``True``,
            tables are cached locally
            in their original format
            and as pickle files.
            This allows for faster loading,
            when loading from cache
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        timeout: maximum time in seconds
            before giving up acquiring a lock to the database cache folder.
            ``None`` is returned in this case
        verbose: show debug messages

    Returns:
        database object

    Raises:
        ValueError: if attachment, table or media is requested
            that is not part of the database
        ValueError: if a non-supported ``bit_depth``,
            ``format``,
            or ``sampling_rate``
            is requested
        RuntimeError: if a flavor is requested,
            but the database contains media files,
            that don't contain audio,
            e.g. text files

    Examples:
        >>> db = audb.load(
        ...     "emodb",
        ...     version="1.4.1",
        ...     tables=["emotion", "files"],
        ...     only_metadata=True,
        ...     full_path=False,
        ...     verbose=False,
        ... )
        >>> list(db.tables)
        ['emotion', 'files']

    """
    if version is None:
        version = latest_version(name)

    db = None
    cached_versions = None
    flavor = Flavor(
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )
    db_root = database_cache_root(name, version, cache_root, flavor)

    if verbose:  # pragma: no cover
        print(f"Get:   {name} v{version}")
        print(f"Cache: {db_root}")

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    try:
        with FolderLock(db_root, timeout=timeout):
            # Start with database header without tables
            db, backend_interface = load_header_to(
                db_root,
                name,
                version,
                flavor=flavor,
                add_audb_meta=True,
            )

            db_is_complete = _database_is_complete(db)

            # load attachments
            if not db_is_complete and not only_metadata:
                # filter attachments
                requested_attachments = filter_deps(
                    attachments,
                    db.attachments,
                    "attachment",
                )

                cached_versions = _load_attachments(
                    requested_attachments,
                    backend_interface,
                    db_root,
                    db,
                    version,
                    cached_versions,
                    deps,
                    flavor,
                    cache_root,
                    num_workers,
                    verbose,
                )

            # filter tables (convert regexp pattern to list of tables)
            requested_tables = filter_deps(tables, list(db), "table")

            # add/split into misc tables used in a scheme
            # and all other (misc) tables
            requested_misc_tables = _misc_tables_used_in_scheme(db)
            requested_tables = [
                table
                for table in requested_tables
                if table not in requested_misc_tables
            ]

            # load missing tables
            if not db_is_complete:
                for _tables in [
                    requested_misc_tables,
                    requested_tables,
                ]:
                    # need to load misc tables used in a scheme first
                    # as loading is done in parallel
                    cached_versions = _load_files(
                        _tables,
                        "table",
                        backend_interface,
                        db_root,
                        db,
                        version,
                        cached_versions,
                        deps,
                        flavor,
                        cache_root,
                        pickle_tables,
                        num_workers,
                        verbose,
                    )
            requested_tables = requested_misc_tables + requested_tables

            # filter tables
            if tables is not None:
                db.pick_tables(requested_tables)

            # load tables
            for table in requested_tables:
                db[table].load(os.path.join(db_root, f"db.{table}"))

            # filter media
            requested_media = filter_deps(
                media,
                db.files,
                "media",
                name,
                version,
            )

            # load missing media
            if not db_is_complete and not only_metadata:
                cached_versions = _load_files(
                    requested_media,
                    "media",
                    backend_interface,
                    db_root,
                    db,
                    version,
                    cached_versions,
                    deps,
                    flavor,
                    cache_root,
                    False,
                    num_workers,
                    verbose,
                )

            # filter media
            if media is not None or tables is not None:
                db.pick_files(requested_media)

            if not removed_media:
                _remove_media(db, deps, num_workers, verbose)

            # Adjust full paths and file extensions in tables
            _update_path(
                db,
                db_root,
                full_path,
                flavor.format,
                num_workers,
                verbose,
            )

            # set file durations
            _files_duration(
                db,
                deps,
                requested_media,
                flavor.format,
            )

            # check if database is now complete
            if not db_is_complete:
                _database_check_complete(
                    db,
                    db_root,
                    flavor,
                    deps,
                )

    except filelock.Timeout:
        utils.timeout_warning()

    return db


def load_attachment(
    name: str,
    attachment: str,
    *,
    version: str = None,
    cache_root: str = None,
    verbose: bool = True,
) -> list[str]:
    r"""Load attachment(s) of database.

    Args:
        name: name of database
        attachment: attachment ID to load
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        verbose: show debug messages

    Returns:
        list of file paths belonging to attachment

    Raises:
        ValueError: if an attachment ID is requested
            that is not part of the database

    Examples:
        >>> paths = audb.load_attachment(
        ...     "emodb",
        ...     "bibtex",
        ...     version="1.4.1",
        ...     verbose=False,
        ... )
        >>> os.path.basename(paths[0])
        'burkhardt2005emodb.bib'

    """
    if version is None:
        version = latest_version(name)

    db_root = database_cache_root(name, version, cache_root)

    if verbose:  # pragma: no cover
        print(f"Get:   {name} v{version}")
        print(f"Cache: {db_root}")

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    if attachment not in deps.archives:
        msg = error_message_missing_object(
            "attachment",
            [attachment],
            name,
            version,
        )
        raise ValueError(msg)

    with FolderLock(db_root):
        # Start with database header
        db, backend_interface = load_header_to(
            db_root,
            name,
            version,
        )

        # Load attachment
        _load_attachments(
            [attachment],
            backend_interface,
            db_root,
            db,
            version,
            None,
            deps,
            Flavor(),
            cache_root,
            1,
            verbose,
        )

    attachment_files = db.attachments[attachment].files
    attachment_files = [
        os.path.join(db_root, os.path.normpath(file))  # convert "/" to os.sep
        for file in attachment_files
    ]
    return attachment_files


def load_header(
    name: str,
    *,
    version: str = None,
    cache_root: str = None,
) -> audformat.Database:
    r"""Load header of database.

    Args:
        name: name of database
        version: version of database
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used

    Returns:
        database object without table data

    """
    if version is None:
        version = latest_version(name)

    db_root = database_cache_root(name, version, cache_root)

    with FolderLock(db_root):
        db, _ = load_header_to(db_root, name, version)

    return db


def load_header_to(
    db_root: str,
    name: str,
    version: str,
    *,
    flavor: Flavor = None,
    add_audb_meta: bool = False,
    overwrite: bool = False,
) -> tuple[audformat.Database, type[audbackend.interface.Base] | None]:
    r"""Load database header from folder or backend.

    If the database header cannot be found in ``db_root``
    it will search for the backend that contains the database,
    load it from there,
    and store it in ``db_root``.

    Args:
        db_root: folder of database
        name: name of database
        version: version of database
        flavor: flavor of database,
            needed if ``add_audb_meta`` is True
        add_audb_meta: if ``True`` it adds an ``audb`` meta entry
            to the database header before storing it in cache
        overwrite: always load header from backend
            and overwrite the one found in ``db_root``

    Returns:
        database header and backend

    """
    backend_interface = None
    local_header = os.path.join(db_root, define.HEADER_FILE)
    if overwrite or not os.path.exists(local_header):
        backend_interface = lookup_backend(name, version)
        remote_header = backend_interface.join("/", name, define.HEADER_FILE)
        if add_audb_meta:
            db_root_tmp = database_tmp_root(db_root)
            local_header = os.path.join(db_root_tmp, define.HEADER_FILE)
        backend_interface.get_file(remote_header, local_header, version)
        if add_audb_meta:
            db = audformat.Database.load(db_root_tmp, load_data=False)
            db.meta["audb"] = {
                "root": db_root,
                "version": version,
                "flavor": flavor.arguments,
                "complete": False,
            }
            db.save(db_root_tmp, header_only=True)
            audeer.move_file(
                os.path.join(db_root_tmp, define.HEADER_FILE),
                os.path.join(db_root, define.HEADER_FILE),
            )
            audeer.rmdir(db_root_tmp)

    return audformat.Database.load(db_root, load_data=False), backend_interface


def load_media(
    name: str,
    media: str | Sequence[str],
    *,
    version: str = None,
    bit_depth: int = None,
    channels: int | Sequence[int] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    cache_root: str = None,
    num_workers: int | None = 1,
    timeout: float = define.TIMEOUT,
    verbose: bool = True,
) -> list | None:
    r"""Load media file(s).

    If you are interested in media files
    and not the corresponding tables,
    you can use :func:`audb.load_media`
    to load them.
    This will not download any table files
    to your disk,
    but share the cache with :func:`audb.load`.

    Args:
        name: name of database
        media: load media files provided in the list
        version: version of database
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`.
            Note that media files with too few channels
            will be first upsampled by repeating the existing channels.
            E.g. ``channels=[0, 1]`` upsamples all mono files to stereo,
            and ``channels=[1]`` returns the second channel
            of all multi-channel files
            and all mono files
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22050``, ``24000``, ``44100``, ``48000``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        timeout: maximum time in seconds
            before giving up acquiring a lock to the database cache folder.
            ``None`` is returned in this case
        verbose: show debug messages

    Returns:
        paths to media files

    Raises:
        ValueError: if a media file is requested
            that is not part of the database
        ValueError: if a non-supported ``bit_depth``,
            ``format``,
            or ``sampling_rate``
            is requested

    Examples:
        >>> paths = audb.load_media(
        ...     "emodb",
        ...     ["wav/03a01Fa.wav"],
        ...     version="1.4.1",
        ...     format="flac",
        ...     verbose=False,
        ... )
        >>> paths[0].split(os.path.sep)[-5:]
        ['emodb', '1.4.1', '40bb2241', 'wav', '03a01Fa.flac']

    """
    media = audeer.to_list(media)
    if len(media) == 0:
        return []

    if version is None:
        version = latest_version(name)

    files = None
    flavor = Flavor(
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )
    db_root = database_cache_root(name, version, cache_root, flavor)

    if verbose:  # pragma: no cover
        print(f"Get:   {name} v{version}")
        print(f"Cache: {db_root}")

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    available_files = deps.media
    for media_file in media:
        if media_file not in available_files:
            msg = error_message_missing_object(
                "media",
                [media_file],
                name,
                version,
            )
            raise ValueError(msg)

    try:
        with FolderLock(db_root, timeout=timeout):
            # Start with database header without tables
            db, backend_interface = load_header_to(
                db_root,
                name,
                version,
                flavor=flavor,
                add_audb_meta=True,
            )

            db_is_complete = _database_is_complete(db)

            # load missing media
            if not db_is_complete:
                _load_files(
                    media,
                    "media",
                    backend_interface,
                    db_root,
                    db,
                    version,
                    None,
                    deps,
                    flavor,
                    cache_root,
                    False,
                    num_workers,
                    verbose,
                )

            if format is not None:
                media = [audeer.replace_file_extension(m, format) for m in media]
            files = [
                os.path.join(db_root, os.path.normpath(file))  # convert "/" to os.sep
                for file in media
            ]

    except filelock.Timeout:
        utils.timeout_warning()

    return files


def load_table(
    name: str,
    table: str,
    *,
    version: str = None,
    map: dict[str, str | Sequence[str]] = None,
    pickle_tables: bool = True,
    cache_root: str = None,
    num_workers: int | None = 1,
    verbose: bool = True,
) -> pd.DataFrame:
    r"""Load a database table.

    If you are interested in a single table
    from a database
    you can use :func:`audb.load_table`
    to directly load it.
    This will not download any media files
    to your disk,
    but share the cache with :func:`audb.load`.

    Args:
        name: name of database
        table: load table from database
        version: version of database
        map: map scheme or scheme fields to column values.
            For example if your table holds a column ``speaker`` with
            speaker IDs, which is assigned to a scheme that contains a
            dict mapping speaker IDs to age and gender entries,
            ``map={'speaker': ['age', 'gender']}``
            will replace the column with two new columns that map ID
            values to age and gender, respectively.
            To also keep the original column with speaker IDS, you can do
            ``map={'speaker': ['speaker', 'age', 'gender']}``
        pickle_tables: if ``True``,
            tables are cached locally
            in their original format
            and as pickle files.
            This allows for faster loading,
            when loading from cache
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        database table

    Raises:
        ValueError: if a table is requested
            that is not part of the database

    Examples:
        >>> df = audb.load_table("emodb", "emotion", version="1.4.1", verbose=False)
        >>> df[:3]
                           emotion  emotion.confidence
        file
        wav/03a01Fa.wav  happiness                0.90
        wav/03a01Nc.wav    neutral                1.00
        wav/03a01Wa.wav      anger                0.95
        >>> df = audb.load_table("emodb", "files", version="1.4.1", verbose=False)
        >>> df[:3]
                                         duration speaker transcription
        file
        wav/03a01Fa.wav    0 days 00:00:01.898250       3           a01
        wav/03a01Nc.wav    0 days 00:00:01.611250       3           a01
        wav/03a01Wa.wav 0 days 00:00:01.877812500       3           a01
        >>> df = audb.load_table(
        ...     "emodb",
        ...     "files",
        ...     version="1.4.1",
        ...     map={"speaker": "age"},
        ...     verbose=False,
        ... )
        >>> df[:3]
                                         duration transcription  age
        file
        wav/03a01Fa.wav    0 days 00:00:01.898250           a01   31
        wav/03a01Nc.wav    0 days 00:00:01.611250           a01   31
        wav/03a01Wa.wav 0 days 00:00:01.877812500           a01   31

    """
    if version is None:
        version = latest_version(name)

    db_root = database_cache_root(name, version, cache_root)

    if verbose:  # pragma: no cover
        print(f"Get:   {name} v{version}")
        print(f"Cache: {db_root}")

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    if table not in deps.table_ids:
        msg = error_message_missing_object(
            "table",
            [table],
            name,
            version,
        )
        raise ValueError(msg)

    with FolderLock(db_root):
        # Start with database header without tables
        db, backend_interface = load_header_to(
            db_root,
            name,
            version,
        )

        # Find only those misc tables used in schemes of the requested table
        scheme_misc_tables = []
        for column_id, column in db[table].columns.items():
            if column.scheme_id is not None:
                scheme = db.schemes[column.scheme_id]
                if scheme.uses_table:
                    scheme_misc_tables.append(scheme.labels)
        scheme_misc_tables = audeer.unique(scheme_misc_tables)

        # Load table
        tables = scheme_misc_tables + [table]
        for _table in tables:
            table_file = os.path.join(db_root, f"db.{_table}")
            # `_load_files()` downloads a table
            # from the backend,
            # if it cannot find its corresponding csv or parquet file
            if not os.path.exists(f"{table_file}.pkl"):
                _load_files(
                    [_table],
                    "table",
                    backend_interface,
                    db_root,
                    db,
                    version,
                    None,
                    deps,
                    Flavor(),
                    cache_root,
                    pickle_tables,
                    num_workers,
                    verbose,
                )
            db[_table].load(table_file)

    if map is None:
        df = db[table]._df
    else:
        df = db[table].get(map=map)

    return df
