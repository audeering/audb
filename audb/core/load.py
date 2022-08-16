import os
import re
import shutil
import typing

import filelock
import pandas as pd

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core import utils
from audb.core.api import (
    cached,
    dependencies,
    latest_version,
)
from audb.core.cache import (
    database_cache_root,
    database_tmp_root,
    default_cache_root,
)
from audb.core.dependencies import (
    Dependencies,
    error_message_missing_object,
    filter_media,
    filter_tables,
)
from audb.core.flavor import Flavor
from audb.core.lock import FolderLock
from audb.core.utils import lookup_backend


CachedVersions = typing.Sequence[
    typing.Tuple[audeer.LooseVersion, str, Dependencies],
]


def _cached_versions(
        name: str,
        version: str,
        flavor: Flavor,
        cache_root: typing.Optional[str],
) -> CachedVersions:
    r"""Find other cached versions of same flavor."""

    df = cached(cache_root=cache_root, name=name)
    # If no explicit cache root is given,
    # we look into the private and shared one.
    # This fixes https://github.com/audeering/audb/issues/101
    if cache_root is None and os.path.exists(default_cache_root(shared=True)):
        df = pd.concat((df, cached(name=name, shared=True)))

    cached_versions = []
    for flavor_root, row in df.iterrows():
        if row['flavor_id'] == flavor.short_id:
            if row['version'] == version:
                continue
            deps = dependencies(
                name,
                version=row['version'],
                cache_root=cache_root,
            )
            # as it is more likely we find files
            # in newer versions, push them to front
            cached_versions.insert(
                0,
                (
                    audeer.LooseVersion(row['version']),
                    str(flavor_root),
                    deps,
                ),
            )

    return cached_versions


def _cached_files(
        files: typing.Sequence[str],
        deps: Dependencies,
        cached_versions: CachedVersions,
        flavor: typing.Optional[Flavor],
        verbose: bool,
) -> (typing.Sequence[typing.Union[str, str]], typing.Sequence[str]):
    r"""Find cached files."""

    cached_files = []
    missing_files = []

    for file in audeer.progress_bar(
            files,
            desc='Cached files',
            disable=not verbose,
    ):
        found = False
        file_version = audeer.LooseVersion(deps.version(file))
        for cache_version, cache_root, cache_deps in cached_versions:
            if cache_version >= file_version:
                if file in cache_deps:
                    if deps.checksum(file) == cache_deps.checksum(file):
                        path = os.path.join(cache_root, file)
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


def _copy_file(
        file: str,
        root_src: str,
        root_tmp: str,
        root_dst: str,
):
    r"""Copy file."""
    src_path = os.path.join(root_src, file)
    tmp_path = os.path.join(root_tmp, file)
    dst_path = os.path.join(root_dst, file)
    audeer.mkdir(os.path.dirname(tmp_path))
    audeer.mkdir(os.path.dirname(dst_path))
    shutil.copy(src_path, tmp_path)
    audeer.move_file(tmp_path, dst_path)


def _database_check_complete(
        db: audformat.Database,
        db_root: str,
        flavor: Flavor,
        deps: Dependencies,
):
    def check() -> bool:
        complete = True
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
        db.meta['audb']['complete'] = True
        db_original = audformat.Database.load(db_root, load_data=False)
        db_original.meta['audb']['complete'] = True
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
    if 'audb' in db.meta:
        if 'complete' in db.meta['audb']:
            complete = db.meta['audb']['complete']
    return complete


def _files_duration(
        db: audformat.Database,
        deps: Dependencies,
        files: typing.Sequence[str],
        format: typing.Optional[str],
):
    field = define.DEPEND_FIELD_NAMES[define.DependField.DURATION]
    durs = deps().loc[files][field]
    durs = durs[durs > 0]
    durs = pd.to_timedelta(durs, unit='s')
    durs.index.name = 'file'
    if format is not None:
        durs.index = audformat.utils.replace_file_extension(durs.index, format)
    # Norm file path under Windows to include `\`
    if os.name == 'nt':  # pragma: nocover as tested in Windows runner
        durs.index = audformat.utils.map_file_path(
            durs.index,
            os.path.normpath,
        )
    durs.index = audformat.utils.expand_file_path(durs.index, db.root)
    db._files_duration = durs.to_dict()


def _get_media_from_backend(
        name: str,
        media: typing.Sequence[str],
        db_root: str,
        flavor: typing.Optional[Flavor],
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
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
        archive = backend.join(
            name,
            define.DEPEND_TYPE_NAMES[define.DependType.MEDIA],
            archive,
        )
        # extract and move all files that are stored in the archive,
        # even if only a single file from the archive was requested
        files = backend.get_archive(archive, db_root_tmp, version)
        for file in files:
            if flavor is not None:
                bit_depth = deps.bit_depth(file)
                channels = deps.channels(file)
                sampling_rate = deps.sampling_rate(file)
                src_path = os.path.join(db_root_tmp, file)
                file = flavor.destination(file)
                dst_path = os.path.join(db_root_tmp, file)
                flavor(
                    src_path,
                    dst_path,
                    src_bit_depth=bit_depth,
                    src_channels=channels,
                    src_sampling_rate=sampling_rate,
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
        task_description='Load media',
    )

    audeer.rmdir(db_root_tmp)


def _get_media_from_cache(
        media: typing.Sequence[str],
        db_root: str,
        deps: Dependencies,
        cached_versions: CachedVersions,
        flavor: Flavor,
        num_workers: int,
        verbose: bool,
) -> typing.Sequence[str]:
    r"""Copy media from cache."""

    db_root_cached = [x[1] for x in cached_versions]

    try:
        with FolderLock(
                db_root_cached,
                timeout=define.CACHED_VERSIONS_TIMEOUT,
        ):

            cached_media, missing_media = _cached_files(
                media,
                deps,
                cached_versions,
                flavor,
                verbose,
            )
            db_root_tmp = database_tmp_root(db_root)

            def job(cache_root: str, file: str):
                _copy_file(file, cache_root, db_root_tmp, db_root)

            audeer.run_tasks(
                job,
                params=[([root, file], {}) for root, file in cached_media],
                num_workers=num_workers,
                progress_bar=verbose,
                task_description='Copy media',
            )

            audeer.rmdir(db_root_tmp)

    except filelock.Timeout:
        missing_media = media

    return missing_media


def _get_tables_from_backend(
        db: audformat.Database,
        tables: typing.Sequence[str],
        db_root: str,
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Load tables from backend."""
    db_root_tmp = database_tmp_root(db_root)

    def job(table: str):
        archive = backend.join(
            db.name,
            define.DEPEND_TYPE_NAMES[define.DependType.META],
            deps.archive(table),
        )
        backend.get_archive(
            archive,
            db_root_tmp,
            deps.version(table),
        )
        table_id = table[3:-4]
        table_path = os.path.join(db_root_tmp, f'db.{table_id}')
        db[table_id].load(table_path)
        db[table_id].save(
            table_path,
            storage_format=audformat.define.TableStorageFormat.PICKLE,
        )
        for storage_format in [
            audformat.define.TableStorageFormat.PICKLE,
            audformat.define.TableStorageFormat.CSV,
        ]:
            file = f'db.{table_id}.{storage_format}'
            audeer.move_file(
                os.path.join(db_root_tmp, file),
                os.path.join(db_root, file),
            )

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Load tables',
    )

    audeer.rmdir(db_root_tmp)


def _get_tables_from_cache(
        tables: typing.Sequence[str],
        db_root: str,
        deps: Dependencies,
        cached_versions: CachedVersions,
        num_workers: int,
        verbose: bool,
) -> typing.Sequence[str]:
    r"""Copy tables from cache."""

    db_root_cached = [x[1] for x in cached_versions]

    try:
        with FolderLock(
                db_root_cached,
                timeout=define.CACHED_VERSIONS_TIMEOUT,
        ):

            cached_tables, missing_tables = _cached_files(
                tables,
                deps,
                cached_versions,
                None,
                verbose,
            )
            db_root_tmp = database_tmp_root(db_root)

            def job(cache_root: str, file: str):
                file_pkl = audeer.replace_file_extension(
                    file,
                    audformat.define.TableStorageFormat.PICKLE,
                )
                _copy_file(file, cache_root, db_root_tmp, db_root)
                _copy_file(file_pkl, cache_root, db_root_tmp, db_root)

            audeer.run_tasks(
                job,
                params=[([root, file], {}) for root, file in cached_tables],
                num_workers=num_workers,
                progress_bar=verbose,
                task_description='Copy tables',
            )

            audeer.rmdir(db_root_tmp)

    except filelock.Timeout:
        missing_tables = tables

    return missing_tables


def _load_media(
        media: typing.Sequence[str],
        backend: audbackend.Backend,
        db_root: str,
        name: str,
        version: str,
        cached_versions: typing.Optional[CachedVersions],
        deps: Dependencies,
        flavor: Flavor,
        cache_root: str,
        num_workers: int,
        verbose: bool,
) -> typing.Optional[CachedVersions]:
    r"""Load media files to cache.

    All media files not existing in cache yet
    are copied from the corresponding flavor cache
    folder of other versions of the database
    or are downloaded from the backend.

    """
    missing_media = _missing_media(
        db_root,
        media,
        flavor,
        verbose,
    )
    if missing_media:
        if cached_versions is None:
            cached_versions = _cached_versions(
                name,
                version,
                flavor,
                cache_root,
            )
        if cached_versions:
            missing_media = _get_media_from_cache(
                missing_media,
                db_root,
                deps,
                cached_versions,
                flavor,
                num_workers,
                verbose,
            )
        if missing_media:
            if backend is None:
                backend = lookup_backend(name, version)
            _get_media_from_backend(
                name,
                missing_media,
                db_root,
                flavor,
                deps,
                backend,
                num_workers,
                verbose,
            )

    return cached_versions


def _load_tables(
        tables: typing.Sequence[str],
        backend: audbackend.Backend,
        db_root: str,
        db: audformat.Database,
        version: str,
        cached_versions: typing.Optional[CachedVersions],
        deps: Dependencies,
        flavor: Flavor,
        cache_root: str,
        num_workers: int,
        verbose: bool,
) -> typing.Optional[CachedVersions]:
    r"""Load table files to cache.

    All table files not existing in cache yet
    are copied from the corresponding flavor cache
    folder of other versions of the database
    or are downloaded from the backend.

    """
    missing_tables = _missing_tables(
        db_root,
        tables,
        verbose,
    )
    if missing_tables:
        if cached_versions is None:
            cached_versions = _cached_versions(
                db.name,
                version,
                flavor,
                cache_root,
            )
        if cached_versions:
            missing_tables = _get_tables_from_cache(
                missing_tables,
                db_root,
                deps,
                cached_versions,
                num_workers,
                verbose,
            )
        if missing_tables:
            if backend is None:
                backend = lookup_backend(db.name, version)
            _get_tables_from_backend(
                db,
                missing_tables,
                db_root,
                deps,
                backend,
                num_workers,
                verbose,
            )

    return cached_versions


def _misc_tables_used_in_scheme(
        db: audformat.Database,
) -> typing.List[str]:
    r"""List of misc tables that are used inside a scheme."""
    misc_tables_used_in_scheme = []
    for scheme in db.schemes.values():
        if scheme.uses_table:
            misc_tables_used_in_scheme.append(scheme.labels)

    return list(set(misc_tables_used_in_scheme))


def _missing_media(
        db_root: str,
        media: typing.Sequence[str],
        flavor: Flavor,
        verbose: bool,
) -> typing.Sequence[str]:
    missing_media = []
    for file in audeer.progress_bar(
            media,
            desc='Missing media',
            disable=not verbose
    ):
        path = os.path.join(db_root, file)
        if flavor.format is not None:
            path = audeer.replace_file_extension(path, flavor.format)
        if not os.path.exists(path):
            missing_media.append(file)
    return missing_media


def _missing_tables(
        db_root: str,
        tables: typing.Sequence[str],
        verbose: bool,
) -> typing.Sequence[str]:
    missing_tables = []
    for table in audeer.progress_bar(
            tables,
            desc='Missing tables',
            disable=not verbose,
    ):
        file = f'db.{table}.csv'
        path = os.path.join(db_root, file)
        if not os.path.exists(path):
            missing_tables.append(file)
    return missing_tables


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
        format: typing.Optional[str],
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
            if os.name == 'nt':  # pragma: nocover as tested in Windows runner
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
        task_description='Update file path',
    )


def filtered_dependencies(
        name: str,
        version: str,
        media: typing.Union[str, typing.Sequence[str]],
        tables: typing.Union[str, typing.Sequence[str]],
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
        tables = filter_tables(tables, list(db))
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
            available_media += list(
                df.index.get_level_values('file').unique()
            )

        if len(available_media) > 0:
            media = filter_media(media, deps.media, name, version)
            available_media = [
                m for m in media
                if m in list(set(available_media))
            ]
        df = deps().loc[available_media]

    return df


def load(
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
        media: typing.Union[str, typing.Sequence[str]] = None,
        removed_media: bool = False,
        full_path: bool = True,
        cache_root: str = None,
        num_workers: typing.Optional[int] = 1,
        timeout: float = -1,
        verbose: bool = True,
) -> typing.Optional[audformat.Database]:
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
        only_metadata: load only metadata
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
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        tables: include only tables matching the regular expression or
            provided in the list
        media: include only media matching the regular expression or
            provided in the list
        removed_media: keep rows that reference removed media
        full_path: replace relative with absolute file paths
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        timeout: maximum wait time if another thread or process is already
            accessing the database. If timeout is reached, ``None`` is
            returned. If timeout < 0 the method will block until the
            database can be accessed
        verbose: show debug messages

    Returns:
        database object

    Raises:
        ValueError: if table or media is requested
            that is not part of the database
        ValueError: if a non-supported ``bit_depth``,
            ``format``,
            or ``sampling_rate``
            is requested

    Example:
        >>> db = audb.load(
        ...     'emodb',
        ...     version='1.3.0',
        ...     tables=['emotion', 'files'],
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
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    try:
        with FolderLock(db_root, timeout=timeout):

            # Start with database header without tables
            db, backend = load_header_to(
                db_root,
                name,
                version,
                flavor=flavor,
                add_audb_meta=True,
            )

            db_is_complete = _database_is_complete(db)

            # filter tables (convert regexp pattern to list of tables)
            requested_tables = filter_tables(tables, list(db))

            # add/split into misc tables used in a scheme
            # and all other (misc) tables
            requested_misc_tables = _misc_tables_used_in_scheme(db)
            requested_tables = [
                table for table in requested_tables
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
                    cached_versions = _load_tables(
                        _tables,
                        backend,
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
            requested_tables = requested_misc_tables + requested_tables

            # filter tables
            if tables is not None:
                db.pick_tables(requested_tables)

            # load tables
            for table in requested_tables:
                db[table].load(os.path.join(db_root, f'db.{table}'))

            # filter media
            requested_media = filter_media(media, db.files, name, version)

            # load missing media
            if not db_is_complete and not only_metadata:
                cached_versions = _load_media(
                    requested_media,
                    backend,
                    db_root,
                    name,
                    version,
                    cached_versions,
                    deps,
                    flavor,
                    cache_root,
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
) -> typing.Tuple[audformat.Database, typing.Optional[audbackend.Backend]]:
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
    backend = None
    local_header = os.path.join(db_root, define.HEADER_FILE)
    if overwrite or not os.path.exists(local_header):
        backend = lookup_backend(name, version)
        remote_header = backend.join(name, define.HEADER_FILE)
        if add_audb_meta:
            db_root_tmp = database_tmp_root(db_root)
            local_header = os.path.join(db_root_tmp, define.HEADER_FILE)
        backend.get_file(remote_header, local_header, version)
        if add_audb_meta:
            db = audformat.Database.load(db_root_tmp, load_data=False)
            db.meta['audb'] = {
                'root': db_root,
                'version': version,
                'flavor': flavor.arguments,
                'complete': False,
            }
            db.save(db_root_tmp, header_only=True)
            audeer.move_file(
                os.path.join(db_root_tmp, define.HEADER_FILE),
                os.path.join(db_root, define.HEADER_FILE),
            )
            audeer.rmdir(db_root_tmp)

    return audformat.Database.load(db_root, load_data=False), backend


def load_media(
        name: str,
        media: typing.Union[str, typing.Sequence[str]],
        *,
        version: str = None,
        bit_depth: int = None,
        channels: typing.Union[int, typing.Sequence[int]] = None,
        format: str = None,
        mixdown: bool = False,
        sampling_rate: int = None,
        cache_root: str = None,
        num_workers: typing.Optional[int] = 1,
        timeout: float = -1,
        verbose: bool = True,
) -> typing.Optional[typing.List]:
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
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        timeout: maximum wait time if another thread or process is already
            accessing the database. If timeout is reached, ``None`` is
            returned. If timeout < 0 the method will block until the
            database can be accessed
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

    Example:
        >>> paths = load_media(
        ...     'emodb',
        ...     ['wav/03a01Fa.wav'],
        ...     version='1.3.0',
        ...     format='flac',
        ...     verbose=False,
        ... )
        >>> paths[0].split(os.path.sep)[-5:]
        ['emodb', '1.3.0', '40bb2241', 'wav', '03a01Fa.flac']

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
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

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
                'media file',
                [media_file],
                name,
                version,
            )
            raise ValueError(msg)

    try:
        with FolderLock(db_root, timeout=timeout):

            # Start with database header without tables
            db, backend = load_header_to(
                db_root,
                name,
                version,
                flavor=flavor,
                add_audb_meta=True,
            )

            db_is_complete = _database_is_complete(db)

            # load missing media
            if not db_is_complete:
                _load_media(
                    media,
                    backend,
                    db_root,
                    name,
                    version,
                    None,
                    deps,
                    flavor,
                    cache_root,
                    num_workers,
                    verbose,
                )

            if format is not None:
                media = [
                    audeer.replace_file_extension(m, format) for m in media
                ]
            files = [
                os.path.join(db_root, os.path.normpath(m)) for m in media
            ]

    except filelock.Timeout:
        utils.timeout_warning()

    return files


def load_table(
        name: str,
        table: str,
        *,
        version: str = None,
        cache_root: str = None,
        num_workers: typing.Optional[int] = 1,
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

    Example:
        >>> df = load_table(
        ...     'emodb',
        ...     'emotion',
        ...     version='1.3.0',
        ...     verbose=False,
        ... )
        >>> df[:3]
                           emotion  emotion.confidence
        file
        wav/03a01Fa.wav  happiness                0.90
        wav/03a01Nc.wav    neutral                1.00
        wav/03a01Wa.wav      anger                0.95

    """
    if version is None:
        version = latest_version(name)

    db_root = database_cache_root(name, version, cache_root)

    if verbose:  # pragma: no cover
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    if table not in deps.table_ids:
        msg = error_message_missing_object(
            'table',
            [table],
            name,
            version,
        )
        raise ValueError(msg)

    with FolderLock(db_root):

        # Start with database header without tables
        db, backend = load_header_to(
            db_root,
            name,
            version,
        )

        # Load table
        tables = _misc_tables_used_in_scheme(db) + [table]
        for table in tables:
            table_file = os.path.join(db_root, f'db.{table}')
            if not (
                    os.path.exists(f'{table_file}.csv')
                    or os.path.exists(f'{table_file}.pkl')
            ):
                _load_tables(
                    [table],
                    backend,
                    db_root,
                    db,
                    version,
                    None,
                    deps,
                    Flavor(),
                    cache_root,
                    num_workers,
                    verbose,
                )
            table = audformat.Table()
            table.load(table_file)

    return table._df
