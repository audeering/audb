from distutils.version import LooseVersion
import os
import re
import shutil
import typing

import pandas as pd

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.api import (
    cached,
    default_cache_root,
    dependencies,
    latest_version,
)
from audb.core.backward import parse_deprecated_load_arguments
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.utils import lookup_backend


def _cached_versions(
        name: str,
        version: str,
        flavor: Flavor,
        cache_root: typing.Optional[str],
) -> typing.Sequence[typing.Tuple[LooseVersion, str, Dependencies]]:
    r"""Find other cached versions of same flavor."""

    df = cached(cache_root=cache_root)
    df = df[df.name == name]

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
                    LooseVersion(row['version']),
                    flavor_root,
                    deps,
                ),
            )

    return cached_versions


def _cached_files(
        files: typing.Sequence[str],
        deps: Dependencies,
        cached_versions: typing.Sequence[
            typing.Tuple[LooseVersion, str, Dependencies],
        ],
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
        file_version = LooseVersion(deps.version(file))
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
    _move_file(root_tmp, root_dst, file)


def _database_check_complete(
        db: audformat.Database,
        db_root: str,
        db_root_tmp: str,
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
        db.meta['audb']['complete'] = True
        db_original = audformat.Database.load(db_root, load_data=False)
        db_original.meta['audb']['complete'] = True
        db_original.save(db_root_tmp, header_only=True)
        _move_file(db_root_tmp, db_root, define.HEADER_FILE)


def _database_is_complete(
        db: audformat.Database,
) -> bool:
    complete = False
    if 'audb' in db.meta:
        if 'complete' in db.meta['audb']:
            complete = db.meta['audb']['complete']
    return complete


def _fix_media_ext(
        tables: typing.Sequence[audformat.Table],
        format: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):

    def job(table):
        # Faster solution then using db.map_files()
        cur_ext = r'\.[a-zA-Z0-9]+$'  # match file extension
        new_ext = f'.{format}'
        if table.is_filewise:
            table.df.index = table.df.index.str.replace(cur_ext, new_ext)
        else:
            table.df.index.set_levels(
                table.df.index.levels[0].str.replace(cur_ext, new_ext),
                'file',
                inplace=True,
            )

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Fix format',
    )


def _full_path(
        db: audformat.Database,
        db_root: str,
):
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


def _get_media_from_backend(
        name: str,
        media: typing.Sequence[str],
        db_root: str,
        db_root_tmp: str,
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
    for file in media:
        audeer.mkdir(os.path.dirname(os.path.join(db_root, file)))
        audeer.mkdir(os.path.dirname(os.path.join(db_root_tmp, file)))

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

            _move_file(db_root_tmp, db_root, file)

    audeer.run_tasks(
        job,
        params=[([archive, version], {}) for archive, version in archives],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Load media',
    )


def _get_media_from_cache(
        media: typing.Sequence[str],
        db_root: str,
        db_root_tmp: str,
        deps: Dependencies,
        cached_versions: typing.Sequence[
            typing.Tuple[LooseVersion, str, Dependencies]
        ],
        flavor: Flavor,
        num_workers: int,
        verbose: bool,
) -> typing.Sequence[str]:
    r"""Copy media from cache."""

    cached_media, missing_media = _cached_files(
        media,
        deps,
        cached_versions,
        flavor,
        verbose,
    )

    def job(cache_root: str, file: str):
        _copy_file(file, cache_root, db_root_tmp, db_root)

    audeer.run_tasks(
        job,
        params=[([root, file], {}) for root, file in cached_media],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Copy media',
    )

    return missing_media


def _get_tables_from_backend(
        db: audformat.Database,
        tables: typing.Sequence[str],
        db_root: str,
        db_root_tmp: str,
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Load tables from backend."""

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
            _move_file(db_root_tmp, db_root, f'db.{table_id}.{storage_format}')

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Load tables',
    )


def _get_tables_from_cache(
        tables: typing.Sequence[str],
        db_root: str,
        db_root_tmp: str,
        deps: Dependencies,
        cached_versions: typing.Sequence[
            typing.Tuple[LooseVersion, str, Dependencies]
        ],
        num_workers: int,
        verbose: bool,
) -> typing.Sequence[str]:
    r"""Copy tables from cache."""

    cached_tables, missing_tables = _cached_files(
        tables,
        deps,
        cached_versions,
        None,
        verbose,
    )

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

    return missing_tables


def _load_media(
        media: typing.Sequence[str],
        backend: audbackend.Backend,
        db_root: str,
        db_root_tmp: str,
        name: str,
        version: str,
        cached_versions: typing.Optional[
            typing.Sequence[typing.Tuple[LooseVersion, str, Dependencies]]
        ],
        deps: Dependencies,
        flavor: Flavor,
        cache_root: str,
        num_workers: int,
        verbose: bool,
):
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
                db_root_tmp,
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
                db_root_tmp,
                flavor,
                deps,
                backend,
                num_workers,
                verbose,
            )


def _load_tables(
        tables: typing.Sequence[str],
        backend: audbackend.Backend,
        db_root: str,
        db_root_tmp: str,
        db: audformat.Database,
        version: str,
        cached_versions: typing.Optional[
            typing.Sequence[typing.Tuple[LooseVersion, str, Dependencies]]
        ],
        deps: Dependencies,
        flavor: Flavor,
        cache_root: str,
        num_workers: int,
        verbose: bool,
):
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
                db_root_tmp,
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
                db_root_tmp,
                deps,
                backend,
                num_workers,
                verbose,
            )


def _media(
        db: audformat.Database,
        media: typing.Optional[typing.Union[str, typing.Sequence[str]]],
) -> typing.Sequence[str]:

    if media is None:
        media = db.files
    elif isinstance(media, str):
        pattern = re.compile(media)
        media = []
        for m in db.files:
            if pattern.search(m):
                media.append(m)

    return media


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


def _tables(
        deps: Dependencies,
        tables: typing.Optional[typing.Union[str, typing.Sequence[str]]],
) -> typing.Sequence[str]:
    if tables is None:
        tables = deps.table_ids
    elif isinstance(tables, str):
        pattern = re.compile(tables)
        tables = []
        for table in deps.table_ids:
            if pattern.search(table):
                tables.append(table)
    return tables


def database_cache_folder(
        name: str,
        version: str,
        cache_root: str = None,
        flavor: Flavor = None,
) -> str:
    r"""Create and return database cache folder.

    Args:
        name: name of database
        version: version of database
        cache_root: path to cache folder
        flavor: flavor of database

    Returns:
        path to cache folder

    """
    if cache_root is None:
        cache_roots = [
            default_cache_root(True),  # check shared cache first
            default_cache_root(False),
        ]
    else:
        cache_roots = [cache_root]
    for cache_root in cache_roots:
        if flavor is None:
            db_root = os.path.join(
                cache_root,
                name,
                version,
            )
        else:
            db_root = os.path.join(
                cache_root,
                flavor.path(name, version),
            )
        db_root = audeer.safe_path(db_root)
        if os.path.exists(db_root):
            break

    audeer.mkdir(db_root)
    return db_root


def database_tmp_folder(
        cache_root: str,
) -> str:
    r"""Create and return temporary database cache folder.

    The temporary cache folder is created under ``cache_root + '~'``.

    Args:
        cache_root: path to cache folder

    Returns:
        path to temporary cache folder

    """
    tmp_root = cache_root + '~'
    tmp_root = audeer.mkdir(tmp_root)
    return tmp_root


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
        verbose: bool = True,
        **kwargs,
) -> audformat.Database:
    r"""Load database.

    Loads meta and media files of a database to the local cache and returns
    a :class:`audformat.Database` object.

    When working with data,
    we often make assumptions about the media files.
    For instance, we expect that audio files are
    have a specific sampling rate.
    By setting
    ``bit_depth``, ``channels``, ``format``, ``mixdown``, and ``sampling_rate``
    we can request a specific flavor of the database.
    In that case media files are automatically converted to the desired
    properties (see also :class:`audb.Flavor`).

    It is possible to filter meta and media files with the arguments
    ``tables`` and ``media``.
    Note that only media files with at least one reference are loaded.
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
            and all mono files.
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
        verbose: show debug messages

    Returns:
        database object

    """
    if version is None:
        version = latest_version(name)
    deps = dependencies(name, version=version, cache_root=cache_root)

    # backward compatibility to audb<1.0.0
    channels, mixdown, media = parse_deprecated_load_arguments(
        channels,
        mixdown,
        media,
        deps,
        kwargs,
    )

    cached_versions = None

    flavor = Flavor(
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )
    db_root = database_cache_folder(name, version, cache_root, flavor)
    db_root_tmp = database_tmp_folder(db_root)

    if verbose:  # pragma: no cover
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

    # Start with database header without tables
    db, backend = load_header(
        db_root,
        name,
        version,
        flavor=flavor,
        add_audb_meta=True,
    )

    db_is_complete = _database_is_complete(db)

    # filter tables
    requested_tables = _tables(deps, tables)

    # load missing tables
    if not db_is_complete:
        _load_tables(
            requested_tables,
            backend,
            db_root,
            db_root_tmp,
            db,
            version,
            cached_versions,
            deps,
            flavor,
            cache_root,
            num_workers,
            verbose,
        )

    # filter tables
    if tables is not None:
        db.pick_tables(requested_tables)

    # load tables
    for table in requested_tables:
        db[table].load(os.path.join(db_root, f'db.{table}'))

    # filter media
    requested_media = _media(db, media)

    # load missing media
    if not db_is_complete and not only_metadata:
        _load_media(
            requested_media,
            backend,
            db_root,
            db_root_tmp,
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

    # fix media extension in tables
    if flavor.format is not None:
        _fix_media_ext(db.tables.values(), flavor.format, num_workers, verbose)

    # convert to full path
    if full_path:
        _full_path(db, db_root)

    # check if database is now complete
    if not db_is_complete:
        _database_check_complete(db, db_root, db_root_tmp, flavor, deps)

    if os.path.exists(db_root_tmp):
        shutil.rmtree(db_root_tmp)

    return db


def load_header(
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
            db_root_tmp = database_tmp_folder(db_root)
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
            _move_file(db_root_tmp, db_root, define.HEADER_FILE)
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
        verbose: bool = True,
) -> typing.List:
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
            and all mono files.
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        paths to media files

    Raises:
        ValueError: if a media file is requested
            that is not part of the database

    Example:
        >>> paths = load_media(
        ...     'emodb',
        ...     ['wav/03a01Fa.wav'],
        ...     version='1.1.1',
        ...     format='flac',
        ...     verbose=False,
        ... )
        >>> cache_root = audb.default_cache_root()
        >>> [p[len(cache_root):] for p in paths]
        ['/emodb/1.1.1/40bb2241/wav/03a01Fa.flac']

    """
    media = audeer.to_list(media)
    if len(media) == 0:
        return []

    if version is None:
        version = latest_version(name)
    deps = dependencies(name, version=version, cache_root=cache_root)

    available_files = deps.media
    for media_file in media:
        if media_file not in available_files:
            raise ValueError(
                f"Could not find '{media_file}' in {name} {version}"
            )

    cached_versions = None

    flavor = Flavor(
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
    )
    db_root = database_cache_folder(name, version, cache_root, flavor)
    db_root_tmp = database_tmp_folder(db_root)

    if verbose:  # pragma: no cover
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

    # Start with database header without tables
    db, backend = load_header(
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
            db_root_tmp,
            name,
            version,
            cached_versions,
            deps,
            flavor,
            cache_root,
            num_workers,
            verbose,
        )

    if format is not None:
        media = [audeer.replace_file_extension(m, format) for m in media]

    return [os.path.join(db_root, m) for m in media]


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
        ...     version='1.1.1',
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
    deps = dependencies(name, version=version, cache_root=cache_root)

    if table not in deps.table_ids:
        raise ValueError(
            f"Could not find table '{table}' in {name} {version}"
        )

    cached_versions = None

    db_root = database_cache_folder(name, version, cache_root)
    db_root_tmp = database_tmp_folder(db_root)

    if verbose:  # pragma: no cover
        print(f'Get:   {name} v{version}')
        print(f'Cache: {db_root}')

    # Start with database header without tables
    db, backend = load_header(
        db_root,
        name,
        version,
    )

    # Load table
    table_file = os.path.join(db_root, f'db.{table}')
    if not (
            os.path.exists(f'{table_file}.csv')
            or os.path.exists(f'{table_file}.pkl')
    ):
        _load_tables(
            [table],
            backend,
            db_root,
            db_root_tmp,
            db,
            version,
            cached_versions,
            deps,
            Flavor(),
            cache_root,
            num_workers,
            verbose,
        )
    table = audformat.Table()
    table.load(table_file)

    return table._df
