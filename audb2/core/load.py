import glob
import os
import re
import typing
import warnings

import audbackend
import audeer
import audformat

from audb2.core import define
from audb2.core.api import (
    default_cache_root,
    latest_version,
)
from audb2.core.dependencies import Dependencies
from audb2.core.flavor import Flavor
from audb2.core.utils import (
    lookup_backend,
    mix_mapping,
)


def _filter_archives(
        match: typing.Union[str, typing.Sequence[str]],
        archives: typing.Set[str],
):
    r"""Filter archives by regular expression."""
    if isinstance(match, str):
        pattern = re.compile(match)
        match = [a for a in archives if pattern.search(a)]
    return match


def _filter_media(
        db: audformat.Database,
        flavor: Flavor,
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Filter media files."""

    if flavor is not None:

        # keep only media files in matching archives
        if flavor.include is not None or flavor.exclude is not None:
            archives = set([deps.archive(f) for f in db.files])
            if flavor.include is not None:
                include = _filter_archives(flavor.include, archives)
                db.pick_files(
                    lambda x: deps.archive(x) in include,
                    num_workers=num_workers,
                    verbose=verbose,
                )
            if flavor.exclude is not None:
                exclude = _filter_archives(flavor.exclude, archives)
                db.pick_files(
                    lambda x: deps.archive(x) not in exclude,
                    num_workers=num_workers,
                    verbose=verbose,
                )


def _filter_tables(
        db_header: audformat.Database,
        db_root: str,
        db_root_tmp: str,
        flavor: typing.Optional[Flavor],
        deps: Dependencies,
):
    r"""Filter tables."""

    if flavor is not None:
        if flavor.tables is not None:
            if isinstance(flavor.tables, str):
                pattern = re.compile(flavor.tables)
                tables = []
                for table in db_header.tables:
                    if pattern.search(table):
                        tables.append(table)
            else:
                tables = flavor.tables
            db_header.pick_tables(tables)
            db_header.save(db_root_tmp, header_only=True)
            _move_file(db_root_tmp, db_root, define.HEADER_FILE)
            for file in deps.tables:
                if not deps.archive(file) in tables:
                    deps.data.pop(file)


def _find_media(
        db: audformat.Database,
        db_root: str,
        flavor: typing.Optional[Flavor],
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Find altered and new media."""

    media = []

    def job(file: str):
        if not deps.is_removed(file):
            full_file = os.path.join(db_root, file)
            if flavor is not None:
                full_file = flavor.destination(full_file)
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
    r"""Find altered and new tables."""

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


def _fix_file_ext(
        db: audformat.Database,
        flavor: typing.Optional[Flavor],
        num_workers: typing.Optional[int],
        verbose: bool,
):
    if flavor is not None and flavor.format is not None:

        def job(table):
            if table.is_filewise:
                table.df.index = table.df.index.str.replace(
                    cur_ext, new_ext,
                )
            else:
                table.df.index.set_levels(
                    table.df.index.levels[0].str.replace(
                        cur_ext, new_ext),
                    'file',
                    inplace=True,
                )

        # Faster solution then using db.map_files()
        cur_ext = r'\.[a-zA-Z0-9]+$'  # match file extension
        new_ext = f'.{flavor.format}'
        audeer.run_tasks(
            job,
            params=[([table], {}) for table in db.tables.values()],
            num_workers=num_workers,
            progress_bar=verbose,
            task_description='Fix file extension',
        )


def _get_media(
        media: typing.List[str],
        db_root: str,
        db_root_tmp: str,
        db_name: str,
        flavor: typing.Optional[Flavor],
        deps: Dependencies,
        backend: audbackend.Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Get media."""

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
            if flavor is not None and deps.format(file) in define.FORMATS:
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
    r"""Get tables."""

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
        version: str,
        db_root: str,
        db_root_tmp: str,
        flavor: Flavor,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Save database."""
    if flavor is not None:
        db.meta['audb'] = {
            'root': db_root,
            'version': version,
            'flavor': flavor.arguments,
        }

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


def _load(
        *,
        name: str,
        db_root: str,
        db_root_tmp: str,
        version: str,
        flavor: typing.Optional[Flavor],
        backend: audbackend.Backend,
        deps: Dependencies,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> audformat.Database:
    r"""Helper function for load()."""

    audeer.mkdir(db_root)
    audeer.mkdir(db_root_tmp)

    # load database header

    remote_header = backend.join(name, define.HEADER_FILE)
    local_header = os.path.join(db_root_tmp, define.HEADER_FILE)
    backend.get_file(remote_header, local_header, version)
    _move_file(db_root_tmp, db_root, define.HEADER_FILE)
    db_header = audformat.Database.load(db_root, load_data=False)

    # get altered and new tables

    _filter_tables(db_header, db_root, db_root_tmp, flavor, deps)
    tables = _find_tables(
        db_header, db_root, deps, num_workers, verbose,
    )
    _get_tables(
        tables, db_root, db_root_tmp, name, deps,
        backend, num_workers, verbose,
    )

    # load database and filter media

    db = audformat.Database.load(
        db_root, num_workers=num_workers, verbose=verbose,
    )
    _filter_media(db, flavor, deps, num_workers, verbose)

    # get altered and new media files,
    # eventually convert them

    if flavor is None or not flavor.only_metadata:
        media = _find_media(
            db, db_root, flavor, deps, num_workers, verbose,
        )
        _get_media(
            media, db_root, db_root_tmp, name,
            flavor, deps, backend,
            num_workers, verbose,
        )

    # save dependencies

    dep_path_tmp = os.path.join(db_root_tmp, define.DEPENDENCIES_FILE)
    deps.save(dep_path_tmp)
    _move_file(db_root_tmp, db_root, define.DEPENDENCIES_FILE)
    _fix_file_ext(db, flavor, num_workers, verbose)

    # save database and remove the temporal directory
    # to signal all files were correctly loaded
    _save_database(
        db, version, db_root, db_root_tmp, flavor, num_workers, verbose,
    )
    try:
        _remove_empty_dirs(db_root_tmp)
    except OSError:  # pragma: no cover
        raise RuntimeError(
            'Could not remove temporary directory, '
            'probably there are some leftover files.'
            'This should not happen.'
        )

    return db


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
        include: typing.Union[str, typing.Sequence[str]] = None,
        exclude: typing.Union[str, typing.Sequence[str]] = None,
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
    properties (see also :class:`audb2.Flavor`).

    It is possible to filter meta and media files with the arguments
    ``tables``, ``include`` and ``exclude``.
    Note that only media files with at least one reference are loaded.
    I.e. filtering meta files, may also remove media files.
    Likewise, references to missing media files will be removed, too.
    I.e. filtering media files, may also remove entries from the meta files.
    Except if ``only_metadata`` is set to ``True``.
    In that case, no media files are loaded
    but all references in the meta files are kept
    and the arguments ``bit_depth``, ``channels``, ``format``,
    ``mixdown``, and ``sampling_rate`` are ignored.

    Args:
        name: name of database
        version: version string, latest if ``None``
        only_metadata: only metadata is stored
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
        include: include only media from archives matching the regular
            expression or provided in the list
        exclude: don't include media from archives matching the regular
            expression or provided in the list. This filter is applied
            after ``include``
        removed_media: keep rows that reference removed media
        full_path: replace relative with absolute file paths
        cache_root: cache folder where databases are stored.
            If not set :meth:`audb2.default_cache_root` is used
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        database object

    """
    if (
            channels is None
            and not mixdown
            and 'mix' in kwargs
    ):  # pragma: no cover
        mix = kwargs['mix']
        channels, mixdown = mix_mapping(mix)

    if version is None:
        version = latest_version(name)
    backend = lookup_backend(name, version)

    flavor = Flavor(
        only_metadata=only_metadata,
        channels=channels,
        format=format,
        mixdown=mixdown,
        bit_depth=bit_depth,
        sampling_rate=sampling_rate,
        tables=tables,
        include=include,
        exclude=exclude,
    )

    if verbose:  # pragma: no cover
        print(f'Get:  {name} v{version}')

    # check if database is already in cache
    #
    # db_root -> final destination of database
    # db_root_tmp -> temporal directory used when loading the database
    #
    # db_root does not exist -> start loading database
    # db_root and db_root_tmp already exist -> continue loading database
    # only db_root exists -> load database from cache
    #
    db = None
    cache_roots = [
        default_cache_root(True),  # check shared cache first
        default_cache_root(False),
    ] if cache_root is None else [cache_root]
    for cache_root in cache_roots:
        db_root = audeer.safe_path(
            os.path.join(
                cache_root,
                flavor.path(name, version),
            )
        )
        db_root_tmp = db_root + '~'
        if os.path.exists(db_root) and not os.path.exists(db_root_tmp):
            db = audformat.Database.load(db_root, num_workers=num_workers)
            if verbose:  # pragma: no cover
                print(f'From: {db_root}')
            break

    # Get list with dependencies
    deps = Dependencies()
    if db is None:
        archive = backend.join(name, define.DB)
        backend.get_archive(archive, db_root_tmp, version)
        deps_path = os.path.join(db_root_tmp, define.DEPENDENCIES_FILE)
    else:
        deps_path = os.path.join(db_root, define.DEPENDENCIES_FILE)
    deps.load(deps_path)

    if db is None:
        if verbose:   # pragma: no cover
            print(f'To: {db_root}')
        db = _load(
            name=name,
            db_root=db_root,
            db_root_tmp=db_root_tmp,
            version=version,
            flavor=flavor,
            backend=backend,
            deps=deps,
            num_workers=num_workers,
            verbose=verbose,
        )

    # Remove rows referencing removed media
    if not removed_media:
        removed_files = []
        for file in deps.removed_media:
            if flavor.format is not None:
                # Rename removed media file to requested format
                name, _ = os.path.splitext(file)
                file = f'{name}.{flavor.format}'
            removed_files.append(file)

        if removed_files:
            db.drop_files(
                removed_files,
                num_workers=num_workers,
                verbose=verbose,
            )

    if full_path:
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

    return db


def load_original_to(
        root: str,
        name: str,
        *,
        version: str = None,
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
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        database object

    """
    if version is None:
        version = latest_version(name)
    backend = lookup_backend(name, version)

    db_root = audeer.safe_path(root)
    db_root_tmp = db_root + '~'

    # remove files with a wrong checksum
    # to ensure we load correct version
    update = os.path.exists(db_root) and os.listdir(db_root)
    audeer.mkdir(db_root)
    archive = backend.join(name, define.DB)
    backend.get_archive(archive, db_root, version)
    deps_path = os.path.join(db_root, define.DEPENDENCIES_FILE)
    deps = Dependencies()
    deps.load(deps_path)
    if update:
        for file in deps.files:
            full_file = os.path.join(db_root, file)
            if os.path.exists(full_file):
                checksum = audbackend.md5(full_file)
                if checksum != deps.checksum(file):
                    os.remove(full_file)

    db = _load(
        name=name,
        db_root=db_root,
        db_root_tmp=db_root_tmp,
        version=version,
        flavor=None,
        backend=backend,
        deps=deps,
        num_workers=num_workers,
        verbose=verbose
    )

    return db
