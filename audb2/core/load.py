import os
import re
import typing
import warnings

import audformat
import audeer

from audb2.core import define
from audb2.core import utils
from audb2.core.api import (
    default_backend,
    default_cache_root,
    repository_and_version,
)
from audb2.core.backend import Backend
from audb2.core.config import config
from audb2.core.depend import Depend
from audb2.core.flavor import Flavor


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
        depend: Depend,
):
    r"""Filter media files."""

    if flavor is not None:

        # keep only media files in matching archives
        if flavor.include is not None or flavor.exclude is not None:
            archives = set([depend.archive(f) for f in db.files])
            if flavor.include is not None:
                include = _filter_archives(flavor.include, archives)
                db.pick_files(lambda x: depend.archive(x) in include)
            if flavor.exclude is not None:
                exclude = _filter_archives(flavor.exclude, archives)
                db.pick_files(lambda x: depend.archive(x) not in exclude)

        # keep only media files with a sufficient number of channels
        if flavor.channels is not None:
            num_channels = max(flavor.channels) + 1
            db.pick_files(lambda x: depend.channels(x) >= num_channels)


def _filter_tables(
        db_header: audformat.Database,
        db_root: str,
        flavor: typing.Optional[Flavor],
        depend: Depend,
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
            db_header.save(db_root, header_only=True)
            for file in depend.tables:
                if not depend.archive(file) in tables:
                    depend.data.pop(file)


def _find_media(
        db: audformat.Database,
        db_root,
        depend: Depend,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Find altered and new media."""

    media = []

    def job(file: str):
        if not depend.removed(file):
            full_file = os.path.join(db_root, file)
            if os.path.exists(full_file):
                checksum = utils.md5(full_file)
                if checksum != depend.checksum(file):
                    media.append(file)
            else:
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
        depend: Depend,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> typing.List[str]:
    r"""Find altered and new tables."""

    tables = []

    def job(table: str):

        file = f'db.{table}.csv'
        full_file = os.path.join(db_root, file)
        if os.path.exists(full_file):
            checksum = utils.md5(full_file)
            if checksum != depend.checksum(file):
                tables.append(file)
        else:
            tables.append(file)

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in db_header.tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Find tables',
    )

    return tables


def _get_media(
        media: typing.List[str],
        db_root: str,
        flavor: typing.Optional[Flavor],
        depend: Depend,
        backend: Backend,
        repository: str,
        group_id: str,
        num_workers: typing.Optional[int],
        verbose: bool,
):
    r"""Get media."""

    # create folder tree to avoid race condition
    # in os.makedirs when files are unpacked
    for file in media:
        audeer.mkdir(os.path.dirname(os.path.join(db_root, file)))

    # figure out archives
    archives = set()
    for file in media:
        archives.add(
            (depend.archive(file), depend.version(file))
        )

    def job(archive: str, version: str):
        files = backend.get_archive(
            db_root, archive, version, repository,
            f'{group_id}.'
            f'{define.DEPEND_TYPE_NAMES[define.DependType.MEDIA]}',
        )
        if flavor is not None:
            for file in files:
                src_path = dst_path = os.path.join(db_root, file)
                if flavor.format is not None:
                    name, ext = os.path.splitext(src_path)
                    if ext[1:].lower() != flavor.format:
                        dst_path = name + '.' + flavor.format
                flavor(src_path, dst_path)
                if src_path != dst_path:
                    os.remove(src_path)

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
        depend: Depend,
        backend: Backend,
        repository: str,
        group_id: str,
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
        backend.get_archive(
            db_root, depend.archive(table), depend.version(table), repository,
            f'{group_id}.{define.DEPEND_TYPE_NAMES[define.DependType.META]}',
        )

    audeer.run_tasks(
        job,
        params=[([table], {}) for table in tables],
        num_workers=num_workers,
        progress_bar=verbose,
        task_description='Get tables',
    )


def _load(
        *,
        name: str,
        db_root: str,
        version: str,
        flavor: typing.Optional[Flavor],
        removed_media: bool,
        repository: str,
        group_id: str,
        backend: Backend,
        num_workers: typing.Optional[int],
        verbose: bool,
) -> audformat.Database:
    r"""Helper function for load()."""

    group_id: str = f'{group_id}.{name}'

    # load database header

    backend.get_file(
        db_root, define.DB_HEADER, version, repository, group_id,
    )
    db_header = audformat.Database.load(db_root, load_data=False)

    # get list with dependencies

    dep_path = backend.get_archive(
        db_root, audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )[0]
    dep_path = os.path.join(db_root, dep_path)
    depend = Depend()
    depend.from_file(dep_path)

    # get altered and new tables

    _filter_tables(db_header, db_root, flavor, depend)
    tables = _find_tables(
        db_header, db_root, depend, num_workers, verbose,
    )
    _get_tables(
        tables, db_root, depend, backend,
        repository, group_id, num_workers, verbose,
    )

    # load database and filter media

    db = audformat.Database.load(db_root)
    _filter_media(db, flavor, depend)

    # get altered and new media files,
    # eventually convert them

    if flavor is None or not flavor.only_metadata:
        media = _find_media(
            db, db_root, depend, num_workers, verbose,
        )
        _get_media(
            media, db_root, flavor, depend,
            backend, repository, group_id,
            num_workers, verbose,
        )

    # save dependencies

    depend.to_file(dep_path)

    # filter rows referencing removed media
    # eventually fix file extension

    if not removed_media:
        db.pick_files(lambda x: not depend.removed(x))

    if flavor is not None and flavor.format is not None:
        # Faster solution then using db.map_files()
        cur_ext = r'\.[a-zA-Z0-9]+$'  # match file extension
        new_ext = f'.{flavor.format}'
        for table in db.tables.values():
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

    # save database

    if flavor is not None:
        db.meta['audb'] = {
            'root': db_root,
            'version': version,
            'flavor': flavor.arguments,
        }

    db.save(db_root)
    db.save(db_root, storage_format=audformat.define.TableStorageFormat.PICKLE)

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
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = False,
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
            Note that media files with too few channels are not loaded.
            E.g. ``channels=[0, 1]`` will skip media files with only
            one channel and also remove their entries from the meta files.
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
        group_id: group ID
        backend: backend object
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        verbose: show debug messages

    Returns:
        database object

    """
    if 'mix' in kwargs:  # pragma: no cover
        warnings.warn(
            "Argument 'mix' is deprecated "
            "and will be removed with version '1.0.0'. "
            "Use 'channels' and 'mixdown' instead.",
            category=UserWarning,
            stacklevel=2,
        )
        mix = kwargs['mix']
        if mix == 'mono':
            mixdown = True
        elif channels is None and mix == 'stereo':
            channels = [0, 1]
        elif channels is None and mix == 'left':
            channels = 0
        elif channels is None and mix == 'right':
            channels = 1
        else:
            raise ValueError(
                f"Using deprecated argument 'mix' with value '{mix}' "
                "is no longer supported."
            )

    backend = default_backend(backend)
    repository, version = repository_and_version(
        name, version, group_id=group_id, backend=backend,
    )

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

    db = None
    cache_roots = [
        default_cache_root(True),  # check shared cache first
        default_cache_root(False),
    ] if cache_root is None else [cache_root]
    for cache_root in cache_roots:
        db_root = audeer.safe_path(
            os.path.join(
                cache_root, flavor.path(name, version, repository, group_id)
            )
        )
        if os.path.exists(db_root):
            db = audformat.Database.load(db_root)
            break

    if db is None:
        db = _load(
            name=name,
            db_root=db_root,
            version=version,
            flavor=flavor,
            removed_media=removed_media,
            repository=repository,
            group_id=group_id,
            backend=backend,
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
        group_id: str = config.GROUP_ID,
        backend: Backend = None,
        num_workers: typing.Optional[int] = 1,
        verbose: bool = False,
) -> audformat.Database:
    r"""Load database to directory.

    Loads the original state of the database
    to a custom directory.
    No conversion or filtering will be applied.

    Args:
        root: target directory
        name: name of database
        version: version string, latest if ``None``
        num_workers: number of parallel jobs or 1 for sequential
            processing. If ``None`` will be set to the number of
            processors on the machine multiplied by 5
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        database object

    """
    backend = default_backend(backend)
    repository, version = repository_and_version(
        name, version, group_id=group_id, backend=backend,
    )

    root = audeer.safe_path(root)
    return _load(
        name=name,
        db_root=root,
        version=version,
        flavor=None,
        removed_media=True,
        repository=repository,
        group_id=group_id,
        backend=backend,
        num_workers=num_workers,
        verbose=verbose
    )
