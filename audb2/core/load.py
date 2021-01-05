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
        verbose: bool,
) -> audformat.Database:
    r"""Helper function for load()."""

    group_id: str = f'{group_id}.{name}'

    # load header and dependencies
    backend.get_file(
        db_root, define.DB_HEADER, version, repository, group_id,
    )
    db_header = audformat.Database.load(db_root, load_data=False)

    dep_path = backend.get_archive(
        db_root, audeer.basename_wo_ext(define.DB_DEPEND),
        version, repository, group_id,
    )[0]
    dep_path = os.path.join(db_root, dep_path)
    depend = Depend()
    depend.from_file(dep_path)

    # filter tables
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

    # figure out tables to download
    tables_to_download = []
    for table in db_header.tables:
        file = f'db.{table}.csv'
        full_file = os.path.join(db_root, file)
        if os.path.exists(full_file):
            checksum = utils.md5(full_file)
            if checksum != depend.checksum(file):
                tables_to_download.append(file)
        else:
            tables_to_download.append(file)

    # download tables
    for file in tables_to_download:
        # If pickled versions of the tables exist,
        # we have to remove them to make sure that
        # we load the new CSV tables.
        # This may happen if we upgrade an existing
        # version of the database to a different version.
        path_pkl = os.path.join(
            db_root, file
        )[:-3] + audformat.define.TableStorageFormat.PICKLE
        if os.path.exists(path_pkl):
            os.remove(path_pkl)
        backend.get_archive(
            db_root, depend.archive(file), depend.version(file), repository,
            f'{group_id}.{define.DEPEND_TYPE_NAMES[define.DependType.META]}',
        )

    db = audformat.Database.load(db_root)

    # filter media
    if flavor is not None:

        if flavor.include is not None or flavor.exclude is not None:
            archives = set()
            for file in db.files:
                archives.add(depend.archive(file))
            if flavor.include is not None:
                if isinstance(flavor.include, str):
                    pattern = re.compile(flavor.include)
                    include = []
                    for archive in archives:
                        if pattern.search(archive):
                            include.append(archive)
                else:
                    include = flavor.include
                db.pick_files(lambda x: depend.archive(x) in include)
            if flavor.exclude is not None:
                if isinstance(flavor.exclude, str):
                    pattern = re.compile(flavor.exclude)
                    exclude = []
                    for archive in archives:
                        if pattern.search(archive):
                            exclude.append(archive)
                else:
                    exclude = flavor.exclude
                db.pick_files(lambda x: depend.archive(x) not in exclude)

        if flavor.channels is not None:
            num_channels = max(flavor.channels) + 1
            db.pick_files(
                lambda x: depend.channels(x) >= num_channels,
            )

    if flavor is None or not flavor.only_metadata:

        # figure out media to download
        media_to_download = []
        for file in db.files:
            if not depend.removed(file):
                full_file = os.path.join(db_root, file)
                if os.path.exists(full_file):
                    checksum = utils.md5(full_file)
                    if checksum != depend.checksum(file):
                        media_to_download.append(file)
                else:
                    media_to_download.append(file)

        # figure out archives to download
        archives_to_download = set()
        for file in media_to_download:
            archives_to_download.add(
                (depend.archive(file), depend.version(file))
            )

        # download archives with media
        for archive, version in archives_to_download:
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

    depend.to_file(dep_path)

    # filter rows referencing removed media
    if not removed_media:
        db.pick_files(lambda x: not depend.removed(x))

    # fix file extension in tables
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

    # store root and flavor
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
        verbose: bool = False,
        **kwargs,
) -> audformat.Database:
    r"""Load database.

    Loads meta and media files of a database to the local cache and returns
    a :class:`audformat.Database` object.

    It is possible to filter meta and media files with the arguments
    ``tables``, ``include`` and ``exclude``.
    Note that only media files with at least one reference are loaded.
    I.e. filtering meta files, may also remove media files.
    Likewise, references to missing media files will be removed, too.
    I.e. filtering media files, may also remove entries from the meta files.
    Except if ``only_metadata`` is set to ``True``.
    In that case, no media files are loaded
    but all references in the meta files are kept.

    When working with data,
    we often make assumptions about the media files.
    For instance, we expect that audio files are
    have a specific sampling rate.
    By setting
    ``bit_depth``, ``channels``, ``format``, ``mixdown``, and ``sampling_rate``
    we can request a specific flavor of the database.
    In that case media files are automatically converted to the desired
    properties (see also :class:`audb2.Flavor`).

    .. note:: If ``only_metadata`` is set ``True``, the arguments
        ``bit_depth``, ``channels``, ``format``, ``mixdown``,
        and ``sampling_rate`` are ignored.

    .. note:: If ``channels`` are selected,
        media files with too few channels are not loaded.
        E.g. ``channels=[0, 1]`` will skip media files with only one channel
        and also remove their entries from the meta files.

    Args:
        name: name of database
        version: version string, latest if ``None``
        only_metadata: only metadata is stored
        bit_depth: bit depth, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
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

    backend = default_backend(backend, verbose=verbose)
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
                cache_root, repository, group_id.replace('.', os.path.sep),
                name, flavor.id, version,
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
        group_id: group ID
        backend: backend object
        verbose: show debug messages

    Returns:
        database object

    """
    backend = default_backend(backend, verbose=verbose)
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
        verbose=verbose
    )
