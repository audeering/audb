import os
import typing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

import audformat

from audb.core.api import dependencies
from audb.core.cache import database_cache_root
from audb.core.dependencies import error_message_missing_object
from audb.core.flavor import Flavor
from audb.core.load import _load_files
from audb.core.load import _misc_tables_used_in_table
from audb.core.load import latest_version
from audb.core.load import load_header_to
from audb.core.load import load_media
from audb.core.lock import FolderLock


class TableIterator:
    def __init__(
        self,
        db: audformat.Database,
        table: str,
        *,
        version: str,
        map: typing.Dict[str, typing.Union[str, typing.Sequence[str]]],
        batch_size: int,
        shuffle: bool,
        buffer_size: int,
        only_metadata: bool,
        bit_depth: int,
        channels: typing.Union[int, typing.Sequence[int]],
        format: str,
        mixdown: bool,
        sampling_rate: int,
        full_path: bool,
        cache_root: str,
        num_workers: typing.Optional[int],
        timeout: float,
        verbose: bool,
    ):
        self.db = db
        self.table = table
        self.version = version
        self.map = map
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.buffer_size = buffer_size
        self.only_metadata = only_metadata
        self.bit_depth = bit_depth
        self.channels = channels
        self.format = format
        self.mixdown = mixdown
        self.sampling_rate = sampling_rate
        self.full_path = full_path
        self.cache_root = cache_root
        self.num_workers = num_workers
        self.timeout = timeout
        self.verbose = verbose

        file = os.path.join(db.root, f"db.{table}.parquet")
        self._stream = parquet.ParquetFile(file).iter_batches(batch_size=batch_size)
        self._current = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.shuffle:
            # TODO: implement
            pass
        else:
            batch = next(iter(self._stream))
            df = batch.to_pandas(
                deduplicate_objects=False,
                types_mapper={
                    pa.string(): pd.StringDtype(),
                }.get,  # we have to provide a callable, not a dict
            )
            # Adjust dtypes and set index
            df = self.db[self.table]._pyarrow_convert_dtypes(df, convert_all=False)
            index_columns = list(self.db[self.table]._levels_and_dtypes.keys())
            df = self.db[self.table]._set_index(df, index_columns)

            if audformat.is_segmented_index(df.index):
                media = list(df.index.get_level_values("file"))
            elif audformat.is_filewise_index(df.index):
                media = list(df.index)
            else:
                media = []
            if not self.only_metadata and len(media) > 0:
                load_media(
                    self.db.name,
                    media,
                    version=self.version,
                    bit_depth=self.bit_depth,
                    channels=self.channels,
                    format=self.format,
                    mixdown=self.mixdown,
                    sampling_rate=self.sampling_rate,
                    cache_root=self.cache_root,
                    num_workers=self.num_workers,
                    timeout=self.timeout,
                    verbose=self.verbose,
                )
        if self.map is not None:
            self.db[self.table]._df = df
            df = self.db[self.table].get(map=map)

        return df


def stream(
    name: str,
    table: str,
    *,
    version: str = None,
    map: typing.Dict[str, typing.Union[str, typing.Sequence[str]]] = None,
    batch_size: int = 16,
    shuffle: bool = False,
    buffer_size: int = 10_000,
    only_metadata: bool = False,
    bit_depth: int = None,
    channels: typing.Union[int, typing.Sequence[int]] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    full_path: bool = True,
    cache_root: str = None,
    num_workers: typing.Optional[int] = 1,
    timeout: float = -1,
    verbose: bool = True,
) -> typing.Optional[audformat.Database]:
    r"""Stream table and media files of a database.

    Loads only the first ``batch_size`` rows
    of a table into memory,
    and downloads only the related media files,
    if any.

    By setting
    ``bit_depth``,
    ``channels``,
    ``format``,
    ``mixdown``,
    and ``sampling_rate``
    we can request a specific flavor of the database.
    In that case media files are automatically converted to the desired
    properties (see also :class:`audb.Flavor`).

    Args:
        name: name of database
        table: name of table
        version: version string, latest if ``None``
        batch_size: number of table rows
            to return in one iteration
        shuffle: if ``True``,
            it first reads ``buffer_size`` rows from the table
            and selects ``batch_size`` randomly from them
        buffer_size: number of table rows
            to be loaded
            when ``shuffle`` is ``True``
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
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
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

    db_root = database_cache_root(name, version, cache_root)

    deps = dependencies(
        name,
        version=version,
        cache_root=cache_root,
        verbose=verbose,
    )

    if table not in deps.table_ids:
        msg = error_message_missing_object("table", [table], name, version)
        raise ValueError(msg)

    with FolderLock(db_root):
        # Start with database header without tables
        db, backend_interface = load_header_to(db_root, name, version)

        # Misc tables required by schemes of requested table
        misc_tables = _misc_tables_used_in_table(db[table])

        # Load table files
        _load_files(
            misc_tables + [table],
            "table",
            backend_interface,
            db_root,
            db,
            version,
            None,
            deps,
            Flavor(),
            cache_root,
            False,  # pickle_tables
            num_workers,
            verbose,
        )

        # Load misc tables completely
        for misc_table in misc_tables:
            table_file = os.path.join(db_root, f"db.{misc_table}")
            db[misc_table].load(table_file)

    table_iterator = TableIterator(
        db,
        table,
        version=version,
        map=map,
        batch_size=batch_size,
        shuffle=shuffle,
        buffer_size=buffer_size,
        only_metadata=only_metadata,
        bit_depth=bit_depth,
        channels=channels,
        format=format,
        mixdown=mixdown,
        sampling_rate=sampling_rate,
        full_path=full_path,
        cache_root=cache_root,
        num_workers=num_workers,
        timeout=timeout,
        verbose=verbose,
    )

    return table_iterator
