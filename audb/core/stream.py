from __future__ import annotations

import abc
from collections.abc import Iterable
from collections.abc import Sequence
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

import audformat

from audb.core import define
from audb.core.api import dependencies
from audb.core.cache import database_cache_root
from audb.core.dependencies import error_message_missing_object
from audb.core.flavor import Flavor
from audb.core.load import _load_files
from audb.core.load import _misc_tables_used_in_table
from audb.core.load import _update_path
from audb.core.load import latest_version
from audb.core.load import load_header_to
from audb.core.load import load_media
from audb.core.lock import FolderLock


class DatabaseIterator(audformat.Database, metaclass=abc.ABCMeta):
    r"""Database iterator.

    This class cannot be created directly,
    but only by calling :func:`audb.stream`.

    Examples:
        Create :class:`audb.DatabaseIterator` object.

        >>> db = audb.stream(
        ...     "emodb",
        ...     "files",
        ...     version="1.4.1",
        ...     batch_size=4,
        ...     only_metadata=True,
        ...     full_path=False,
        ...     verbose=False,
        ... )

        The :class:`audb.DatabaseIterator` object
        is restricted to the requested table,
        and all related schemes
        and misc tables
        used as labels in a related scheme.

        >>> db
        name: emodb
        ...
        schemes:
          age: {description: Age of speaker, dtype: int, minimum: 0}
          duration: {dtype: time}
          gender:
            description: Gender of speaker
            dtype: str
            labels: [female, male]
          language: {description: Language of speaker, dtype: str}
          speaker: {description: The actors could produce each sentence as often as they liked
              and were asked to remember a real situation from their past when they had felt
              this emotion., dtype: int, labels: speaker}
          transcription:
            description: Sentence produced by actor.
            dtype: str
            labels: ...
        tables:
          files:
            type: filewise
            columns:
              duration: {scheme_id: duration}
              speaker: {scheme_id: speaker}
              transcription: {scheme_id: transcription}
        misc_tables:
          speaker:
            levels: {speaker: int}
            columns:
              age: {scheme_id: age}
              gender: {scheme_id: gender}
              language: {scheme_id: language}
        ...

        Request the first batch of data.

        >>> next(db)
                                         duration  speaker transcription
        file
        wav/03a01Fa.wav    0 days 00:00:01.898250        3           a01
        wav/03a01Nc.wav    0 days 00:00:01.611250        3           a01
        wav/03a01Wa.wav 0 days 00:00:01.877812500        3           a01
        wav/03a02Fc.wav    0 days 00:00:02.006250        3           a02

        During the iteration,
        the :class:`audb.DatabaseIterator` object
        provides access to the current batch of data.

        >>> db["files"].get(map={"speaker": "age"})
                                         duration transcription  age
        file
        wav/03a01Fa.wav    0 days 00:00:01.898250           a01   31
        wav/03a01Nc.wav    0 days 00:00:01.611250           a01   31
        wav/03a01Wa.wav 0 days 00:00:01.877812500           a01   31
        wav/03a02Fc.wav    0 days 00:00:02.006250           a02   31

    """  # noqa: E501

    def __init__(
        self,
        db: audformat.Database,
        table: str,
        *,
        version: str,
        map: dict[str, str | Sequence[str]],
        batch_size: int,
        shuffle: bool,
        buffer_size: int,
        only_metadata: bool,
        bit_depth: int,
        channels: int | Sequence[int],
        format: str,
        mixdown: bool,
        sampling_rate: int,
        full_path: bool,
        cache_root: str,
        num_workers: int | None,
        timeout: float,
        verbose: bool,
    ):
        self._cleanup_database(db, table)

        # Transfer attributes of database object
        for attr in db.__dict__.keys():
            setattr(self, attr, getattr(db, attr))

        self._table = table
        self._version = version
        self._map = map
        self._batch_size = batch_size
        self._shuffle = shuffle
        self._buffer_size = buffer_size
        self._only_metadata = only_metadata
        self._bit_depth = bit_depth
        self._channels = channels
        self._format = format
        self._mixdown = mixdown
        self._sampling_rate = sampling_rate
        self._full_path = full_path
        self._cache_root = cache_root
        self._num_workers = num_workers
        self._timeout = timeout
        self._verbose = verbose

        if shuffle:
            self._samples = buffer_size
        else:
            self._samples = batch_size
        self._buffer = pd.DataFrame()
        self._stream = self._initialize_stream()

    def __iter__(self) -> DatabaseIterator:
        r"""Iterator generator."""
        return self

    def __next__(self) -> pd.DataFrame:
        r"""Iterate database."""
        # Load part of table
        df = self._get_batch()
        self[self._table]._df = df

        # Load corresponding media files
        self._load_media(df)

        # Map column values
        if self._map is not None:
            df = self[self._table].get(map=self._map)

        # Adjust full paths and file extensions in table
        _update_path(
            self,
            self.root,
            self._full_path,
            self._format,
            self._num_workers,
            self._verbose,
        )

        return df

    @abc.abstractmethod
    def _initialize_stream(self) -> Iterable:
        r"""Create table iterator object.

        This method needs to be implemented
        for the table file types
        in the classes,
        that inherit from :class:`audb.DatabaseIterator`.

        Returns:
            table iterator

        """
        return  # pragma: nocover

    @staticmethod
    def _cleanup_database(db: audformat.Database, table: str):
        r"""Remove parts of database, not used by table.

        Args:
            db: database object
            table: table ID

        """
        tables = [table]
        misc_tables = _misc_tables_used_in_table(db[table])

        # Remove non-requested table
        db.drop_tables([table for table in list(db.tables) if table not in tables])

        # Remove unused schemes
        used_schemes = []
        for table in misc_tables + tables:
            for column_id, column in db[table].columns.items():
                if column.scheme_id is not None:
                    used_schemes.append(column.scheme_id)
        for scheme in list(db.schemes):
            if scheme not in used_schemes:
                del db.schemes[scheme]

        # Remove misc tables not required by the schemes of table
        db.drop_tables(
            [
                misc_table
                for misc_table in list(db.misc_tables)
                if misc_table not in misc_tables + tables
            ]
        )

        # Remove unused splits
        used_splits = [
            db[table].split_id for table in list(db) if db[table].split_id is not None
        ]
        for split in list(db.splits):
            if split not in used_splits:
                del db.splits[split]

    def _get_batch(self) -> pd.DataFrame:
        r"""Read table batch.

        Returns:
            dataframe

        """
        if self._shuffle:
            buffer_read_length = self._batch_size
            df1 = pd.DataFrame()
            if len(self._buffer) < self._batch_size:
                if len(self._buffer) > 0:
                    # Empty current buffer,
                    # before refilling
                    df1 = self._buffer
                    buffer_read_length = self._batch_size - len(self._buffer)
                self._buffer = self._read_dataframe()
                # Shuffle data
                self._buffer = self._buffer.sample(frac=1)

            df2 = self._buffer.iloc[:buffer_read_length, :]
            self._buffer.drop(index=df2.index, inplace=True)
            df = pd.concat([df1, df2])

        else:
            df = self._read_dataframe()

        if len(df) == 0:
            raise StopIteration

        return df

    def _load_media(self, df: pd.DataFrame):
        r"""Load media file for batch.

        Args:
            df: dataframe of batch

        """
        if audformat.is_segmented_index(df.index):
            media = list(df.index.get_level_values("file"))
        elif audformat.is_filewise_index(df.index):
            media = list(df.index)
        else:
            media = []
        if not self._only_metadata and len(media) > 0:
            load_media(
                self.name,
                media,
                version=self._version,
                bit_depth=self._bit_depth,
                channels=self._channels,
                format=self._format,
                mixdown=self._mixdown,
                sampling_rate=self._sampling_rate,
                cache_root=self._cache_root,
                num_workers=self._num_workers,
                timeout=self._timeout,
                verbose=self._verbose,
            )

    def _postprocess_batch(self, batch: object) -> pd.DataFrame:
        r"""Post-process batch data to desired dataframe.

        Args:
            batch: input data

        Returns:
            dataframe

        """
        return batch  # pragma: nocover

    def _postprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        r"""Post-process dataframe to have correct index and data types.

        Args:
            df: dataframe

        Returns:
            dataframe

        """
        # Adjust dtypes and set index
        df = self[self._table]._pyarrow_convert_dtypes(df, convert_all=False)
        index_columns = list(self[self._table]._levels_and_dtypes.keys())
        df = self[self._table]._set_index(df, index_columns)
        return df

    def _read_dataframe(self) -> pd.DataFrame:
        r"""Read dataframe from table.

        Returns:
            dataframe

        """
        try:
            df = self._postprocess_dataframe(
                self._postprocess_batch(next(iter(self._stream)))
            )
        except StopIteration:
            # Ensure return an empty dataframe,
            # at the last iteration,
            # when no remaining data is left
            df = pd.DataFrame()
        return df


class DatabaseIteratorCsv(DatabaseIterator):
    def _initialize_stream(self):
        # Prepare settings for csv file reading

        # index
        columns_and_dtypes = self[self._table]._levels_and_dtypes.copy()
        # add columns
        for column_id, column in self[self._table].columns.items():
            if column.scheme_id is not None:
                columns_and_dtypes[column_id] = self.schemes[column.scheme_id].dtype
            else:
                columns_and_dtypes[column_id] = audformat.define.DataType.OBJECT

        # Replace data type with converter for dates or timestamps
        converters = {}
        dtypes_wo_converters = {}
        for column, dtype in columns_and_dtypes.items():
            if dtype == audformat.define.DataType.DATE:
                converters[column] = lambda x: pd.to_datetime(x)
            elif dtype == audformat.define.DataType.TIME:
                converters[column] = lambda x: pd.to_timedelta(x)
            else:
                dtypes_wo_converters[column] = audformat.core.common.to_pandas_dtype(
                    dtype
                )

        if self._samples == 0:
            # `pandas.read_csv()` does not support a `chunksize=0`
            return []
        else:
            file = os.path.join(self.root, f"db.{self._table}.csv")
            return pd.read_csv(
                file,
                chunksize=self._samples,
                usecols=list(columns_and_dtypes.keys()),
                dtype=dtypes_wo_converters,
                converters=converters,
                float_precision="round_trip",
            )


class DatabaseIteratorParquet(DatabaseIterator):
    def _initialize_stream(self) -> pa.RecordBatch:
        file = os.path.join(self.root, f"db.{self._table}.parquet")
        return parquet.ParquetFile(file).iter_batches(batch_size=self._samples)

    def _postprocess_batch(self, batch: pa.RecordBatch) -> pd.DataFrame:
        df = batch.to_pandas(
            deduplicate_objects=False,
            types_mapper={
                pa.string(): pd.StringDtype(),
            }.get,  # we have to provide a callable, not a dict
        )
        return df


def stream(
    name: str,
    table: str,
    *,
    version: str = None,
    map: dict[str, str | Sequence[str]] = None,
    batch_size: int = 16,
    shuffle: bool = False,
    buffer_size: int = 100_000,
    only_metadata: bool = False,
    bit_depth: int = None,
    channels: int | Sequence[int] = None,
    format: str = None,
    mixdown: bool = False,
    sampling_rate: int = None,
    full_path: bool = True,
    cache_root: str = None,
    num_workers: int | None = 1,
    timeout: float = define.TIMEOUT,
    verbose: bool = True,
) -> DatabaseIterator:
    r"""Stream table and media files of a database.

    Loads only the first ``batch_size`` rows
    of a table into memory,
    and downloads only the related media files,
    if any media files are requested.

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
        map: map scheme or scheme fields to column values.
            For example if your table holds a column ``speaker`` with
            speaker IDs, which is assigned to a scheme that contains a
            dict mapping speaker IDs to age and gender entries,
            ``map={'speaker': ['age', 'gender']}``
            will replace the column with two new columns that map ID
            values to age and gender, respectively.
            To also keep the original column with speaker IDS, you can do
            ``map={'speaker': ['speaker', 'age', 'gender']}``
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
            ``8000``, ``16000``, ``22050``, ``24000``, ``44100``, ``48000``
        full_path: replace relative with absolute file paths
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
        ValueError: if table is requested
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
        >>> import numpy as np
        >>> np.random.seed(1)
        >>> db = audb.stream(
        ...     "emodb",
        ...     "files",
        ...     version="1.4.1",
        ...     batch_size=4,
        ...     shuffle=True,
        ...     only_metadata=True,
        ...     full_path=False,
        ...     verbose=False,
        ... )
        >>> next(db)
                                         duration  speaker transcription
        file
        wav/14a05Fb.wav 0 days 00:00:03.128687500       14           a05
        wav/15a05Eb.wav 0 days 00:00:03.993562500       15           a05
        wav/12a05Nd.wav    0 days 00:00:03.185875       12           a05
        wav/13a07Na.wav 0 days 00:00:01.911687500       13           a07

    """
    if version is None:
        version = latest_version(name)

    # Extract kwargs
    # to pass on to the DatabaseIterator constructor
    kwargs = {k: v for (k, v) in locals().items() if k not in ["name", "table"]}

    flavor = Flavor(
        bit_depth=bit_depth,
        channels=channels,
        format=format,
        mixdown=mixdown,
        sampling_rate=sampling_rate,
    )
    db_root = database_cache_root(name, version, cache_root, flavor)

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
        db, backend_interface = load_header_to(
            db_root,
            name,
            version,
            flavor=flavor,
            add_audb_meta=True,
        )

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

    if os.path.exists(os.path.join(db_root, f"db.{table}.parquet")):
        return DatabaseIteratorParquet(db, table, **kwargs)
    else:
        return DatabaseIteratorCsv(db, table, **kwargs)
