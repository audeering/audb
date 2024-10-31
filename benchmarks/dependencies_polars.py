from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
import errno
import os
import re
import tempfile

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet

import audbackend
import audeer

from audb.core import define


pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_formatting("NOTHING")
pl.Config.set_tbl_hide_column_data_types(True)


class Dependencies:
    r"""Dependencies of a database.

    :class:`audb.Dependencies` gathers
    all database media, table, and attachment files
    and metadata about them
    in a single object.
    The metadata contains information
    about the single files
    like duration,
    but also what version of the file is required.

    The dependencies of a database can be requested with
    :func:`audb.dependencies`.

    Examples:
        >>> deps = Dependencies()
        >>> deps()
        Empty DataFrame
        Columns: [archive, bit_depth, channels, checksum, duration, format, removed, sampling_rate, type, version]
        Index: []
        >>> # Request dependencies for emodb 1.4.1
        >>> deps = audb.dependencies("emodb", version="1.4.1")
        >>> # List all files or archives
        >>> deps.files[:3]
        ['db.emotion.csv', 'db.files.csv', 'wav/03a01Fa.wav']
        >>> deps.archives[:2]
        ['005d2b91-5317-0c80-d602-6d55f0323f8c', '014f82d8-3491-fd00-7397-c3b2ac3b2875']
        >>> # Access properties for a given file
        >>> deps.archive("wav/03a01Fa.wav")
        'c1f5cc6f-6d00-348a-ba3b-4adaa2436aad'
        >>> deps.duration("wav/03a01Fa.wav")
        1.89825
        >>> deps.removed("wav/03a01Fa.wav")
        False
        >>> # Check if a file is part of the dependencies
        >>> "wav/03a01Fa.wav" in deps
        True

    """  # noqa: E501

    def __init__(self):
        data = {}
        for name, dtype in zip(
            define.DEPEND_FIELD_NAMES.values(),
            define.DEPEND_FIELD_DTYPES.values(),
        ):
            # data[name] = pd.Series(dtype=dtype)
            data[name] = pl.Series(dtype=dtype)

        # self._df = pd.DataFrame(data)

        # always an empty frame.
        # deps can only be loaded or self._df set
        self._df = pl.DataFrame(data)
        self.index_col = define.DEPEND_INDEX_COLNAME
        # self._df.index = self._df.index.astype(define.DEPEND_INDEX_DTYPE)

        # pyarrow schema
        # used for reading and writing files
        # self._schema = pa.schema(
        #     [
        #         ("file", pa.string()),
        #         ("archive", pa.string()),
        #         ("bit_depth", pa.int32()),
        #         ("channels", pa.int32()),
        #         ("checksum", pa.string()),
        #         ("duration", pa.float64()),
        #         ("format", pa.string()),
        #         ("removed", pa.int32()),
        #         ("sampling_rate", pa.int32()),
        #         ("type", pa.int32()),
        #         ("version", pa.string()),
        #     ]
        # )

        # self._schema = utils.pascheme_to_plscheme(self._schema)
        # polars the df has a schema, it can be converted to dict
        # using dict(self._df.schema)
        self._schema = self._df.schema

    def __call__(self) -> pd.DataFrame:
        r"""Return dependencies as a table.

        Returns:
            table with dependencies

        """
        return self._df

    def __contains__(self, file: str) -> bool:
        r"""Check if file is part of dependencies.

        Args:
            file: relative file path

        Returns:
            ``True`` if a dependency to the file exists

        """
        return file in self._idx

    def __eq__(self, other: "Dependencies") -> bool:
        r"""Check if two dependency tables are equal.

        Args:
            other: dependency table to compare against

        Returns:
            ``True`` if both dependency tables have the same entries

        """
        return self._df.equals(other._df)

    def __getitem__(self, file: str) -> list:
        r"""File information.

        Args:
            file: relative file path

        Returns:
            list with meta information


        """
        # (self._df[self.index_col] == file).to_list()
        # return .loc[file].tolist()
        # item = pl.Series(self._df.filter(pl.col(self.index_col) == file)).to_list()
        # even slower:
        # item = np.array(self._df.filter(pl.col(self.index_col) == file)).tolist()[0]
        # pandas:
        # self._df.loc[file].tolist()
        item = list(self._df.row(self._idx[file]))[1:]
        return item

    def __len__(self) -> int:
        r"""Number of all media, table, attachment files."""
        return len(self._idx)

    def __str__(self) -> str:  # noqa: D105
        n = 15  # number of lines identical to pandas
        str_repr = str(self._df.head(n))
        return str_repr

    @property
    def archives(self) -> list[str]:
        r"""All media, table, attachment archives.

        Return:
            list of archives


        This is not benchmarked at the moment
        Current implementation is the fastest of

        self._df["archive"].sort(descending=False).unique(maintain_order=True).to_list()
        self._df["archive"].unique(maintain_order=False).sort(descending=False).to_list()
        0.144546
        sorted(self._df["archive"].unique().to_list())

        Sorting?

        Result:
        ['archive-0', 'archive-1', 'archive-2', 'archive-3', ...]
        """
        # 0.014
        # return (
        #     self._df["archive"]
        #     .unique(maintain_order=False)
        #     .sort(descending=False)
        #     .to_list()
        # )

        # 0.014
        # return (
        #     self._df.lazy()
        #     .select(pl.col("archive"))
        #     .unique()
        #     .sort(pl.col("archive"))
        #     .collect()["archive"]
        #     .to_list()
        # )

        # 0.014
        return pl.Series(self._df.select(pl.col("archive"))).unique().sort().to_list()

        # 0.413 - very slow to use list sorting
        # return sorted(
        #     pl.Series(self._df.select(pl.col("archive"))).unique().to_list()
        # )

    @property
    def attachments(self) -> list[str]:
        r"""Attachment paths (can be a file or a folder).

        Returns:
            list of attachments

        """
        deptype = define.DependType.ATTACHMENT
        return self._df.filter(pl.col("type") == deptype)[self.index_col].to_list()

    @property
    def attachment_ids(self) -> list[str]:
        r"""Attachment IDs.

        Returns:
            list of attachment IDs

        """
        return self._df.filter(pl.col("type") == define.DependType.ATTACHMENT)[
            "archive"
        ].to_list()

    @property
    def files(self) -> list[str]:
        r"""All media, table, attachments.

        Returns:
            list of files

        """
        return list(self._idx.keys())

    @property
    def media(self) -> list[str]:
        r"""Media files.

        Returns:
            list of media

        """
        return self._df.filter(pl.col("type") == define.DependType.MEDIA)[
            self.index_col
        ].to_list()

    @property
    def removed_media(self) -> list[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        return self._df.filter(
            (pl.col("type") == define.DependType.MEDIA) & (self._df["removed"] == 1)
        )[self.index_col].to_list()

    @property
    def table_ids(self) -> list[str]:
        r"""Table IDs.

        Like :meth:`audb.Dependencies.tables`,
        but only returns the table ID,
        i.e. ``db.<id>.csv``.

        Returns:
            list of table IDs

        """
        return [table[3:-4] for table in self.tables]

    @property
    def tables(self) -> list[str]:
        r"""Tables files.

        Returns:
            list of tables

        """
        return self._df.filter(pl.col("type") == define.DependType.META)[
            self.index_col
        ].to_list()

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        Example: 'archive-0'
        """
        # return self._df.filter(pl.col("file") == file).item(0, "archive")
        # pandas implementation
        # return self._df.archive[file]
        return self._df.row(self._idx[file], named=True)["archive"]

    def bit_depth(self, file: str) -> int:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
        # works for both pandas and polars
        return self._column_loc("bit_depth", file, int)

    def channels(self, file: str) -> int:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        return self._column_loc("channels", file, int)

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        return self._column_loc("checksum", file)

    def duration(self, file: str) -> float:
        r"""Duration of file.

        Args:
            file: relative file path

        Returns:
            duration in seconds

        """
        return self._column_loc("duration", file, float)

    def format(self, file: str) -> str:
        r"""Format of file.

        Args:
            file: relative file path

        Returns:
            file format (always lower case)

        """
        return self._column_loc("format", file)

    def _update_idx(self):
        """Update index as polars does not have any."""
        self._idx = dict(
            zip(self._df[self.index_col], range(len(self._df[self.index_col])))
        )

    def load(self, path: str):
        r"""Read dependencies from file.

        Clears existing dependencies.

        Args:
            path: path to file.
                File extension can be ``csv``
                ``pkl``,
                or ``parquet``

        Raises:
            ValueError: if file extension is not one of
                ``csv``, ``pkl``, ``parquet``
            FileNotFoundError: if ``path`` does not exists

        """
        path = audeer.path(path)
        extension = audeer.file_extension(path)
        if extension not in ["csv", "pkl", "parquet"]:
            raise ValueError(
                f"File extension of 'path' has to be 'csv', 'pkl', or 'parquet' "
                f"not '{extension}'"
            )
        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            )
        if extension == "pkl":
            self._df = pd.read_pickle(path)
            # Correct dtype of index
            # to make backward compatiple
            # with old pickle files in cache
            # that might use `string` as dtype
            if self._df.index.dtype != define.DEPEND_INDEX_DTYPE:
                self._df.index = self._df.index.astype(define.DEPEND_INDEX_DTYPE)

        elif extension == "csv":
            table = csv.read_csv(
                path,
                read_options=csv.ReadOptions(
                    column_names=self._schema.names,
                    skip_rows=1,
                ),
                convert_options=csv.ConvertOptions(column_types=self._schema),
            )
            self._df = self._table_to_dataframe(table)

        elif extension == "parquet":
            table = parquet.read_table(path)
            # path
            # self._df = self._table_to_dataframe(table)
            # self._df = pl.read_parquet(path)
            # there is no index!
            # self._df = pl.read_parquet(path).rename(
            #     {"__index_level_0__": self.index_col}
            # )
            # new polars version reads index (name file) directly into variable
            self._df = pl.read_parquet(path)
            # need to maintain an index
            self._update_idx()

    def removed(self, file: str) -> bool:
        r"""Check if file is marked as removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        return self._column_loc("removed", file, bool)

    def sampling_rate(self, file: str) -> int:
        r"""Sampling rate of media file.

        Args:
            file: relative file path

        Returns:
            sampling rate in Hz

        """
        return self._column_loc("sampling_rate", file, int)

    def save(self, path: str):
        r"""Write dependencies to file.

        Args:
            path: path to file.
                File extension can be ``csv``, ``pkl``, or ``parquet``

        """
        path = audeer.path(path)
        if path.endswith("csv"):
            table = self._dataframe_to_table(self._df)
            csv.write_csv(
                table,
                path,
                write_options=csv.WriteOptions(quoting_style="none"),
            )
        elif path.endswith("pkl"):
            self._df.to_pickle(
                path,
                protocol=4,  # supported by Python >= 3.4
            )
        elif path.endswith("parquet"):
            table = self._dataframe_to_table(self._df, file_column=True)
            parquet.write_table(table, path)

    def type(self, file: str) -> int:
        r"""Type of file.

        Args:
            file: relative file path

        Returns:
            type

        """
        return self._column_loc("type", file, int)

    def version(self, file: str) -> str:
        r"""Version of file.

        Args:
            file: relative file path

        Returns:
            version string

        """
        return self._column_loc("version", file)

    def _add_attachment(
        self,
        file: str,
        version: str,
        archive: str,
        checksum: str,
    ):
        r"""Add or update attachment.

        Args:
            file: relative path of attachment
            version: version string
            archive: archive name without extension
            checksum: checksum of file

        Note that this is considered unstable, see documentation
        we use  pl.DataFrame.update

        """
        format = audeer.file_extension(file).lower()
        entry = [
            file,
            archive,  # archive
            0,  # bit_depth
            0,  # channels
            checksum,  # checksum
            0.0,  # duration
            format,  # format
            0,  # removed
            0,  # sampling_rate
            define.DependType.ATTACHMENT,  # type
            version,  # version
        ]

        self._df = self._df.update(
            pl.from_records([entry], schema=self._df.schema, orient="row"),
            how="full",
            left_on=["file"],
            right_on=["file"],
        )

        # update index only if a new attachmenbt is coming in
        if file not in self._idx:
            self._idx[file] = len(self._idx)

    def _add_media(
        self,
        values: Sequence[
            tuple[
                str,  # file
                str,  # archive
                int,  # bit_depth
                int,  # channels
                str,  # checksum
                float,  # duration
                str,  # format
                int,  # removed
                float,  # sampling_rate
                int,  # type
                str,  # version
            ]
        ],
    ):
        r"""Add media files.

        Args:
            values: list of tuples,
                where each tuple holds the values of a new media entry

        """
        self._df = pl.concat(
            [self._df, pl.from_records(values, schema=self._df.schema, orient="row")],
            how="vertical_relaxed",
        )

    def _add_meta(
        self,
        file: str,
        version: str,
        archive: str,
        checksum: str,
    ):
        r"""Add or update table file.

        Args:
            file: relative file path
            archive: archive name without extension
            checksum: checksum of file
            version: version string

        """
        format = audeer.file_extension(file).lower()

        entry = [
            file,
            archive,  # archive
            0,  # bit_depth
            0,  # channels
            checksum,  # checksum
            0.0,  # duration
            format,  # format
            0,  # removed
            0,  # sampling_rate
            define.DependType.META,  # type
            version,  # version
        ]

        self._df = self._df.update(
            pl.from_records([entry], schema=self._df.schema, orient="row"),
            how="full",
            left_on=["file"],
            right_on=["file"],
        )

        # update index only if a new attachmenbt is coming in
        if file not in self._idx:
            self._idx[file] = len(self._idx)

    def _column_loc(
        self,
        column: str,
        file: str,
        dtype: Callable = None,
    ) -> object:
        r"""Column content for selected files and column."""
        value = self._df.row(self._idx[file], named=True)[column]
        if dtype is not None:
            value = dtype(value)
        return value

    def _dataframe_to_table(
        self,
        df: pd.DataFrame,
        *,
        file_column: bool = False,
    ) -> pa.Table:
        r"""Convert pandas dataframe to pyarrow table.

        Args:
            df: dependency table as pandas dataframe
            file_column: if ``False``
                the ``"file"`` column
                is renamed to ``""``

        Returns:
            dependency table as pyarrow table

        """
        table = pa.Table.from_pandas(
            df.reset_index().rename(columns={"index": "file"}),
            preserve_index=False,
            schema=self._schema,
        )
        if not file_column:
            columns = table.column_names
            columns = ["" if c == "file" else c for c in columns]
            table = table.rename_columns(columns)
        return table

    def _drop(self, files: Sequence[str]):
        r"""Drop files from table.

        Args:
            files: relative file paths

        """
        # self._df.drop is slow,
        # see https://stackoverflow.com/a/53394627.
        # The solution presented in https://stackoverflow.com/a/53395360
        # self._df = self._df.loc[self._df.index.drop(files)]
        # which is claimed to be faster,
        # isn't.
        # pandas:
        # self._df = self._df[~self._df.index.isin(files)]

        # polars
        self._df = self._df.filter(~self._df[self.index_col].is_in(files))

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._df.with_columns(
            pl.when(pl.col("file") == file)
            .then(1)
            .otherwise(pl.col("removed"))
            .alias("removed")
        )

    def _table_to_dataframe(self, table: pa.Table) -> pd.DataFrame:
        r"""Convert pyarrow table to pandas dataframe.

        Args:
            table: dependency table as pyarrow table

        Returns:
            dependency table as pandas dataframe

        """
        df = table.to_pandas(
            deduplicate_objects=False,
            # Convert to pyarrow dtypes,
            # but ensure we use pd.StringDtype("pyarrow")
            # and not pd.ArrowDtype(pa.string())
            # see https://pandas.pydata.org/docs/user_guide/pyarrow.html
            types_mapper={
                pa.string(): pd.StringDtype("pyarrow"),
                pa.int32(): pd.ArrowDtype(pa.int32()),
                pa.float64(): pd.ArrowDtype(pa.float64()),
            }.get,  # we have to provide a callable, not a dict
        )
        df.set_index("file", inplace=True)
        df.index.name = None
        df.index = df.index.astype(define.DEPEND_INDEX_DTYPE)
        return df

    def _update_media(
        self,
        values: Sequence[
            tuple[
                str,  # file
                str,  # archive
                int,  # bit_depth
                int,  # channels
                str,  # checksum
                float,  # duration
                str,  # format
                int,  # removed
                float,  # sampling_rate
                int,  # type
                str,  # version
            ]
        ],
    ):
        r"""Update media files.

        Args:
            values: list of tuples,
                where each tuple holds the new values for a media entry

        """
        # Slow implementations:
        #
        # newer concat
        # self._df = pl.concat(
        #     [self._df, pl.from_records(values, schema=self._df.schema)],
        #     how="vertical_relaxed",
        # )

        # predicate replacement example
        # self._df.with_columns(
        #     pl.when(pl.col("file") == file)
        #     .then(1)
        #     .otherwise(pl.col("removed")).alias("removed")
        # )

        self._df = self._df.update(
            pl.from_records(values, schema=self._df.schema, orient="row")
        )
        # no need to update self._idx as this is updating existing media

    def _update_media_version(
        self,
        files: Sequence[str],
        version: str,
    ):
        r"""Update version of media files.

        Args:
            files: relative file paths
            version: version string

        """
        # NOTE: pandas way of counting index
        field = define.DEPEND_FIELD_NAMES[define.DependField.VERSION]
        # using new data allocation!
        self._df = pl.concat(
            [
                self._df.filter(~pl.col(self.index_col).is_in(files)),
                self._df.filter(pl.col(self.index_col).is_in(files)).with_columns(
                    pl.col(field).str.replace(".*", version)
                ),
            ],
            how="vertical",
        )
        # no need to update self._idx as only updating


def error_message_missing_object(
    object_type: str,
    missing_object_id: str | Sequence,
    database_name: str = None,
    database_version: str = None,
) -> str:
    r"""Error message for missing objects.

    Args:
        object_type: object that is supposed to contain ``missing_object_id``,
            should be
            ``'media'``,
            ``'table'``
            or ``'attachment'``
        missing_object_id: ID of missing object
        database_name: name of affected database
        database_version: name of affected database

    Returns:
        error message

    """
    if object_type == "media":
        object_name = f"{object_type} file"
    else:
        object_name = object_type

    if isinstance(missing_object_id, str):
        msg = f"Could not find a {object_name} " f"matching '{missing_object_id}'"
    else:
        msg = f"Could not find the {object_name} " f"'{missing_object_id[0]}'"
    if database_name is not None and database_version is not None:
        msg += f" in {database_name} v{database_version}"
    return msg


def filter_deps(
    requested_deps: str | Sequence[str] | None,
    available_deps: Sequence[str],
    deps_type: str,
    database_name: str = None,
    database_version: str = None,
) -> Sequence[str]:
    r"""Filter dependency files by requested files.

    Args:
        requested_deps: include only media, tables
            matching the regular expression
            or provided in the list
        available_deps: sequence of available media files or tables
        deps_type: ``'attachment'``, ``'media'`` or ``'table'``
        database_name: name of affected database
        database_version: name of affected database

    Returns:
        list of attachments, media or tables
            inside the dependency object
            matching ``requested_deps``

    """
    if requested_deps is None:
        return available_deps
    elif len(requested_deps) == 0:
        return []

    if isinstance(requested_deps, str):
        request = requested_deps
        pattern = re.compile(request)
        requested_deps = []
        for dep in available_deps:
            if pattern.search(dep):
                requested_deps.append(dep)
        if len(requested_deps) == 0:
            msg = error_message_missing_object(
                deps_type,
                request,
                database_name,
                database_version,
            )
            raise ValueError(msg)
    else:
        for dep in requested_deps:
            if dep not in available_deps:
                msg = error_message_missing_object(
                    deps_type,
                    [dep],
                    database_name,
                    database_version,
                )
                raise ValueError(msg)

    return requested_deps


def download_dependencies(
    backend_interface: type[audbackend.interface.Base],
    name: str,
    version: str,
    verbose: bool,
) -> Dependencies:
    r"""Load dependency file from backend.

    Download dependency file
    for requested database
    to a temporary folder,
    and return an dependency object
    loaded from that file.

    Args:
        backend_interface: backend interface
        name: database name
        version: database version
        verbose: if ``True`` a message is shown during download

    Returns:
        dependency object

    """
    with tempfile.TemporaryDirectory() as tmp_root:
        # Load `db.parquet` file,
        # or if non-existent `db.zip`
        # from backend
        remote_deps_file = backend_interface.join("/", name, define.DEPENDENCIES_FILE)
        if backend_interface.exists(remote_deps_file, version):
            local_deps_file = os.path.join(tmp_root, define.DEPENDENCIES_FILE)
            backend_interface.get_file(
                remote_deps_file,
                local_deps_file,
                version,
                verbose=verbose,
            )
        else:
            remote_deps_file = backend_interface.join("/", name, define.DB + ".zip")
            local_deps_file = os.path.join(
                tmp_root,
                define.LEGACY_DEPENDENCIES_FILE,
            )
            backend_interface.get_archive(
                remote_deps_file,
                tmp_root,
                version,
                verbose=verbose,
            )
        # Create deps object from downloaded file
        deps = Dependencies()
        deps.load(local_deps_file)
    return deps


def upload_dependencies(
    backend_interface: type[audbackend.interface.Base],
    deps: Dependencies,
    db_root: str,
    name: str,
    version: str,
):
    r"""Upload dependency file to backend.

    Store a dependency file
    in the local database root folder,
    and upload it to the backend.

    Args:
        backend_interface: backend interface
        deps: dependency object
        db_root: database root folder
        name: database name
        version: database version

    """
    local_deps_file = os.path.join(db_root, define.DEPENDENCIES_FILE)
    remote_deps_file = backend_interface.join("/", name, define.DEPENDENCIES_FILE)
    deps.save(local_deps_file)
    backend_interface.put_file(local_deps_file, remote_deps_file, version)
