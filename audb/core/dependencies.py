from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
import errno
import os
import re
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import pyarrow.parquet as parquet

import audbackend
import audeer

from audb.core import define


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
        >>> deps = audb.Dependencies()
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
        # pyarrow schema
        # used for reading and writing files
        self._schema = pa.schema(
            [
                ("file", pa.string()),
                ("archive", pa.string()),
                ("bit_depth", pa.int32()),
                ("channels", pa.int32()),
                ("checksum", pa.string()),
                ("duration", pa.float64()),
                ("format", pa.string()),
                ("removed", pa.int32()),
                ("sampling_rate", pa.int32()),
                ("type", pa.int32()),
                ("version", pa.string()),
            ]
        )
        # Internal Arrow table (immutable)
        self._table = pa.table(
            {
                "file": pa.array([], type=pa.string()),
                "archive": pa.array([], type=pa.string()),
                "bit_depth": pa.array([], type=pa.int32()),
                "channels": pa.array([], type=pa.int32()),
                "checksum": pa.array([], type=pa.string()),
                "duration": pa.array([], type=pa.float64()),
                "format": pa.array([], type=pa.string()),
                "removed": pa.array([], type=pa.int32()),
                "sampling_rate": pa.array([], type=pa.int32()),
                "type": pa.array([], type=pa.int32()),
                "version": pa.array([], type=pa.string()),
            },
            schema=self._schema,
        )
        # File path to row index mapping for O(1) lookups
        self._file_index: dict[str, int] = {}
        # Cached DataFrame for __call__()
        self._df_cache: pd.DataFrame | None = None
        # Track if table was modified to invalidate cache
        self._table_modified: bool = True

    @property
    def _df(self) -> pd.DataFrame:
        r"""Backward compatibility property for accessing the DataFrame.

        Returns the cached DataFrame representation of the Arrow table.
        This property is provided for backward compatibility with code
        that directly accesses ``_df``.

        Returns:
            DataFrame with dependencies

        """
        return self.__call__()

    @_df.setter
    def _df(self, df: pd.DataFrame):
        r"""Backward compatibility setter for the DataFrame.

        Converts the DataFrame to an Arrow table and updates internal state.
        This setter is provided for backward compatibility with code
        that directly sets ``_df`` (e.g., test fixtures).

        Args:
            df: DataFrame to set

        """
        self._table = self._dataframe_to_table(df, file_column=True)
        self._rebuild_index()
        self._invalidate_df_cache()

    def __call__(self) -> pd.DataFrame:
        r"""Return dependencies as a table.

        Returns:
            table with dependencies

        """
        if self._df_cache is None or self._table_modified:
            self._df_cache = self._table_to_dataframe(self._table)
            self._table_modified = False
        return self._df_cache

    def __contains__(self, file: str) -> bool:
        r"""Check if file is part of dependencies.

        Args:
            file: relative file path

        Returns:
            ``True`` if a dependency to the file exists

        """
        return file in self._file_index

    def __eq__(self, other: "Dependencies") -> bool:
        r"""Check if two dependency tables are equal.

        Args:
            other: dependency table to compare against

        Returns:
            ``True`` if both dependency tables have the same entries

        """
        # Compare using DataFrames for compatibility with code
        # that modifies _df directly
        return self.__call__().equals(other.__call__())

    def __getitem__(self, file: str) -> list:
        r"""File information.

        Args:
            file: relative file path

        Returns:
            list with meta information

        """
        if file not in self._file_index:
            raise KeyError(file)
        row_idx = self._file_index[file]
        # Use Arrow's take() for O(1) row access, skip first column (file)
        row = self._table.take([row_idx])
        return [row.column(i)[0].as_py() for i in range(1, row.num_columns)]

    def __len__(self) -> int:
        r"""Number of all media, table, attachment files."""
        return len(self._table)

    def __str__(self) -> str:  # noqa: D105
        return str(self.__call__())

    @property
    def archives(self) -> list[str]:
        r"""All media, table, attachment archives.

        Return:
            list of archives

        """
        unique_archives = pc.unique(self._table.column("archive"))
        return sorted(unique_archives.to_pylist())

    @property
    def attachments(self) -> list[str]:
        r"""Attachment paths (can be a file or a folder).

        Returns:
            list of attachments

        """
        type_col = self._table.column("type")
        mask = pc.equal(type_col, define.DEPENDENCY_TYPE["attachment"])
        filtered_table = self._table.filter(mask)
        return filtered_table.column("file").to_pylist()

    @property
    def attachment_ids(self) -> list[str]:
        r"""Attachment IDs.

        Returns:
            list of attachment IDs

        """
        type_col = self._table.column("type")
        mask = pc.equal(type_col, define.DEPENDENCY_TYPE["attachment"])
        filtered_table = self._table.filter(mask)
        return filtered_table.column("archive").to_pylist()

    @property
    def files(self) -> list[str]:
        r"""All media, table, attachments.

        Returns:
            list of files

        """
        return self._table.column("file").to_pylist()

    @property
    def media(self) -> list[str]:
        r"""Media files.

        Returns:
            list of media

        """
        type_col = self._table.column("type")
        mask = pc.equal(type_col, define.DEPENDENCY_TYPE["media"])
        filtered_table = self._table.filter(mask)
        return filtered_table.column("file").to_pylist()

    @property
    def removed_media(self) -> list[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        type_col = self._table.column("type")
        removed_col = self._table.column("removed")
        mask = pc.and_(
            pc.equal(type_col, define.DEPENDENCY_TYPE["media"]),
            pc.equal(removed_col, 1),
        )
        filtered_table = self._table.filter(mask)
        return filtered_table.column("file").to_pylist()

    @property
    def table_ids(self) -> list[str]:
        r"""Table IDs.

        Like :meth:`audb.Dependencies.tables`,
        but only returns the table ID,
        i.e. ``db.<id>.csv``.

        Returns:
            list of table IDs

        """
        return [os.path.splitext(table[3:])[0] for table in self.tables]

    @property
    def tables(self) -> list[str]:
        r"""Tables files.

        Returns:
            list of tables

        """
        type_col = self._table.column("type")
        mask = pc.equal(type_col, define.DEPENDENCY_TYPE["meta"])
        filtered_table = self._table.filter(mask)
        return filtered_table.column("file").to_pylist()

    def get_files_with_sampling_rate(self) -> list[str]:
        r"""Get files that have a non-zero sampling rate.

        This is a helper method for filtering files that need
        flavor conversion based on sampling rate.

        Returns:
            list of file paths with non-zero sampling rate

        """
        sampling_rate_col = self._table.column("sampling_rate")
        mask = pc.not_equal(sampling_rate_col, 0)
        filtered_table = self._table.filter(mask)
        return filtered_table.column("file").to_pylist()

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        return self._column_loc("archive", file)

    def bit_depth(self, file: str) -> int:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
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

    def load(self, path: str):
        r"""Read dependencies from file.

        Clears existing dependencies.

        Supports auto-detection of file format.
        If no extension is provided or the file doesn't exist,
        tries loading in order: Arrow IPC (``.arrow``),
        Parquet (``.parquet``), CSV (``.csv``).

        Args:
            path: path to file.
                File extension can be ``arrow``, ``parquet``, or ``csv``.
                If the path doesn't exist, will attempt auto-detection
                by trying different extensions in order

        Raises:
            ValueError: if file extension is not one of
                ``arrow``, ``parquet``, ``csv``
            FileNotFoundError: if ``path`` does not exist
                and auto-detection fails

        """
        path = audeer.path(path)

        # Check extension validity
        extension = audeer.file_extension(path)
        # If extension is provided and invalid, raise error
        if extension and extension not in ["arrow", "parquet", "csv"]:
            raise ValueError(
                f"File extension of 'path' has to be "
                f"'arrow', 'parquet', or 'csv', "
                f"not '{extension}'"
            )

        # Auto-detection: try to find the file with different extensions
        # if file doesn't exist or extension is empty
        if not os.path.exists(path) or not extension:
            base_path = os.path.splitext(path)[0]
            for ext in [".arrow", ".parquet", ".csv"]:
                candidate_path = base_path + ext
                if os.path.exists(candidate_path):
                    path = candidate_path
                    extension = audeer.file_extension(path)
                    break
            else:
                # No file found with any extension
                raise FileNotFoundError(
                    errno.ENOENT,
                    os.strerror(errno.ENOENT),
                    path,
                )

        if extension == "arrow":
            with ipc.open_file(path) as reader:
                table = reader.read_all()
            self._table = table

        elif extension == "parquet":
            table = parquet.read_table(path)
            self._table = table

        elif extension == "csv":
            table = csv.read_csv(
                path,
                read_options=csv.ReadOptions(
                    column_names=self._schema.names,
                    skip_rows=1,
                ),
                convert_options=csv.ConvertOptions(column_types=self._schema),
            )
            self._table = table

        # Rebuild index and invalidate cache
        self._rebuild_index()
        self._invalidate_df_cache()

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
                File extension can be ``arrow``, ``parquet``, or ``csv``

        Raises:
            ValueError: if file extension is not one of
                ``arrow``, ``parquet``, ``csv``

        """
        path = audeer.path(path)
        extension = audeer.file_extension(path)

        # Convert from DataFrame to capture any in-place modifications
        # made via _df property
        table = self._dataframe_to_table(self.__call__(), file_column=True)

        if extension == "arrow":
            # Write as Arrow IPC format with LZ4 compression
            with ipc.RecordBatchFileWriter(
                path,
                schema=self._schema,
                options=ipc.IpcWriteOptions(compression="lz4"),
            ) as writer:
                writer.write_table(table)
        elif extension == "parquet":
            parquet.write_table(table, path)
        elif extension == "csv":
            # For CSV, rename file column to empty string for compatibility
            columns = table.column_names
            columns = ["" if c == "file" else c for c in columns]
            table = table.rename_columns(columns)
            csv.write_csv(
                table,
                path,
                write_options=csv.WriteOptions(quoting_style="none"),
            )
        else:
            raise ValueError(
                f"File extension of 'path' has to be "
                f"'arrow', 'parquet', or 'csv', "
                f"not '{extension}'"
            )

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

        """
        format = audeer.file_extension(file).lower()

        # If file exists, update it in place to preserve row order
        if file in self._file_index:
            row_idx = self._file_index[file]
            # Update each column value at the specific row index
            for col_name, value in [
                ("archive", archive),
                ("bit_depth", 0),
                ("channels", 0),
                ("checksum", checksum),
                ("duration", 0.0),
                ("format", format),
                ("removed", 0),
                ("sampling_rate", 0),
                ("type", define.DEPENDENCY_TYPE["attachment"]),
                ("version", version),
            ]:
                col = self._table.column(col_name)
                col_list = col.to_pylist()
                col_list[row_idx] = value
                col_type = self._schema.field(col_name).type
                new_col = pa.array(col_list, type=col_type)
                col_idx = self._schema.get_field_index(col_name)
                self._table = self._table.set_column(col_idx, col_name, new_col)
            self._invalidate_df_cache()
        else:
            # Build single-row table for new file
            new_row = pa.table(
                {
                    "file": [file],
                    "archive": [archive],
                    "bit_depth": [0],
                    "channels": [0],
                    "checksum": [checksum],
                    "duration": [0.0],
                    "format": [format],
                    "removed": [0],
                    "sampling_rate": [0],
                    "type": [define.DEPENDENCY_TYPE["attachment"]],
                    "version": [version],
                },
                schema=self._schema,
            )
            # Append new row
            self._table = pa.concat_tables([self._table, new_row])
            self._rebuild_index()
            self._invalidate_df_cache()

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
        # Build table directly from tuples (transpose to get column arrays)
        if not values:
            return

        arrays = list(zip(*values))
        new_rows = pa.table(
            {
                "file": arrays[0],
                "archive": arrays[1],
                "bit_depth": arrays[2],
                "channels": arrays[3],
                "checksum": arrays[4],
                "duration": arrays[5],
                "format": arrays[6],
                "removed": arrays[7],
                "sampling_rate": arrays[8],
                "type": arrays[9],
                "version": arrays[10],
            },
            schema=self._schema,
        )

        # Concatenate with existing table
        self._table = pa.concat_tables([self._table, new_rows])
        self._rebuild_index()
        self._invalidate_df_cache()

    def _add_meta(
        self,
        file: str,
        version: str,
        checksum: str,
    ):
        r"""Add or update table file.

        Args:
            file: relative file path
            checksum: checksum of file
            version: version string

        """
        format = audeer.file_extension(file).lower()
        if format == "parquet":
            archive = ""
        else:
            archive = os.path.splitext(file[3:])[0]

        # If file exists, update it in place to preserve row order
        if file in self._file_index:
            row_idx = self._file_index[file]
            # Update each column value at the specific row index
            for col_name, value in [
                ("archive", archive),
                ("bit_depth", 0),
                ("channels", 0),
                ("checksum", checksum),
                ("duration", 0.0),
                ("format", format),
                ("removed", 0),
                ("sampling_rate", 0),
                ("type", define.DEPENDENCY_TYPE["meta"]),
                ("version", version),
            ]:
                col = self._table.column(col_name)
                col_list = col.to_pylist()
                col_list[row_idx] = value
                col_type = self._schema.field(col_name).type
                new_col = pa.array(col_list, type=col_type)
                col_idx = self._schema.get_field_index(col_name)
                self._table = self._table.set_column(col_idx, col_name, new_col)
            self._invalidate_df_cache()
        else:
            # Build single-row table for new file
            new_row = pa.table(
                {
                    "file": [file],
                    "archive": [archive],
                    "bit_depth": [0],
                    "channels": [0],
                    "checksum": [checksum],
                    "duration": [0.0],
                    "format": [format],
                    "removed": [0],
                    "sampling_rate": [0],
                    "type": [define.DEPENDENCY_TYPE["meta"]],
                    "version": [version],
                },
                schema=self._schema,
            )
            # Append new row
            self._table = pa.concat_tables([self._table, new_row])
            self._rebuild_index()
            self._invalidate_df_cache()

    def _column_loc(
        self,
        column: str,
        file: str,
        dtype: Callable = None,
    ) -> object:
        r"""Column content for selected file.

        Args:
            column: one of the names in ``Dependencies._schema``
            file: row to query, index is a filename
            dtype: convert data to dtype

        Returns:
            scalar value

        """
        if file not in self._file_index:
            raise KeyError(file)
        row_idx = self._file_index[file]
        value = self._table.column(column)[row_idx].as_py()
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
        # Use Arrow compute filter with inverted is_in mask
        file_col = self._table.column("file")
        mask = pc.invert(pc.is_in(file_col, pa.array(files, type=pa.string())))
        self._table = self._table.filter(mask)
        self._rebuild_index()
        self._invalidate_df_cache()

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        if file not in self._file_index:
            raise KeyError(file)

        # Get row index and update removed column
        row_idx = self._file_index[file]
        removed_col = self._table.column("removed")
        removed_list = removed_col.to_pylist()
        removed_list[row_idx] = 1

        # Replace column
        new_removed_col = pa.array(removed_list, type=pa.int32())
        col_idx = self._schema.get_field_index("removed")
        self._table = self._table.set_column(col_idx, "removed", new_removed_col)
        self._invalidate_df_cache()

    @staticmethod
    def _set_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        r"""Set dependency table dtypes.

        Args:
            df: dataframe representing dependency table

        Returns:
            dataframe representing dependency table
            with correct dtypes

        """
        df.index = df.index.astype(define.DEPENDENCY_INDEX_DTYPE, copy=False)
        df = df.astype(define.DEPENDENCY_TABLE, copy=False)
        return df

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
        df.index = df.index.astype(define.DEPENDENCY_INDEX_DTYPE)
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
        if not values:
            return

        # Build table from tuples
        arrays = list(zip(*values))
        files_to_update = list(arrays[0])

        # Check that all files exist (to match pandas behavior)
        for file in files_to_update:
            if file not in self._file_index:
                raise KeyError(file)

        # Filter out rows that need updating
        file_col = self._table.column("file")
        mask = pc.invert(
            pc.is_in(file_col, pa.array(files_to_update, type=pa.string()))
        )
        filtered_table = self._table.filter(mask)

        # Build new rows table
        new_rows = pa.table(
            {
                "file": arrays[0],
                "archive": arrays[1],
                "bit_depth": arrays[2],
                "channels": arrays[3],
                "checksum": arrays[4],
                "duration": arrays[5],
                "format": arrays[6],
                "removed": arrays[7],
                "sampling_rate": arrays[8],
                "type": arrays[9],
                "version": arrays[10],
            },
            schema=self._schema,
        )

        # Concatenate filtered table with new rows
        self._table = pa.concat_tables([filtered_table, new_rows])
        self._rebuild_index()
        self._invalidate_df_cache()

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
        # Get row indices for files
        row_indices = [self._file_index[f] for f in files if f in self._file_index]

        if not row_indices:
            return

        # Update version column
        version_col = self._table.column("version")
        version_list = version_col.to_pylist()
        for idx in row_indices:
            version_list[idx] = version

        # Replace column
        new_version_col = pa.array(version_list, type=pa.string())
        col_idx = self._schema.get_field_index("version")
        self._table = self._table.set_column(col_idx, "version", new_version_col)
        self._invalidate_df_cache()

    def _rebuild_index(self):
        r"""Rebuild file path to row index mapping.

        Creates a dictionary mapping each file path
        to its row index in the Arrow table
        for O(1) file lookups.
        Called after any operation that changes row order.

        """
        file_col = self._table.column("file")
        self._file_index = {file: i for i, file in enumerate(file_col.to_pylist())}

    def _invalidate_df_cache(self):
        r"""Invalidate the cached DataFrame.

        Marks the DataFrame cache as stale
        so it will be regenerated on next __call__().
        Called after any mutation operation.

        """
        self._table_modified = True


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
        msg = f"Could not find a {object_name} matching '{missing_object_id}'"
    else:
        msg = f"Could not find the {object_name} '{missing_object_id[0]}'"
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

    Tries formats in order:
    Arrow IPC (``.arrow``),
    Parquet (``.parquet``),
    CSV (``.csv`` in ``.zip``).

    Args:
        backend_interface: backend interface
        name: database name
        version: database version
        verbose: if ``True`` a message is shown during download

    Returns:
        dependency object

    """
    with tempfile.TemporaryDirectory() as tmp_root:
        # Try loading dependency file in order of preference:
        # 1. db.arrow (Arrow IPC format, newest)
        # 2. db.parquet (Parquet format, introduced in v1.7.0)
        # 3. db.zip containing db.csv (legacy format, pre-v1.7.0)

        local_deps_file = None

        # Try Arrow IPC first
        remote_deps_file = backend_interface.join("/", name, define.DEPENDENCY_FILE)
        if backend_interface.exists(remote_deps_file, version):
            local_deps_file = os.path.join(tmp_root, define.DEPENDENCY_FILE)
            backend_interface.get_file(
                remote_deps_file,
                local_deps_file,
                version,
                verbose=verbose,
            )
        else:
            # Fall back to Parquet
            remote_deps_file = backend_interface.join(
                "/", name, define.PARQUET_DEPENDENCY_FILE
            )
            if backend_interface.exists(remote_deps_file, version):
                local_deps_file = os.path.join(tmp_root, define.PARQUET_DEPENDENCY_FILE)
                backend_interface.get_file(
                    remote_deps_file,
                    local_deps_file,
                    version,
                    verbose=verbose,
                )
            else:
                # Fall back to legacy CSV in ZIP
                remote_deps_file = backend_interface.join("/", name, define.DB + ".zip")
                local_deps_file = os.path.join(
                    tmp_root,
                    define.LEGACY_DEPENDENCY_FILE,
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
    local_deps_file = os.path.join(db_root, define.DEPENDENCY_FILE)
    remote_deps_file = backend_interface.join("/", name, define.DEPENDENCY_FILE)
    deps.save(local_deps_file)
    backend_interface.put_file(local_deps_file, remote_deps_file, version)
