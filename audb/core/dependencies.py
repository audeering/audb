from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
import errno
import os
import re
import tempfile

from lance.file import LanceFileReader
from lance.file import LanceFileWriter
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
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
        ['db.emotion.categories.test.gold_standard.csv', 'db.emotion.categories.train.gold_standard.csv', 'db.emotion.csv']
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
        # Store dependencies as an in-memory PyArrow table
        # Initialize with empty table
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

        # Create empty table with the schema
        self._table = pa.table(
            {field.name: pa.array([], type=field.type) for field in self._schema},
            schema=self._schema,
        )

        # Create indices for fast lookups
        # We'll use dictionaries to cache file->row_index mappings
        self._file_index = {}

    def __call__(self) -> pd.DataFrame:
        r"""Return dependencies as a table.

        Returns:
            table with dependencies

        """
        df = self._table_to_dataframe(self._table)
        return df

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
        # Compare by converting to DataFrames
        return self().equals(other())

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
        row = self._table.slice(row_idx, 1)

        # Return all columns except 'file' as a list
        return [
            row["archive"][0].as_py(),
            row["bit_depth"][0].as_py(),
            row["channels"][0].as_py(),
            row["checksum"][0].as_py(),
            row["duration"][0].as_py(),
            row["format"][0].as_py(),
            row["removed"][0].as_py(),
            row["sampling_rate"][0].as_py(),
            row["type"][0].as_py(),
            row["version"][0].as_py(),
        ]

    def __len__(self) -> int:
        r"""Number of all media, table, attachment files."""
        return len(self._table)

    def __str__(self) -> str:  # noqa: D105
        return str(self())

    def __getstate__(self):
        """Make object serializable."""
        # Get all data as a DataFrame for serialization
        df = self()
        # Return the DataFrame data for reconstruction
        return {
            "data": df.to_dict("records"),
            "index": df.index.tolist(),
        }

    def __setstate__(self, state):
        """Restore object from serialized state."""
        # Recreate the schema
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

        # Restore the data from serialized state
        if state["data"]:
            data = state["data"]
            index = state["index"]

            # Build lists for each column
            files = index
            archives = [row["archive"] for row in data]
            bit_depths = [row["bit_depth"] for row in data]
            channels = [row["channels"] for row in data]
            checksums = [row["checksum"] for row in data]
            durations = [row["duration"] for row in data]
            formats = [row["format"] for row in data]
            removed = [row["removed"] for row in data]
            sampling_rates = [row["sampling_rate"] for row in data]
            types = [row["type"] for row in data]
            versions = [row["version"] for row in data]

            # Create PyArrow table
            self._table = pa.table(
                {
                    "file": pa.array(files, type=pa.string()),
                    "archive": pa.array(archives, type=pa.string()),
                    "bit_depth": pa.array(bit_depths, type=pa.int32()),
                    "channels": pa.array(channels, type=pa.int32()),
                    "checksum": pa.array(checksums, type=pa.string()),
                    "duration": pa.array(durations, type=pa.float64()),
                    "format": pa.array(formats, type=pa.string()),
                    "removed": pa.array(removed, type=pa.int32()),
                    "sampling_rate": pa.array(sampling_rates, type=pa.int32()),
                    "type": pa.array(types, type=pa.int32()),
                    "version": pa.array(versions, type=pa.string()),
                },
                schema=self._schema,
            )

            # Rebuild file index
            self._file_index = {file: idx for idx, file in enumerate(files)}
        else:
            # Create empty table
            self._table = pa.table(
                {field.name: pa.array([], type=field.type) for field in self._schema},
                schema=self._schema,
            )
            self._file_index = {}

    @property
    def archives(self) -> list[str]:
        r"""All media, table, attachment archives.

        Return:
            list of archives

        """
        if len(self._table) == 0:
            return []
        archives = pc.unique(self._table["archive"])
        return sorted([a.as_py() for a in archives])

    @property
    def attachments(self) -> list[str]:
        r"""Attachment paths (can be a file or a folder).

        Returns:
            list of attachments

        """
        mask = pc.equal(self._table["type"], define.DEPENDENCY_TYPE["attachment"])
        filtered = self._table.filter(mask)
        return [f.as_py() for f in filtered["file"]]

    @property
    def attachment_ids(self) -> list[str]:
        r"""Attachment IDs.

        Returns:
            list of attachment IDs

        """
        mask = pc.equal(self._table["type"], define.DEPENDENCY_TYPE["attachment"])
        filtered = self._table.filter(mask)
        return [a.as_py() for a in filtered["archive"]]

    @property
    def files(self) -> list[str]:
        r"""All media, table, attachments.

        Returns:
            list of files

        """
        return [f.as_py() for f in self._table["file"]]

    @property
    def media(self) -> list[str]:
        r"""Media files.

        Returns:
            list of media

        """
        mask = pc.equal(self._table["type"], define.DEPENDENCY_TYPE["media"])
        filtered = self._table.filter(mask)
        return [f.as_py() for f in filtered["file"]]

    @property
    def removed_media(self) -> list[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        type_mask = pc.equal(self._table["type"], define.DEPENDENCY_TYPE["media"])
        removed_mask = pc.equal(self._table["removed"], 1)
        combined_mask = pc.and_(type_mask, removed_mask)
        filtered = self._table.filter(combined_mask)
        return [f.as_py() for f in filtered["file"]]

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
        mask = pc.equal(self._table["type"], define.DEPENDENCY_TYPE["meta"])
        filtered = self._table.filter(mask)
        return [f.as_py() for f in filtered["file"]]

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

        Args:
            path: path to file.
                File extension can be ``csv``,
                ``parquet``, or ``lance``

        Raises:
            ValueError: if file extension is not one of
                ``csv``, ``parquet``, ``lance``
            FileNotFoundError: if ``path`` does not exists

        """
        path = audeer.path(path)
        extension = audeer.file_extension(path)
        if extension not in ["csv", "parquet", "lance"]:
            raise ValueError(
                f"File extension of 'path' has to be 'csv', 'parquet', or 'lance' "
                f"not '{extension}'"
            )
        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            )

        # Load data based on format
        if extension == "lance":
            # Read from Lance file
            reader = LanceFileReader(path)
            results = reader.read_all()
            # Convert ReaderResults to PyArrow table
            table = results.to_table()
            # Note: LanceFileReader doesn't need explicit close

        elif extension == "csv":
            # Read from CSV file
            # The CSV writer creates a duplicate header (known issue), so skip the first data row
            # and use the column names from the header
            table = csv.read_csv(
                path,
                read_options=csv.ReadOptions(
                    skip_rows=1,  # Skip the duplicate header row
                    autogenerate_column_names=False,
                ),
            )
            # Rename the empty column to "file"
            columns = table.column_names
            columns = ["file" if c == "" else c for c in columns]
            table = table.rename_columns(columns)
            # Ensure correct schema types
            table = table.cast(self._schema)

        elif extension == "parquet":
            # Read from Parquet file
            table = parquet.read_table(path)

        # Set the table and rebuild index
        self._table = table
        self._rebuild_index()

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
                File extension can be ``csv``, ``parquet``, or ``lance``

        """
        path = audeer.path(path)
        if path.endswith("csv"):
            # Write to CSV
            df = self()
            table = self._dataframe_to_table(df)
            csv.write_csv(
                table,
                path,
                write_options=csv.WriteOptions(quoting_style="none"),
            )
        elif path.endswith("parquet"):
            # Write to Parquet
            df = self()
            table = self._dataframe_to_table(df, file_column=True)
            parquet.write_table(table, path)
        elif path.endswith("lance"):
            # Write to Lance file
            # Remove existing file if it exists
            if os.path.exists(path):
                os.remove(path)

            # Create a new Lance file
            with LanceFileWriter(path) as writer:
                writer.write_batch(self._table)

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

        # Create a new row
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

        # Remove existing entry if present
        if file in self._file_index:
            self._drop([file])

        # Append new row
        self._table = pa.concat_tables([self._table, new_row])
        self._file_index[file] = len(self._table) - 1

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
        if not values:
            return

        # Unpack values into column lists
        files = [v[0] for v in values]
        archives = [v[1] for v in values]
        bit_depths = [v[2] for v in values]
        channels = [v[3] for v in values]
        checksums = [v[4] for v in values]
        durations = [v[5] for v in values]
        formats = [v[6] for v in values]
        removed = [v[7] for v in values]
        sampling_rates = [v[8] for v in values]
        types = [v[9] for v in values]
        versions = [v[10] for v in values]

        # Create new table
        new_rows = pa.table(
            {
                "file": pa.array(files, type=pa.string()),
                "archive": pa.array(archives, type=pa.string()),
                "bit_depth": pa.array(bit_depths, type=pa.int32()),
                "channels": pa.array(channels, type=pa.int32()),
                "checksum": pa.array(checksums, type=pa.string()),
                "duration": pa.array(durations, type=pa.float64()),
                "format": pa.array(formats, type=pa.string()),
                "removed": pa.array(removed, type=pa.int32()),
                "sampling_rate": pa.array(sampling_rates, type=pa.int32()),
                "type": pa.array(types, type=pa.int32()),
                "version": pa.array(versions, type=pa.string()),
            },
            schema=self._schema,
        )

        # Append new rows
        self._table = pa.concat_tables([self._table, new_rows])

        # Update index
        start_idx = len(self._table) - len(values)
        for i, file in enumerate(files):
            self._file_index[file] = start_idx + i

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

        # Create a new row
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

        # Remove existing entry if present
        if file in self._file_index:
            self._drop([file])

        # Append new row
        self._table = pa.concat_tables([self._table, new_row])
        self._file_index[file] = len(self._table) - 1

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
        value = self._table[column][row_idx].as_py()

        if dtype is not None:
            value = dtype(value)
        return value

    def _rebuild_index(self):
        r"""Rebuild the file index from the current table."""
        self._file_index = {}
        files = self._table["file"].to_pylist()
        for idx, file in enumerate(files):
            self._file_index[file] = idx

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
        if not files:
            return

        # Create a mask for rows to keep
        files_to_drop = set(files)
        mask = pc.invert(pc.is_in(self._table["file"], pa.array(list(files_to_drop))))

        # Filter the table
        self._table = self._table.filter(mask)

        # Rebuild index
        self._rebuild_index()

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        if file not in self._file_index:
            return

        row_idx = self._file_index[file]

        # Create a new removed column with the updated value
        removed_array = self._table["removed"].to_pylist()
        removed_array[row_idx] = 1

        # Replace the removed column
        self._table = self._table.set_column(
            self._table.schema.get_field_index("removed"),
            "removed",
            pa.array(removed_array, type=pa.int32()),
        )

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

        Raises:
            KeyError: if a file in values does not exist in dependencies

        """
        if not values:
            return

        # Check if all files exist before updating
        for value in values:
            file = value[0]
            if file not in self._file_index:
                raise KeyError(file)

        # Convert table to mutable lists
        files_list = self._table["file"].to_pylist()
        archives_list = self._table["archive"].to_pylist()
        bit_depths_list = self._table["bit_depth"].to_pylist()
        channels_list = self._table["channels"].to_pylist()
        checksums_list = self._table["checksum"].to_pylist()
        durations_list = self._table["duration"].to_pylist()
        formats_list = self._table["format"].to_pylist()
        removed_list = self._table["removed"].to_pylist()
        sampling_rates_list = self._table["sampling_rate"].to_pylist()
        types_list = self._table["type"].to_pylist()
        versions_list = self._table["version"].to_pylist()

        # Update values
        for value in values:
            file = value[0]
            row_idx = self._file_index[file]
            archives_list[row_idx] = value[1]
            bit_depths_list[row_idx] = value[2]
            channels_list[row_idx] = value[3]
            checksums_list[row_idx] = value[4]
            durations_list[row_idx] = value[5]
            formats_list[row_idx] = value[6]
            removed_list[row_idx] = value[7]
            sampling_rates_list[row_idx] = value[8]
            types_list[row_idx] = value[9]
            versions_list[row_idx] = value[10]

        # Rebuild table
        self._table = pa.table(
            {
                "file": pa.array(files_list, type=pa.string()),
                "archive": pa.array(archives_list, type=pa.string()),
                "bit_depth": pa.array(bit_depths_list, type=pa.int32()),
                "channels": pa.array(channels_list, type=pa.int32()),
                "checksum": pa.array(checksums_list, type=pa.string()),
                "duration": pa.array(durations_list, type=pa.float64()),
                "format": pa.array(formats_list, type=pa.string()),
                "removed": pa.array(removed_list, type=pa.int32()),
                "sampling_rate": pa.array(sampling_rates_list, type=pa.int32()),
                "type": pa.array(types_list, type=pa.int32()),
                "version": pa.array(versions_list, type=pa.string()),
            },
            schema=self._schema,
        )

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
        if not files:
            return

        # Convert version column to mutable list
        versions_list = self._table["version"].to_pylist()

        # Update versions for specified files
        for file in files:
            if file in self._file_index:
                row_idx = self._file_index[file]
                versions_list[row_idx] = version

        # Replace the version column
        self._table = self._table.set_column(
            self._table.schema.get_field_index("version"),
            "version",
            pa.array(versions_list, type=pa.string()),
        )


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

    Args:
        backend_interface: backend interface
        name: database name
        version: database version
        verbose: if ``True`` a message is shown during download

    Returns:
        dependency object

    """
    with tempfile.TemporaryDirectory() as tmp_root:
        # Try to load in order: db.lance, db.parquet, db.zip (legacy CSV)
        # First, try Lance (current format)
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
            # Try parquet (previous format)
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
                # Fall back to legacy CSV format
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
