import errno
import os
import re
import typing

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as dataset
from pyarrow.lib import ArrowIndexError
from pyarrow.lib import ArrowInvalid
import pyarrow.parquet as parquet

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
        self._table = None
        self._dataset = None
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

    def __call__(self) -> pd.DataFrame:
        r"""Return dependencies as a table.

        Returns:
            table with dependencies

        """
        if self._table is None:
            df = pd.DataFrame(
                [],
                columns=[
                    "archive",
                    "bit_depth",
                    "channels",
                    "checksum",
                    "duration",
                    "format",
                    "removed",
                    "sampling_rate",
                    "type",
                    "version",
                ],
            )
        else:
            df = self._table.to_pandas()
            df.set_index("file", inplace=True)
            df.index.rename("", inplace=True)
        return df

    def __contains__(self, file: str) -> bool:
        r"""Check if file is part of dependencies.

        Args:
            file: relative file path

        Returns:
            ``True`` if a dependency to the file exists

        """
        row = self._table_row(file)
        return len(row) > 0

    def __getitem__(self, file: str) -> typing.List:
        r"""File information.

        Args:
            file: relative file path

        Returns:
            list with meta information

        """
        row = self._table_row(file, raise_error=True)
        return [value for column, value in row.items() if column != "file"]

    def __len__(self) -> int:
        r"""Number of all media, table, attachment files."""
        if self._table is None:
            length = 0
        else:
            length = len(self._table)
        return length

    def __str__(self) -> str:  # noqa: D105
        if self._table is None:
            _str = ""
        else:
            df = self._table.slice(length=5).to_pandas().set_index("file")
            _str = df.to_string()
        return _str

    @property
    def archives(self) -> typing.List[str]:
        r"""All media, table, attachment archives.

        Return:
            list of archives

        """
        archives = self._table_column("archive")
        return sorted(list(set(archives)))

    @property
    def attachments(self) -> typing.List[str]:
        r"""Attachment paths (can be a file or a folder).

        Returns:
            list of attachments

        """
        return self._table_column("file", "type", define.DependType.ATTACHMENT)

    @property
    def attachment_ids(self) -> typing.List[str]:
        r"""Attachment IDs.

        Returns:
            list of attachment IDs

        """
        return self._table_column("archive", "type", define.DependType.ATTACHMENT)

    @property
    def files(self) -> typing.List[str]:
        r"""All media, table, attachments.

        Returns:
            list of files

        """
        return self._table_column("file")

    @property
    def media(self) -> typing.List[str]:
        r"""Media files.

        Returns:
            list of media

        """
        return self._table_column("file", "type", define.DependType.MEDIA)

    @property
    def removed_media(self) -> typing.List[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        mask = (dataset.field("type") == define.DependType.MEDIA) & (
            dataset.field("removed") == 1
        )
        return self._table.filter(mask).column("file").to_pylist()

    @property
    def table_ids(self) -> typing.List[str]:
        r"""Table IDs.

        Like :meth:`audb.Dependencies.tables`,
        but only returns the table ID,
        i.e. ``db.<id>.csv``.

        Returns:
            list of table IDs

        """
        return [table[3:-4] for table in self.tables]

    @property
    def tables(self) -> typing.List[str]:
        r"""Tables files.

        Returns:
            list of tables

        """
        return self._table_column("file", "type", define.DependType.META)

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        row = self._table_row(file, raise_error=True)
        return row["archive"]

    def bit_depth(self, file: str) -> int:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
        row = self._table_row(file, raise_error=True)
        return int(row["bit_depth"])

    def channels(self, file: str) -> int:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        row = self._table_row(file, raise_error=True)
        return int(row["channels"])

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        row = self._table_row(file, raise_error=True)
        return row["checksum"]

    def duration(self, file: str) -> float:
        r"""Duration of file.

        Args:
            file: relative file path

        Returns:
            duration in seconds

        """
        row = self._table_row(file, raise_error=True)
        return float(row["duration"])

    def format(self, file: str) -> str:
        r"""Format of file.

        Args:
            file: relative file path

        Returns:
            file format (always lower case)

        """
        row = self._table_row(file, raise_error=True)
        return row["format"]

    def load(self, path: str):
        r"""Read dependencies from file.

        Clears existing dependencies.

        Args:
            path: path to file.
                File extension can be ``csv`` or ``pkl``

        Raises:
            ValueError: if file extension is not ``csv`` or ``pkl``
            FileNotFoundError: if ``path`` does not exists

        """
        path = audeer.path(path)
        extension = audeer.file_extension(path)
        if extension not in ["csv", "pkl"]:
            raise ValueError(
                f"File extension of 'path' has to be 'csv' or 'pkl' "
                f"not '{extension}'"
            )
        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            )
        if extension == "parquet":
            self._table = parquet.read_table(path)
        if extension == "pkl":
            # Legacy cache format
            df = pd.read_pickle(path)
            df.index.rename("file", inplace=True)
            df = df.reset_index()
            # TODO: check if the conversion is faster when providing dtypes
            self._table = pa.Table.from_pandas(df, preserve_index=False)
        elif extension == "csv":
            self._table = csv.read_csv(
                path,
                read_options=csv.ReadOptions(
                    column_names=self._schema.names,
                    skip_rows=1,
                ),
                convert_options=csv.ConvertOptions(column_types=self._schema),
            )
        self._dataset = dataset.dataset(self._table)

    def removed(self, file: str) -> bool:
        r"""Check if file is marked as removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        row = self._table_row(file, raise_error=True)
        print(f"{row=}")
        return bool(row["removed"])

    def sampling_rate(self, file: str) -> int:
        r"""Sampling rate of media file.

        Args:
            file: relative file path

        Returns:
            sampling rate in Hz

        """
        row = self._table_row(file, raise_error=True)
        return int(row["sampling_rate"])

    def save(self, path: str):
        r"""Write dependencies to file.

        Args:
            path: path to file.
                File extension can be ``csv`` or ``parquet``

        """
        path = audeer.path(path)
        if path.endswith("csv"):
            # For legacy reasons
            # don't store column name for `"file"`
            columns = self._table.column_names
            columns[0] = ""
            table = self._table.rename_columns(columns)
            csv.write_csv(table, path)
        elif path.endswith("parquet"):
            parquet.write_table(self._table, path)

    def type(self, file: str) -> int:
        r"""Type of file.

        Args:
            file: relative file path

        Returns:
            type

        """
        row = self._table_row(file, raise_error=True)
        return int(row["type"])

    def version(self, file: str) -> str:
        r"""Version of file.

        Args:
            file: relative file path

        Returns:
            version string

        """
        row = self._table_row(file, raise_error=True)
        return row["version"]

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
        row = {
            "file": file,
            "archive": archive,
            "bit_depth": 0,
            "channels": 0,
            "checksum": checksum,
            "duration": 0.0,
            "format": audeer.file_extension(file).lower(),
            "removed": 0,
            "sampling_rate": 0,
            "type": define.DependType.ATTACHMENT,
            "version": version,
        }
        self._table_add_rows([row])

    def _add_media(
        self,
        values: typing.Sequence[
            typing.Tuple[
                str,  # file
                str,  # archive
                int,  # bit_depth
                int,  # channels
                str,  # checksum
                float,  # duration
                int,  # sampling_rate
                str,  # version
            ]
        ],
    ):
        r"""Add media files.

        Args:
            values: list of tuples,
                where each tuple holds the values of a new media entry

        """
        rows = [
            {
                "file": value[0],
                "archive": value[1],
                "bit_depth": value[2],
                "channels": value[3],
                "checksum": value[4],
                "duration": value[5],
                "format": audeer.file_extension(value[0]).lower(),
                "removed": 0,
                "sampling_rate": value[6],
                "type": define.DependType.MEDIA,
                "version": value[7],
            }
            for value in values
        ]
        self._table_add_rows(rows)

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
        row = {
            "file": file,
            "archive": archive,
            "bit_depth": 0,
            "channels": 0,
            "checksum": checksum,
            "duration": 0.0,
            "format": audeer.file_extension(file).lower(),
            "removed": 0,
            "sampling_rate": 0,
            "type": define.DependType.META,
            "version": version,
        }
        self._table_add_rows([row])

    def _drop(self, file: str):
        r"""Drop file from table.

        Args:
            file: relative file path

        """
        # Check first if `file` is in table
        # and raise `KeyError` if not
        self._table_row(file, raise_error=True)
        # If `file` is in table remove it
        mask = dataset.field("file") != file
        self._table = self._table.filter(mask)

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        # We cannot change values directly in a arrow table,
        # hence we remove the matching row
        # and add it as a new one
        # TODO: is there a better way
        # (e.g. replacing the "removed" column instead,
        # as proposed in https://stackoverflow.com/a/73779252)
        row = self._table_row(file, raise_error=True)
        row["removed"] = 1
        # Remove old row
        mask = dataset.field("file") != file
        self._table = self._table.filter(mask)
        # Add new row
        table = pa.Table.from_pylist([row], schema=self._schema)
        self._table_append(table)

    def _table_add_rows(
        self,
        rows: typing.Sequence[typing.Dict[str, typing.Any]],
    ):
        r"""Add rows.

        Args:
            rows: list of tuples,
                where each tuple holds the values of a new row

        """
        table = pa.Table.from_pylist(rows, schema=self._schema)
        self._table_append(table)

    def _table_append(
        self,
        table: pa.Table,
    ):
        r"""Append table to dependency table.

        Args:
            table: table to append

        """
        if self._table is None:
            self._table = table
        else:
            self._table = pa.concat_tables([self._table, table])
        # Update dataset every time table changes
        self._dataset = dataset.dataset(table)

    def _table_column(
        self,
        column: str,
        filter_column: str = None,
        match: str = None,
    ):
        r"""Table column based on matched value in other column.

        Args:
            column: selected column
            filter_column: selected column
                to filter rows
                based on ``match``
            match: value to select rows in ``filter_column``

        Returns:
            selected values from ``column`` as list

        """
        if self._table is None:
            column = []
        elif filter_column is None:
            column = self._table.column(column).to_pylist()
        else:
            mask = dataset.field(filter_column) == match
            column = self._table.filter(mask).column(column).to_pylist()
        return column

    def _table_row(self, file: str, raise_error: bool = False) -> typing.Dict:
        r"""Table row corresponding to file.

        The file column is included in the returned row.

        Args:
            file: relative file path
            raise_error: if ``True``
                it raises a ``KeyError``
                if ``file`` is not in dependency table

        Returns:
            row as dictionary with columns as keys

        """
        mask = dataset.field("file") == file
        # `.take([0], filter=mask)`
        # is 10x faster than
        # `.filter(mask).to_table()`
        try:
            list_of_row_dicts = self._dataset.take([0], filter=mask).to_pylist()
            row = list_of_row_dicts[0]
            return row
        except (
            ArrowIndexError,
            ArrowInvalid,
            AttributeError,
        ):  # if file cannot be found
            row = {}
            if raise_error:
                row[file]
            else:
                return row

    def _update_media(
        self,
        values: typing.Sequence[
            typing.Tuple[
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
        # Remove all selected files
        files = [value[0] for value in values]
        mask = dataset.field("file").isin(files)
        self._table = self._table.filter(~mask)
        # Add updates as new entries
        self._add_media(values)

    def _update_media_version(
        self,
        files: typing.Sequence[str],
        version: str,
    ):
        r"""Update version of media files.

        Args:
            files: relative file paths
            version: version string

        """

        def update_version(row):
            row["version"] = version

        rows = [
            update_version(self._table_row(file, raise_error=True)) for file in files
        ]
        # Remove all selected files
        mask = dataset.field("file").isin(files)
        self._table = self._table.filter(~mask)
        # Add updates as new entries
        table = pa.Table.from_pylist(rows, schema=self._schema)
        self._table_append(table)


def error_message_missing_object(
    object_type: str,
    missing_object_id: typing.Union[str, typing.Sequence],
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
    requested_deps: typing.Optional[typing.Union[str, typing.Sequence[str]]],
    available_deps: typing.Sequence[str],
    deps_type: str,
    database_name: str = None,
    database_version: str = None,
) -> typing.Sequence[str]:
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
