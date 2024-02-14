import errno
import os
import re
import typing

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
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
        data = {}
        for name, dtype in zip(
            define.DEPEND_FIELD_NAMES.values(),
            define.DEPEND_FIELD_DTYPES.values(),
        ):
            data[name] = pd.Series(dtype=dtype)
        self._df = pd.DataFrame(data)
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
        # Store location of last loaded dependency file
        self._path = None

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
        return file in self._df.index

    def __getitem__(self, file: str) -> typing.List:
        r"""File information.

        Args:
            file: relative file path

        Returns:
            list with meta information

        """
        return self._df.loc[file].tolist()

    def __len__(self) -> int:
        r"""Number of all media, table, attachment files."""
        return len(self._df)

    def __str__(self) -> str:  # noqa: D105
        return str(self._df)

    @property
    def archives(self) -> typing.List[str]:
        r"""All media, table, attachment archives.

        Return:
            list of archives

        """
        return sorted(self._df.archive.unique().tolist())

    @property
    def attachments(self) -> typing.List[str]:
        r"""Attachment paths (can be a file or a folder).

        Returns:
            list of attachments

        """
        return self._df[self._df["type"] == define.DependType.ATTACHMENT].index.tolist()

    @property
    def attachment_ids(self) -> typing.List[str]:
        r"""Attachment IDs.

        Returns:
            list of attachment IDs

        """
        return self._df[
            self._df["type"] == define.DependType.ATTACHMENT
        ].archive.tolist()

    @property
    def files(self) -> typing.List[str]:
        r"""All media, table, attachments.

        Returns:
            list of files

        """
        return self._df.index.tolist()

    @property
    def media(self) -> typing.List[str]:
        r"""Media files.

        Returns:
            list of media

        """
        return self._df[self._df["type"] == define.DependType.MEDIA].index.tolist()

    @property
    def removed_media(self) -> typing.List[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        return self._df[
            (self._df["type"] == define.DependType.MEDIA) & (self._df["removed"] == 1)
        ].index.tolist()

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
        return self._df[self._df["type"] == define.DependType.META].index.tolist()

    def archive(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[str, typing.List[str]]:
        r"""Name of archive a file belong to.

        Args:
            files: relative file path(s)

        Returns:
            archive name(s)

        """
        return self._column_loc("archive", files)

    def bit_depth(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[int, typing.List[int]]:
        r"""Bit depth of media file.

        Args:
            files: relative file path(s)

        Returns:
            bit depth(s)

        """
        return self._column_loc("bit_depth", files, int)

    def channels(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[int, typing.List[int]]:
        r"""Number of channels of media file.

        Args:
            files: relative file path(s)

        Returns:
            number(s) of channels

        """
        return self._column_loc("channels", files, int)

    def checksum(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[str, typing.List[str]]:
        r"""Checksum of file.

        Args:
            files: relative file path(s)

        Returns:
            checksum of file(s)

        """
        return self._column_loc("checksum", files)

    def duration(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[float, typing.List[float]]:
        r"""Duration of file.

        Args:
            files: relative file path(s)

        Returns:
            duration(s) in seconds

        """
        return self._column_loc("duration", files, float)

    def format(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[str, typing.List[str]]:
        r"""Format of file.

        Args:
            files: relative file path(s)

        Returns:
            file format(s) (always lower case)

        """
        return self._column_loc("format", files)

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
        self._df = pd.DataFrame(columns=define.DEPEND_FIELD_NAMES.values())
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
            self._df = self._table_to_dataframe(table)

        # Set dtype of index for both CSV and PKL
        # to make backward compatiple
        # with old pickle files in cache
        # that might use `string` as dtype
        self._df.index = self._df.index.astype(define.DEPEND_INDEX_DTYPE)
        self._path = path

    def removed(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[bool, typing.List[bool]]:
        r"""Check if file is marked as removed.

        Args:
            files: relative file path(s)

        Returns:
            ``True`` if file was removed

        """
        return self._column_loc("removed", files, bool)

    def sampling_rate(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[int, typing.List[int]]:
        r"""Sampling rate of media file.

        Args:
            files: relative file path(s)

        Returns:
            sampling rate(s) in Hz

        """
        return self._column_loc("sampling_rate", files, int)

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

    def type(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[int, typing.List[int]]:
        r"""Type of file.

        Args:
            files: relative file path(s)

        Returns:
            type(s)

        """
        return self._column_loc("type", files, int)

    def version(
        self,
        files: typing.Union[str, typing.Sequence[str]],
    ) -> typing.Union[str, typing.List[str]]:
        r"""Version of file.

        Args:
            files: relative file path(s)

        Returns:
            version string(s)

        """
        return self._column_loc("version", files)

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

        self._df.loc[file] = [
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
        df = pd.DataFrame.from_records(
            values,
            columns=["file"] + list(define.DEPEND_FIELD_NAMES.values()),
        ).set_index("file")
        df.index = df.index.astype(define.DEPEND_INDEX_DTYPE)

        self._df = pd.concat([self._df, df])

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

        self._df.loc[file] = [
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

    def _column_loc(
        self,
        column: str,
        files: typing.Union[str, typing.Sequence[str]],
        dtype: typing.Callable = None,
    ) -> typing.Union[typing.Any, typing.List[typing.Any]]:
        r"""Column content for selected files."""
        # Single file
        if isinstance(files, str):
            value = self._df.at[files, column]
            if dtype is not None:
                value = dtype(value)
            return value

        # Multiple files
        else:
            values = self._df.loc[files, column]
            if dtype is not None:
                values = [dtype(value) for value in values]
            else:
                values = values.tolist()
            return values

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

    def _drop(self, files: typing.Sequence[str]):
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
        self._df = self._df[~self._df.index.isin(files)]

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._df.at[file, "removed"] = 1

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
        return df

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
        df = pd.DataFrame.from_records(
            values,
            columns=["file"] + list(define.DEPEND_FIELD_NAMES.values()),
        ).set_index("file")
        df.index = df.index.astype(define.DEPEND_INDEX_DTYPE)
        for name, dtype in zip(
            define.DEPEND_FIELD_NAMES.values(),
            define.DEPEND_FIELD_DTYPES.values(),
        ):
            df[name] = df[name].astype(dtype)
        self._df.loc[df.index] = df

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
        field = define.DEPEND_FIELD_NAMES[define.DependField.VERSION]
        self._df.loc[files, field] = version


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
