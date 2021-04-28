import os
import typing

import pandas as pd

import audeer
import audiofile

from audb.core import define


class Dependencies:
    r"""Dependencies of a database.

    :class:`audb.Dependencies` gathers all files
    a database contains
    and metadata about them
    in a single object.
    The metadata contains information
    about the single files
    like duration,
    but also what version of the file is required.

    The dependencies of a database can be requested with
    :func:`audb.dependencies`.

    Example:
        >>> deps = Dependencies()
        >>> deps()
        Empty DataFrame
        Columns: [archive, bit_depth, channels, checksum, duration, format, removed, sampling_rate, type, version]
        Index: []
        >>> # Request dependencies for emodb 1.1.0
        >>> deps = audb.dependencies('emodb', version='1.1.0')
        >>> # List all files or archives
        >>> deps.files[:3]
        ['db.emotion.csv', 'db.files.csv', 'wav/03a01Fa.wav']
        >>> deps.archives[:2]
        ['005d2b91-5317-0c80-d602-6d55f0323f8c', '014f82d8-3491-fd00-7397-c3b2ac3b2875']
        >>> # Access properties for a given file
        >>> deps.archive('wav/03a01Fa.wav')
        'c1f5cc6f-6d00-348a-ba3b-4adaa2436aad'
        >>> deps.duration('wav/03a01Fa.wav')
        1.89825
        >>> deps.is_removed('wav/03a01Fa.wav')
        False
        >>> # Check if a file is part of the dependencies
        >>> 'wav/03a01Fa.wav' in deps
        True

    """  # noqa: E501

    def __init__(self):
        self._data = {}

    def __call__(self) -> pd.DataFrame:
        r"""Return dependencies as a table.

        Returns:
            table with dependencies

        """
        return pd.DataFrame.from_dict(
            self._data,
            orient='index',
            columns=list(define.DEPEND_FIELD_NAMES.values()),
        )

    def __contains__(self, file: str) -> bool:
        r"""Check if file is part of dependencies.

        Args:
            file: relative file path

        Returns:
            ``True`` if a dependency to the file exists

        """
        return file in self._data

    def __getitem__(self, file: str) -> typing.List:
        r"""File information.

        Args:
            file: relative file path

        Returns:
            list with meta information

        """
        return self._data[file]

    def __len__(self) -> int:
        return len(self._data)

    def __str__(self) -> str:
        return self().to_string()

    @property
    def archives(self) -> typing.List[str]:
        r"""All archives (table and media).

        Return:
            list of archives

        """
        archives = [self.archive(file) for file in self.files]
        return sorted(list(set(archives)))

    @property
    def data(self) -> typing.Dict[str, typing.List]:
        r"""Get table data.

        Returns:
            dictionary with table entries

        """
        return self._data

    @property
    def files(self) -> typing.List[str]:
        r"""All files (table and media).

        Returns:
            list of files

        """
        return list(self._data)

    @property
    def media(self) -> typing.List[str]:
        r"""Media files.

        Returns:
            list of media

        """
        select = [
            file for file in self.files
            if self.type(file) == define.DependType.MEDIA
        ]
        return select

    @property
    def removed_media(self) -> typing.List[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        select = [
            file for file in self.media if self.is_removed(file)
        ]
        return select

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
        select = [
            file for file in self.files
            if self.type(file) == define.DependType.META
        ]
        return select

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        return self[file][define.DependField.ARCHIVE]

    def bit_depth(self, file: str) -> int:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
        return self[file][define.DependField.BIT_DEPTH]

    def channels(self, file: str) -> int:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        return self[file][define.DependField.CHANNELS]

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        return self[file][define.DependField.CHECKSUM]

    def duration(self, file: str) -> float:
        r"""Duration of file.

        Args:
            file: relative file path

        Returns:
            duration in seconds

        """
        return self[file][define.DependField.DURATION]

    def format(self, file: str) -> str:
        r"""Format of file.

        Args:
            file: relative file path

        Returns:
            file format (always lower case)

        """
        return self[file][define.DependField.FORMAT]

    def is_removed(self, file: str) -> bool:
        r"""Check if file is marked as removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        return self[file][define.DependField.REMOVED] != 0

    def load(self, path: str):
        r"""Read dependencies from CSV file.

        Clears existing dependencies.

        Args:
            path: path to file

        """
        self._data = {}
        path = audeer.safe_path(path)
        if os.path.exists(path):
            # Data type of dependency columns
            dtype_mapping = {
                name: dtype for name, dtype in zip(
                    define.DEPEND_FIELD_NAMES.values(),
                    define.DEPEND_FIELD_DTYPES.values(),
                )
            }
            # Data type of index
            index = 0
            dtype_mapping[index] = str
            df = pd.read_csv(
                path,
                index_col=index,
                na_filter=False,
                dtype=dtype_mapping,
            )
            self._data = {
                file: list(row) for file, row in df.iterrows()
            }

    def sampling_rate(self, file: str) -> int:
        r"""Sampling rate of media file.

        Args:
            file: relative file path

        Returns:
            sampling rate in Hz

        """
        return self[file][define.DependField.SAMPLING_RATE]

    def save(self, path: str):
        r"""Write dependencies to CSV file.

        Args:
            path: path to file

        """
        path = audeer.safe_path(path)
        self().to_csv(path)

    def type(self, file: str) -> define.DependType:
        r"""Type of file.

        Args:
            file: relative file path

        Returns:
            type

        """
        return self[file][define.DependField.TYPE]

    def version(self, file: str) -> str:
        r"""Version of file.

        Args:
            file: relative file path

        Returns:
            version string

        """
        return self[file][define.DependField.VERSION]

    def _add_media(
            self,
            root: str,
            file: str,
            archive: str,
            checksum: str,
            version: str,
    ):
        r"""Add media file.

        Args:
            root: root directory
            file: relative file path
            archive: archive name without extension
            checksum: checksum of file
            version: version string

        """
        format = audeer.file_extension(file).lower()

        bit_depth = channels = sampling_rate = 0
        duration = 0.0
        if format in define.FORMATS:
            path = os.path.join(root, file)
            bit_depth = audiofile.bit_depth(path)
            channels = audiofile.channels(path)
            duration = audiofile.duration(path)
            sampling_rate = audiofile.sampling_rate(path)

        self.data[file] = [
            archive,
            bit_depth,
            channels,
            checksum,
            duration,
            format,
            0,  # removed
            sampling_rate,
            define.DependType.MEDIA,
            version,
        ]

    def _add_meta(
            self,
            file: str,
            archive: str,
            checksum: str,
            version: str,
    ):
        r"""Add table file.

        Args:
            file: relative file path
            archive: archive name without extension
            checksum: checksum of file
            version: version string

        """
        format = audeer.file_extension(file).lower()

        self.data[file] = [
            archive,                 # archive
            0,                       # bit_depth
            0,                       # channels
            checksum,                # checksum
            0.0,                     # duration
            format,                  # format
            0,                       # removed
            0,                       # sampling_rate
            define.DependType.META,  # type
            version,                 # version
        ]

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._data[file][define.DependField.REMOVED] = 1
