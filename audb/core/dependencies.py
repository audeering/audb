import os
import typing

import pandas as pd

import audeer
import audiofile

from audb.core import define


class Dependencies:
    r"""Hold dependencies of a database.

    """
    def __init__(self):
        self._data = {}

    def __call__(self) -> pd.DataFrame:
        r"""Create dependency table.

        Returns:
            table with dependencies

        """
        return pd.DataFrame.from_dict(
            self._data,
            orient='index',
            columns=list(define.DEPEND_FIELD_NAMES.values()),
        )

    def __contains__(self, file: str):
        r"""Check if file exists.

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

    @property
    def archives(self) -> typing.List[str]:
        r"""All archives (table and media).

        Return:
            list of archives

        """
        archives = [self.archive(file) for file in self.files]
        return list(set(archives))

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

    def add_media(
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

    def add_meta(
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

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        return self[file][define.DependField.ARCHIVE]

    def bit_depth(self, file: str) -> typing.Optional[int]:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
        return self[file][define.DependField.BIT_DEPTH] or None

    def channels(self, file: str) -> typing.Optional[int]:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        return self[file][define.DependField.CHANNELS] or None

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        return self[file][define.DependField.CHECKSUM]

    def duration(self, file: str) -> typing.Optional[float]:
        r"""Duration of file.

        Args:
            file: relative file path

        Returns:
            duration in seconds

        """
        return self[file][define.DependField.DURATION] or None

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

    def remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._data[file][define.DependField.REMOVED] = 1

    def sampling_rate(self, file: str) -> typing.Optional[int]:
        r"""Sampling rate of media file.

        Args:
            file: relative file path

        Returns:
            sampling rate in Hz

        """
        return self[file][define.DependField.SAMPLING_RATE] or None

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

    def __len__(self) -> int:
        return len(self._data)

    def __str__(self) -> str:
        return self().to_string()
