import errno
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
        >>> # Request dependencies for emodb 1.1.1
        >>> deps = audb.dependencies('emodb', version='1.1.1')
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
        >>> deps.removed('wav/03a01Fa.wav')
        False
        >>> # Check if a file is part of the dependencies
        >>> 'wav/03a01Fa.wav' in deps
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
        return list(self._df.loc[file])

    def __len__(self) -> int:
        return len(self._df)

    def __str__(self) -> str:
        return self._df.to_string()

    @property
    def archives(self) -> typing.List[str]:
        r"""All archives (table and media).

        Return:
            list of archives

        """
        archives = self._df.archive.to_list()
        return sorted(list(set(archives)))

    @property
    def files(self) -> typing.List[str]:
        r"""All files (table and media).

        Returns:
            list of files

        """
        return list(self._df.index)

    @property
    def media(self) -> typing.List[str]:
        r"""Media files.

        Returns:
            list of media

        """
        return list(
            self._df[
                self._df['type'] == define.DependType.MEDIA
            ].index
        )

    @property
    def removed_media(self) -> typing.List[str]:
        r"""Removed media files.

        Returns:
            list of media

        """
        return list(
            self._df[
                (self._df['type'] == define.DependType.MEDIA)
                & (self._df['removed'] == 1)
            ].index
        )

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
        return list(
            self._df[
                self._df['type'] == define.DependType.META
            ].index
        )

    def archive(self, file: str) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        return self._df.archive[file]

    def bit_depth(self, file: str) -> int:
        r"""Bit depth of media file.

        Args:
            file: relative file path

        Returns:
            bit depth

        """
        return int(self._df.bit_depth[file])

    def channels(self, file: str) -> int:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        return int(self._df.channels[file])

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        return self._df.checksum[file]

    def duration(self, file: str) -> float:
        r"""Duration of file.

        Args:
            file: relative file path

        Returns:
            duration in seconds

        """
        return float(self._df.duration[file])

    def format(self, file: str) -> str:
        r"""Format of file.

        Args:
            file: relative file path

        Returns:
            file format (always lower case)

        """
        return self._df.format[file]

    def load(self, path: str):
        r"""Read dependencies from file.

        Clears existing dependencies.

        Args:
            path: path to file.
                File extension can be ``csv`` or ``pkl``.

        Raises:
            ValueError: if file extension is not ``csv`` or ``pkl``
            FileNotFoundError: if ``path`` does not exists

        """
        self._df = pd.DataFrame(columns=define.DEPEND_FIELD_NAMES.values())
        path = audeer.safe_path(path)
        extension = audeer.file_extension(path)
        if extension not in ['csv', 'pkl']:
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
        if extension == 'pkl':
            self._df = pd.read_pickle(path)
        elif extension == 'csv':
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
            self._df = pd.read_csv(
                path,
                index_col=index,
                na_filter=False,
                dtype=dtype_mapping,
            )

    def removed(self, file: str) -> bool:
        r"""Check if file is marked as removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        return bool(self._df.removed[file])

    def sampling_rate(self, file: str) -> int:
        r"""Sampling rate of media file.

        Args:
            file: relative file path

        Returns:
            sampling rate in Hz

        """
        return int(self._df.sampling_rate[file])

    def save(self, path: str):
        r"""Write dependencies to file.

        Args:
            path: path to file.
                File extension can be ``csv`` or ``pkl``.

        """
        path = audeer.safe_path(path)
        if path.endswith('csv'):
            self._df.to_csv(path)
        elif path.endswith('pkl'):
            self._df.to_pickle(path)

    def type(self, file: str) -> int:
        r"""Type of file.

        Args:
            file: relative file path

        Returns:
            type

        """
        return int(self._df.type[file])

    def version(self, file: str) -> str:
        r"""Version of file.

        Args:
            file: relative file path

        Returns:
            version string

        """
        return self._df.version[file]

    def _add_media(
            self,
            root: str,
            file: str,
            version: str,
            archive: str = None,
            checksum: str = None,
    ):
        r"""Add or update media file.

        If you want to update only the version
        of an unaltered media file,
        don't specify ``archive`` and ``checksum``.

        Args:
            root: root directory
            file: relative file path
            archive: archive name without extension
            checksum: checksum of file
            version: version string

        """
        format = audeer.file_extension(file).lower()

        if archive is None:
            archive = self.archive(file)

        if checksum is None:
            checksum = self.checksum(file)
            bit_depth = self.bit_depth(file)
            channels = self.channels(file)
            duration = self.duration(file)
            sampling_rate = self.sampling_rate(file)
        else:
            bit_depth = channels = sampling_rate = 0
            duration = 0.0
            if format in define.FORMATS:
                path = os.path.join(root, file)
                bit_depth = audiofile.bit_depth(path)
                channels = audiofile.channels(path)
                duration = audiofile.duration(path)
                sampling_rate = audiofile.sampling_rate(path)

        self._df.loc[file] = [
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

    def _drop(self, file: str):
        r"""Drop file from table.

        Args:
            file: relative file path

        """
        self._df.drop(file, inplace=True)

    def _remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._df.at[file, 'removed'] = 1
