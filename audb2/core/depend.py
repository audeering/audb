import os
import typing

import pandas as pd

import audeer

from audb2.core import define


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
            columns=list(define.FIELD_NAMES.values()),
        )

    def __contains__(self, file: str):
        r"""Check if dependency to file exists.

        Args:
            file: relative file path

        Returns:
            ``True`` if a dependency to the file exists

        """
        return file in self._data

    def __getitem__(self, file: str) -> typing.Tuple[str, str]:
        r"""Meta information of dependency.

        Args:
            file: relative file path

        Returns:
            list with meta information

        """
        if file not in self:
            raise RuntimeError(f"An entry for '{file}' does not exist.")
        return self._data[file]

    @property
    def data(self) -> typing.Dict[str, typing.List]:
        r"""Get table data.

        Returns:
            dictionary with table entries

        """
        return self._data

    @property
    def files(self) -> typing.List[str]:
        r"""Files to which a dependency exists.

        Returns:
            list of files

        """
        return list(self._data)

    @property
    def media(self) -> typing.List[str]:
        r"""Media to which a dependency exists.

        Returns:
            list of media

        """
        select = [
            file for file in self.files if self.type(file) == define.Type.MEDIA
        ]
        return select

    @property
    def tables(self) -> typing.List[str]:
        r"""Tables to which a dependency exists.

        Returns:
            list of tables

        """
        select = [
            file for file in self.files if self.type(file) == define.Type.META
        ]
        return select

    def archive(
            self,
            file: str,
    ) -> str:
        r"""Name of archive the file belongs to.

        Args:
            file: relative file path

        Returns:
            archive name

        """
        return self[file][define.Field.ARCHIVE]

    def channels(self, file: str) -> str:
        r"""Number of channels of media file.

        Args:
            file: relative file path

        Returns:
            number of channels

        """
        return self[file][define.Field.CHANNELS]

    def checksum(self, file: str) -> str:
        r"""Checksum of file.

        Args:
            file: relative file path

        Returns:
            checksum of file

        """
        return self[file][define.Field.CHECKSUM]

    def from_file(
            self,
            path: str,
    ):
        r"""Read dependencies from CSV file.

        Clears existing dependencies.

        Args:
            path: path to file

        """
        self._data = {}
        path = audeer.safe_path(path)
        if os.path.exists(path):
            df = pd.read_csv(path, index_col=0, na_filter=False)
            for file, row in df.iterrows():
                self._data[file] = list(row)

    def remove(self, file: str):
        r"""Mark file as removed.

        Args:
            file: relative file path

        """
        self._data[file][define.Field.REMOVED] = 1

    def removed(self, file: str) -> bool:
        r"""Check if file is marked as removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        return self[file][define.Field.REMOVED] != 0

    def to_file(
            self,
            path: str,
    ):
        r"""Write dependencies to CSV file.

        Args:
            path: path to file

        """
        path = audeer.safe_path(path)
        self().to_csv(path)

    def type(self, file: str) -> define.Type:
        r"""File type.

        Args:
            file: relative file path

        Returns:
            type

        """
        return self[file][define.Field.TYPE]

    def version(self, file: str) -> str:
        r"""Version of file.

        Args:
            file: relative file path

        Returns:
            version string

        """
        return self[file][define.Field.VERSION]
