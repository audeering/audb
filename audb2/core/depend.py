import collections
import os
import re
import typing

import pandas as pd

import audata
import audeer
import audiofile

from audb2.core import define
from audb2.core import utils
from audb2.core.backend import Backend
from audb2.core.flavor import Flavor


class Dependencies:
    r"""Manage dependencies of a database.

    Args:
        dep_path: path to dependencies file
        db_root: root directory of database
        backend: backend object
        verbose: show debug messages

    """
    def __init__(
            self,
            dep_path: str,
            db_root: str,
            backend: Backend,
            *,
            verbose: bool = False,
    ):
        self.dep_path = audeer.safe_path(dep_path)
        r"""Path to dependencies file."""
        self.db_root = audeer.safe_path(db_root)
        r"""Root directory of database."""
        self.backend = backend
        r"""Backend object."""
        self.verbose = verbose
        r"""Show debug messages."""

        self._data = None

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

    def __enter__(self) -> 'Dependencies':
        r"""Read dependencies from file."""
        self._data = self._from_file()
        return self

    def __exit__(self, type, value, traceback):
        r"""Store dependencies to file."""
        self._to_file()
        self._data = None

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

    def load(
            self,
            flavor: Flavor = None,
    ) -> audata.Database:
        r"""Load dependencies from Artifactory.

        Skips files that exist and have a matching checksum.

        Args:
            flavor: database flavor

        """
        db_header = audata.Database.load(self.db_root, load_data=False)

        # filter tables
        if flavor is not None:
            if flavor.tables is not None:
                if isinstance(flavor.tables, str):
                    pattern = re.compile(flavor.tables)
                    tables = []
                    for table in db_header.tables:
                        if pattern.search(table):
                            tables.append(table)
                else:
                    tables = flavor.tables
                db_header.pick(tables, inplace=True)
                db_header.save(self.db_root, header_only=True)
                for file in self.tables:
                    if not self.archive(file) in tables:
                        self._data.pop(file)

        # figure out tables to download
        tables_to_download = []
        for table in db_header.tables:
            file = f'db.{table}.csv'
            full_file = os.path.join(self.db_root, file)
            if os.path.exists(full_file):
                checksum = utils.md5(full_file)
                if checksum != self.checksum(file):
                    tables_to_download.append(file)
            else:
                tables_to_download.append(file)

        # download tables
        for file in tables_to_download:
            self.backend.get_archive(
                self.db_root,
                self.archive(file),
                self.version(file),
                group=define.TYPE_NAMES[define.Type.META],
            )

        db = audata.Database.load(self.db_root)

        # filter media
        if flavor is not None:
            if flavor.include is not None or flavor.exclude is not None:
                archives = set()
                for file in db.files:
                    archives.add(self.archive(file))
                if flavor.include is not None:
                    if isinstance(flavor.include, str):
                        pattern = re.compile(flavor.include)
                        include = []
                        for archive in archives:
                            if pattern.search(archive):
                                include.append(archive)
                    else:
                        include = flavor.include
                    db.filter_files(lambda x: self.archive(x) in include)
                if flavor.exclude is not None:
                    if isinstance(flavor.exclude, str):
                        pattern = re.compile(flavor.exclude)
                        exclude = []
                        for archive in archives:
                            if pattern.search(archive):
                                exclude.append(archive)
                    else:
                        exclude = flavor.exclude
                    db.filter_files(lambda x: self.archive(x) not in exclude)
            if flavor.mix is not None:
                if isinstance(flavor.mix, str):
                    if flavor.mix == define.Mix.MONO_ONLY:
                        # keep only mono
                        db.filter_files(
                            lambda x: self.channels(x) == 1,
                        )
                    if flavor.mix in (
                        define.Mix.LEFT,
                        define.Mix.RIGHT,
                        define.Mix.STEREO_ONLY,
                    ):
                        # keep only stereo
                        db.filter_files(
                            lambda x: self.channels(x) == 2,
                        )
                    elif flavor.mix == define.Mix.STEREO:
                        # keep only mono or stereo
                        db.filter_files(
                            lambda x: self.channels(x) in [1, 2],
                        )
                else:
                    num_channels = max(flavor.mix) + 1
                    db.filter_files(
                        lambda x: self.channels(x) >= num_channels,
                    )

                db.save(self.db_root)

        if flavor is None or not flavor.only_metadata:

            # figure out media to download
            media_to_download = []
            for file in db.files:
                if not self.removed(file):
                    full_file = os.path.join(self.db_root, file)
                    if os.path.exists(full_file):
                        checksum = utils.md5(full_file)
                        if checksum != self.checksum(file):
                            media_to_download.append(file)
                    else:
                        media_to_download.append(file)

            # figure out archives to download
            archives_to_download = set()
            for file in media_to_download:
                archives_to_download.add(
                    (self.archive(file), self.version(file))
                )

            # download archives with media
            for archive, version in archives_to_download:
                self._download_media(
                    archive,
                    version,
                    flavor,
                )

        return db

    def publish(
            self,
            version: str,
            *,
            archives: typing.Mapping[str, str] = None,
    ):
        r"""Publish dependencies to Artifactory.

        Skips files that already exists.

        Args:
            version: version string
            archives: map files to archives

        """
        db = audata.Database.load(self.db_root)

        if version in self.backend.versions():
            raise RuntimeError(
                f"A version '{version}' already exists for "
                f"database '{self.backend.db_name}'."
            )

        archives = archives or {}
        for name in archives.values():
            if name and define.ARCHIVE_NAME_PATTERN.fullmatch(name) is None:
                raise ValueError(
                    f"Invalid archive name '{name}', "
                    "allowed characters are '[0-9][a-z][A-Z].-_'."
                )

        # release dependencies to removed tables
        db_tables = [f'db.{table}.csv' for table in db.tables]
        for file in set(self.tables) - set(db_tables):
            self._data.pop(file)

        # update version of altered tables and insert new ones
        tables_to_upload = []
        for table in db.tables:
            file = f'db.{table}.csv'
            checksum = utils.md5(os.path.join(self.db_root, file))
            if file not in self:
                self._data[file] = [
                    table, 0, checksum, 0, define.Type.META, version,
                ]
                tables_to_upload.append(table)
            elif checksum != self.checksum(file):
                self._data[file][define.Field.CHANNELS] = 0
                self._data[file][define.Field.CHECKSUM] = checksum
                self._data[file][define.Field.VERSION] = version
                tables_to_upload.append(table)

        # upload tables
        for table in tables_to_upload:
            file = f'db.{table}.csv'
            self.backend.put_archive(
                self.db_root,
                file,
                table,
                version,
                group=define.TYPE_NAMES[define.Type.META],
            )

        # release dependencies to removed media
        # and select according archives for upload
        media_to_upload = set()
        db_media = db.files
        for file in set(self.media) - set(db_media):
            media_to_upload.add(self.archive(file))
            self._data.pop(file)

        # update version of altered media and insert new ones
        for file in db_media:
            path = os.path.join(self.db_root, file)
            if file not in self:
                checksum = utils.md5(path)
                if file in archives:
                    archive = archives[file]
                else:
                    archive = audeer.uid(from_string=file)
                channels = audiofile.channels(path)
                self._data[file] = [
                    archive, channels, checksum, 0, define.Type.MEDIA, version,
                ]
            elif not self.removed(file):
                checksum = utils.md5(path)
                if checksum != self.checksum(file):
                    channels = audiofile.channels(path)
                    self._data[file][define.Field.CHECKSUM] = channels
                    self._data[file][define.Field.CHECKSUM] = checksum
                    self._data[file][define.Field.VERSION] = version

        # create a mapping from archives to media and
        # select archives with new or altered files for upload
        map_media_to_files = collections.defaultdict(list)
        for file in self.media:
            if not self.removed(file):
                map_media_to_files[self.archive(file)].append(file)
                if self.version(file) == version:
                    media_to_upload.add(self.archive(file))

        # upload new and altered archives if it contains at least one file
        for archive in media_to_upload:
            if archive in map_media_to_files:
                for file in map_media_to_files[archive]:
                    self._data[file][define.Field.VERSION] = version
                self._upload_archive(
                    archive=archive,
                    mapping=map_media_to_files,
                    group=define.TYPE_NAMES[define.Type.MEDIA],
                    version=version,
                )

    def remove(self, file: str):
        r"""Remove file.

        Args:
            file: relative file path

        """
        self._data[file][define.Field.REMOVED] = 1

    def removed(self, file: str) -> bool:
        r"""Check if file was removed.

        Args:
            file: relative file path

        Returns:
            ``True`` if file was removed

        """
        return self[file][define.Field.REMOVED] != 0

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

    def _from_file(self) -> typing.Dict[str, list]:
        r"""Store dependencies to file."""
        data = {}
        if os.path.exists(self.dep_path):
            df = pd.read_csv(self.dep_path, index_col=0, na_filter=False)
            for file, row in df.iterrows():
                data[file] = list(row)
        return data

    def _to_file(self):
        r"""Read dependencies from file."""
        self().to_csv(self.dep_path)

    def _download_media(
            self,
            archive: str,
            version: str,
            flavor: Flavor = None,
    ):
        files = self.backend.get_archive(
            self.db_root,
            archive,
            version,
            group=define.TYPE_NAMES[define.Type.MEDIA],
        )
        if flavor is not None:
            for file in files:
                flavor(os.path.join(self.db_root, file))

    def _upload_archive(
            self,
            archive: str,
            mapping: typing.Dict[str, typing.List[str]],
            group: str,
            version: str,
    ):
        self.backend.put_archive(
            self.db_root,
            mapping[archive],
            archive,
            version,
            group=group,
        )
