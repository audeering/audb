import errno
import os
import shutil
import tempfile
import typing
import zipfile

import audfactory
import audeer

from audb2.core import define
from audb2.core.config import config


# replace once https://github.com/audeering/audeer/issues/19 is solved
def create_archive(
        root: str,
        files: typing.Sequence[str],
        out_file: str,
):
    r"""Create archive."""
    out_file = audeer.safe_path(out_file)
    audeer.mkdir(os.path.dirname(out_file))
    with zipfile.ZipFile(out_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            full_file = audeer.safe_path(os.path.join(root, file))
            if not os.path.exists(full_file):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), full_file,
                )
            zf.write(full_file, arcname=file)


def sort_versions(versions: typing.List[str]):
    r"""Sort versions inplace."""
    versions.sort(key=lambda s: list(map(int, s.split('.'))))


class Backend:
    r"""Abstract backend.

    A backend stores files and archives.

    Args:
        db_name: name of database
        host: host path
        repository: repository name
        group_id: group ID
        verbose: show debug messages

    """
    def __init__(
            self,
            db_name: str,
            *,
            host: str,
            repository: str,
            group_id: str,
            verbose: bool = False,
    ):
        self.db_name = db_name
        r"""Database name"""
        self.host = host
        r"""Host path"""
        self.repository = repository
        r"""Repository name"""
        self.group_id = group_id
        r"""Group ID"""
        self.verbose = verbose
        r"""Verbose flag"""

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:  # pragma: no cover
        r"""File path or URL on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            path or URL

        """
        raise NotImplementedError()

    def exists(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> bool:  # pragma: no cover
        r"""Check if file or URL exists on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            ``True if file exists``

        """
        raise NotImplementedError()

    def get_archive(
            self,
            root: str,
            name: str,
            version: str,
            *,
            group: str = None,
    ) -> typing.List[str]:
        r"""Get archive from backend and extract.

        Args:
            root: root directory
            name: alias name of archive
            version: version string
            group: group string

        Returns:
            extracted files

        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = os.path.join(tmp, os.path.basename(root))
            file = name + '.zip'
            path = self.get_file(
                tmp_root, file, version, name=name, group=group,
            )
            return audeer.extract_archive(path, root)

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:  # pragma: no cover
        r"""Get file from backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            local file path

        """
        raise NotImplementedError()

    def latest_version(self) -> str:
        r"""Latest version of database.

        Returns:
            version string

        """
        v = self.versions()
        sort_versions(v)
        if not v:
            raise RuntimeError(
                f"There is no published version for "
                f"database '{self.db_name}'.",
            )
        return v[-1]

    def put_archive(
            self,
            root: str,
            files: typing.Union[str, typing.Sequence[str]],
            name: str,
            version: str,
            *,
            group: str = None,
            force: bool = False,
    ) -> str:
        r"""Create archive and put to backend.

        Args:
            root: root directory
            files: relative path to file(s)
            name: alias name of archive
            version: version string
            group: group string
            force: overwrite archive if it exists

        Returns:
            path or URL

        Raises:
            RuntimeError: if archive already exists

        """
        if isinstance(files, str):
            files = [files]
        with tempfile.TemporaryDirectory() as tmp:
            file = f'{name}-{version}.zip'
            outfile = os.path.join(tmp, file)
            create_archive(root, files, outfile)
            return self.put_file(
                tmp, file, version, name=name, group=group, force=force,
            )

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
            force: bool = False,
    ) -> str:  # pragma: no cover
        r"""Put file to backend.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            name: alias name of file
            group: group string
            force: overwrite archive if it exists

        Returns:
            path or URL

        Raises:
            RuntimeError: if file already exists on backend

        """
        raise NotImplementedError()

    def versions(self) -> typing.List[str]:  # pragma: no cover
        r"""Versions of database.

        Returns:
            list of versions

        """
        raise NotImplementedError()


class Artifactory(Backend):
    r"""Artifactory backend.

    Stores files and archives on Artifactory.

    Args:
        db_name: name of database
        repository: repository name
        group_id: group ID
        verbose: show debug messages
    """

    def __init__(
            self,
            db_name: str,
            *,
            repository: str = config.REPOSITORY_PUBLIC,
            group_id: str = config.GROUP_ID,
            verbose: bool = False,
    ):
        super().__init__(
            db_name,
            host=audfactory.config.ARTIFACTORY_ROOT,
            repository=repository,
            group_id=group_id,
            verbose=verbose,
        )

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:
        r"""URL of a file on Artifactory.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            URL

        """
        if name is None:
            if file != os.path.basename(file):
                name = audeer.uid(from_string=file)
            else:
                name = audeer.basename_wo_ext(file)
        _, ext = os.path.splitext(os.path.basename(file))
        server_url = audfactory.server_url(
            self._group(group),
            name=name,
            repository=self.repository,
            version=version,
        )
        return f'{server_url}/{name}-{version}{ext}'

    def exists(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> bool:
        r"""Check if URL exists.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            ``True if URL exists``

        """
        url = self.destination(root, file, version, name=name, group=group)
        return audfactory.artifactory_path(url).exists()

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:
        r"""Download file from Artifactory.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            local file path

        """

        url = self.destination(root, file, version, name=name, group=group)

        if not audfactory.artifactory_path(url).exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), url,
            )

        path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(path))

        return audfactory.download_artifact(url, path, verbose=self.verbose)

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
            force: bool = False,
    ) -> str:
        r"""Upload file to Artifactory.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            name: alias name of file
            group: group string
            force: overwrite archive if it exists

        Returns:
            URL

        Raises:
            RuntimeError: if URL already exists on backend

        """
        if name is None:
            if file != os.path.basename(file):
                name = audeer.uid(from_string=file)
            else:
                name = audeer.basename_wo_ext(file)
        _, ext = os.path.splitext(os.path.basename(file))

        url = self.destination(root, file, version, name=name, group=group)
        if not force and audfactory.artifactory_path(url).exists():
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), url,
            )

        if file == f'{name}-{version}{ext}':
            return audfactory.upload_artifact(
                os.path.join(root, file),
                self.repository,
                self._group(group),
                name,
                version,
                verbose=self.verbose
            )
        else:
            with tempfile.TemporaryDirectory() as tmp:
                tmp_file = os.path.join(
                    tmp, f'{name}-{version}{ext}'
                )
                shutil.copy(
                    audeer.safe_path(os.path.join(root, file)),
                    tmp_file,
                )
                return audfactory.upload_artifact(
                    tmp_file,
                    self.repository,
                    self._group(group),
                    name,
                    version,
                    verbose=self.verbose
                )

    def versions(self) -> typing.List[str]:
        r"""Versions of database.

        Returns:
            list of versions

        """
        return audfactory.versions(
            self.group_id,
            self.db_name,
            repository=self.repository,
        )

    def _group(
            self,
            group: str = None,
    ):
        if group is not None:
            return f'{self.group_id}.{self.db_name}.{group}'
        else:
            return f'{self.group_id}.{self.db_name}'


class FileSystem(Backend):
    r"""File system backend.

    Stores files and archives on a file system.

    Args:
        db_name: name of database
        host: root directory of repository
        repository: repository name
        group_id: group ID
        verbose: show debug messages

    """
    def __init__(
            self,
            db_name: str,
            host: str,
            *,
            repository: str = config.REPOSITORY_PUBLIC,
            group_id: str = config.GROUP_ID,
            verbose: bool = False,
    ):
        super().__init__(
            db_name,
            host=host,
            group_id=group_id,
            repository=repository,
            verbose=verbose,
        )
        self._repo_root = audeer.safe_path(
            os.path.join(
                host, repository, group_id.replace('.', os.path.sep),
            )
        )

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:
        r"""File path on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            path

        """
        group = group or ''
        if name is None:
            if file != os.path.basename(file):
                name = audeer.uid(from_string=file)
            else:
                name = audeer.basename_wo_ext(file)
        _, ext = os.path.splitext(os.path.basename(file))
        return os.path.join(
            self._repo_root, self.db_name, group,
            name, version, f'{name}-{version}{ext}',
        )

    def exists(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> bool:
        r"""Check if file exists on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            ``True if file exists``

        """
        path = self.destination(root, file, version, name=name, group=group)
        return os.path.exists(path)

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
    ) -> str:
        r"""Copy file from backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            name: alias name of file
            group: group string

        Returns:
            local file path

        """
        src_path = self.destination(
            root, file, version, name=name, group=group,
        )

        if not os.path.exists(src_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), src_path,
            )

        dst_path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(dst_path))
        shutil.copy(src_path, dst_path)

        return dst_path

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            *,
            name: str = None,
            group: str = None,
            force: bool = False,
    ) -> str:
        r"""Copy file to backend.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            name: alias name of file
            group: group string
            force: overwrite archive if it exists

        Returns:
            path

        Raises:
            RuntimeError: if file already exists on backend

        """
        dst_path = self.destination(
            root, file, version, name=name, group=group,
        )

        if not force and os.path.exists(dst_path):
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), dst_path,
            )

        src_path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(dst_path))
        shutil.copy(src_path, dst_path)

        return dst_path

    def versions(self) -> typing.List[str]:
        r"""Versions of database.

        Returns:
            list of versions

        """
        root = os.path.join(
            self._repo_root, self.db_name, 'db',
        )
        if os.path.exists(root):
            return [
                v for v in os.listdir(root) if os.path.isdir(
                    os.path.join(root, v))
            ]
        else:
            return []
