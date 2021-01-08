import errno
import glob
import os
import shutil
import tempfile
import typing

from artifactory import ArtifactoryPath

import audfactory
import audeer

from audb2.core import utils
from audb2.core.config import config


def _alias(file: str, name: str):
    r"""convert file to alias"""
    if name is None:
        if file != os.path.basename(file):
            name = audeer.uid(from_string=file)
        else:
            name = audeer.basename_wo_ext(file)
    return name


class Backend:
    r"""Abstract backend.

    A backend stores files and archives.

    Args:
        host: host address

    """
    def __init__(
            self,
            host: str
    ):
        self.host = host
        r"""Host path"""

    def checksum(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:  # pragma: no cover
        r"""Get MD5 checksum for file on backend.

        Args:
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            MD5 checksum

        """
        raise NotImplementedError()

    def destination(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:  # pragma: no cover
        r"""File path or URL on backend.

        Args:
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            path or URL

        """
        raise NotImplementedError()

    def exists(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:  # pragma: no cover
        r"""Check if file or URL exists on backend.

        Args:
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True`` if file exists

        """
        raise NotImplementedError()

    def get_archive(
            self,
            root: str,
            name: str,
            version: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Get archive from backend and extract.

        Args:
            root: root directory
            name: alias name of archive
            version: version string
            repository: repository name
            group_id: group ID

        Returns:
            extracted files

        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = os.path.join(tmp, os.path.basename(root))
            file = name + '.zip'
            path = self.get_file(
                tmp_root, file, version, repository, group_id, name=name,
            )
            return audeer.extract_archive(path, root)

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:  # pragma: no cover
        r"""Get file from backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            local file path

        """
        raise NotImplementedError()

    def glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:  # pragma: no cover
        r"""Return matching files names.

        Use ``'**'`` to scan into sub-directories.

        Args:
            pattern: pattern string
            repository: repository name
            group_id: group ID

        Returns:
            file names

        """
        raise NotImplementedError()

    def latest_version(
            self,
            file: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""Latest version of a file.

        Args:
            file: relative path to file
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            version string

        """
        vs = self.versions(
            file, repository, group_id, name=name,
        )
        if not vs:
            raise RuntimeError(
                f"Cannot find a version for file '{file}'.",
            )
        utils.sort_versions(vs)
        return vs[-1]

    def put_archive(
            self,
            root: str,
            files: typing.Union[str, typing.Sequence[str]],
            name: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            force: bool = False,
    ) -> str:
        r"""Create archive and put to backend.

        Args:
            root: root directory
            files: relative path to file(s)
            name: alias name of archive
            version: version string
            repository: repository name
            group_id: group ID
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
            utils.create_archive(root, files, outfile)
            return self.put_file(
                tmp, file, version, repository=repository,
                group_id=group_id, name=name, force=force,
            )

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
            force: bool = False,
    ) -> str:  # pragma: no cover
        r"""Put file to backend.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file
            force: overwrite archive if it exists

        Returns:
            path or URL

        Raises:
            RuntimeError: if file already exists on backend

        """
        raise NotImplementedError()

    def versions(
            self,
            file: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> typing.List[str]:  # pragma: no cover
        r"""Versions of a file.

        Args:
            file: relative path to file
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            list of versions

        """
        raise NotImplementedError()


class Artifactory(Backend):
    r"""Artifactory backend.

    Stores files and archives on Artifactory.

    Args:
        host: host address

    """

    def __init__(
            self,
            host=config.ARTIFACTORY_HOST,
    ):
        super().__init__(host)

    def checksum(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""MD5 checksum of file on backend.

        Args:
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            MD5 checksum

        Raises:
            FileNotFoundError: if file does not exist

        """
        url = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        path = audfactory.artifactory_path(url)

        if not path.exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), url,
            )

        return ArtifactoryPath.stat(path).md5

    def destination(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""URL of a file on Artifactory.

        Args:
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

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
            group_id,
            name=name,
            repository=repository,
            version=version,
        )
        return f'{server_url}/{name}-{version}{ext}'

    def exists(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:
        r"""Check if URL exists.

        Args:
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True`` if URL exists

        """
        url = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        return audfactory.artifactory_path(url).exists()

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""Download file from Artifactory.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            local file path

        Raises:
            FileNotFoundError: if file does not exist

        """

        url = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        if not audfactory.artifactory_path(url).exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), url,
            )

        path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(path))

        return audfactory.download_artifact(url, path, verbose=False)

    def glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Return matching files names.

        Use ``'**'`` to scan into sub-directories.

        Args:
            pattern: pattern string
            repository: repository name
            group_id: group ID

        Returns:
            file names

        """
        url = audfactory.server_url(
            group_id,
            repository=repository,
        )
        path = audfactory.artifactory_path(url)
        try:
            result = [str(x) for x in path.glob(pattern)]
        except RuntimeError:  # pragma: no cover
            result = []
        return result

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
            force: bool = False,
    ) -> str:
        r"""Upload file to Artifactory.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file
            force: overwrite archive if it exists

        Returns:
            URL

        Raises:
            FileExistsError: if URL already exists

        """
        name = _alias(file, name)
        _, ext = os.path.splitext(os.path.basename(file))

        url = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        if not force and audfactory.artifactory_path(url).exists():
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), url,
            )

        if file == f'{name}-{version}{ext}':
            return audfactory.upload_artifact(
                os.path.join(root, file),
                repository,
                group_id,
                name,
                version,
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
                    repository,
                    group_id,
                    name,
                    version,
                )

    def versions(
            self,
            file: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> typing.List[str]:
        r"""Versions of a file.

        Args:
            file: relative path to file
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            list of versions in ascending order

        """
        name = _alias(file, name)
        return audfactory.versions(
            group_id,
            name,
            repository=repository,
        )


class FileSystem(Backend):
    r"""File system backend.

    Stores files and archives on a file system.

    Args:
        host: host directory

    """
    def __init__(
            self,
            host: str = config.FILE_SYSTEM_HOST,
    ):
        super().__init__(audeer.safe_path(host))

    def _root(
            self,
            repository: str,
            group_id: str,
    ) -> str:
        return os.path.join(
            self.host, repository, group_id.replace('.', os.path.sep),
        )

    def checksum(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""MD5 checksum of file on backend.

        Args:
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            MD5 checksum

        Raises:
            FileNotFoundError: if file does not exist

        """
        path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        if not os.path.exists(path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path,
            )

        return utils.md5(path)

    def destination(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""File path on backend.

        Args:
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            path

        """
        name = _alias(file, name)
        _, ext = os.path.splitext(os.path.basename(file))
        return os.path.join(
            self._root(repository, group_id),
            name, version, f'{name}-{version}{ext}',
        )

    def exists(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:
        r"""Check if file exists on backend.

        Args:
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True`` if file exists

        """
        path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        return os.path.exists(path)

    def get_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""Copy file from backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            local file path

        Raises:
            FileNotFoundError: if file does not exist

        """
        src_path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        if not os.path.exists(src_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), src_path,
            )

        dst_path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(dst_path))
        shutil.copy(src_path, dst_path)

        return dst_path

    def glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Return matching files names.

        Use ``'**'`` to scan into sub-directories.

        Args:
            pattern: pattern string
            repository: repository name
            group_id: group ID

        Returns:
            file names

        """
        root = os.path.join(
            self.host, repository, group_id.replace('.', os.path.sep),
        )
        path = os.path.join(root, pattern)
        return [os.path.join(root, p) for p in glob.glob(path, recursive=True)]

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
            force: bool = False,
    ) -> str:
        r"""Copy file to backend.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file
            force: overwrite archive if it exists

        Returns:
            path

        Raises:
            FileExistsError: if file already exists

        """
        dst_path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        if not force and os.path.exists(dst_path):
            raise FileExistsError(
                errno.EEXIST, os.strerror(errno.EEXIST), dst_path,
            )

        src_path = audeer.safe_path(os.path.join(root, file))
        audeer.mkdir(os.path.dirname(dst_path))
        shutil.copy(src_path, dst_path)

        return dst_path

    def versions(
            self,
            file: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> typing.List[str]:
        r"""Versions of database.

        Returns:
            list of versions in ascending order

        """
        name = _alias(file, name)
        root = os.path.join(self._root(repository, group_id), name)
        vs = []
        if os.path.exists(root):
            vs = [
                v for v in os.listdir(root)
                if os.path.isdir(os.path.join(root, v))
            ]
        utils.sort_versions(vs)
        return vs
