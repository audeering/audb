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


def _alias(file: str, name: typing.Optional[str]):
    r"""Convert file to alias."""
    if name is None:
        if file != os.path.basename(file):
            # replace file sep with '/' to get same uid on all platforms
            name = audeer.uid(from_string=file.replace(os.path.sep, '/'))
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

    def _checksum(
            self,
            path: str,
    ) -> str:  # pragma: no cover
        r"""MD5 checksum of file on backend."""
        raise NotImplementedError()

    def checksum(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""Get MD5 checksum for file on backend.

        Args:
            file: file name
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            MD5 checksum

        Raises:
            FileNotFoundError: if file does not exist on backend

        """
        path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        if not self.exists(
                file, version, repository=repository,
                group_id=group_id, name=name,
        ):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path,
            )

        return self._checksum(path)

    def _destination(
            self,
            name: str,
            ext: str,
            version: str,
            repository: str,
            group_id: str,
    ) -> str:  # pragma: no cover
        r"""File path on backend."""
        raise NotImplementedError()

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
            file: file name
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            file path on backend

        """
        name = _alias(file, name)
        _, ext = os.path.splitext(os.path.basename(file))
        return self._destination(name, ext, version, repository, group_id)

    def _exists(
            self,
            path: str,
    ) -> bool:  # pragma: no cover
        r"""Check if file exists on backend."""
        raise NotImplementedError()

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
            file: file name
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
        return self._exists(path)

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

        Raises:
            FileNotFoundError: if archive does not exist on backend

        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = os.path.join(tmp, os.path.basename(root))
            file = name + '.zip'
            path = self.get_file(
                tmp_root, file, version, repository, group_id, name=name,
            )
            return audeer.extract_archive(path, root)

    def _get_file(
            self,
            src_path: str,
            dst_path: str,
    ) -> str:  # pragma: no cover
        r"""Get file from backend."""
        raise NotImplementedError()

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
        r"""Get file from backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            full path to local file

        Raises:
            FileNotFoundError: if file does not exist on backend

        """
        src_path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        if not self._exists(src_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), src_path,
            )

        root = audeer.safe_path(root)
        dst_path = os.path.join(root, file)
        audeer.mkdir(os.path.dirname(dst_path))

        self._get_file(src_path, dst_path)

        return dst_path

    def _glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:  # pragma: no cover
        r"""Return matching files names."""
        raise NotImplementedError()

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
            matching files on backend

        """
        return self._glob(pattern, repository, group_id)

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
        return vs[-1]

    def put_archive(
            self,
            root: str,
            files: typing.Union[str, typing.Sequence[str]],
            name: str,
            version: str,
            repository: str,
            group_id: str,
    ) -> str:
        r"""Create archive and put on backend.

        The operation is silently skipped,
        if an archive with the same checksum
        already exists on the backend.

        Args:
            root: root directory
            files: relative path to file(s)
            name: alias name of archive
            version: version string
            repository: repository name
            group_id: group ID

        Returns:
            archive path on backend

        Raises:
            FileNotFoundError: if one or more files do not exist

        """
        root = audeer.safe_path(root)

        if isinstance(files, str):
            files = [files]

        for file in files:
            path = os.path.join(root, file)
            if not os.path.exists(path):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), path,
                )

        with tempfile.TemporaryDirectory() as tmp:
            file = f'{name}-{version}.zip'
            utils.create_archive(root, files, os.path.join(tmp, file))
            return self.put_file(
                tmp, file, version, repository=repository,
                group_id=group_id, name=name,
            )

    def _put_file(
            self,
            src_path: str,
            dst_path: str,
    ):  # pragma: no cover
        r"""Put file to backend."""
        raise NotImplementedError()

    def put_file(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ):
        r"""Put file on backend.

        The operation is silently skipped,
        if a file with the same checksum
        already exists on the backend.

        Args:
            root: root directory
            file: relative path to file
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            file path on backend

        Raises:
            FileNotFoundError: if local file does not exist

        """
        root = audeer.safe_path(root)
        src_path = os.path.join(root, file)
        if not os.path.exists(src_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), src_path,
            )

        dst_path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )

        # skip if file with same checksum exists on backend
        skip = self._exists(dst_path) and \
            utils.md5(src_path) == self._checksum(dst_path)
        if not skip:
            self._put_file(src_path, dst_path)

        return dst_path

    def _rem_file(
            self,
            path: str,
    ):  # pragma: no cover
        r"""Remove file from backend."""
        raise NotImplementedError()

    def rem_file(
            self,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""Remove file from backend.

        Args:
            file: relative path to file
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            path of removed file on backend

        Raises:
            FileNotFoundError: if file does not exist on backend

        """
        path = self.destination(
            file, version, repository=repository,
            group_id=group_id, name=name,
        )
        if not self._exists(path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path,
            )

        self._rem_file(path)

        return path

    def _versions(
            self,
            name: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:  # pragma: no cover
        r"""Versions of a file."""
        raise NotImplementedError()

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
        vs = self._versions(name, repository, group_id)
        utils.sort_versions(vs)
        return vs


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

    def _checksum(
            self,
            path: str,
    ) -> str:
        r"""MD5 checksum of file on backend."""
        return ArtifactoryPath.stat(audfactory.artifactory_path(path)).md5

    def _destination(
            self,
            name: str,
            ext: str,
            version: str,
            repository: str,
            group_id: str,
    ) -> str:
        r"""File path on backend."""
        server_url = audfactory.server_url(
            group_id,
            name=name,
            repository=repository,
            version=version,
        )
        return f'{server_url}/{name}-{version}{ext}'

    def _exists(
            self,
            path: str,
    ) -> bool:
        r"""Check if file exists on backend."""
        return audfactory.artifactory_path(path).exists()

    def _get_file(
            self,
            src_path: str,
            dst_path: str,
    ):
        r"""Get file from backend."""
        audfactory.download_artifact(src_path, dst_path, verbose=False)

    def _glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Return matching files names."""
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

    def _put_file(
            self,
            src_path: str,
            dst_path: str,
    ):
        r"""Put file to backend."""
        dst_path = audfactory.artifactory_path(dst_path)
        if not dst_path.parent.exists():
            dst_path.parent.mkdir()
        with open(src_path, "rb") as fobj:
            dst_path.deploy(fobj, md5=utils.md5(src_path))

    def _rem_file(
            self,
            path: str,
    ):
        r"""Remove file from backend."""
        audfactory.artifactory_path(path).unlink()

    def _versions(
            self,
            name: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Versions of a file."""
        return audfactory.versions(group_id, name, repository=repository)


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

    def _checksum(
            self,
            path: str,
    ) -> str:
        r"""MD5 checksum of file on backend."""
        return utils.md5(path)

    def _destination(
            self,
            name: str,
            ext: str,
            version: str,
            repository: str,
            group_id: str,
    ) -> str:
        r"""File path on backend."""
        return os.path.join(
            self._root(repository, group_id),
            name, version, f'{name}-{version}{ext}',
        )

    def _exists(
            self,
            path: str,
    ) -> bool:
        r"""Check if file exists on backend."""
        return os.path.exists(path)

    def _get_file(
            self,
            src_path: str,
            dst_path: str,
    ):
        r"""Get file from backend."""
        shutil.copy(src_path, dst_path)

    def _glob(
            self,
            pattern: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Return matching files names."""
        root = os.path.join(
            self.host, repository, group_id.replace('.', os.path.sep),
        )
        path = os.path.join(root, pattern)
        return [os.path.join(root, p) for p in glob.glob(path, recursive=True)]

    def _put_file(
            self,
            src_path: str,
            dst_path: str,
    ):
        r"""Put file to backend."""
        audeer.mkdir(os.path.dirname(dst_path))
        shutil.copy(src_path, dst_path)

    def _rem_file(
            self,
            path: str,
    ):
        r"""Remove file from backend."""
        os.remove(path)

    def _versions(
            self,
            name: str,
            repository: str,
            group_id: str,
    ) -> typing.List[str]:
        r"""Versions of a file."""
        root = os.path.join(self._root(repository, group_id), name)
        vs = []
        if os.path.exists(root):
            vs = [
                v for v in os.listdir(root)
                if os.path.isdir(os.path.join(root, v))
            ]
        return vs
