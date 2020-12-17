import errno
import os
import shutil
import tempfile
import typing

import audfactory
import audeer

from audb2.core import utils


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
        verbose: show debug messages

    """
    def __init__(
            self,
            host: str,
            *,
            verbose: bool = False,
    ):
        self.host = host
        r"""Host path"""
        self.verbose = verbose
        r"""Verbose flag"""

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:  # pragma: no cover
        r"""File path or URL on backend.

        Args:
            root: root directory
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
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:  # pragma: no cover
        r"""Check if file or URL exists on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True if file exists``

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
            root: root directory
            file: relative path to file
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            version string

        """
        v = self.versions(
            file, repository, group_id, name=name,
        )
        utils.sort_versions(v)
        if not v:
            raise RuntimeError(
                f"There is no published version for "
                f"file '{file}'.",
            )
        return v[-1]

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
        verbose: show debug messages

    """

    def __init__(
            self,
            host=audfactory.config.ARTIFACTORY_ROOT,
            *,
            verbose: bool = False,
    ):
        super().__init__(host, verbose=verbose)

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""URL of a file on Artifactory.

        Args:
            root: root directory
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
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:
        r"""Check if URL exists.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True if URL exists``

        """
        url = self.destination(
            root, file, version, repository=repository,
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

        """

        url = self.destination(
            root, file, version, repository=repository,
            group_id=group_id, name=name,
        )

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
            RuntimeError: if URL already exists on backend

        """
        name = _alias(file, name)
        _, ext = os.path.splitext(os.path.basename(file))

        url = self.destination(
            root, file, version, repository=repository,
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
                    repository,
                    group_id,
                    name,
                    version,
                    verbose=self.verbose
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
            list of versions

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
        host: host address
        verbose: show debug messages

    """
    def __init__(
            self,
            host: str,
            *,
            verbose: bool = False,
    ):
        super().__init__(audeer.safe_path(host), verbose=verbose)

    def _root(
            self,
            repository: str,
            group_id: str,
    ) -> str:
        return os.path.join(
            self.host, repository, group_id.replace('.', os.path.sep),
        )

    def destination(
            self,
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> str:
        r"""File path on backend.

        Args:
            root: root directory
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
            root: str,
            file: str,
            version: str,
            repository: str,
            group_id: str,
            *,
            name: str = None,
    ) -> bool:
        r"""Check if file exists on backend.

        Args:
            root: root directory
            file: file path relative to root
            version: version string
            repository: repository name
            group_id: group ID
            name: alias name of file

        Returns:
            ``True if file exists``

        """
        path = self.destination(
            root, file, version, repository=repository,
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

        """
        src_path = self.destination(
            root, file, version, repository=repository,
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
            RuntimeError: if file already exists on backend

        """
        dst_path = self.destination(
            root, file, version, repository=repository,
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
            list of versions

        """
        name = _alias(file, name)
        root = os.path.join(self._root(repository, group_id), name)
        if os.path.exists(root):
            return [
                v for v in os.listdir(root) if os.path.isdir(
                    os.path.join(root, v))
            ]
        else:
            return []
