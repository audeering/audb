import typing

import audbackend


class Repository:
    r"""Repository object.

    It stores all information
    needed to address a repository:
    the repository name,
    host,
    and the backend name.

    Args:
        name: repository name
        host: repository host
        backend: repository backend

    Examples:
        >>> Repository("data-local", "/data", "file-system")
        Repository('data-local', '/data', 'file-system')

    """

    backend_registry = {
        "file-system": audbackend.backend.FileSystem,
        "artifactory": audbackend.backend.Artifactory,
    }
    r"""Backend registry."""

    def __init__(
        self,
        name: str,
        host: str,
        backend: str,
    ):
        self.name = name
        r"""Repository name."""
        self.host = host
        r"""Repository host."""
        self.backend = backend
        r"""Repository backend."""

    def __repr__(self):  # noqa: D105
        return (
            f"Repository("
            f"'{self.name}', "
            f"'{self.host}', "
            f"'{self.backend}'"
            f")"
        )

    def __call__(self) -> typing.Type[audbackend.interface.Base]:
        r"""Return interface to access repository.

        Returns:
            interface to repository

        """
        backend_class = self.backend_registry[self.backend]
        backend = backend_class(self.host, self.name)
        backend.open()
        if self.backend == "artifactory":
            # Maven version file structure on Artifactory
            interface = audbackend.interface.Maven(backend)
        else:
            interface = audbackend.interface.Versioned(backend)
        return interface

    @classmethod
    def register(
        cls,
        backend_name: str,
        backend_class: typing.Type[audbackend.backend.Base],
    ):
        r"""Register backend class.

        Args:
            backend_name: name of the backend,
                e.g. ``"file-system"``
            backend_class: class of the backend,
                that should be associated with ``backend_name``,
                e.g. ``"audbackend.backend.Filesystem"``

        Examples:
            >>> import audbackend
            >>> Repository.register("file-system", audbackend.backend.FileSystem)

        """
        cls.backend_registry[backend_name] = backend_class
