import sys

import audbackend


class Repository:
    r"""Repository object.

    It stores all information
    needed to address a repository:
    the repository name,
    host,
    and the backend name.
    With :meth:`Repository.create_backend_interface`
    it also provides a method
    to create a backend interface
    to access the repository.

    Args:
        name: repository name
        host: repository host
        backend: repository backend

    Examples:
        >>> Repository("data-local", "/data", "file-system")
        Repository('data-local', '/data', 'file-system')

    """

    _backends = {
        "file-system": audbackend.backend.FileSystem,
        "minio": audbackend.backend.Minio,
        "s3": audbackend.backend.Minio,
    }

    if hasattr(audbackend.backend, "Artifactory"):
        _backends["artifactory"] = audbackend.backend.Artifactory  # pragma: no cover

    backend_registry = _backends
    r"""Backend registry.

    Holds mapping between registered backend names,
    and their corresponding backend classes.
    The ``"artifactory"`` backend is currently not available
    under Python >=3.13.

    """

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
        r"""Repository backend name."""

    def __eq__(self, other) -> bool:
        """Compare two repository instances.

        Args:
            other: repository instance

        Returns:
            ``True`` if the string representation of the repositories matches

        """
        return str(self) == str(other)

    def __hash__(self) -> int:
        """Hash of repository.

        Returns:
            hash of repository

        """
        return hash(str(self))

    def __repr__(self):  # noqa: D105
        return (
            f"Repository("
            f"'{self.name}', "
            f"'{self.host}', "
            f"'{self.backend}'"
            f")"
        )

    def create_backend_interface(self) -> type[audbackend.interface.Base]:
        r"""Create backend interface to access repository.

        It wraps an :class:`audbackend.interface.Versioned` interface
        around it.
        The files will then be stored
        with the following structure on the backend
        (shown by the example of version 1.0.0 of the emodb dataset)::

            emodb/1.0.0/db.yaml            <-- header
            emodb/1.0.0/db.zip             <-- dependency table
            emodb/attachment/1.0.0/...     <-- attachments
            emodb/media/1.0.0/...          <-- media files
            emodb/meta/1.0.0/...           <-- tables

        When :attr:`Repository.backend` equals ``artifactory``,
        it wraps an :class:`audbackend.interface.Maven` interface
        around it.
        The files will then be stored
        with the following structure on the Artifactory backend
        (shown by the example of version 1.0.0 of the emodb dataset)::

            emodb/db/1.0.0/db-1.0.0.yaml   <-- header
            emodb/db/1.0.0/db-1.0.0.zip    <-- dependency table
            emodb/attachment/.../1.0.0/... <-- attachments
            emodb/media/.../1.0.0/...      <-- media files
            emodb/meta/.../1.0.0/...       <-- tables

        The returned backend instance
        has not yet established a connection to the backend.
        To establish a connection,
        use the backend with a ``with`` statement,
        or use the ``open()`` and ``close()`` methods of the backend class.
        The backend is stored as the inside the ``backend`` attribute
        of the returned backend interface.

        Returns:
            interface to repository

        Raises:
            ValueError: if an artifactory backend is requested in Python>=3.13
            ValueError: if a non-supported backend is requested

        """
        if sys.version_info >= (3, 13) and self.backend == "artifactory":
            raise ValueError(  # pragma: no cover
                "The 'artifactory' backend is not supported in Python>=3.13"
            )
        if self.backend not in self.backend_registry:
            raise ValueError(f"'{self.backend}' is not a registered backend")
        backend_class = self.backend_registry[self.backend]
        backend = backend_class(self.host, self.name)
        if self.backend == "artifactory":
            interface = audbackend.interface.Maven(backend)  # pragma: no cover
        else:
            interface = audbackend.interface.Versioned(backend)
        return interface

    @classmethod
    def register(
        cls,
        backend_name: str,
        backend_class: type[audbackend.backend.Base],
    ):
        r"""Register backend class.

        Adds an entry to the dictionary
        stored in the class variable :data:`Repository.backend_registry`,
        mapping a backend name
        to an actual backend class.

        Args:
            backend_name: name of the backend,
                e.g. ``"file-system"``
            backend_class: class of the backend,
                that should be associated with ``backend_name``,
                e.g. ``"audbackend.backend.Filesystem"``

        Examples:
            >>> import audbackend
            >>> audb.Repository.register("file-system", audbackend.backend.FileSystem)

        """
        cls.backend_registry[backend_name] = backend_class
