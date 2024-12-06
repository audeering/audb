from collections.abc import Sequence
import os
import warnings

import pyarrow.parquet as parquet

import audbackend
import audeer

from audb.core import define
from audb.core.config import config
from audb.core.repository import Repository


def lookup_backend(
    name: str,
    version: str,
) -> type[audbackend.interface.Base]:
    r"""Return backend of requested database.

    If the database is stored in several repositories,
    only the first one is considered.
    The order of the repositories to look for the database
    is given by :attr:`config.REPOSITORIES`.

    Args:
        name: database name
        version: version string

    Returns:
        backend interface

    Raises:
        RuntimeError: if database is not found

    """
    return _lookup(name, version)[1]


def md5(file: str) -> str:
    r"""MD5 checksum of file.

    PARQUET files are stored non-deterministically.
    To ensure tracking changes to those files correctly,
    the checksum can be provided
    under the key ``b"hash"`` in its metadata,
    e.g. which is done when creating a PARQUET file
    with :meth:`audformat.Table.save`.

    If the key is not present in its metadata,
    or the file is not a PARQUET file
    :func:`audeer.md5` is used to calculate the checksum.

    Args:
        file: file path with extension

    Returns:
        MD5 checksum of file

    """
    ext = audeer.file_extension(file)
    if ext == "parquet":
        metadata = parquet.read_schema(file).metadata
        if b"hash" in metadata:
            return metadata[b"hash"].decode()
    return audeer.md5(file)


def mkdir_tree(
    files: Sequence[str],
    root: str,
):
    r"""Helper function to create folder tree."""
    folders = set()
    for file in files:
        folders.add(os.path.dirname(file))
    for folder in folders:
        audeer.mkdir(root, folder)


def _lookup(
    name: str,
    version: str,
) -> tuple[Repository, type[audbackend.interface.Base]]:
    r"""Helper function to look up database in all repositories.

    Returns repository, version and backend object.

    """
    for repository in config.REPOSITORIES:
        try:
            backend_interface = repository.create_backend_interface()
            backend_interface.backend.open()
        except (audbackend.BackendError, ValueError):
            continue

        header = backend_interface.join("/", name, "db.yaml")
        if backend_interface.exists(header, version, suppress_backend_errors=True):
            return repository, backend_interface
        else:
            backend_interface.backend.close()

    raise RuntimeError(
        f"Cannot find version " f"'{version}' " f"for database " f"'{name}'."
    )


def timeout_warning():
    warnings.warn(
        define.TIMEOUT_MSG,
        category=UserWarning,
    )
