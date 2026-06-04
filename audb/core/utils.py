from collections.abc import Sequence
import contextlib
import os
import warnings

import pyarrow.parquet as parquet

import audbackend
import audeer
import audformat

from audb.core import define
from audb.core.config import config
from audb.core.lock import FolderLock
from audb.core.repository import Repository


def database_is_complete(db_root: str) -> bool:
    r"""Check if a database is completely cached.

    A database is considered complete,
    if a ``.complete`` file is present
    in its cache folder.
    As this file can be checked
    without acquiring a lock on the cache folder,
    it allows to load a complete database
    without locking,
    see https://github.com/audeering/audb/issues/197.

    Databases that were cached
    with a version of audb
    before the introduction of the ``.complete`` file
    do not contain this file.
    For those, completeness is still stored
    in the database header (``db.yaml``),
    which has to be checked separately.

    Args:
        db_root: database cache folder

    Returns:
        ``True`` if the ``.complete`` file exists

    """
    return os.path.exists(os.path.join(db_root, define.COMPLETE_FILE))


def mark_database_complete(db_root: str):
    r"""Mark a database as completely cached.

    Creates a ``.complete`` file
    in the database cache folder,
    see :func:`database_is_complete`.
    The file is never removed afterwards,
    as a complete database
    does not change anymore.

    Args:
        db_root: database cache folder

    """
    audeer.touch(os.path.join(db_root, define.COMPLETE_FILE))


def database_is_complete_in_header(db: audformat.Database) -> bool:
    r"""Check if a database is marked complete in its header.

    Before the introduction of the ``.complete`` file
    (see :func:`database_is_complete`),
    the information if a database is complete
    was stored in the database header (``db.yaml``).
    This is still checked
    for databases that were cached
    with such an older version of audb.

    Args:
        db: database header object

    Returns:
        ``True`` if the header marks the database as complete

    """
    return db.meta.get("audb", {}).get("complete", False)


def legacy_complete(db_root: str, db: audformat.Database) -> bool:
    r"""Create a ``.complete`` file from a legacy header flag.

    Databases cached with a version of audb
    before the introduction of the ``.complete`` file
    store their completeness in the database header instead
    (see :func:`database_is_complete_in_header`).
    If the header marks the database as complete,
    the ``.complete`` file is created,
    so the database can be loaded
    without locking its cache folder afterwards.

    Args:
        db_root: database cache folder
        db: database header object

    Returns:
        ``True`` if the header marks the database as complete

    """
    if database_is_complete_in_header(db):
        mark_database_complete(db_root)
        return True
    return False


def lock_cache(
    db_root: str,
    timeout: float = define.TIMEOUT,
) -> contextlib.AbstractContextManager:
    r"""Context manager to lock the cache folder of a database.

    If the database is already complete
    (see :func:`database_is_complete`),
    a no-op context manager is returned,
    as a complete database is never modified
    and therefore does not need to be locked.
    Otherwise a :class:`audb.core.lock.FolderLock`
    for ``db_root`` is returned.

    Args:
        db_root: database cache folder
        timeout: maximum time in seconds
            before giving up acquiring a lock

    Returns:
        context manager locking the cache folder

    """
    if database_is_complete(db_root):
        return contextlib.nullcontext()
    return FolderLock(db_root, timeout=timeout)


def is_empty(path: str) -> bool:
    """Check if path is an empty folder.

    Args:
        path: path to folder

    Returns:
        ``True`` if folder is empty

    """
    with os.scandir(path) as entries:
        return next(entries, None) is None


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

    raise RuntimeError(f"Cannot find version '{version}' for database '{name}'.")


def timeout_warning():
    warnings.warn(
        define.TIMEOUT_MSG,
        category=UserWarning,
    )
