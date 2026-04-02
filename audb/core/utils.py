from collections.abc import Sequence
from contextlib import contextmanager
import os
import sys
import warnings

import pyarrow.parquet as parquet

import audbackend
import audeer

from audb.core import define
from audb.core.config import config
from audb.core.repository import Repository


@contextmanager
def status_line(message="...", verbose=True):
    r"""Show a persistent status message between progress bars.

    Displays ``message`` on stderr as a temporary status line.
    When a progress bar starts, it overwrites the status.
    When the progress bar finishes, the status is restored.

    This works by temporarily wrapping ``audeer.progress_bar``
    so that each bar clears the status on open
    and restores it on close.

    When ``verbose`` is ``False``, no output is produced
    and ``audeer.progress_bar`` is not wrapped.

    """
    if not verbose:
        yield
        return

    def _show():
        sys.stderr.write(f"\r{message}")
        sys.stderr.flush()

    def _clear():
        sys.stderr.write("\r\033[K")
        sys.stderr.flush()

    original_progress_bar = audeer.progress_bar

    def _wrapped_progress_bar(*args, **kwargs):
        _clear()
        bar = original_progress_bar(*args, **kwargs)
        if bar.disable:
            return bar
        original_close = bar.close

        def _patched_close():
            original_close()
            _show()

        bar.close = _patched_close
        return bar

    _show()
    audeer.progress_bar = _wrapped_progress_bar
    try:
        yield
    finally:
        audeer.progress_bar = original_progress_bar
        _clear()


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
