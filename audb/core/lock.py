from __future__ import annotations

from collections.abc import Sequence
import os
import types
import warnings

import filelock

import audeer

import audb.core.define as define


# File permissions for lock files.
# We use ``0o664`` (``-rw-rw-r--``)
# to allow group-write access,
# which is needed for shared caches.
LOCK_FILE_MODE = 0o664


class FolderLock:
    def __init__(
        self,
        folders: str | Sequence[str],
        *,
        timeout: float = define.TIMEOUT,
        warning_timeout: float = 2,
    ):
        r"""Lock one or more folders.

        Waits until the locks of all folders can be acquired.
        While a folder ``.../parent/name`` is locked,
        a file ``.../parent/.name.lock`` is created next to it.

        The lock file is placed *outside* the locked folder,
        so the folder itself can be deleted or overwritten
        while the lock is held.

        Args:
            folders: path to one or more folders that should be locked
            timeout: maximum time in seconds
                before giving up acquiring a lock to the database cache folder.
                If timeout is reached,
                an exception is raised
            warning_timeout: time in seconds
                after which a warning is shown to the user
                that the lock could not yet get acquired

        Raises:
            :class:`filelock.Timeout`: if a timeout is reached

        """
        folders = audeer.to_list(folders)

        # In the past we used ``-1`` as default value for timeout
        # to wait infinitely until the lock is acquired.
        if timeout < 0:
            warnings.warn(
                "'timeout' values <0 are no longer supported. "
                f"Changing your provided value of {timeout} to {define.TIMEOUT}"
            )
            timeout = define.TIMEOUT

        self.lock_files = [lock_file(folder) for folder in folders]
        self.locks = [
            filelock.FileLock(file, mode=LOCK_FILE_MODE) for file in self.lock_files
        ]
        self.timeout = timeout
        self.warning_timeout = warning_timeout

    def __enter__(self) -> "FolderLock":
        r"""Acquire the lock(s)."""
        for lock, file in zip(self.locks, self.lock_files):
            remaining_time = self.timeout
            acquired = False
            # First try to acquire lock in warning_timeout time
            if self.warning_timeout < self.timeout:
                try:
                    lock.acquire(timeout=self.warning_timeout)
                    acquired = True
                except filelock.Timeout:
                    warnings.warn(
                        f"Lock could not be acquired immediately.\n"
                        f"Still trying for {self.timeout - self.warning_timeout:.1f} "
                        "more seconds...\n"
                    )
                    remaining_time = self.timeout - self.warning_timeout

            if not acquired:
                lock.acquire(timeout=remaining_time)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        """Release the lock(s)."""
        for lock in self.locks:
            lock.release()

    def __del__(self) -> None:
        """Called when the lock object is deleted."""
        for lock in self.locks:
            lock.release(force=True)


def lock_file(folder: str) -> str:
    r"""Return lock file path for a folder.

    The lock file is placed outside the folder,
    so the folder can be deleted or overwritten
    while the lock is held.
    For a folder ``.../parent/name``
    the lock file is ``.../parent/.name.lock``.

    The lock file itself is created
    by :class:`filelock.FileLock` during acquisition,
    the parent folder is ensured to exist here.

    Args:
        folder: folder that should be locked

    Returns:
        path to lock file

    """
    folder = audeer.path(folder)
    parent = audeer.mkdir(os.path.dirname(folder))
    name = os.path.basename(folder)
    return audeer.path(parent, f".{name}{define.LOCK_FILE}")
