import types
import typing

import filelock

import audeer

import audb.core.define as define


class FolderLock:

    def __init__(
            self,
            folders: typing.Union[str, typing.Sequence[str]],
            *,
            timeout: float = -1,
    ):
        r"""Lock one or more folders.

        Waits until the locks of all folders can be acquired.
        While a folder is locked,
        a file '.lock' will be created inside the folder.

        Args:
            folders: path to one or more folders that should be locked
            timeout: maximum wait time if another thread or process
                is already accessing one or more locks.
                If timeout is reached,
                an exception is raised.
                If timeout < 0 the method will block
                until the resource can be accessed

        Raises:
            :class:`filelock.Timeout`: if a timeout is reached

        """
        folders = audeer.to_list(folders)
        files = [audeer.path(folder, define.LOCK_FILE) for folder in folders]

        self.locks = [
            filelock.SoftFileLock(file)
            for file in files
        ]
        self.timeout = timeout

    def __enter__(self) -> 'FolderLock':
        r"""Acquire the lock(s)."""
        for lock in self.locks:
            lock.acquire(self.timeout)
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_value: typing.Optional[BaseException],
        traceback: typing.Optional[types.TracebackType],
    ) -> None:
        """Release the lock(s)."""
        for lock in self.locks:
            lock.release()

    def __del__(self) -> None:
        """Called when the lock object is deleted."""
        for lock in self.locks:
            lock.release(force=True)
