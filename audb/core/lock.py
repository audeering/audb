import types
import typing

import filelock

import audeer


class Lock:

    def __init__(
            self,
            files: typing.Union[str, typing.Sequence[str]],
            *,
            timeout: float = -1,
    ):
        r"""Manage one or more file locks.

        Args:
            files: lock file or list of lock files
            timeout: maximum wait time if another thread or process is already
                accessing the database. If timeout is reached, ``None`` is
                returned. If timeout < 0 the method will block until the
                database can be accessed

        """
        files = audeer.to_list(files)
        files = [audeer.path(file) for file in files]
        self.locks = [
            filelock.SoftFileLock(file, timeout=timeout)
            for file in files
        ]

    def __enter__(self) -> Lock:
        r"""Acquire the lock(s)."""
        for lock in self.locks:
            lock.acquire()
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
