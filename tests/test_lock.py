import os
import re
import stat
import threading
import time

import filelock
import pytest

import audeer

from audb.core.lock import FolderLock
from audb.core.lock import lock_file as get_lock_file


event = threading.Event()


def job(lock, wait, sleep):
    if wait:
        event.wait()  # wait for another thread to enter the lock
    try:
        with lock:
            if not wait:
                event.set()  # notify waiting threads to enter the lock
            time.sleep(sleep)
    except filelock.Timeout:
        return 0
    return 1


def test_lock_file(tmpdir):
    """Lock file is placed outside the folder with ``.<name>.lock`` naming."""
    folder = audeer.mkdir(tmpdir, "parent", "db")
    lock_file = get_lock_file(folder)

    # Expected: `.../parent/.db.lock`, i.e. next to `db`, not inside it
    assert lock_file == audeer.path(tmpdir, "parent", ".db.lock")
    assert os.path.dirname(lock_file) == os.path.dirname(folder)
    assert not lock_file.startswith(folder + os.sep)


def test_lock(tmpdir):
    # create two lock folders

    lock_folders = [audeer.mkdir(tmpdir, str(idx)) for idx in range(2)]

    # lock 1 and 2

    lock_1 = FolderLock(lock_folders[0])
    lock_2 = FolderLock(lock_folders[1])

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, False, 0], {}),
            ([lock_2, False, 0], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 1]

    # lock 1, 2 and 1+2

    lock_1 = FolderLock(lock_folders[0])
    lock_2 = FolderLock(lock_folders[1])
    lock_12 = FolderLock(lock_folders)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, False, 0], {}),
            ([lock_2, False, 0], {}),
            ([lock_12, False, 0], {}),
        ],
        num_workers=3,
    )
    assert result == [1, 1, 1]

    # lock 1, then 1+2 + wait

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders)

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, False, 0.2], {}),
            ([lock_12, True, 0], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 1]

    # lock 1, then 1+2 + timeout

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders, timeout=0)

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, False, 0.2], {}),
            ([lock_12, True, 0], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 0]

    # lock 1+2, then 1 + wait

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders)

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, True, 0], {}),
            ([lock_12, False, 0.2], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 1]

    # lock 1+2, then 1 + timeout

    lock_1 = FolderLock(lock_folders[0], timeout=0)
    lock_12 = FolderLock(lock_folders)

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, True, 0], {}),
            ([lock_12, False, 0.2], {}),
        ],
        num_workers=2,
    )
    assert result == [0, 1]

    # lock 1+2, then 1 + wait and 2 + timeout

    lock_1 = FolderLock(lock_folders[0])
    lock_2 = FolderLock(lock_folders[1], timeout=0)
    lock_12 = FolderLock(lock_folders)

    event.clear()
    result = audeer.run_tasks(
        job,
        [
            ([lock_1, True, 0], {}),
            ([lock_2, True, 0], {}),
            ([lock_12, 0, 0.2], {}),
        ],
        num_workers=3,
    )
    assert result == [1, 0, 1]


@pytest.mark.skipif(os.name != "posix", reason="POSIX file permissions required")
def test_lock_file_permissions(tmpdir):
    """Lock files are created with group-write permissions."""
    folder = audeer.mkdir(tmpdir, "db")
    lock_file = get_lock_file(folder)
    with FolderLock(folder):
        mode = os.stat(lock_file).st_mode
        assert mode & stat.S_IWGRP


def test_lock_warning_and_failure(tmpdir):
    """Test user warning and lock failure messages."""
    folder = audeer.mkdir(tmpdir, "db")
    lock_file = get_lock_file(folder)
    lock_error = filelock.Timeout
    lock_error_msg = f"The file lock '{lock_file}' could not be acquired."
    warning_msg = (
        "Lock could not be acquired immediately.\n"
        "Another process might be loading the same database.\n"
        "Still trying for 0.1 "
        "more seconds...\n"
    )
    # Hold the lock to force failing acquiring of a second lock
    with FolderLock(folder):
        with pytest.warns(UserWarning, match=re.escape(warning_msg)):
            with pytest.raises(lock_error, match=re.escape(lock_error_msg)):
                with FolderLock(folder, warning_timeout=0.1, timeout=0.2):
                    pass
