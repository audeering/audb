import threading
import time

import filelock

import audeer

from audb.core.lock import FolderLock


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


def test_lock(tmpdir):

    # create two lock folders

    lock_folders = [
        audeer.mkdir(audeer.path(tmpdir, str(idx)))
        for idx in range(2)
    ]

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
