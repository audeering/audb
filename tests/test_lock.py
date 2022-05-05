import time

import filelock

import audeer

from audb.core.lock import FolderLock


def job(lock, sleep_pre_lock, sleep_in_lock):
    time.sleep(sleep_pre_lock)
    try:
        with lock:
            time.sleep(sleep_in_lock)
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

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0, 0], {}),
            ([lock_2, 0, 0], {}),
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
            ([lock_1, 0, 0], {}),
            ([lock_2, 0, 0], {}),
            ([lock_12, 0, 0], {}),
        ],
        num_workers=3,
    )
    assert result == [1, 1, 1]

    # lock 1, then 1+2 + wait

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0, 0.2], {}),
            ([lock_12, 0.1, 0], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 1]

    # lock 1, then 1+2 + timeout

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders, timeout=0)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0, 0.2], {}),
            ([lock_12, 0.1, 0], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 0]

    # lock 1+2, then 1 + wait

    lock_1 = FolderLock(lock_folders[0])
    lock_12 = FolderLock(lock_folders)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0.1, 0], {}),
            ([lock_12, 0, 0.2], {}),
        ],
        num_workers=2,
    )
    assert result == [1, 1]

    # lock 1+2, then 1 + timeout

    lock_1 = FolderLock(lock_folders[0], timeout=0)
    lock_12 = FolderLock(lock_folders)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0.1, 0], {}),
            ([lock_12, 0, 0.2], {}),
        ],
        num_workers=2,
    )
    assert result == [0, 1]

    # lock 1+2, then 1 + wait and 2 + timeout

    lock_1 = FolderLock(lock_folders[0])
    lock_2 = FolderLock(lock_folders[1], timeout=0)
    lock_12 = FolderLock(lock_folders)

    result = audeer.run_tasks(
        job,
        [
            ([lock_1, 0.1, 0], {}),
            ([lock_2, 0.1, 0], {}),
            ([lock_12, 0, 0.2], {}),
        ],
        num_workers=3,
    )
    assert result == [1, 0, 1]
