import os
import sys
import threading
import time

import pytest

import audbackend
import audeer
import audformat.testing

import audb


class SlowFileSystem(audbackend.FileSystem):
    r"""Emulate a slow file system.

    Introduces a short delay when getting a file from the backend.
    This ensures that timeouts are reached in the tests.

    """
    def _get_file(self, *args):
        time.sleep(0.1)
        super()._get_file(*args)


audbackend.register(
    'slow-file-system',
    SlowFileSystem,
)


class CrashFileSystem(audbackend.FileSystem):
    r"""Emulate a file system that crashes.

    Raises an exception when getting a file from the backend.

    """
    def _get_file(self, *args):
        assert any([os.path.exists(path) for path in DB_LOCK_PATHS])
        raise RuntimeError()


audbackend.register(
    'crash-file-system',
    CrashFileSystem,
)


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_ensure_lock_file_deleted():
    assert not any([os.path.exists(path) for path in DB_LOCK_PATHS])
    yield
    assert not any([os.path.exists(path) for path in DB_LOCK_PATHS])


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_set_repositories(request):
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=pytest.REPOSITORY_NAME,
            host=pytest.FILE_SYSTEM_HOST,
            backend=request.param,
        ),
    ]


DB_NAME = f'test_lock-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_VERSIONS = ['1.0.0', '2.0.0']

DB_LOCK_PATHS = []
for version in DB_VERSIONS:
    DB_LOCK_PATHS.append(
        audeer.path(
            pytest.CACHE_ROOT,
            DB_NAME,
            version,
            '.lock',
        )
    )
    DB_LOCK_PATHS.append(
        audeer.path(
            pytest.CACHE_ROOT,
            DB_NAME,
            version,
            audb.Flavor().short_id,
            '.lock',
        )
    )


def clear_root(root: str):
    audeer.rmdir(root)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_remove_db_from_cache():
    root = audeer.path(pytest.CACHE_ROOT, DB_NAME)
    clear_root(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    audb.config.REPOSITORIES = pytest.REPOSITORIES

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes['scheme'] = audformat.Scheme()
    audformat.testing.add_table(
        db,
        'table',
        'filewise',
        num_files=[0, 1, 2],
    )
    db.save(DB_ROOT)
    audformat.testing.create_audio_files(db)

    # publish 1.0.0

    audb.publish(
        DB_ROOT,
        DB_VERSIONS[0],
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    # publish 2.0.0

    audformat.testing.add_table(
        db,
        'empty',
        'filewise',
        num_files=0,
    )
    db.save(DB_ROOT)
    audb.publish(
        DB_ROOT,
        DB_VERSIONS[1],
        pytest.PUBLISH_REPOSITORY,
        previous_version=DB_VERSIONS[0],
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


def load_deps():
    return audb.dependencies(
        DB_NAME,
        version=DB_VERSIONS[0],
    )


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['slow-file-system'],
    indirect=True,
)
@pytest.mark.parametrize(
    'multiprocessing',
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    'num_workers',
    [
        10,
    ]
)
def test_lock_dependencies(fixture_set_repositories, multiprocessing,
                           num_workers):

    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ['win32', 'darwin']:
        return

    result = audeer.run_tasks(
        load_deps,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers


def load_header():
    return audb.info.header(
        DB_NAME,
        version=DB_VERSIONS[0],
    )


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['slow-file-system'],
    indirect=True,
)
@pytest.mark.parametrize(
    'multiprocessing',
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    'num_workers',
    [
        10,
    ]
)
def test_lock_header(fixture_set_repositories, multiprocessing, num_workers):

    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ['win32', 'darwin']:
        return

    result = audeer.run_tasks(
        load_header,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers


def load_db(timeout):
    return audb.load(
        DB_NAME,
        version=DB_VERSIONS[0],
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['slow-file-system'],
    indirect=True,
)
@pytest.mark.parametrize(
    'multiprocessing',
    [
        False,
        True,
    ]
)
@pytest.mark.parametrize(
    'num_workers, timeout, expected',
    [
        (2, -1, 2),
        (2, 9999, 2),
        (2, 0, 1),
    ]
)
def test_lock_load(fixture_set_repositories, multiprocessing, num_workers,
                   timeout, expected):

    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ['win32', 'darwin']:
        return

    warns = not multiprocessing and num_workers != expected
    with pytest.warns(
            UserWarning if warns else None,
            match=audb.core.define.TIMEOUT_MSG,
    ):
        result = audeer.run_tasks(
            load_db,
            [([timeout], {})] * num_workers,
            num_workers=num_workers,
            multiprocessing=multiprocessing,
        )
    result = [x for x in result if x is not None]

    assert len(result) == expected


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['crash-file-system'],
    indirect=True,
)
def test_lock_load_crash(fixture_set_repositories):

    with pytest.raises(RuntimeError):
        load_db(-1)


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['file-system'],
    indirect=True,
)
def test_lock_load_from_cached_versions(fixture_set_repositories):

    # ensure immediate timeout if cache folder is locked
    cached_version_timeout = audb.core.define.CACHED_VERSIONS_TIMEOUT
    audb.core.define.CACHED_VERSIONS_TIMEOUT = 0

    # load version 1.0.0
    db_v1 = audb.load(
        DB_NAME,
        version=DB_VERSIONS[0],
        verbose=False,
    )

    # load new files added in version 2.0.0
    audb.load(
        DB_NAME,
        version=DB_VERSIONS[1],
        tables='empty',
        verbose=False,
    )

    # switch to crash backend to ensure remaining files
    # must be copied from version 1.0.0
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=pytest.REPOSITORY_NAME,
            host=pytest.FILE_SYSTEM_HOST,
            backend='crash-file-system',
        ),
    ]

    # lock cache folder of version 1.0.0
    def lock_v1():
        with audb.core.lock.FolderLock(db_v1.root):
            event.wait()

    event = threading.Event()
    thread = threading.Thread(target=lock_v1)
    thread.start()

    # -> loading missing table from cache fails
    with pytest.raises(RuntimeError):
        audb.load(
            DB_NAME,
            version=DB_VERSIONS[1],
            tables='table',
            only_metadata=True,
            verbose=False,
        )

    # release cache folder of version 1.0.0
    event.set()
    thread.join()

    # -> loading missing table from cache succeeds
    audb.load(
        DB_NAME,
        version=DB_VERSIONS[1],
        tables='table',
        only_metadata=True,
        verbose=False,
    )

    # lock cache folder of version 1.0.0
    event.clear()
    thread = threading.Thread(target=lock_v1)
    thread.start()

    # -> loading missing media from cache fails
    with pytest.raises(RuntimeError):
        audb.load(
            DB_NAME,
            version=DB_VERSIONS[1],
            verbose=False,
        )

    # release cache folder of version 1.0.0
    event.set()
    thread.join()

    # -> loading missing media from cache succeeds
    audb.load(
        DB_NAME,
        version=DB_VERSIONS[1],
        verbose=False,
    )

    # reset timeout
    audb.core.define.CACHED_VERSIONS_TIMEOUT = cached_version_timeout


def load_media(timeout):
    return audb.load_media(
        DB_NAME,
        'audio/001.wav',
        version=DB_VERSIONS[0],
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['slow-file-system'],
    indirect=True,
)
@pytest.mark.parametrize(
    'multiprocessing',
    [
        False,
        True,
    ]
)
@pytest.mark.parametrize(
    'num_workers, timeout, expected',
    [
        (2, -1, 2),
        (2, 9999, 2),
        (2, 0, 1),
    ]
)
def test_lock_load_media(fixture_set_repositories, multiprocessing,
                         num_workers, timeout, expected):

    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ['win32', 'darwin']:
        return

    warns = not multiprocessing and num_workers != expected
    with pytest.warns(
            UserWarning if warns else None,
            match=audb.core.define.TIMEOUT_MSG,
    ):
        result = audeer.run_tasks(
            load_media,
            [([timeout], {})] * num_workers,
            num_workers=num_workers,
            multiprocessing=multiprocessing,
        )
    result = [x for x in result if x is not None]

    assert len(result) == expected


def load_table():
    return audb.load_table(
        DB_NAME,
        'table',
        version=DB_VERSIONS[0],
        verbose=False,
    )


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['slow-file-system'],
    indirect=True,
)
@pytest.mark.parametrize(
    'multiprocessing',
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    'num_workers',
    [
        10,
    ]
)
def test_lock_load_table(fixture_set_repositories, multiprocessing,
                         num_workers):

    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ['win32', 'darwin']:
        return

    result = audeer.run_tasks(
        load_table,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers
