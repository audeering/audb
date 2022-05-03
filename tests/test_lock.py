import os
import shutil
import sys
import time

import pandas as pd
import pytest

import audbackend
import audeer
import audformat.testing
import audiofile

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
        assert os.path.exists(DB_FLAVOR_LOCK_PATH) or \
               os.path.exists(DB_LOCK_PATH)
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
DB_VERSION = '1.0.0'

DB_LOCK_PATH = audeer.path(
    pytest.CACHE_ROOT,
    DB_NAME,
    DB_VERSION,
    '.lock',
)
DB_FLAVOR_LOCK_PATH = audeer.path(
    pytest.CACHE_ROOT,
    DB_NAME,
    DB_VERSION,
    audb.Flavor().short_id,
    '.lock',
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
        DB_VERSION,
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


def load_deps():
    return audb.dependencies(
        DB_NAME,
        version=DB_VERSION,
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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

    result = audeer.run_tasks(
        load_deps,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)


def load_header():
    return audb.info.header(
        DB_NAME,
        version=DB_VERSION,
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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

    result = audeer.run_tasks(
        load_header,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)


def load_db(timeout):
    return audb.load(
        DB_NAME,
        version=DB_VERSION,
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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)


@pytest.mark.parametrize(
    'fixture_set_repositories',
    ['crash-file-system'],
    indirect=True,
)
def test_lock_load_crash(fixture_set_repositories):

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

    with pytest.raises(RuntimeError):
        load_db(-1)

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)


def load_media(timeout):
    return audb.load_media(
        DB_NAME,
        'audio/001.wav',
        version=DB_VERSION,
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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)


def load_table():
    return audb.load_table(
        DB_NAME,
        'table',
        version=DB_VERSION,
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

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)

    result = audeer.run_tasks(
        load_table,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers

    assert not os.path.exists(DB_LOCK_PATH)
    assert not os.path.exists(DB_FLAVOR_LOCK_PATH)
