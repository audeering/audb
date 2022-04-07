import os
import shutil
import time

import pandas as pd
import pytest

import audformat.testing
import audeer
import audiofile

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_lock-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_VERSION = '1.0.0'


def clear_root(root: str):
    root = audeer.path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


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


def load_db(timeout):
    return audb.load(
        DB_NAME,
        version=DB_VERSION,
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    'multiprocessing',
    [
        (False, True),
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
def test_lock_load(multiprocessing, num_workers, timeout, expected):

    result = audeer.run_tasks(
        load_db,
        [([timeout], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )
    result = [x for x in result if x is not None]

    assert len(result) == expected


def load_header(timeout):
    return audb.info.header(
        DB_NAME,
        version=DB_VERSION,
        timeout=timeout,
    )


@pytest.mark.parametrize(
    'multiprocessing',
    [
        (False, True),
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
def test_lock_header(multiprocessing, num_workers, timeout, expected):

    result = audeer.run_tasks(
        load_header,
        [([timeout], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )
    result = [x for x in result if x is not None]

    assert len(result) == expected


def load_media(timeout):
    return audb.load_media(
        DB_NAME,
        'audio/001.wav',
        version=DB_VERSION,
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    'multiprocessing',
    [
        (False, True),
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
def test_lock_load_media(multiprocessing, num_workers, timeout, expected):

    result = audeer.run_tasks(
        load_media,
        [([timeout], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )
    result = [x for x in result if x is not None]

    assert len(result) == expected


def load_table(timeout):
    return audb.load_table(
        DB_NAME,
        'table',
        version=DB_VERSION,
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    'multiprocessing',
    [
        (False, True),
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
def test_lock_load_table(multiprocessing, num_workers, timeout, expected):

    result = audeer.run_tasks(
        load_table,
        [([timeout], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )
    result = [x for x in result if x is not None]

    assert len(result) == expected
