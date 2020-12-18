import os
import shutil

import pytest

import audata.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORY_PUBLIC = pytest.REPOSITORY_PUBLIC
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = 'test_filter'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
BACKEND = audb2.backend.FileSystem(pytest.HOST)


def clear_root(root: str):
    root = audeer.safe_path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)

    # create db

    db = audata.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes['scheme'] = audata.Scheme(
        labels=['some', 'test', 'labels']
    )
    audata.testing.add_table(
        db,
        'test',
        audata.define.TableType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[0, 1],
    )
    audata.testing.add_table(
        db,
        'dev',
        audata.define.TableType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[10, 11],
    )
    audata.testing.add_table(
        db,
        'train',
        audata.define.TableType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[20, 21],
    )
    audata.testing.create_audio_files(db, DB_ROOT)
    db.save(DB_ROOT)

    # publish db

    archives = {}
    for table in db.tables:
        archives.update(
            {
                file: table for file in db[table].files
            }
        )
    audb2.publish(
        DB_ROOT, '1.0.0', archives=archives,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_clear_cache():
    clear_root(pytest.CACHE_ROOT)
    yield
    clear_root(pytest.CACHE_ROOT)


@pytest.mark.parametrize(
    'tables, expected_tables, expected_files',
    [
        (
            None,
            ['dev', 'test', 'train'],
            ['audio/000.wav', 'audio/001.wav',
             'audio/010.wav', 'audio/011.wav',
             'audio/020.wav', 'audio/021.wav'],
        ),
        (
            'test',
            ['test'],
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            't.*',
            ['test', 'train'],
            ['audio/000.wav', 'audio/001.wav',
             'audio/020.wav', 'audio/021.wav'],
        ),
        (
            ['dev', 'train'],
            ['dev', 'train'],
            ['audio/010.wav', 'audio/011.wav',
             'audio/020.wav', 'audio/021.wav'],
        ),
        (
            'bad',
            [],
            [],
        ),
    ]
)
def test_tables(tables, expected_tables, expected_files):
    db = audb2.load(
        DB_NAME, tables=tables, full_path=False,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )
    assert list(db.tables) == expected_tables
    assert list(db.files) == expected_files


@pytest.mark.parametrize(
    'include, exclude, expected_files',
    [
        (
            None,
            None,
            ['audio/000.wav', 'audio/001.wav',
             'audio/010.wav', 'audio/011.wav',
             'audio/020.wav', 'audio/021.wav'],
        ),
        (
            't.*',
            None,
            ['audio/000.wav', 'audio/001.wav',
             'audio/020.wav', 'audio/021.wav'],
        ),
        (
            None,
            't.*',
            ['audio/010.wav', 'audio/011.wav'],
        ),
        (
            't.*',
            'train',
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            ['test'],
            None,
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            None,
            ['train', 'dev'],
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            'test',
            'test',
            [],
        ),
    ]
)
def test_files(include, exclude, expected_files):
    db = audb2.load(
        DB_NAME, include=include, exclude=exclude,
        group_id=pytest.GROUP_ID, full_path=False, backend=BACKEND,
    )
    assert list(db.files) == expected_files
