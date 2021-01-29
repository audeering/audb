import os
import shutil

import pytest

import audformat.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.REPOSITORIES = pytest.REPOSITORIES
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = f'test_filter-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')


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

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes['scheme'] = audformat.Scheme(
        labels=['some', 'test', 'labels']
    )
    audformat.testing.add_table(
        db,
        'test',
        audformat.define.IndexType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[0, 1],
    )
    audformat.testing.add_table(
        db,
        'dev',
        audformat.define.IndexType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[10, 11],
    )
    audformat.testing.add_table(
        db,
        'train',
        audformat.define.IndexType.SEGMENTED,
        columns={'label': ('scheme', None)},
        num_files=[20, 21],
    )
    audformat.testing.create_audio_files(db, DB_ROOT)
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
        DB_ROOT,
        '1.0.0',
        pytest.REPOSITORY,
        archives=archives,
        backend=pytest.BACKEND,
        host=pytest.HOST,
        verbose=False,
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
        DB_NAME,
        tables=tables,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
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
        DB_NAME,
        include=include,
        exclude=exclude,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert list(db.files) == expected_files
