import os
import shutil

import pytest

import audformat.testing
import audeer

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb.config.REPOSITORIES = pytest.REPOSITORIES


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
    clear_root(pytest.FILE_SYSTEM_HOST)

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
    # Add nested folder structure to ensure not all files in an archive
    # are stored in the same folder
    mapping = {
        'audio/020.wav': 'audio/1/020.wav',
        'audio/021.wav': 'audio/2/021.wav',
    }
    files = db['train'].df.index.get_level_values('file')
    starts = db['train'].df.index.get_level_values('start')
    ends = db['train'].df.index.get_level_values('end')
    db['train'].df.index = audformat.segmented_index(
        files=[mapping[f] for f in files],
        starts=starts,
        ends=ends,
    )
    db.save(DB_ROOT)
    audformat.testing.create_audio_files(db)

    # publish db

    archives = {}
    for table in db.tables:
        archives.update(
            {
                file: table for file in db[table].files
            }
        )
    audb.publish(
        DB_ROOT,
        '1.0.0',
        pytest.PUBLISH_REPOSITORY,
        archives=archives,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_clear_cache():
    clear_root(pytest.CACHE_ROOT)
    yield
    clear_root(pytest.CACHE_ROOT)


@pytest.mark.parametrize(
    'include, exclude, expected_files',
    [
        (
            None,
            None,
            ['audio/000.wav', 'audio/001.wav',
             'audio/010.wav', 'audio/011.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
        ),
        (
            't.*',
            None,
            ['audio/000.wav', 'audio/001.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
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
def test_include_and_exclude(include, exclude, expected_files):
    # Test for backward compatibility
    with pytest.warns(UserWarning):
        db = audb.load(
            DB_NAME,
            include=include,
            exclude=exclude,
            full_path=False,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )
    assert list(db.files) == expected_files


@pytest.mark.parametrize(
    'media, format, expected_files',
    [
        (
            None,
            None,
            ['audio/000.wav', 'audio/001.wav',
             'audio/010.wav', 'audio/011.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
        ),
        (
            [],
            None,
            [],
        ),
        (
            ['audio/000.wav', 'audio/001.wav'],
            None,
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            ['audio/000.wav', 'audio/001.wav'],
            'flac',
            ['audio/000.flac', 'audio/001.flac'],
        ),
        (
            r'.*0\.wav',
            None,
            ['audio/000.wav', 'audio/010.wav', 'audio/1/020.wav'],
        ),
    ]
)
def test_media(media, format, expected_files):
    db = audb.load(
        DB_NAME,
        media=media,
        format=format,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert list(db.files) == expected_files
    assert list(db.tables) == ['dev', 'test', 'train']


@pytest.mark.parametrize(
    'tables, format, expected_tables, expected_files',
    [
        (
            None,
            None,
            ['dev', 'test', 'train'],
            ['audio/000.wav', 'audio/001.wav',
             'audio/010.wav', 'audio/011.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
        ),
        (
            'test',
            None,
            ['test'],
            ['audio/000.wav', 'audio/001.wav'],
        ),
        (
            't.*',
            None,
            ['test', 'train'],
            ['audio/000.wav', 'audio/001.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
        ),
        (
            ['dev', 'train'],
            None,
            ['dev', 'train'],
            ['audio/010.wav', 'audio/011.wav',
             'audio/1/020.wav', 'audio/2/021.wav'],
        ),
        (
            ['dev', 'train'],
            'flac',
            ['dev', 'train'],
            ['audio/010.flac', 'audio/011.flac',
             'audio/1/020.flac', 'audio/2/021.flac'],
        ),
        (
            'bad',
            None,
            [],
            [],
        ),
    ]
)
def test_tables(tables, format, expected_tables, expected_files):
    db = audb.load(
        DB_NAME,
        tables=tables,
        format=format,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert list(db.tables) == expected_tables
    assert list(db.files) == expected_files
