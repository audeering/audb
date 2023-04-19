import os
import shutil

import pandas as pd
import pytest

import audformat.testing
import audeer

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


@pytest.fixture(
    scope='session',
    autouse=True,
)
def fixture_set_repositories():
    audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_load_on_demand-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_VERSION = '1.0.0'


def clear_root(root: str):
    root = audeer.path(root)
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
    db.schemes['scheme'] = audformat.Scheme()
    audformat.testing.add_table(
        db,
        'table1',
        'filewise',
        num_files=[0, 1, 2],
    )
    audformat.testing.add_table(
        db,
        'table2',
        'filewise',
        num_files=[1, 2, 3],
    )
    audformat.testing.add_misc_table(
        db,
        'misc-in-scheme',
        pd.Index([0, 1, 2], dtype='Int64', name='idx'),
        columns={'emotion': ('scheme', None)}
    )
    audformat.testing.add_misc_table(
        db,
        'misc-not-in-scheme',
        pd.Index([0, 1, 2], dtype='Int64', name='idx'),
        columns={'emotion': ('scheme', None)}
    )
    db.schemes['misc'] = audformat.Scheme(
        'int',
        labels='misc-in-scheme',
    )
    db.attachments['file'] = audformat.Attachment('file.txt')
    db.attachments['folder'] = audformat.Attachment('folder/')
    audeer.mkdir(audeer.path(DB_ROOT, 'folder'))
    audeer.touch(audeer.path(DB_ROOT, 'file.txt'))
    audeer.touch(audeer.path(DB_ROOT, 'folder/file1.txt'))
    audeer.touch(audeer.path(DB_ROOT, 'folder/file2.txt'))
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


def test_load_only_metadata():

    db_original = audformat.Database.load(DB_ROOT)

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

    assert db['table1'] == db_original['table1']
    assert db['table2'] == db_original['table2']
    assert db['misc-in-scheme'] == db_original['misc-in-scheme']
    assert db['misc-not-in-scheme'] == db_original['misc-not-in-scheme']
    pd.testing.assert_index_equal(db.files, db_original.files)
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        assert not os.path.exists(path)
    for file in list(db.files):
        path = audeer.path(db.root, file)
        assert not os.path.exists(path)
    assert not db.meta['audb']['complete']

    # Delete table1
    # to force downloading from backend again
    os.remove(os.path.join(db.meta['audb']['root'], 'db.table1.csv'))
    os.remove(os.path.join(db.meta['audb']['root'], 'db.table1.pkl'))
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )
    assert db['table1'] == db_original['table1']
    assert db['table2'] == db_original['table2']
    assert db['misc-in-scheme'] == db_original['misc-in-scheme']
    assert db['misc-not-in-scheme'] == db_original['misc-not-in-scheme']
    pd.testing.assert_index_equal(db.files, db_original.files)
    assert not db.meta['audb']['complete']

    # Load whole database
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_original.meta = []
    db.meta = []
    assert db == db_original


@pytest.mark.parametrize(
    'attachments, '
    'media, '
    'tables, '
    'expected_attachments, '
    'expected_media, '
    'expected_tables, '
    'complete',
    [
        (
            None,
            None,
            None,
            ['file', 'folder'],
            [
                'audio/000.wav',
                'audio/001.wav',
                'audio/002.wav',
                'audio/003.wav',
            ],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            True,
        ),
        (
            [],
            None,
            None,
            [],
            [
                'audio/000.wav',
                'audio/001.wav',
                'audio/002.wav',
                'audio/003.wav',
            ],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            False,
        ),
        (
            None,
            [],
            None,
            ['file', 'folder'],
            [],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            False,
        ),
        (
            None,
            None,
            [],
            ['file', 'folder'],
            [],
            ['misc-in-scheme'],  # misc table used in scheme is still loaded
            False,
        ),
        (
            [],
            [],
            [],
            [],
            [],
            ['misc-in-scheme'],
            False,
        ),
        (
            [],
            [],
            None,
            [],
            [],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            False,
        ),
        (
            [],
            None,
            [],
            [],
            [],
            ['misc-in-scheme'],
            False,
        ),
        (
            None,
            [],
            [],
            ['file', 'folder'],
            [],
            ['misc-in-scheme'],
            False,
        ),
        (
            'folder',
            None,
            None,
            ['folder'],
            [
                'audio/000.wav',
                'audio/001.wav',
                'audio/002.wav',
                'audio/003.wav',
            ],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            False,
        ),
        (
            ['file'],
            None,
            ['table1'],
            ['file'],
            [
                'audio/000.wav',
                'audio/001.wav',
                'audio/002.wav',
            ],
            ['misc-in-scheme', 'table1'],
            False,
        ),
        (
            ['file', 'folder'],
            None,
            '.*1',
            ['file', 'folder'],
            [
                'audio/000.wav',
                'audio/001.wav',
                'audio/002.wav',
            ],
            ['misc-in-scheme', 'table1'],
            False,
        ),
        (
            None,
            '.3.wav',
            None,
            ['file', 'folder'],
            [
                'audio/003.wav',
            ],
            ['misc-in-scheme', 'misc-not-in-scheme', 'table1', 'table2'],
            False,
        ),
    ]
)
def test_load_filter(
        tmpdir,
        attachments,
        media,
        tables,
        expected_attachments,
        expected_media,
        expected_tables,
        complete,
):

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        attachments=attachments,
        media=media,
        tables=tables,
        full_path=False,
        cache_root=tmpdir,
        verbose=False,
    )
    assert list(db) == expected_tables
    assert list(db.files) == expected_media
    assert list(db.attachments) == ['file', 'folder']
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if attachment in expected_attachments:
            assert os.path.exists(path)
            for file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, file))
        else:
            assert not os.path.exists(path)
    assert db.meta['audb']['complete'] == complete
