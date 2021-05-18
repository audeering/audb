import os
import shutil

import pandas as pd
import pytest

import audiofile

import audformat.testing
import audeer

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_load_on_demand-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_VERSION = '1.0.0'


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


def test_load_on_demand():

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
    pd.testing.assert_index_equal(db.files, db_original.files)
    assert not db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        tables=['table1'],
        full_path=False,
        verbose=False,
    )

    assert db['table1'] == db_original['table1']
    assert 'table2' not in db.tables
    pd.testing.assert_index_equal(db.files, db_original['table1'].files)
    assert not db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        tables='.*1',
        full_path=False,
        verbose=False,
    )

    assert db['table1'] == db_original['table1']
    assert 'table2' not in db.tables
    pd.testing.assert_index_equal(db.files, db_original['table1'].files)
    assert not db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        tables=['table1'],
        full_path=False,
        verbose=False,
    )

    assert db['table1'] == db_original['table1']
    assert 'table2' not in db.tables
    pd.testing.assert_index_equal(db.files, db_original['table1'].files)
    assert not db.meta['audb']['complete']

    # Remove table to force downloading from backend again
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
    pd.testing.assert_index_equal(db.files, db_original.files)
    assert not db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        media=['audio/000.wav', 'audio/001.wav'],
        full_path=False,
        verbose=False,
    )

    assert 'table1' in db.tables
    assert 'table2' in db.tables
    pd.testing.assert_index_equal(
        db.files,
        audformat.filewise_index(['audio/000.wav', 'audio/001.wav']),
    )
    assert not db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        tables=['table2'],
        full_path=False,
        verbose=False,
    )

    assert 'table1' not in db.tables
    assert db['table2'] == db_original['table2']
    pd.testing.assert_index_equal(db.files, db_original['table2'].files)
    assert db.meta['audb']['complete']

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_original.meta = []
    db.meta = []
    assert db == db_original
