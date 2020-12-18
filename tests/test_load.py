import os
import shutil

import pandas as pd
import pytest

import audiofile

import audformat.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORY_PUBLIC = pytest.REPOSITORY_PUBLIC
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = 'test_load'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['1.0.0', '1.1.0', '1.1.1', '2.0.0', '3.0.0']
}
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

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes['scheme'] = audformat.Scheme(
        labels=['positive', 'neutral', 'negative']
    )
    audformat.testing.add_table(
        db,
        'emotion',
        audformat.define.IndexType.SEGMENTED,
        num_files=5,
        columns={'emotion': ('scheme', None)}
    )
    db.schemes['speaker'] = audformat.Scheme(
        labels=['adam', 'eve']
    )
    db['files'] = audformat.Table(db.files)
    db['files']['speaker'] = audformat.Column(scheme_id='speaker')
    db['files']['speaker'].set(
        ['adam', 'adam', 'eve', 'eve'],
        index=audformat.filewise_index(db.files[:4]),
    )

    # publish 1.0.0

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['1.0.0'])
    db.save(DB_ROOT_VERSION['1.0.0'])
    archives = db['files']['speaker'].get().dropna().to_dict()
    audb2.publish(
        DB_ROOT_VERSION['1.0.0'], '1.0.0', archives=archives,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    # publish 1.1.0, add table

    audformat.testing.add_table(
        db, 'train', audformat.define.IndexType.SEGMENTED,
        columns={'label': ('scheme', None)}
    )

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['1.1.0'])
    db.save(DB_ROOT_VERSION['1.1.0'])
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['1.0.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['1.1.0'], 'db.csv'),
    )
    audb2.publish(
        DB_ROOT_VERSION['1.1.0'], '1.1.0',
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    # publish 1.1.1, change label

    db['train'].df['label'][0] = None

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['1.1.1'])
    db.save(DB_ROOT_VERSION['1.1.1'])
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['1.1.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['1.1.1'], 'db.csv'),
    )
    audb2.publish(
        DB_ROOT_VERSION['1.1.1'], '1.1.1',
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    # publish 2.0.0, alter and remove media

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['2.0.0'])
    file = os.path.join(DB_ROOT_VERSION['2.0.0'], db.files[0])
    y, sr = audiofile.read(file)
    y[0] = 1
    audiofile.write(file, y, sr)
    file = db.files[-1]
    db.pick_files(lambda x: x != file)
    os.remove(audeer.safe_path(os.path.join(DB_ROOT_VERSION['2.0.0'], file)))

    db.save(DB_ROOT_VERSION['2.0.0'])
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['1.1.1'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['2.0.0'], 'db.csv'),
    )
    audb2.publish(
        DB_ROOT_VERSION['2.0.0'], '2.0.0',
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    # publish 3.0.0, remove table

    db.drop_tables('train')

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['3.0.0'])
    db.save(DB_ROOT_VERSION['3.0.0'])
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['2.0.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['3.0.0'], 'db.csv'),
    )
    audb2.publish(
        DB_ROOT_VERSION['3.0.0'], '3.0.0',
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.mark.parametrize(
    'version',
    [
        None,
        '1.0.0',
        '1.1.0',
        '1.1.1',
        '2.0.0',
        '3.0.0',
        pytest.param(
            '4.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ]
)
def test_load(version):

    db = audb2.load(
        DB_NAME, version=version, full_path=False,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )
    db_root = db.meta['audb']['root']

    if version is None:
        version = audb2.latest_version(
            DB_NAME, group_id=pytest.GROUP_ID, backend=BACKEND,
        )
    db_original = audformat.Database.load(DB_ROOT_VERSION[version])

    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )

    # from cache with full path

    db = audb2.load(
        DB_NAME, version=version, full_path=True,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )
    for file in db.files:
        assert os.path.exists(file)
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))


@pytest.mark.parametrize(
    'version',
    [
        None,
        '1.0.0',
        '1.1.0',
        '1.1.1',
        '2.0.0',
        '3.0.0',
        pytest.param(
            '4.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ]
)
def test_load_raw(version):

    db_root = os.path.join(DB_ROOT, 'raw')

    db = audb2.load_raw(
        db_root, DB_NAME, version=version,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    if version is None:
        version = audb2.latest_version(
            DB_NAME, group_id=pytest.GROUP_ID, backend=BACKEND,
        )
    db_original = audformat.Database.load(DB_ROOT_VERSION[version])

    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )
