import os
import shutil

import pandas as pd
import pytest

import audiofile

import audata.testing
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
    ['1.0.0', '1.1.0', '1.1.1', '2.0.0']
}
BACKEND = audb2.backend.FileSystem(DB_NAME, pytest.HOST)


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
        labels=['positive', 'neutral', 'negative']
    )
    audata.testing.add_table(
        db,
        'emotion',
        audata.define.TableType.SEGMENTED,
        num_files=5,
        columns={'emotion': ('scheme', None)}
    )
    db.schemes['speaker'] = audata.Scheme(
        labels=['adam', 'eve']
    )
    db['files'] = audata.Table(db.files)
    db['files']['speaker'] = audata.Column(scheme_id='speaker')
    db['files']['speaker'].set(
        ['adam', 'adam', 'eva', 'eva'],
        files=db.files[:4],
    )

    # publish 1.0.0

    audata.testing.create_audio_files(db, DB_ROOT_VERSION['1.0.0'])
    db.save(DB_ROOT_VERSION['1.0.0'])
    archives = db['files']['speaker'].get().dropna().to_dict()
    audb2.publish(
        DB_ROOT_VERSION['1.0.0'], '1.0.0', archives=archives, backend=BACKEND,
    )

    # publish 1.1.0, add table

    audata.testing.add_table(
        db,
        'train',
        audata.define.TableType.SEGMENTED,
        columns={'label': ('scheme', None)}
    )

    audata.testing.create_audio_files(db, DB_ROOT_VERSION['1.1.0'])
    db.save(DB_ROOT_VERSION['1.1.0'])
    audb2.publish(
        DB_ROOT_VERSION['1.1.0'], '1.1.0', backend=BACKEND,
    )

    # publish 1.1.1, change label

    db['train'].df['label'][0] = None

    audata.testing.create_audio_files(db, DB_ROOT_VERSION['1.1.1'])
    db.save(DB_ROOT_VERSION['1.1.1'])
    audb2.publish(
        DB_ROOT_VERSION['1.1.1'], '1.1.1', backend=BACKEND,
    )

    # publish 2.0.0, alter and remove media

    audata.testing.create_audio_files(db, DB_ROOT_VERSION['2.0.0'])
    file = os.path.join(DB_ROOT_VERSION['2.0.0'], db.files[0])
    y, sr = audiofile.read(file)
    y[0] = 1
    audiofile.write(file, y, sr)
    file = db.files[-1]
    db.filter_files(lambda x: x != file)
    os.remove(audeer.safe_path(os.path.join(DB_ROOT_VERSION['2.0.0'], file)))

    db.save(DB_ROOT_VERSION['2.0.0'])
    audb2.publish(
        DB_ROOT_VERSION['2.0.0'], '2.0.0', backend=BACKEND,
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
        pytest.param(
            '3.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ]
)
def test_load(version):

    db = audb2.load(
        DB_NAME,
        version,
        full_path=False,
        backend=BACKEND,
    )
    db_root = db.meta['audb']['root']

    if version is None:
        version = audb2.latest_version(DB_NAME, backend=BACKEND)
    db_original = audata.Database.load(DB_ROOT_VERSION[version])
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )
