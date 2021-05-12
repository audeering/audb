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


DB_NAME = f'test_load-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['1.0.0', '1.1.0', '1.1.1', '2.0.0', '3.0.0']
}


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

    db.save(DB_ROOT_VERSION['1.0.0'])
    audformat.testing.create_audio_files(db)
    archives = db['files']['speaker'].get().dropna().to_dict()
    audb.publish(
        DB_ROOT_VERSION['1.0.0'],
        '1.0.0',
        pytest.PUBLISH_REPOSITORY,
        archives=archives,
        verbose=False,
    )

    # publish 1.1.0, add table

    audformat.testing.add_table(
        db, 'train', audformat.define.IndexType.SEGMENTED,
        columns={'label': ('scheme', None)}
    )

    db.save(DB_ROOT_VERSION['1.1.0'])
    audformat.testing.create_audio_files(db)
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['1.0.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['1.1.0'], 'db.csv'),
    )
    audb.publish(
        DB_ROOT_VERSION['1.1.0'],
        '1.1.0',
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    # publish 1.1.1, change label

    db['train'].df['label'][0] = None

    db.save(DB_ROOT_VERSION['1.1.1'])
    audformat.testing.create_audio_files(db)
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['1.1.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['1.1.1'], 'db.csv'),
    )
    audb.publish(
        DB_ROOT_VERSION['1.1.1'],
        '1.1.1',
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    # publish 2.0.0, alter and remove media

    db.save(DB_ROOT_VERSION['2.0.0'])
    audformat.testing.create_audio_files(db)
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
    audb.publish(
        DB_ROOT_VERSION['2.0.0'],
        '2.0.0',
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    # publish 3.0.0, remove table

    db.drop_tables('train')

    db.save(DB_ROOT_VERSION['3.0.0'])
    audformat.testing.create_audio_files(db)
    shutil.copy(
        os.path.join(DB_ROOT_VERSION['2.0.0'], 'db.csv'),
        os.path.join(DB_ROOT_VERSION['3.0.0'], 'db.csv'),
    )
    audb.publish(
        DB_ROOT_VERSION['3.0.0'],
        '3.0.0',
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


def test_database_cache_folder():
    cache_root = os.path.join(pytest.CACHE_ROOT, 'cache')
    version = '1.0.0'
    db_root = audb.core.load.database_cache_folder(
        DB_NAME,
        version,
        cache_root,
    )
    expected_db_root = os.path.join(
        cache_root,
        DB_NAME,
        version,
    )
    assert db_root == expected_db_root


@pytest.mark.parametrize(
    'format',
    [
        None,
        'flac',
    ]
)
@pytest.mark.parametrize(
    'version',
    [
        None,  # 3.0.0
        '1.0.0',
        '1.1.0',
        '1.1.1',
        '2.0.0',
        pytest.param(
            '4.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ]
)
def test_load(format, version):

    assert not audb.exists(
        DB_NAME,
        version=version,
        format=format,
    )

    db = audb.load(
        DB_NAME,
        version=version,
        format=format,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db_root = db.meta['audb']['root']

    assert audb.exists(DB_NAME, version=version)

    if version is None:
        resolved_version = audb.latest_version(DB_NAME)
    else:
        resolved_version = version
    db_original = audformat.Database.load(DB_ROOT_VERSION[resolved_version])

    if format is not None:
        db_original.map_files(
            lambda x: audeer.replace_file_extension(x, format)
        )

    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )

    df = audb.cached()
    assert df.loc[db_root]['version'] == resolved_version

    deps = audb.dependencies(DB_NAME, version=version)
    assert str(deps().to_string()) == str(deps)
    assert len(deps) == len(db.files) + len(db.tables)

    # from cache with full path

    db = audb.load(
        DB_NAME,
        version=version,
        full_path=True,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    for file in db.files:
        assert os.path.exists(file)
    for table in db.tables:
        assert os.path.exists(os.path.join(db_root, f'db.{table}.csv'))


@pytest.mark.parametrize(
    'version, media, format',
    [
        (
            '1.0.0',
            [],
            None,
        ),
        (
            '1.0.0',
            'audio/001.wav',
            'wav',
        ),
        pytest.param(
            '1.0.0',
            ['audio/001.flac', 'audio/002.flac'],
            'flac',
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            '1.0.0',
            ['audio/001.wav', 'audio/002.wav'],
            'flac',
        ),
        (
            None,
            ['audio/001.wav'],
            None,
        ),
    ]
)
def test_load_media(version, media, format):

    paths = audb.load_media(
        DB_NAME,
        media,
        version=version,
        format=format,
        verbose=False,
    )
    expected_paths = [
        os.path.join(pytest.CACHE_ROOT, p)
        for p in paths
    ]
    if format is not None:
        expected_paths = [
            audeer.replace_file_extension(p, format)
            for p in expected_paths
        ]
    assert paths == expected_paths

    # Clear cache to force loading from other cache
    if version is None:
        version = audb.latest_version(DB_NAME)
    cache_root = audb.core.load.database_cache_folder(
        DB_NAME,
        version,
        pytest.CACHE_ROOT,
        audb.Flavor(format=format),
    )
    shutil.rmtree(cache_root)
    paths2 = audb.load_media(
        DB_NAME,
        media,
        version=version,
        format=format,
        verbose=False,
    )
    assert paths2 == paths


@pytest.mark.parametrize(
    'version, table',
    [
        (
            '1.0.0',
            'emotion',
        ),
        pytest.param(
            '1.0.0',
            'non-existing-table',
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            None,
            'emotion',
        ),
    ]
)
def test_load_table(version, table):

    df = audb.load_table(
        DB_NAME,
        table,
        version=version,
        verbose=False,
    )
    if version is None:
        expected_files = [
            'audio/001.wav',
            'audio/002.wav',
            'audio/003.wav',
            'audio/004.wav',
        ]
    elif version == '1.0.0':
        expected_files = [
            'audio/001.wav',
            'audio/002.wav',
            'audio/003.wav',
            'audio/004.wav',
            'audio/005.wav',
        ]
    files = sorted(list(set(df.index.get_level_values('file'))))
    assert files == expected_files


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
def test_load_to(version):

    db_root = os.path.join(DB_ROOT, 'raw')

    db = audb.load_to(
        db_root,
        DB_NAME,
        version=version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    if version is None:
        version = audb.latest_version(DB_NAME)
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


@pytest.mark.parametrize(
    'name, version',
    [
        (DB_NAME, None),
        (DB_NAME, '1.0.0'),
        pytest.param(  # database does not exist
            'does-not-exist', None,
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        pytest.param(  # version does not exist
            DB_NAME, 'does-not-exist',
            marks=pytest.mark.xfail(raises=RuntimeError),
        )
    ]
)
def test_repository(name, version):
    repository = audb.repository(name, version)
    assert repository == pytest.PUBLISH_REPOSITORY
