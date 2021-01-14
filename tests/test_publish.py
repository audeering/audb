import os
import shutil

import pytest

import audformat.testing
import audeer
import audiofile

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORIES = [pytest.REPOSITORY]
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = 'test_publish'
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
    audformat.testing.create_audio_files(db, DB_ROOT)
    db.save(DB_ROOT, storage_format=audformat.define.TableStorageFormat.PICKLE)

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.mark.parametrize(
    'version',
    [
        '1.0.0',
        pytest.param(
            '1.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        '2.0.0',
    ]
)
def test_publish(version):

    db = audformat.Database.load(DB_ROOT)

    if not audb2.versions(DB_NAME, backend=BACKEND):
        with pytest.raises(RuntimeError):
            audb2.latest_version(DB_NAME, backend=BACKEND)

    archives = db['files']['speaker'].get().dropna().to_dict()
    depend = audb2.publish(
        DB_ROOT, version, pytest.REPOSITORY, archives=archives,
        group_id=pytest.GROUP_ID, backend=BACKEND,
        num_workers=pytest.NUM_WORKERS, verbose=False,
    )
    versions = audb2.versions(
        DB_NAME, group_id=pytest.GROUP_ID, backend=BACKEND,
    )
    latest_version = audb2.latest_version(
        DB_NAME, group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    assert version in versions
    assert latest_version == versions[-1]

    df = audb2.available(pytest.GROUP_ID, latest_only=False, backend=BACKEND)
    assert DB_NAME in df.index
    assert set(df[df.index == DB_NAME]['version']) == set(versions)

    df = audb2.available(pytest.GROUP_ID, latest_only=True, backend=BACKEND)
    assert DB_NAME in df.index
    assert df[df.index == DB_NAME]['version'][0] == latest_version

    for file in db.files:
        BACKEND.exists(
            file, version, pytest.REPOSITORY,
            f'{pytest.GROUP_ID}.{db.name}.media',
            name=archives[file] if file in archives else None,
        )
        path = os.path.join(DB_ROOT, file)
        assert depend.channels(file) == audiofile.channels(path)
        assert depend.checksum(file) == audb2.core.utils.md5(path)
        assert depend.duration(file) == audiofile.duration(path)


@pytest.mark.parametrize(
    'name',
    ['?', '!', ','],
)
def test_invalid_archives(name):

    archives = {
        os.path.join('audio', '001.wav'): name
    }
    with pytest.raises(ValueError):
        audb2.publish(
            DB_ROOT, 'x.x.x', pytest.REPOSITORY, archives=archives,
            group_id=pytest.GROUP_ID, backend=BACKEND,
            num_workers=pytest.NUM_WORKERS, verbose=False,
        )
