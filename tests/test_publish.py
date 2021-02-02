import os
import shutil

import pytest

import audformat.testing
import audeer
import audiofile

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.REPOSITORIES = pytest.REPOSITORIES
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = f'test_publish-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['1.0.0', '2.0.0', '3.0.0']
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
        labels=['adam', '11']
    )
    db['files'] = audformat.Table(db.files)
    db['files']['speaker'] = audformat.Column(scheme_id='speaker')
    db['files']['speaker'].set(
        ['adam', 'adam', '11', '11'],
        index=audformat.filewise_index(db.files[:4]),
    )
    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['1.0.0'])
    db.save(
        DB_ROOT_VERSION['1.0.0'],
        storage_format=audformat.define.TableStorageFormat.PICKLE,
    )

    # Extend version 2.0.0 by a new file
    new_file = os.path.join('audio', 'new.wav')
    db['files'].extend_index(audformat.filewise_index(new_file), inplace=True)
    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['2.0.0'])
    db.save(DB_ROOT_VERSION['2.0.0'])

    # Remove one file in version 3.0.0
    remove_file = os.path.join('audio', '001.wav')
    db.drop_files(remove_file)
    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['3.0.0'])
    db.save(DB_ROOT_VERSION['3.0.0'])

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


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
            DB_ROOT_VERSION['1.0.0'],
            '1.0.1',
            pytest.REPOSITORY,
            archives=archives,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )


@pytest.mark.parametrize(
    'version',
    [
        '1.0.0',
        pytest.param(
            '1.0.0',
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        '2.0.0',
        '3.0.0',
    ]
)
def test_publish(version):

    db = audformat.Database.load(DB_ROOT_VERSION[version])

    if not audb2.versions(DB_NAME):
        with pytest.raises(RuntimeError):
            audb2.latest_version(DB_NAME)

    archives = db['files']['speaker'].get().dropna().to_dict()
    deps = audb2.publish(
        DB_ROOT_VERSION[version],
        version,
        pytest.REPOSITORY,
        archives=archives,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    _, _, backend = audb2.core.api.lookup(DB_NAME, version)

    db = audb2.load(
        DB_NAME,
        version=version,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
    )
    assert db.name == DB_NAME

    versions = audb2.versions(DB_NAME)
    latest_version = audb2.latest_version(DB_NAME)

    assert version in versions
    assert latest_version == versions[-1]

    df = audb2.available(latest_only=False)
    assert DB_NAME in df.index
    assert set(df[df.index == DB_NAME]['version']) == set(versions)

    df = audb2.available(latest_only=True)
    assert DB_NAME in df.index
    assert df[df.index == DB_NAME]['version'][0] == latest_version

    for file in db.files:
        name = archives[file] if file in archives else file
        file_path = backend.join(db.name, 'media', name)
        backend.exists(file_path, version, pytest.REPOSITORY_NAME)
        path = os.path.join(DB_ROOT_VERSION[version], file)
        assert deps.checksum(file) == audb2.core.utils.md5(path)
        if deps.format(file) in [
            audb2.define.Format.WAV,
            audb2.define.Format.FLAC,
        ]:
            assert deps.bit_depth(file) == audiofile.bit_depth(path)
            assert deps.channels(file) == audiofile.channels(path)
            assert deps.duration(file) == audiofile.duration(path)
            assert deps.sampling_rate(file) == audiofile.sampling_rate(path)


@pytest.mark.parametrize(
    'version1, version2, media_difference',
    [
        (
            '1.0.0',
            '1.0.0',
            [],
        ),
        (
            '1.0.0',
            '2.0.0',
            [],
        ),
        (
            '2.0.0',
            '1.0.0',
            [os.path.join('audio', 'new.wav')],
        ),
        (
            '2.0.0',
            '3.0.0',
            [os.path.join('audio', '001.wav')],
        ),
        (
            '3.0.0',
            '2.0.0',
            [],
        ),
        (
            '1.0.0',
            '3.0.0',
            [os.path.join('audio', '001.wav')],
        ),
        (
            '3.0.0',
            '1.0.0',
            [os.path.join('audio', 'new.wav')],
        ),
    ]
)
def test_publish_changed_db(version1, version2, media_difference):

    depend1 = audb2.dependencies(DB_NAME, version=version1)
    depend2 = audb2.dependencies(DB_NAME, version=version2)

    media1 = set(sorted(depend1.media))
    media2 = set(sorted(depend2.media))

    assert media1 - media2 == set(media_difference)
