import os
import re
import shutil

import pytest

import audbackend
import audeer
import audformat.testing
import audiofile

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_publish-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['1.0.0', '2.0.0', '2.1.0', '3.0.0', '4.0.0', '5.0.0', '6.0.0']
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
    db.save(
        DB_ROOT_VERSION['1.0.0'],
        storage_format=audformat.define.TableStorageFormat.PICKLE,
    )
    audformat.testing.create_audio_files(db)

    # Extend version 2.0.0 by a new file
    new_file = os.path.join('audio', 'new.wav')
    db['files'].extend_index(audformat.filewise_index(new_file), inplace=True)
    db.save(DB_ROOT_VERSION['2.0.0'])
    audformat.testing.create_audio_files(db)

    # Remove one file in version 3.0.0
    remove_file = os.path.join('audio', '001.wav')
    db.drop_files(remove_file)
    db.save(DB_ROOT_VERSION['3.0.0'])
    audformat.testing.create_audio_files(db)

    # Store without audio files in version 4.0.0
    db.save(DB_ROOT_VERSION['4.0.0'])

    # Extend database to >20 files and store without audio in version 5.0.0
    db['files'] = db['files'].extend_index(
        audformat.filewise_index([f'file{n}.wav' for n in range(20)])
    )
    assert len(db.files) > 20
    db.save(DB_ROOT_VERSION['5.0.0'])

    # Make database non-portable in version 6.0.0
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
    db.save(DB_ROOT_VERSION['6.0.0'])
    audformat.testing.create_audio_files(db)
    db.map_files(lambda x: os.path.join(db.root, x))  # make paths absolute
    db.save(DB_ROOT_VERSION['6.0.0'])

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
        audb.publish(
            DB_ROOT_VERSION['1.0.0'],
            '1.0.1',
            pytest.PUBLISH_REPOSITORY,
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
        pytest.param(
            '4.0.0',
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason='Files missing (fewer than 20)',
            ),
        ),
        pytest.param(
            '5.0.0',
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason='Files missing (more than 20)',
            ),
        ),
        pytest.param(
            '6.0.0',
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason='Database not portable',
            ),
        ),
    ]
)
def test_publish(version):

    db = audformat.Database.load(DB_ROOT_VERSION[version])
    print(db.is_portable)
    print(db.files)

    if not audb.versions(DB_NAME):
        with pytest.raises(RuntimeError):
            audb.latest_version(DB_NAME)

    archives = db['files']['speaker'].get().dropna().to_dict()
    deps = audb.publish(
        DB_ROOT_VERSION[version],
        version,
        pytest.PUBLISH_REPOSITORY,
        archives=archives,
        previous_version=None,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    backend = audb.core.utils.lookup_backend(DB_NAME, version)
    number_of_files = len(set(archives.keys()))
    number_of_archives = len(set(archives.values()))
    assert len(deps.files) - len(deps.archives) == (
        number_of_files - number_of_archives
    )
    for archive in set(archives.values()):
        assert archive in deps.archives

    db = audb.load(
        DB_NAME,
        version=version,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
    )
    assert db.name == DB_NAME

    versions = audb.versions(DB_NAME)
    latest_version = audb.latest_version(DB_NAME)

    assert version in versions
    assert latest_version == versions[-1]

    df = audb.available(only_latest=False)
    assert DB_NAME in df.index
    assert set(df[df.index == DB_NAME]['version']) == set(versions)

    df = audb.available(only_latest=True)
    assert DB_NAME in df.index
    assert df[df.index == DB_NAME]['version'][0] == latest_version

    for file in db.files:
        name = archives[file] if file in archives else file
        file_path = backend.join(db.name, 'media', name)
        backend.exists(file_path, version)
        path = os.path.join(DB_ROOT_VERSION[version], file)
        assert deps.checksum(file) == audbackend.md5(path)
        if deps.format(file) in [
            audb.core.define.Format.WAV,
            audb.core.define.Format.FLAC,
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

    depend1 = audb.dependencies(DB_NAME, version=version1)
    depend2 = audb.dependencies(DB_NAME, version=version2)

    media1 = set(sorted(depend1.media))
    media2 = set(sorted(depend2.media))

    assert media1 - media2 == set(media_difference)


def test_publish_error_messages():

    for version in ['1.0.0', '4.0.0', '5.0.0']:

        if version == '1.0.0':
            error_msg = (
                "A version '1.0.0' already exists for database "
                f"'{DB_NAME}'."
            )
        elif version == '4.0.0':
            error_msg = (
                "5 files are referenced in tables that cannot be found. "
                "Missing files are: '['audio/002.wav', 'audio/003.wav', "
                "'audio/004.wav', 'audio/005.wav', 'audio/new.wav']'."
            )
        elif version == '5.0.0':
            error_msg = (
                "25 files are referenced in tables that cannot be found. "
                "Missing files are: '['audio/002.wav', 'audio/003.wav', "
                "'audio/004.wav', 'audio/005.wav', 'audio/new.wav', "
                "'file0.wav', 'file1.wav', 'file10.wav', 'file11.wav', "
                "'file12.wav', 'file13.wav', 'file14.wav', 'file15.wav', "
                "'file16.wav', 'file17.wav', 'file18.wav', 'file19.wav', "
                "'file2.wav', 'file3.wav', 'file4.wav'], ...'."
            )

        with pytest.raises(RuntimeError, match=re.escape(error_msg)):

            audb.publish(
                DB_ROOT_VERSION[version],
                version,
                pytest.PUBLISH_REPOSITORY,
                previous_version=None,
                num_workers=pytest.NUM_WORKERS,
                verbose=False,
            )


def test_update_database():

    version = '2.1.0'
    start_version = '2.0.0'

    db = audb.load_to(
        DB_ROOT_VERSION[version],
        DB_NAME,
        version=start_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    # == Fail with missing dependency file
    previous_version = start_version
    dep_file = os.path.join(
        DB_ROOT_VERSION[version],
        audb.core.define.DEPENDENCIES_FILE,
    )
    os.remove(dep_file)
    print(audeer.list_file_names(DB_ROOT_VERSION[version]))
    error_msg = (
        f"You want to depend on '{previous_version}' "
        f"of {DB_NAME}, "
        f"but you don't have a '{audb.core.define.DEPENDENCIES_FILE}' "
        f"file present "
        f"in {DB_ROOT_VERSION[version]}. "
        f"Did you forgot to call "
        f"'audb.load_to({DB_ROOT_VERSION[version]}, {DB_NAME}, "
        f"version={previous_version}?"
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            DB_ROOT_VERSION[version],
            version,
            pytest.PUBLISH_REPOSITORY,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    # Reload data to restore dependency file
    shutil.rmtree(DB_ROOT_VERSION[version])
    db = audb.load_to(
        DB_ROOT_VERSION[version],
        DB_NAME,
        version=start_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    # Remove one file as in version 3.0.0
    remove_file = os.path.join('audio', '001.wav')
    remove_path = os.path.join(DB_ROOT_VERSION[version], remove_file)
    os.remove(remove_path)
    db.drop_files(remove_file)
    db.save(DB_ROOT_VERSION[version])

    # == Fail as 2.0.0 is not the latest version
    previous_version = 'latest'
    error_msg = (
        f"You want to depend on '{audb.latest_version(DB_NAME)}' "
        f"of {DB_NAME}, "
        f"but the MD5 sum of your "
        f"'{audb.core.define.DEPENDENCIES_FILE}' file "
        f"in {DB_ROOT_VERSION[version]} "
        f"does not match the MD5 sum of the corresponding file "
        f"for the requested version in the repository. "
        f"Did you forgot to call "
        f"'audb.load_to({DB_ROOT_VERSION[version]}, {DB_NAME}, "
        f"version='{audb.latest_version(DB_NAME)}') "
        f"or modified the file manually?"
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            DB_ROOT_VERSION[version],
            version,
            pytest.PUBLISH_REPOSITORY,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    # == Fail as we require a previous version
    previous_version = None
    error_msg = (
        f"You did not set a dependency to a previous version, "
        f"but you have a '{audb.core.define.DEPENDENCIES_FILE}' file present "
        f"in {DB_ROOT_VERSION[version]}."
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            DB_ROOT_VERSION[version],
            version,
            pytest.PUBLISH_REPOSITORY,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    previous_version = start_version
    deps = audb.publish(
        DB_ROOT_VERSION[version],
        version,
        pytest.PUBLISH_REPOSITORY,
        previous_version=previous_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    # Check that depencies include previous and actual version only
    versions = audeer.sort_versions([deps.version(f) for f in deps.files])
    assert versions[-1] == version
    assert versions[0] == previous_version

    # Check that there is no difference in the database
    # if published from scratch or from previous version
    db1 = audb.load(
        DB_NAME,
        version=version,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db2 = audb.load(
        DB_NAME,
        version='3.0.0',
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db1.meta['audb'] = {}
    db2.meta['audb'] = {}
    assert db1 == db2
