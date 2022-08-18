import filecmp
import os
import re
import shutil

import numpy as np
import pandas as pd
import pytest

import audbackend
import audeer
import audformat.testing
import audiofile

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


@pytest.fixture(
    scope='session',
    autouse=True,
)
def fixture_set_repositories():
    audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_publish-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['0.1.0', '1.0.0', '2.0.0', '2.1.0', '3.0.0', '4.0.0', '5.0.0', '6.0.0']
}
LONG_PATH = '/'.join(['audio'] * 50) + '/new.wav'


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
    db.schemes['speaker'] = audformat.Scheme(
        labels=['adam', '11']
    )
    db.schemes['misc'] = audformat.Scheme(
        'int',
        labels='misc-in-scheme',
    )
    db['files'] = audformat.Table(db.files)
    db['files']['speaker'] = audformat.Column(scheme_id='speaker')
    db['files']['speaker'].set(
        ['adam', 'adam', 'adam', '11'],
        index=audformat.filewise_index(db.files[:4]),
    )
    db['files']['misc'] = audformat.Column(scheme_id='misc')
    db['files']['misc'].set(
        [0, 1, 1, 2],
        index=audformat.filewise_index(db.files[:4]),
    )
    db.save(
        DB_ROOT_VERSION['1.0.0'],
        storage_format=audformat.define.TableStorageFormat.PICKLE,
    )
    audformat.testing.create_audio_files(db)

    # Extend version 2.0.0 by a new file with a path >260 characters
    db['files'].extend_index(audformat.filewise_index(LONG_PATH), inplace=True)
    db.save(DB_ROOT_VERSION['2.0.0'])
    audformat.testing.create_audio_files(db)

    # Remove one file in version 3.0.0
    remove_file = 'audio/001.wav'
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
    db.schemes['speaker'] = audformat.Scheme(
        labels=['adam', '11']
    )
    db.schemes['misc'] = audformat.Scheme(
        'int',
        labels='misc-in-scheme',
    )
    db['files'] = audformat.Table(db.files)
    db['files']['speaker'] = audformat.Column(scheme_id='speaker')
    db['files']['speaker'].set(
        ['adam', 'adam', 'adam', '11'],
        index=audformat.filewise_index(db.files[:4]),
    )
    db['files']['misc'] = audformat.Column(scheme_id='misc')
    db['files']['misc'].set(
        [0, 1, 1, 2],
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
        'audio/001.wav': name
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
        verbose=False,
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
            [LONG_PATH],
        ),
        (
            '2.0.0',
            '3.0.0',
            ['audio/001.wav'],
        ),
        (
            '3.0.0',
            '2.0.0',
            [],
        ),
        (
            '1.0.0',
            '3.0.0',
            ['audio/001.wav'],
        ),
        (
            '3.0.0',
            '1.0.0',
            [LONG_PATH],
        ),
    ]
)
def test_publish_changed_db(version1, version2, media_difference):

    depend1 = audb.dependencies(DB_NAME, version=version1)
    depend2 = audb.dependencies(DB_NAME, version=version2)

    media1 = set(sorted(depend1.media))
    media2 = set(sorted(depend2.media))

    assert media1 - media2 == set(media_difference)


@pytest.mark.parametrize(
    'version, previous_version, error_type, error_msg',
    [
        (
            '1.0.0',
            None,
            RuntimeError,
            (
                "A version '1.0.0' already exists for database "
                f"'{DB_NAME}'."
            ),
        ),
        (
            '4.0.0',
            None,
            RuntimeError,
            (
                "The following 5 files are referenced in tables "
                "that cannot be found on disk "
                "and are not yet part of the database: "
                "['audio/002.wav', 'audio/003.wav', "
                "'audio/004.wav', 'audio/005.wav', "
                f"'{LONG_PATH}']."
            ),
        ),
        (
            '5.0.0',
            None,
            RuntimeError,
            (
                "The following 25 files are referenced in tables "
                "that cannot be found on disk "
                "and are not yet part of the database: "
                "['audio/002.wav', 'audio/003.wav', "
                "'audio/004.wav', 'audio/005.wav', "
                f"'{LONG_PATH}', "
                "'file0.wav', 'file1.wav', 'file10.wav', 'file11.wav', "
                "'file12.wav', 'file13.wav', 'file14.wav', 'file15.wav', "
                "'file16.wav', 'file17.wav', 'file18.wav', 'file19.wav', "
                "'file2.wav', 'file3.wav', 'file4.wav', ...]."
            ),
        ),
        (
            '5.0.0',
            '1.0.0',
            RuntimeError,
            (
                "The following 21 files are referenced in tables "
                "that cannot be found on disk "
                "and are not yet part of the database: "
                f"['{LONG_PATH}', "
                "'file0.wav', 'file1.wav', 'file10.wav', 'file11.wav', "
                "'file12.wav', 'file13.wav', 'file14.wav', 'file15.wav', "
                "'file16.wav', 'file17.wav', 'file18.wav', 'file19.wav', "
                "'file2.wav', 'file3.wav', 'file4.wav', 'file5.wav', "
                "'file6.wav', 'file7.wav', 'file8.wav', ...]."
            ),
        ),
        (
            '0.1.0',
            '1.0.0',
            ValueError,
            (
                "'previous_version' needs to be smaller than 'version', "
                "but yours is 1.0.0 >= 0.1.0."
            ),
        ),
        (
            '0.1.0',
            '0.1.0',
            ValueError,
            (
                "'previous_version' needs to be smaller than 'version', "
                "but yours is 0.1.0 >= 0.1.0."
            ),
        ),
    ]
)
def test_publish_error_messages(
        version,
        previous_version,
        error_type,
        error_msg,
):
    with pytest.raises(error_type, match=re.escape(error_msg)):
        if (
                previous_version
                and (
                    audeer.LooseVersion(previous_version)
                    < audeer.LooseVersion(version)
                )
        ):
            deps = audb.dependencies(
                DB_NAME,
                version=previous_version,
            )
            path = os.path.join(
                DB_ROOT_VERSION[version],
                audb.core.define.DEPENDENCIES_FILE,
            )
            deps.save(path)
        audb.publish(
            DB_ROOT_VERSION[version],
            version,
            pytest.PUBLISH_REPOSITORY,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )


def test_update_database():

    version = '2.1.0'
    start_version = '2.0.0'

    audb.load_to(
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
    remove_file = 'audio/001.wav'
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

    # Check that dependencies include previous and actual version only
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


def test_update_database_without_media(tmpdir):

    build_root = tmpdir
    previous_version = '1.0.0'
    version = '1.1.0'

    new_table = 'new'
    new_files = [
        'new/001.wav',
        'new/002.wav',
    ]
    alter_files = [
        'audio/001.wav',  # same archive as 'audio/00[2,3].wav'
        'audio/005.wav',
    ]
    rem_files = [
        'new/003.wav',  # same archive as 'audio/00[1,2].wav'
    ]

    # load without media
    db = audb.load_to(
        build_root,
        DB_NAME,
        only_metadata=True,
        version=previous_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    for file in db.files:
        assert not os.path.exists(audeer.path(build_root, file))

    # update and save database

    # remove files
    for file in rem_files:
        db.drop_files(file)

    # create and alter files
    sampling_rate = 16000
    signal = np.ones((1, sampling_rate), np.float32)
    for file in new_files + alter_files:
        path = audeer.path(build_root, file)
        audeer.mkdir(os.path.dirname(path))
        audiofile.write(path, signal, sampling_rate)

    # add new table
    db[new_table] = audformat.Table(audformat.filewise_index(new_files))

    db.save(build_root)

    # publish database
    audb.publish(
        build_root,
        version,
        pytest.PUBLISH_REPOSITORY,
        previous_version=previous_version,
        verbose=False,
    )

    # check if missing archive files were downloaded during publish
    assert os.path.exists(audeer.path(build_root, 'audio/002.wav'))

    # load new version and check media
    db_load = audb.load(
        DB_NAME,
        version=version,
    )
    for file in db.files:
        assert os.path.exists(audeer.path(db_load.root, file))
    for file in rem_files:
        assert not os.path.exists(audeer.path(db_load.root, file))
    for file in new_files + alter_files:
        assert filecmp.cmp(
            audeer.path(build_root, file),
            audeer.path(db_load.root, file),
        )


def test_cached():
    # Check first that we have different database names available
    df = audb.cached()
    names = list(set(df.name))
    assert len(names) > 1
    df = audb.cached(name=names[0])
    assert set(df.name) == set([names[0]])
