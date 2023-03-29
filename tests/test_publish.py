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

    # Version 1.0.0
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    # attachment files:
    #   - extra/file.txt
    #   - extra/folder/file1.txt
    #   - extra/folder/file2.txt
    #   - extra/folder/sub-folder/file3.txt
    # schemes:
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['1.0.0']
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
    audeer.mkdir(audeer.path(db_root, 'extra/folder'))
    audeer.touch(audeer.path(db_root, 'extra/file.txt'))
    audeer.touch(audeer.path(db_root, 'extra/folder/file1.txt'))
    audeer.touch(audeer.path(db_root, 'extra/folder/file2.txt'))
    audeer.mkdir(audeer.path(db_root, 'extra/folder/sub-folder'))
    audeer.touch(audeer.path(db_root, 'extra/folder/sub-folder/file3.txt'))
    # Create one file with different content to force different checksum
    file_with_different_content = audeer.path(
        db_root,
        'extra/folder/sub-folder/file3.txt',
    )
    with open(file_with_different_content, 'w') as fp:
        fp.write('test')
    db.attachments['file'] = audformat.Attachment('extra/file.txt')
    db.attachments['folder'] = audformat.Attachment('extra/folder')
    db.save(
        db_root,
        storage_format=audformat.define.TableStorageFormat.PICKLE,
    )
    audformat.testing.create_audio_files(db)

    # Version 2.0.0
    #
    # Changes:
    #   * Added: new file with a path >260 characters
    #   * Added: 1 attachment file
    #   * Removed: 1 attachment file
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # attachment files:
    #   - extra/file.txt
    #   - extra/folder/file1.txt
    #   - extra/folder/file3.txt
    #   - extra/folder/sub-folder/file3.txt
    # schemes:
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['2.0.0']
    shutil.copytree(
        audeer.path(DB_ROOT_VERSION['1.0.0'], 'extra'),
        audeer.path(db_root, 'extra'),
    )
    os.remove(audeer.path(db_root, 'extra/folder/file2.txt'))
    audeer.touch(audeer.path(db_root, 'extra/folder/file3.txt'))
    db['files'].extend_index(audformat.filewise_index(LONG_PATH), inplace=True)
    db.save(db_root)
    audformat.testing.create_audio_files(db)

    # Version 3.0.0
    #
    # Changes:
    #   * Removed: 1 media files
    #   * Removed: all attachments
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # schemes:
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['3.0.0']
    remove_file = 'audio/001.wav'
    db.drop_files(remove_file)
    del db.attachments['file']
    del db.attachments['folder']
    db.save(db_root)
    audformat.testing.create_audio_files(db)

    # Version 4.0.0
    #
    # Changes:
    #   * Changed: store without media files
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # schemes:
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['4.0.0']
    db.save(db_root)

    # Version 5.0.0
    #
    # Changes:
    #   * Added: 20 `file{n}.wav` media files in metadata
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/.../audio/new.wav  # >260 chars
    #   - file0.wav
    #   - ...
    #   - file19.wav
    # schemes:
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['5.0.0']
    db['files'] = db['files'].extend_index(
        audformat.filewise_index([f'file{n}.wav' for n in range(20)])
    )
    assert len(db.files) > 20
    db.save(db_root)

    # Version 6.0.0
    #
    # Changes:
    #   * Added: 'scheme' scheme
    #   * Changed: include media files (not metadata only database)
    #   * Changed: make database non-portable
    #   * Removed: media file with >260 chars
    #   * Removed: 20 file{n}.wav media files
    #
    # tables:
    #   - emotion
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/.../audio/new.wav  # >260 chars
    #   - file0.wav
    #   - ...
    #   - file19.wav
    # schemes:
    #   - scheme
    #   - speaker
    #   - misc
    db_root = DB_ROOT_VERSION['6.0.0']
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
    db.save(db_root)
    audformat.testing.create_audio_files(db)
    db.map_files(lambda x: os.path.join(db.root, x))  # make paths absolute
    db.save(db_root)

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
    number_of_media_files_in_custom_archives = len(set(archives.keys()))
    number_of_custom_archives = len(set(archives.values()))
    number_of_media_files = len(deps.media)
    number_of_media_archives = len(
        set([deps.archive(file) for file in deps.media])
    )
    assert (
        number_of_media_files_in_custom_archives
        - number_of_custom_archives
    ) == (
        number_of_media_files
        - number_of_media_archives
    )
    for archive in set(archives.values()):
        assert archive in deps.archives

    # Check checksums of attachment files
    expected_checksum_empty_attachment = 'd41d8cd98f00b204e9800998ecf8427e'
    expected_checksum_file3_attachment = '098f6bcd4621d373cade4e832627b4f6'
    for file in deps.attachment_files:
        if file == 'extra/folder/sub-folder/file3.txt':
            assert deps.checksum(file) == expected_checksum_file3_attachment
        else:
            assert deps.checksum(file) == expected_checksum_empty_attachment

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


def test_publish_attachment(tmpdir):

    # Create database (path does not need to exist)
    file_path = 'attachments/file.txt'
    folder_path = 'attachments/folder'
    db = audformat.Database('db-with-attachments')
    db.attachments['file'] = audformat.Attachment(
        file_path,
        description='Attached file',
        meta={'mime': 'text'},
    )
    db.attachments['folder'] = audformat.Attachment(
        folder_path,
        description='Attached folder',
        meta={'mime': 'inode/directory'},
    )

    assert list(db.attachments) == ['file', 'folder']
    assert db.attachments['file'].path == file_path
    assert db.attachments['file'].description == 'Attached file'
    assert db.attachments['file'].meta == {'mime': 'text'}
    assert db.attachments['folder'].path == folder_path
    assert db.attachments['folder'].description == 'Attached folder'
    assert db.attachments['folder'].meta == {'mime': 'inode/directory'}

    db_path = audeer.path(tmpdir, 'db')
    audeer.mkdir(db_path)
    db.save(db_path)

    # Publish database, path needs to exist
    error_msg = (
        f"The provided path '{file_path}' "
        f"of attachment 'file' "
        "does not exist."
    )
    with pytest.raises(FileNotFoundError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)

    # Publish database, path is not allowed to be a symlink
    audeer.mkdir(audeer.path(db_path, folder_path))
    os.symlink(
        audeer.path(db_path, folder_path),
        audeer.path(db_path, file_path),
    )
    error_msg = (
        f"The provided path '{file_path}' "
        f"of attachment 'file' "
        "must not be a symlink."
    )
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)

    os.remove(os.path.join(db_path, file_path))
    audeer.touch(audeer.path(db_path, file_path))
    db.save(db_path)

    # File exist now, folder is empty
    assert db.attachments['file'].files == [file_path]
    assert db.attachments['folder'].files == []
    error_msg = (
        "An attached folder must "
        "contain at least one file. "
        "But attachment 'folder' "
        "doesn't contain any files."
    )
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)

    # Add empty sub-folder
    subfolder_path = f'{folder_path}/sub-folder'
    audeer.mkdir(audeer.path(db_path, subfolder_path))
    assert db.attachments['folder'].files == []
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)

    # Add file to folder
    file2_path = f'{folder_path}/file.txt'
    audeer.touch(audeer.path(db_path, file2_path))
    assert db.attachments['file'].files == [file_path]
    assert db.attachments['folder'].files == [file2_path]

    # Publish and load database
    audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)
    db = audb.load(db.name, version='1.0.0')
    assert list(db.attachments) == ['file', 'folder']
    assert db.attachments['file'].files == [file_path]
    assert db.attachments['folder'].files == [file2_path]
    assert db.attachments['file'].path == file_path
    assert db.attachments['file'].description == 'Attached file'
    assert db.attachments['file'].meta == {'mime': 'text'}
    assert db.attachments['folder'].path == folder_path
    assert db.attachments['folder'].description == 'Attached folder'
    assert db.attachments['folder'].meta == {'mime': 'inode/directory'}


@pytest.mark.parametrize(
    'version1, version2, media_difference, attachment_difference',
    [
        (
            '1.0.0',
            '1.0.0',
            [],
            [],
        ),
        (
            '1.0.0',
            '2.0.0',
            [],
            ['extra/folder/file2.txt'],
        ),
        (
            '2.0.0',
            '1.0.0',
            [LONG_PATH],
            ['extra/folder/file3.txt'],
        ),
        (
            '2.0.0',
            '3.0.0',
            ['audio/001.wav'],
            [
                'extra/file.txt',
                'extra/folder/file1.txt',
                'extra/folder/file3.txt',
                'extra/folder/sub-folder/file3.txt',
            ],
        ),
        (
            '3.0.0',
            '2.0.0',
            [],
            [],
        ),
        (
            '1.0.0',
            '3.0.0',
            ['audio/001.wav'],
            [
                'extra/file.txt',
                'extra/folder/file1.txt',
                'extra/folder/file2.txt',
                'extra/folder/sub-folder/file3.txt',
            ],
        ),
        (
            '3.0.0',
            '1.0.0',
            [LONG_PATH],
            [],
        ),
    ]
)
def test_publish_changed_db(
        version1,
        version2,
        media_difference,
        attachment_difference,
):

    depend1 = audb.dependencies(DB_NAME, version=version1)
    depend2 = audb.dependencies(DB_NAME, version=version2)

    media1 = set(sorted(depend1.media))
    media2 = set(sorted(depend2.media))
    assert media1 - media2 == set(media_difference)

    attachment1 = set(sorted(depend1.attachment_files))
    attachment2 = set(sorted(depend2.attachment_files))
    assert attachment1 - attachment2 == set(attachment_difference)


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


def test_publish_error_changed_deps_file_type(tmpdir):
    # As we allow for every possible filename for attachments
    # and store them in the dependency table
    # besides media and table files
    # there can be a naming clash between those entries.
    # See https://github.com/audeering/audb/pull/244#issuecomment-1412211131

    # media => attachment
    error_msg = (
        "The type of an existing dependency must not change, "
        "but you are trying to change the type of the dependency "
        "'data/file.wav'. "
        'You might have a naming clash between a media file '
        'and an attached file.'
    )
    db_name = 'test_publish_error_changed_deps_file_type-1'
    db_path = audeer.mkdir(audeer.path(tmpdir, 'db'))
    data_path = audeer.mkdir(audeer.path(db_path, 'data'))
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, 'file.wav'), signal, sampling_rate)
    db = audformat.Database(db_name)
    db['table'] = audformat.Table(audformat.filewise_index('data/file.wav'))
    db.attachments['attachment'] = audformat.Attachment('data/file.wav')
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)

    # table => attachment
    error_msg = (
        "The type of an existing dependency must not change, "
        "but you are trying to change the type of the dependency "
        "'db.table.csv'. "
        'You might have a naming clash between a table '
        'and an attached file.'
    )
    db_name = 'test_publish_error_changed_deps_file_type-2'
    db_path = audeer.mkdir(audeer.path(tmpdir, 'db'))
    data_path = audeer.mkdir(audeer.path(db_path, 'data'))
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, 'file.wav'), signal, sampling_rate)
    db = audformat.Database(db_name)
    db['table'] = audformat.Table(audformat.filewise_index('data/file.wav'))
    db.attachments['attachment'] = audformat.Attachment('db.table.csv')
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)

    # attachment => media
    error_msg = (
        "The type of an existing dependency must not change, "
        "but you are trying to change the type of the dependency "
        "'data/file2.wav'. "
        'You might have a naming clash between a media file '
        'and an attached file.'
    )
    db_name = 'test_publish_error_changed_deps_file_type-3'
    db_path = audeer.mkdir(audeer.path(tmpdir, 'db'))
    data_path = audeer.mkdir(audeer.path(db_path, 'data'))
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, 'file1.wav'), signal, sampling_rate)
    audiofile.write(audeer.path(data_path, 'file2.wav'), signal, sampling_rate)
    db = audformat.Database(db_name)
    db['table'] = audformat.Table(audformat.filewise_index('data/file1.wav'))
    db.attachments['attachment'] = audformat.Attachment('data/file2.wav')
    db.save(db_path)
    audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)
    db = audb.load_to(db_path, db_name, version='1.0.0')
    db['table'] = audformat.Table(
        audformat.filewise_index(['data/file1.wav', 'data/file2.wav'])
    )
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '2.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)

    # attachment => table
    error_msg = (
        "The type of an existing dependency must not change, "
        "but you are trying to change the type of the dependency "
        "'db.table2.csv'. "
        'You might have a naming clash between a table '
        'and an attached file.'
    )
    db_name = 'test_publish_error_changed_deps_file_type-4'
    db_path = audeer.mkdir(audeer.path(tmpdir, 'db'))
    data_path = audeer.mkdir(audeer.path(db_path, 'data'))
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, 'file.wav'), signal, sampling_rate)
    db = audformat.Database(db_name)
    db['table1'] = audformat.Table(audformat.filewise_index('data/file.wav'))
    db.attachments['attachment'] = audformat.Attachment('db.table2.csv')
    audeer.touch(audeer.path(db_path, 'db.table2.csv'))
    db.save(db_path)
    audb.publish(db_path, '1.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)
    db = audb.load_to(db_path, db_name, version='1.0.0')
    db['table2'] = audformat.Table(audformat.filewise_index('data/file.wav'))
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, '2.0.0', pytest.PUBLISH_REPOSITORY)
    audeer.rmdir(db_path)


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
    # Remove one media file and all attachments as in version 3.0.0
    remove_file = 'audio/001.wav'
    remove_path = os.path.join(DB_ROOT_VERSION[version], remove_file)
    os.remove(remove_path)
    del db.attachments['file']
    del db.attachments['folder']
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

    # add changes to build folder
    # and call again load_to()
    # to revert them
    os.remove(audeer.path(build_root, 'extra/folder/file2.txt'))
    db = audb.load_to(
        build_root,
        DB_NAME,
        only_metadata=True,
        version=previous_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert os.path.exists(audeer.path(build_root, 'extra/folder/file2.txt'))

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

    # remove one attachment file
    os.remove(audeer.path(build_root, 'extra/folder/file2.txt'))

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
