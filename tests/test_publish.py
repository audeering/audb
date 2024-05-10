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


DB_NAME = "test_publish"
LONG_PATH = "/".join(["audio"] * 50) + "/new.wav"


@pytest.fixture(
    scope="session",
    autouse=True,
)
def dbs(tmpdir_factory):
    r"""Store different versions of the same database.

    Returns:
        dictionary containing root folder for each version

    """
    # Collect single database paths
    # and return them in the end
    paths = {}

    # Version 0.1.0
    #
    # Folder without content
    version = "0.1.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = db_root

    # Version 1.0.0
    #
    # tables:
    #   - emotion
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    # attachment files:
    #   - extra/file.txt
    #   - extra/folder/file1.txt
    #   - extra/folder/file2.txt
    #   - extra/folder/sub-folder/file3.txt
    # schemes:
    #   - speaker
    #   - misc
    version = "1.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes["scheme"] = audformat.Scheme(labels=["positive", "neutral", "negative"])
    audformat.testing.add_table(
        db,
        "emotion",
        audformat.define.IndexType.SEGMENTED,
        num_files=5,
        columns={"emotion": ("scheme", None)},
    )
    audformat.testing.add_misc_table(
        db,
        "misc-in-scheme",
        pd.Index([0, 1, 2], dtype="Int64", name="idx"),
        columns={"emotion": ("scheme", None)},
    )
    audformat.testing.add_misc_table(
        db,
        "misc-not-in-scheme",
        pd.Index([0, 1, 2], dtype="Int64", name="idx"),
        columns={"emotion": ("scheme", None)},
    )
    db.schemes["speaker"] = audformat.Scheme(labels=["adam", "11"])
    db.schemes["misc"] = audformat.Scheme(
        "int",
        labels="misc-in-scheme",
    )
    db["files"] = audformat.Table(db.files)
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(
        ["adam", "adam", "adam", "11"],
        index=audformat.filewise_index(db.files[:4]),
    )
    db["files"]["misc"] = audformat.Column(scheme_id="misc")
    db["files"]["misc"].set(
        [0, 1, 1, 2],
        index=audformat.filewise_index(db.files[:4]),
    )
    audeer.mkdir(db_root, "extra/folder")
    audeer.touch(db_root, "extra/file.txt")
    audeer.touch(db_root, "extra/folder/file1.txt")
    audeer.touch(db_root, "extra/folder/file2.txt")
    audeer.mkdir(db_root, "extra/folder/sub-folder")
    audeer.touch(db_root, "extra/folder/sub-folder/file3.txt")
    # Create one file with different content to force different checksum
    file_with_different_content = audeer.path(
        db_root,
        "extra/folder/sub-folder/file3.txt",
    )
    with open(file_with_different_content, "w") as fp:
        fp.write("test")
    db.attachments["file"] = audformat.Attachment("extra/file.txt")
    db.attachments["folder"] = audformat.Attachment("extra/folder")
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
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # attachment files:
    #   - extra/file.txt
    #   - extra/folder/file1.txt
    #   - extra/folder/file3.txt
    #   - extra/folder/sub-folder/file3.txt
    # schemes:
    #   - speaker
    #   - misc
    version = "2.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    shutil.copytree(
        audeer.path(paths["1.0.0"], "extra"),
        audeer.path(db_root, "extra"),
    )
    os.remove(audeer.path(db_root, "extra/folder/file2.txt"))
    audeer.touch(db_root, "extra/folder/file3.txt")
    db["files"].extend_index(audformat.filewise_index(LONG_PATH), inplace=True)
    db.save(db_root)
    audformat.testing.create_audio_files(db)

    # Version 2.1.0
    #
    # Folder without content
    version = "2.1.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    # Version 3.0.0
    #
    # Changes:
    #   * Removed: 1 media files
    #   * Removed: all attachments
    #
    # tables:
    #   - emotion
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # schemes:
    #   - speaker
    #   - misc
    version = "3.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    remove_file = "audio/001.wav"
    db.drop_files(remove_file)
    del db.attachments["file"]
    del db.attachments["folder"]
    db.save(db_root)
    audformat.testing.create_audio_files(db)

    # Version 4.0.0
    #
    # Changes:
    #   * Changed: store without media files
    #
    # tables:
    #   - emotion
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    #   - audio/.../audio/new.wav  # >260 chars
    # schemes:
    #   - speaker
    #   - misc
    version = "4.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    db.save(db_root)

    # Version 5.0.0
    #
    # Changes:
    #   * Added: 20 `file{n}.wav` media files in metadata
    #
    # tables:
    #   - emotion
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    #   - audio/.../audio/new.wav  # >260 chars
    #   - file0.wav
    #   - ...
    #   - file19.wav
    # schemes:
    #   - speaker
    #   - misc
    version = "5.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    db["files"] = db["files"].extend_index(
        audformat.filewise_index([f"file{n}.wav" for n in range(20)])
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
    #   - files
    # misc tables:
    #   - misc-in-scheme
    #   - misc-not-in-scheme
    # media:
    #   - audio/001.wav
    #   - audio/002.wav
    #   - audio/003.wav
    #   - audio/004.wav
    #   - audio/005.wav
    # schemes:
    #   - scheme
    #   - speaker
    #   - misc
    version = "6.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)
    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes["scheme"] = audformat.Scheme(labels=["positive", "neutral", "negative"])
    audformat.testing.add_table(
        db,
        "emotion",
        audformat.define.IndexType.SEGMENTED,
        num_files=5,
        columns={"emotion": ("scheme", None)},
    )
    audformat.testing.add_misc_table(
        db,
        "misc-in-scheme",
        pd.Index([0, 1, 2], dtype="Int64", name="idx"),
        columns={"emotion": ("scheme", None)},
    )
    audformat.testing.add_misc_table(
        db,
        "misc-not-in-scheme",
        pd.Index([0, 1, 2], dtype="Int64", name="idx"),
        columns={"emotion": ("scheme", None)},
    )
    db.schemes["speaker"] = audformat.Scheme(labels=["adam", "11"])
    db.schemes["misc"] = audformat.Scheme(
        "int",
        labels="misc-in-scheme",
    )
    db["files"] = audformat.Table(db.files)
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(
        ["adam", "adam", "adam", "11"],
        index=audformat.filewise_index(db.files[:4]),
    )
    db["files"]["misc"] = audformat.Column(scheme_id="misc")
    db["files"]["misc"].set(
        [0, 1, 1, 2],
        index=audformat.filewise_index(db.files[:4]),
    )
    db.save(db_root)
    audformat.testing.create_audio_files(db)
    db.map_files(lambda x: os.path.join(db.root, x))  # make paths absolute
    db.save(db_root)

    return paths


@pytest.mark.parametrize(
    "name",
    ["?", "!", ","],
)
def test_invalid_archives(dbs, persistent_repository, name):
    archives = {"audio/001.wav": name}
    with pytest.raises(ValueError):
        audb.publish(
            dbs["1.0.0"],
            "1.0.1",
            persistent_repository,
            archives=archives,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )


@pytest.mark.parametrize(
    "version",
    [
        "1.0.0",
        pytest.param(
            "1.0.0",
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        "2.0.0",
        "3.0.0",
        pytest.param(
            "4.0.0",
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason="Files missing (fewer than 20)",
            ),
        ),
        pytest.param(
            "5.0.0",
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason="Files missing (more than 20)",
            ),
        ),
        pytest.param(
            "6.0.0",
            marks=pytest.mark.xfail(
                raises=RuntimeError,
                reason="Database not portable",
            ),
        ),
    ],
)
def test_publish(tmpdir, dbs, persistent_repository, version):
    db = audformat.Database.load(dbs[version])

    if not audb.versions(DB_NAME):
        with pytest.raises(RuntimeError):
            audb.latest_version(DB_NAME)

    # Copy database folder to build folder
    # to avoid storing dependency table files
    # inside the database folders
    build_dir = audeer.path(tmpdir, "build")
    shutil.copytree(dbs[version], build_dir)
    archives = db["files"]["speaker"].get().dropna().to_dict()
    deps = audb.publish(
        build_dir,
        version,
        persistent_repository,
        archives=archives,
        previous_version=None,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    backend_interface = audb.core.utils.lookup_backend(DB_NAME, version)
    number_of_media_files_in_custom_archives = len(set(archives.keys()))
    number_of_custom_archives = len(set(archives.values()))
    number_of_media_files = len(deps.media)
    number_of_media_archives = len(set([deps.archive(file) for file in deps.media]))
    assert (number_of_media_files_in_custom_archives - number_of_custom_archives) == (
        number_of_media_files - number_of_media_archives
    )
    # Check if media files are sorted.
    # This does mean that media files are
    # always sorted by alphabetical order
    # but only in this specific test case.
    # Here we're testing for determinism rather
    # than ordering
    assert deps.media == sorted(deps.media)

    for archive in set(archives.values()):
        assert archive in deps.archives

    # Check checksums of attachment files
    for path in deps.attachments:
        expected_checksum = audeer.md5(audeer.path(db.root, path))
        assert deps.checksum(path) == expected_checksum

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
    assert set(df[df.index == DB_NAME]["version"]) == set(versions)

    df = audb.available(only_latest=True)
    assert DB_NAME in df.index
    assert df[df.index == DB_NAME]["version"].iat[0] == latest_version

    for file in db.files:
        name = archives[file] if file in archives else file
        file_path = backend_interface.join("/", db.name, "media", name)
        backend_interface.exists(file_path, version)
        path = os.path.join(dbs[version], file)
        assert deps.checksum(file) == audeer.md5(path)
        if deps.format(file) in [
            audb.core.define.Format.WAV,
            audb.core.define.Format.FLAC,
        ]:
            assert deps.bit_depth(file) == audiofile.bit_depth(path)
            assert deps.channels(file) == audiofile.channels(path)
            assert deps.duration(file) == audiofile.duration(path)
            assert deps.sampling_rate(file) == audiofile.sampling_rate(path)


def test_publish_attachment(tmpdir, repository):
    # Create database (path does not need to exist)
    file_path = "attachments/file.txt"
    folder_path = "attachments/folder"
    db = audformat.Database("db-with-attachments")
    db.attachments["file"] = audformat.Attachment(
        file_path,
        description="Attached file",
        meta={"mime": "text"},
    )
    db.attachments["folder"] = audformat.Attachment(
        folder_path,
        description="Attached folder",
        meta={"mime": "inode/directory"},
    )

    assert list(db.attachments) == ["file", "folder"]
    assert db.attachments["file"].path == file_path
    assert db.attachments["file"].description == "Attached file"
    assert db.attachments["file"].meta == {"mime": "text"}
    assert db.attachments["folder"].path == folder_path
    assert db.attachments["folder"].description == "Attached folder"
    assert db.attachments["folder"].meta == {"mime": "inode/directory"}

    db_path = audeer.path(tmpdir, "db")
    audeer.mkdir(db_path)
    db.save(db_path)

    # Publish database, path needs to exist
    error_msg = (
        f"The provided path '{file_path}' " f"of attachment 'file' " "does not exist."
    )
    with pytest.raises(FileNotFoundError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)

    # Publish database, path is not allowed to be a symlink
    audeer.mkdir(db_path, folder_path)
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
        audb.publish(db_path, "1.0.0", repository)

    os.remove(os.path.join(db_path, file_path))
    audeer.touch(db_path, file_path)
    db.save(db_path)

    # File exist now, folder is empty
    assert db.attachments["file"].files == [file_path]
    assert db.attachments["folder"].files == []
    error_msg = (
        "An attached folder must "
        "contain at least one file. "
        "But attachment 'folder' "
        "points to an empty folder."
    )
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)

    # Add empty sub-folder
    subfolder_path = f"{folder_path}/sub-folder"
    audeer.mkdir(db_path, subfolder_path)
    assert db.attachments["folder"].files == []
    error_msg = (
        "An attachment must not "
        "contain empty sub-folders. "
        "But attachment 'folder' "
        "contains the empty sub-folder 'sub-folder'."
    )
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)

    # Add file to folder, sub-folder still empty
    file2_path = f"{folder_path}/file.txt"
    audeer.touch(db_path, file2_path)
    assert db.attachments["file"].files == [file_path]
    assert db.attachments["folder"].files == [file2_path]
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)

    # Add file to sub-folder
    file3_path = f"{subfolder_path}/file.txt"
    audeer.touch(db_path, file3_path)
    assert db.attachments["file"].files == [file_path]
    assert db.attachments["folder"].files == [file2_path, file3_path]

    # Publish and load database
    audb.publish(db_path, "1.0.0", repository)
    db = audb.load(db.name, version="1.0.0", verbose=False)
    assert list(db.attachments) == ["file", "folder"]
    assert db.attachments["file"].files == [file_path]
    assert db.attachments["folder"].files == [file2_path, file3_path]
    assert db.attachments["file"].path == file_path
    assert db.attachments["file"].description == "Attached file"
    assert db.attachments["file"].meta == {"mime": "text"}
    assert db.attachments["folder"].path == folder_path
    assert db.attachments["folder"].description == "Attached folder"
    assert db.attachments["folder"].meta == {"mime": "inode/directory"}


def test_publish_change_archive(tmpdir, dbs, repository):
    """Test removing file from archive.

    When a file is removed,
    which is stored in an archive
    together with other files,
    the archive needs to be updated.

    As described in
    https://github.com/audeering/audb/issues/377
    this was failing
    when the archive was not added
    in ``previous_version``,
    but an earlier version.

    """
    # Store two files in an archive in version 1.0.0
    build_dir = audeer.path(tmpdir, "build")
    shutil.copytree(dbs["1.0.0"], build_dir)
    archives = {
        "audio/001.wav": "common_archive",
        "audio/002.wav": "common_archive",
    }
    audb.publish(
        build_dir,
        "1.0.0",
        repository,
        archives=archives,
        previous_version=None,
        verbose=False,
    )
    # Remove a file stored in another archive for version 2.0.0
    os.remove(audeer.path(build_dir, "audio/003.wav"))
    audb.publish(
        build_dir,
        "2.0.0",
        repository,
        archives=archives,
        previous_version="1.0.0",
        verbose=False,
    )
    # Remove file `audio/001.wav`
    # to require archive update
    # of archive added in version "1.0.0"
    audeer.rmdir(build_dir)
    db = audb.load_to(
        build_dir,
        DB_NAME,
        version="2.0.0",
        only_metadata=True,
    )
    db.drop_files(["audio/001.wav"])
    db.save(build_dir)
    audb.publish(
        build_dir,
        "3.0.0",
        repository,
        previous_version="2.0.0",
        verbose=False,
    )


@pytest.mark.parametrize(
    "version1, version2, media_difference, attachment_difference",
    [
        (
            "1.0.0",
            "1.0.0",
            [],
            [],
        ),
        (
            "1.0.0",
            "2.0.0",
            [],
            [os.path.join("extra", "folder", "file2.txt")],
        ),
        (
            "2.0.0",
            "1.0.0",
            [LONG_PATH],
            [os.path.join("extra", "folder", "file3.txt")],
        ),
        (
            "2.0.0",
            "3.0.0",
            ["audio/001.wav"],
            [
                os.path.join("extra", "file.txt"),
                os.path.join("extra", "folder", "file1.txt"),
                os.path.join("extra", "folder", "file3.txt"),
                os.path.join("extra", "folder", "sub-folder", "file3.txt"),
            ],
        ),
        (
            "3.0.0",
            "2.0.0",
            [],
            [],
        ),
        (
            "1.0.0",
            "3.0.0",
            ["audio/001.wav"],
            [
                os.path.join("extra", "file.txt"),
                os.path.join("extra", "folder", "file1.txt"),
                os.path.join("extra", "folder", "file2.txt"),
                os.path.join("extra", "folder", "sub-folder", "file3.txt"),
            ],
        ),
        (
            "3.0.0",
            "1.0.0",
            [LONG_PATH],
            [],
        ),
    ],
)
def test_publish_changed_db(
    dbs,
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

    attachment1 = []
    for path in depend1.attachments:
        root = dbs[version1]
        files = audeer.list_file_names(
            audeer.path(root, path),
            recursive=True,
            hidden=True,
        )
        files = [file[len(root) + 1 :] for file in files]
        attachment1.extend(files)

    attachment2 = []
    for path in depend2.attachments:
        root = dbs[version2]
        files = audeer.list_file_names(
            audeer.path(root, path),
            recursive=True,
            hidden=True,
        )
        files = [file[len(root) + 1 :] for file in files]
        attachment2.extend(files)

    assert set(attachment1) - set(attachment2) == set(attachment_difference)


@pytest.mark.parametrize(
    "version, previous_version, error_type, error_msg",
    [
        (
            "1.0.0",
            None,
            RuntimeError,
            ("A version '1.0.0' already exists for database " f"'{DB_NAME}'."),
        ),
        (
            "4.0.0",
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
            "5.0.0",
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
            "5.0.0",
            "1.0.0",
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
            "0.1.0",
            "1.0.0",
            ValueError,
            (
                "'previous_version' needs to be smaller than 'version', "
                "but yours is 1.0.0 >= 0.1.0."
            ),
        ),
        (
            "0.1.0",
            "0.1.0",
            ValueError,
            (
                "'previous_version' needs to be smaller than 'version', "
                "but yours is 0.1.0 >= 0.1.0."
            ),
        ),
    ],
)
def test_publish_error_messages(
    dbs,
    persistent_repository,
    version,
    previous_version,
    error_type,
    error_msg,
):
    with pytest.raises(error_type, match=re.escape(error_msg)):
        if previous_version and (
            audeer.StrictVersion(previous_version) < audeer.StrictVersion(version)
        ):
            deps = audb.dependencies(
                DB_NAME,
                version=previous_version,
            )
            path = os.path.join(
                dbs[version],
                audb.core.define.DEPENDENCIES_FILE,
            )
            deps.save(path)
        audb.publish(
            dbs[version],
            version,
            persistent_repository,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )


def test_publish_error_allowed_chars(tmpdir, repository):
    # Table and attachment IDs are only allow
    # to contain chars that can be used as filenames
    # on the backends

    # Prepare database files
    db_path = audeer.mkdir(tmpdir, "db")
    audio_file = audeer.path(db_path, "f1.wav")
    attachment_file = audeer.path(db_path, "attachment.txt")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audio_file, signal, sampling_rate)
    audeer.touch(attachment_file)

    # Database with not allowed table ID
    db = audformat.Database("db")
    index = audformat.filewise_index(os.path.basename(audio_file))
    db["table/"] = audformat.Table(index)
    db["table/"]["column"] = audformat.Column()
    db["table/"]["column"].set(["label"])
    db.save(db_path)
    error_msg = (
        "Table IDs must only contain chars from [A-Za-z0-9._-], "
        "which is not the case for table 'table/'."
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(db_path, "1.0.0", repository)

    # Database with not allowed attachment ID
    db = audformat.Database("db")
    index = audformat.filewise_index(os.path.basename(audio_file))
    db["table"] = audformat.Table(index)
    db["table"]["column"] = audformat.Column()
    db["table"]["column"].set(["label"])
    db.attachments["attachment?"] = audformat.Attachment(
        os.path.basename(attachment_file)
    )
    db.save(db_path)
    error_msg = (
        "Attachment IDs must only contain chars from [A-Za-z0-9._-], "
        "which is not the case for attachment 'attachment?'."
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(db_path, "1.0.0", repository)

    # Ensure column and scheme IDs are not affected
    db = audformat.Database("db")
    index = audformat.filewise_index(os.path.basename(audio_file))
    db.schemes["scheme?"] = audformat.Scheme("str")
    db["table"] = audformat.Table(index)
    db["table"]["column?"] = audformat.Column(scheme_id="scheme?")
    db["table"]["column?"].set(["label"])
    db.save(db_path)
    audb.publish(db_path, "1.0.0", repository)


def test_publish_error_changed_deps_file_type(tmpdir, repository):
    # As we allow for every possible filename for attachments
    # and store them in the dependency table
    # besides media and table files
    # there can be a naming clash between those entries.
    # See https://github.com/audeering/audb/pull/244#issuecomment-1412211131

    # media => attachment
    error_msg = (
        "An attachment must not overlap with media or tables. "
        "But attachment 'attachment' contains 'data/file.wav'."
    )
    db_name = "test_publish_error_changed_deps_file_type-1"
    db_path = audeer.mkdir(tmpdir, "db")
    data_path = audeer.mkdir(db_path, "data")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, "file.wav"), signal, sampling_rate)
    db = audformat.Database(db_name)
    db["table"] = audformat.Table(audformat.filewise_index("data/file.wav"))
    db.attachments["attachment"] = audformat.Attachment("data/file.wav")
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)
    audeer.rmdir(db_path)

    # table => attachment
    error_msg = (
        "An attachment must not overlap with media or tables. "
        "But attachment 'attachment' contains 'db.table.csv'."
    )
    db_name = "test_publish_error_changed_deps_file_type-2"
    db_path = audeer.mkdir(tmpdir, "db")
    data_path = audeer.mkdir(db_path, "data")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, "file.wav"), signal, sampling_rate)
    db = audformat.Database(db_name)
    db["table"] = audformat.Table(audformat.filewise_index("data/file.wav"))
    db.attachments["attachment"] = audformat.Attachment("db.table.csv")
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)
    audeer.rmdir(db_path)

    # attachment => media
    error_msg = (
        "An attachment must not overlap with media or tables. "
        "But attachment 'attachment' contains 'data/file2.wav'."
    )
    db_name = "test_publish_error_changed_deps_file_type-3"
    db_path = audeer.mkdir(tmpdir, "db")
    data_path = audeer.mkdir(db_path, "data")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, "file1.wav"), signal, sampling_rate)
    audiofile.write(audeer.path(data_path, "file2.wav"), signal, sampling_rate)
    db = audformat.Database(db_name)
    db["table"] = audformat.Table(audformat.filewise_index("data/file1.wav"))
    db.attachments["attachment"] = audformat.Attachment("data/file2.wav")
    db.save(db_path)
    audb.publish(db_path, "1.0.0", repository)
    audeer.rmdir(db_path)
    db = audb.load_to(db_path, db_name, version="1.0.0")
    db["table"] = audformat.Table(
        audformat.filewise_index(["data/file1.wav", "data/file2.wav"])
    )
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "2.0.0", repository)
    audeer.rmdir(db_path)

    # attachment => table
    error_msg = (
        "An attachment must not overlap with media or tables. "
        "But attachment 'attachment' contains 'db.table2.csv'."
    )
    db_name = "test_publish_error_changed_deps_file_type-4"
    db_path = audeer.mkdir(tmpdir, "db")
    data_path = audeer.mkdir(db_path, "data")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audeer.path(data_path, "file.wav"), signal, sampling_rate)
    db = audformat.Database(db_name)
    db["table1"] = audformat.Table(audformat.filewise_index("data/file.wav"))
    db.attachments["attachment"] = audformat.Attachment("db.table2.csv")
    audeer.touch(db_path, "db.table2.csv")
    db.save(db_path)
    audb.publish(db_path, "1.0.0", repository)
    audeer.rmdir(db_path)
    db = audb.load_to(db_path, db_name, version="1.0.0")
    db["table2"] = audformat.Table(audformat.filewise_index("data/file.wav"))
    db.save(db_path)
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "2.0.0", repository)
    audeer.rmdir(db_path)


def test_publish_error_repository_does_not_exist(tmpdir, repository):
    db = audformat.Database("test")
    db.save(tmpdir)

    repository.name = "does-not-exist"
    with pytest.raises(audbackend.BackendError) as ex:
        audb.publish(tmpdir, "1.0.0", repository)
    assert "No such file or directory" in str(ex.value.exception)


@pytest.mark.parametrize(
    "file",
    [
        "file.Wav",
        "file.WAV",
        "file.1A",
    ],
)
def test_publish_error_uppercase_file_extension(tmpdir, repository, file):
    # Prepare files
    db_path = audeer.mkdir(tmpdir, "db")
    audeer.touch(db_path, file)
    # Prepare database
    db = audformat.Database("db")
    db["table"] = audformat.Table(audformat.filewise_index([file]))
    db["table"]["column"] = audformat.Column()
    db["table"]["column"].set(["label"])
    db.save(db_path)
    # Fail as we include file with uppercase letter
    error_msg = (
        "The file extension of a media file must be lowercase, "
        f"but '{file}' includes at least one uppercase letter."
    )
    with pytest.raises(RuntimeError, match=error_msg):
        audb.publish(db_path, "1.0.0", repository)


def test_publish_error_version(tmpdir, repository):
    # Only versions supported by audeer.StrictVersion
    # are allowed

    # Create simple database
    db_path = audeer.mkdir(tmpdir, "db")
    audio_file = audeer.path(db_path, "f1.wav")
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audio_file, signal, sampling_rate)
    db = audformat.Database("db")
    index = audformat.filewise_index(os.path.basename(audio_file))
    db["table"] = audformat.Table(index)
    db["table"]["column"] = audformat.Column()
    db["table"]["column"].set(["label"])
    db.save(db_path)

    error_msg = "invalid version number '1.0.0?'"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        audb.publish(db_path, "1.0.0?", repository)

    # Publish to check previous_version afterwards
    audb.publish(db_path, "1.0.0", repository)

    # Update database
    db["table"]["column"].set(["different-label"])
    db.save(db_path)

    error_msg = "invalid version number '1.0.0?'"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        audb.publish(db_path, "2.0.0", repository, previous_version="1.0.0?")


def test_publish_text_media_files(tmpdir, dbs, repository):
    r"""Test publishing databases containing text files as media files."""
    # Create a database, containing text media file
    build_dir = audeer.path(tmpdir, "./build")
    audeer.mkdir(build_dir)
    data_dir = audeer.mkdir(build_dir, "data")
    with open(audeer.path(data_dir, "file1.txt"), "w") as file:
        file.write("Text written by a person.\n")
    name = "text-db"
    db = audformat.Database(name)
    db.schemes["speaker"] = audformat.Scheme("str")
    index = audformat.filewise_index(["data/file1.txt"])
    db["files"] = audformat.Table(index)
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(["adam"])
    db.save(build_dir)

    # Publish database, containing text media file
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)

    assert deps.tables == ["db.files.csv"]
    file = "data/file1.txt"
    assert deps.media == [file]
    assert deps.bit_depth(file) == 0
    assert deps.channels(file) == 0
    assert deps.duration(file) == 0.0
    assert deps.format(file) == "txt"
    assert deps.sampling_rate(file) == 0

    db = audb.load(name, version=version, verbose=False, full_path=False)
    assert db.files == [file]
    assert list(db) == ["files"]
    assert os.path.exists(audeer.path(db.root, file))

    error_msg = f"Media file '{file}' does not support requesting a flavor."
    with pytest.raises(RuntimeError, match=error_msg):
        db = audb.load(name, version=version, channels=[0], verbose=False)

    # Publish database, containing text and media files
    audeer.rmdir(build_dir)
    shutil.copytree(dbs["1.0.0"], build_dir)  # start with db containing audio files
    db = audformat.Database.load(build_dir)
    speaker = db["files"]["speaker"].get()
    files = list(db.files)
    tables = list(db)
    data_dir = audeer.mkdir(build_dir, "data")
    with open(audeer.path(data_dir, "file1.txt"), "w") as file:
        file.write("Text written by a person.\n")
    index = audformat.filewise_index(["data/file1.txt"])
    db["files"].extend_index(index, inplace=True)
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(list(speaker.values) + ["adam"])
    db.name = name
    db.save(build_dir)

    # Publish database, containing text media file
    version = "2.0.0"
    deps = audb.publish(build_dir, version, repository, previous_version=None)

    assert deps.table_ids == tables
    file = "data/file1.txt"
    assert deps.media == files + [file]
    assert deps.bit_depth(file) == 0
    assert deps.channels(file) == 0
    assert deps.duration(file) == 0.0
    assert deps.format(file) == "txt"
    assert deps.sampling_rate(file) == 0

    db = audb.load(name, version=version, verbose=False, full_path=False)
    assert db.files == files + [file]
    assert list(db) == tables
    assert os.path.exists(audeer.path(db.root, file))

    error_msg = f"Media file '{file}' does not support requesting a flavor."
    with pytest.raises(RuntimeError, match=error_msg):
        db = audb.load(name, version=version, channels=[0], verbose=False)


def test_update_database(dbs, persistent_repository):
    version = "2.1.0"
    start_version = "2.0.0"

    audb.load_to(
        dbs[version],
        DB_NAME,
        version=start_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    # == Fail with missing dependency file
    previous_version = start_version
    dep_file = os.path.join(
        dbs[version],
        audb.core.define.DEPENDENCIES_FILE,
    )
    os.remove(dep_file)
    error_msg = (
        f"You want to depend on '{previous_version}' "
        f"of {DB_NAME}, "
        f"but you don't have a '{audb.core.define.DEPENDENCIES_FILE}' "
        f"file present "
        f"in {dbs[version]}. "
        f"Did you forgot to call "
        f"'audb.load_to({dbs[version]}, {DB_NAME}, "
        f"version={previous_version}?"
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            dbs[version],
            version,
            persistent_repository,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    # Reload data to restore dependency file
    shutil.rmtree(dbs[version])
    db = audb.load_to(
        dbs[version],
        DB_NAME,
        version=start_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    # Remove one media file and all attachments as in version 3.0.0
    remove_file = "audio/001.wav"
    remove_path = os.path.join(dbs[version], remove_file)
    os.remove(remove_path)
    del db.attachments["file"]
    del db.attachments["folder"]
    db.drop_files(remove_file)
    db.save(dbs[version])

    # == Fail as 2.0.0 is not the latest version
    previous_version = "latest"
    error_msg = (
        f"You want to depend on '{audb.latest_version(DB_NAME)}' "
        f"of {DB_NAME}, "
        f"but the dependency file "
        f"'{audb.core.define.DEPENDENCIES_FILE}' "
        f"in {dbs[version]} "
        f"does not match the dependency file "
        f"for the requested version in the repository. "
        f"Did you forgot to call "
        f"'audb.load_to({dbs[version]}, {DB_NAME}, "
        f"version='{audb.latest_version(DB_NAME)}') "
        f"or modified the file manually?"
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            dbs[version],
            version,
            persistent_repository,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    # == Fail as we require a previous version
    previous_version = None
    error_msg = (
        f"You did not set a dependency to a previous version, "
        f"but you have a '{audb.core.define.DEPENDENCIES_FILE}' file present "
        f"in {dbs[version]}."
    )
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        audb.publish(
            dbs[version],
            version,
            persistent_repository,
            previous_version=previous_version,
            num_workers=pytest.NUM_WORKERS,
            verbose=False,
        )

    previous_version = start_version
    deps = audb.publish(
        dbs[version],
        version,
        persistent_repository,
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
        version="3.0.0",
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db1.meta["audb"] = {}
    db2.meta["audb"] = {}
    assert db1 == db2


def test_update_database_without_media(tmpdir, persistent_repository):
    build_root = tmpdir
    previous_version = "1.0.0"
    version = "1.1.0"

    # attachments
    new_attachment = "new-atatchment"
    # attachment files
    # (all for attachment ID `folder`)
    new_attachment_files = [
        "extra/folder/file4.txt",
    ]
    altered_attachment_files = [
        "extra/folder/sub-folder/file3.txt",
    ]
    removed_attachment_files = [
        "extra/folder/file1.txt",
        "extra/folder/file2.txt",
    ]

    # tables
    new_table = "new-table"

    # media
    new_files = [
        "new/001.wav",
        "new/002.wav",
    ]
    alter_files = [
        "audio/001.wav",  # same archive as 'audio/00[2,3].wav'
        "audio/005.wav",
    ]
    rem_files = [
        "new/003.wav",  # same archive as 'audio/00[1,2].wav'
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

    os.remove(audeer.path(build_root, "db.emotion.csv"))
    db = audb.load_to(
        build_root,
        DB_NAME,
        only_metadata=True,
        version=previous_version,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert os.path.exists(audeer.path(build_root, "db.emotion.csv"))

    # update and save database

    # remove media files
    for file in rem_files:
        db.drop_files(file)

    # create and alter media files
    sampling_rate = 16000
    signal = np.ones((1, sampling_rate), np.float32)
    for file in new_files + alter_files:
        path = audeer.path(build_root, file)
        audeer.mkdir(os.path.dirname(path))
        audiofile.write(path, signal, sampling_rate)

    # add new table
    db[new_table] = audformat.Table(audformat.filewise_index(new_files))

    # Add, alter, and remove attachment files for `folder` attachment.
    # The removal is done automatically as we alter
    # and add a file in the same folder
    # as the to be removed attachment files are located.
    for attachment_file in new_attachment_files:
        path = audeer.path(build_root, attachment_file)
        audeer.mkdir(os.path.dirname(path))
        audeer.touch(path)
    for attachment_file in altered_attachment_files:
        path = audeer.path(build_root, attachment_file)
        audeer.mkdir(os.path.dirname(path))
        with open(path, "w") as fp:
            fp.write("abc")
    for attachment_file in removed_attachment_files:
        path = audeer.path(build_root, attachment_file)
        assert not os.path.exists(path)

    # add new attachment
    audeer.mkdir(build_root, "extra")
    audeer.touch(build_root, "extra/new.txt")
    db.attachments[new_attachment] = audformat.Attachment("extra/new.txt")

    db.save(build_root)

    # publish database
    audb.publish(
        build_root,
        version,
        persistent_repository,
        previous_version=previous_version,
        verbose=False,
    )

    # check if media files missing from the same archive
    # were downloaded during publish
    assert os.path.exists(audeer.path(build_root, "audio/002.wav"))

    # load new version and check media and attachments
    db_load = audb.load(
        DB_NAME,
        version=version,
        verbose=False,
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
    assert new_attachment in list(db_load.attachments)
    assert db.attachments == db_load.attachments
    for attachment in db_load.attachments:
        path = audeer.path(db_load.root, db_load.attachments[attachment].path)
        assert os.path.exists(path)
        for attachment_file in db_load.attachments[attachment].files:
            assert os.path.exists(audeer.path(db_load.root, attachment_file))
    for attachment_file in removed_attachment_files:
        assert not os.path.exists(audeer.path(db_load.root, attachment_file))
    for attachment_file in new_attachment_files + altered_attachment_files:
        assert os.path.exists(audeer.path(db_load.root, attachment_file))
        assert filecmp.cmp(
            audeer.path(build_root, attachment_file),
            audeer.path(db_load.root, attachment_file),
        )
