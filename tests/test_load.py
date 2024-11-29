import os
import random
import shutil

import numpy as np
import pandas as pd
import pytest

import audeer
import audformat.testing
import audiofile

import audb


DB_NAME = "test_load"


@pytest.fixture(
    scope="function",
    autouse=True,
)
def assert_database_tmp_folder_is_deleted():
    """Fixture to test that the ~ tmp folder gets deleted.

    audb.load() first loads files to a folder
    named after the database
    and appended by ``'~'``.
    This folder should be deleted in the end.

    """
    yield

    dirs = audeer.list_dir_names(audb.default_cache_root(), recursive=True)
    assert len([d for d in dirs if d.endswith("~")]) == 0


@pytest.fixture(
    scope="module",
    autouse=True,
)
def dbs(tmpdir_factory, persistent_repository, storage_format):
    r"""Publish different versions of the same database.

    Returns:
        dictionary containing root folder for each version

    """
    # Fix seed for audformat.testing
    random.seed(1)

    # Collect single database paths
    # and return them in the end
    paths = {}

    # create db

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
    db.schemes["speaker"] = audformat.Scheme(labels=["adam", "eve"])
    db.schemes["misc"] = audformat.Scheme(
        "int",
        labels="misc-in-scheme",
    )
    db["files"] = audformat.Table(db.files)
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(
        ["adam", "adam", "eve", "eve"],
        index=audformat.filewise_index(db.files[:4]),
    )
    db["files"]["misc"] = audformat.Column(scheme_id="misc")
    db["files"]["misc"].set(
        [0, 1, 1, 2],
        index=audformat.filewise_index(db.files[:4]),
    )
    db.attachments["file"] = audformat.Attachment("extra/file.txt")
    db.attachments["folder"] = audformat.Attachment("extra/folder")

    # publish 1.0.0

    version = "1.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    audeer.mkdir(db_root, "extra/folder/sub-folder")
    audeer.touch(db_root, "extra/file.txt")
    audeer.touch(db_root, "extra/folder/file1.txt")
    audeer.touch(db_root, "extra/folder/file2.txt")
    audeer.touch(db_root, "extra/folder/sub-folder/file3.txt")
    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)
    archives = db["files"]["speaker"].get().dropna().to_dict()
    audb.publish(
        db_root,
        version,
        persistent_repository,
        archives=archives,
        verbose=False,
    )

    # publish 1.1.0, add table, remove attachment file

    previous_db_root = db_root
    version = "1.1.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    audformat.testing.add_table(
        db,
        "train",
        audformat.define.IndexType.SEGMENTED,
        columns={"label": ("scheme", None)},
    )
    shutil.copytree(
        audeer.path(previous_db_root, "extra"),
        audeer.path(db_root, "extra"),
    )
    os.remove(audeer.path(db_root, "extra/folder/file2.txt"))

    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)
    shutil.copy(
        audeer.path(previous_db_root, audb.core.define.DEPENDENCY_FILE),
        audeer.path(db_root, audb.core.define.DEPENDENCY_FILE),
    )
    audb.publish(
        db_root,
        version,
        persistent_repository,
        verbose=False,
    )

    # publish 1.1.1, change label

    previous_db_root = db_root
    version = "1.1.1"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    row = db["train"].index[0]
    db["train"].df.at[row, "label"] = None
    shutil.copytree(
        audeer.path(previous_db_root, "extra"),
        audeer.path(db_root, "extra"),
    )

    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)
    shutil.copy(
        audeer.path(previous_db_root, audb.core.define.DEPENDENCY_FILE),
        audeer.path(db_root, audb.core.define.DEPENDENCY_FILE),
    )
    audb.publish(
        db_root,
        version,
        persistent_repository,
        verbose=False,
    )

    # publish 2.0.0, alter and remove media, remove attachment

    previous_db_root = db_root
    version = "2.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    shutil.copytree(
        audeer.path(previous_db_root, "extra"),
        audeer.path(db_root, "extra"),
    )
    del db.attachments["file"]
    os.remove(audeer.path(db_root, "extra/file.txt"))

    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)
    file = os.path.join(db_root, db.files[0])
    y, sr = audiofile.read(file)
    y[0] = 1
    audiofile.write(file, y, sr)
    file = db.files[-1]
    db.pick_files(lambda x: x != file)
    os.remove(audeer.path(db_root, file))
    db.save(db_root, storage_format=storage_format)

    shutil.copy(
        os.path.join(previous_db_root, audb.core.define.DEPENDENCY_FILE),
        os.path.join(db_root, audb.core.define.DEPENDENCY_FILE),
    )
    audb.publish(
        db_root,
        version,
        persistent_repository,
        verbose=False,
    )

    # publish 3.0.0, remove table, alter attachment file

    previous_db_root = db_root
    version = "3.0.0"
    db_root = tmpdir_factory.mktemp(version)
    paths[version] = str(db_root)

    shutil.copytree(
        audeer.path(previous_db_root, "extra"),
        audeer.path(db_root, "extra"),
    )
    with open(audeer.path(db_root, "extra/folder/file1.txt"), "a") as fp:
        fp.write("text")

    db.drop_tables("train")
    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)
    shutil.copy(
        os.path.join(previous_db_root, audb.core.define.DEPENDENCY_FILE),
        os.path.join(db_root, audb.core.define.DEPENDENCY_FILE),
    )
    audb.publish(
        db_root,
        version,
        persistent_repository,
        verbose=False,
    )

    return paths


def test_database_cache_folder(cache):
    cache_root = os.path.join(cache, "cache")
    version = "1.0.0"
    db_root = audb.core.load.database_cache_root(
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


def test_load_wrong_argument():
    with pytest.raises(TypeError):
        audb.load(DB_NAME, typo="1.0.0")


@pytest.mark.parametrize("only_metadata", [True, False])
@pytest.mark.parametrize(
    "format",
    [
        None,
        "wav",
        "flac",
    ],
)
@pytest.mark.parametrize(
    "version",
    [
        None,  # 3.0.0
        "1.0.0",
        "1.1.0",
        "1.1.1",
        "2.0.0",
        pytest.param(
            "4.0.0",
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ],
)
def test_load(dbs, format, version, only_metadata):
    # When loading the first time (only_metadata=True)
    # the database should not exists in cache
    if only_metadata:
        assert not audb.exists(
            DB_NAME,
            version=version,
            format=format,
        )

    # === Load from REPOSITORY with full_path=False ===

    db = audb.load(
        DB_NAME,
        version=version,
        format=format,
        only_metadata=only_metadata,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]

    # Load original database from folder (expected database)
    resolved_version = version or audb.latest_version(DB_NAME)
    db_original = audformat.Database.load(dbs[resolved_version])
    if format is not None:
        db_original.map_files(lambda x: audeer.replace_file_extension(x, format))

    # Assert database exists in cache
    assert audb.exists(DB_NAME, version=version, format=format)
    df = audb.cached()
    assert df.loc[db_root, "version"] == resolved_version

    # Assert files duration are stored as hidden attribute
    if not only_metadata:
        files_duration = {
            os.path.join(db_root, os.path.normpath(file)): pd.to_timedelta(
                audiofile.duration(os.path.join(db_root, file)), unit="s"
            )
            for file in db.files
        }
        assert db._files_duration == files_duration

    # Assert media files are identical and (not) exist
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        if only_metadata:
            assert not os.path.exists(os.path.join(db_root, file))
        else:
            assert os.path.exists(os.path.join(db_root, file))

    # Assert tables are identical and table files exist
    for table in db:
        assert os.path.exists(os.path.join(db_root, f"db.{table}.csv"))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )

    # Assert attachments are identical and files do (not) exist
    assert db.attachments == db_original.attachments
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if only_metadata:
            assert not os.path.exists(path)
        else:
            assert os.path.exists(path)
            for attachment_file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, attachment_file))

    # Assert all files are listed in dependency table
    deps = audb.dependencies(DB_NAME, version=version)
    assert len(deps) == (
        len(db.files) + len(db.tables) + len(db.misc_tables) + len(db.attachments)
    )

    # === Load from CACHE with full_path=True ===

    db = audb.load(
        DB_NAME,
        version=version,
        full_path=True,
        format=format,
        only_metadata=only_metadata,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    # Assert files duration are stored as hidden attribute
    if not only_metadata:
        files_duration = {
            os.path.normpath(file): pd.to_timedelta(audiofile.duration(file), unit="s")
            for file in db.files
        }
        assert db._files_duration == files_duration

    # Assert media files do (not) exist
    for file in db.files:
        if only_metadata:
            assert not os.path.exists(file)
        else:
            assert os.path.exists(file)

    # Assert table files exist
    for table in db:
        assert os.path.exists(os.path.join(db_root, f"db.{table}.csv"))

    # Assert attachments files do (not) exist
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if only_metadata:
            assert not os.path.exists(path)
        else:
            assert os.path.exists(path)
            for attachment_file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, attachment_file))


def test_load_from_cache(dbs):
    # Load a database with flavor to cache
    # and reload afterwards from cache
    format = "flac"
    version = "1.0.0"
    db = audb.load(
        DB_NAME,
        version="1.0.0",
        format="flac",
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]

    # Load original database from folder (expected database)
    db_original = audformat.Database.load(dbs[version])
    db_original.map_files(lambda x: audeer.replace_file_extension(x, format))

    # Assert database exists in cache
    assert audb.exists(DB_NAME, version=version, format=format)
    df = audb.cached()
    assert df.loc[db_root, "version"] == version

    # Assert media files are identical and exist
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))

    # Ensure no media files in flavor cache are marked as missing files,
    # when flavor format is different from original format
    # (https://github.com/audeering/audb/issues/324)
    original_files = audformat.utils.replace_file_extension(db.files, "wav")
    assert (
        audb.core.load._missing_files(
            original_files,
            "media",
            db_root,
            audb.Flavor(format="flac"),
            False,
        )
        == []
    )

    version = "2.0.0"
    db = audb.load(
        DB_NAME,
        version="2.0.0",
        format="flac",
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]

    # Load original database from folder (expected database)
    db_original = audformat.Database.load(dbs[version])
    db_original.map_files(lambda x: audeer.replace_file_extension(x, format))

    # Assert database exists in cache
    assert audb.exists(DB_NAME, version=version, format=format)
    df = audb.cached()
    assert df.loc[db_root, "version"] == version

    # Assert media files are identical and exist
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        assert os.path.exists(os.path.join(db_root, file))


@pytest.mark.parametrize(
    "version, attachment_id",
    [
        (
            "1.0.0",
            "file",
        ),
        (
            "1.0.0",
            "folder",
        ),
        (
            None,
            "folder",
        ),
    ],
)
def test_load_attachment(cache, version, attachment_id):
    db = audb.load(
        DB_NAME,
        version=version,
        verbose=False,
    )

    paths = audb.load_attachment(
        DB_NAME,
        attachment_id,
        version=version,
        verbose=False,
    )

    if version is None:
        version = audb.latest_version(DB_NAME)

    expected_paths = [
        os.path.join(
            cache,
            DB_NAME,
            version,
            os.path.normpath(file),
        )
        for file in db.attachments[attachment_id].files
    ]
    assert paths == expected_paths

    # Clear cache to force loading from other cache
    cache_root = audb.core.load.database_cache_root(
        DB_NAME,
        version,
        cache,
        audb.Flavor(),
    )
    shutil.rmtree(cache_root)
    paths2 = audb.load_attachment(
        DB_NAME,
        attachment_id,
        version=version,
        verbose=False,
    )
    assert paths2 == expected_paths


@pytest.mark.parametrize(
    "version, attachment_id, error, error_msg",
    [
        ("1.0.0", "", ValueError, "Could not find the attachment ''"),
        (
            "1.0.0",
            "non-existent",
            ValueError,
            "Could not find the attachment 'non-existent'",
        ),
    ],
)
def test_load_attachment_errors(version, attachment_id, error, error_msg):
    with pytest.raises(error, match=error_msg):
        audb.load_attachment(
            DB_NAME,
            attachment_id,
            version=version,
            verbose=False,
        )


@pytest.mark.parametrize(
    "version, media, format",
    [
        (
            "1.0.0",
            [],
            None,
        ),
        (
            "1.0.0",
            "audio/001.wav",
            "wav",
        ),
        pytest.param(
            "1.0.0",
            ["audio/001.flac", "audio/002.flac"],
            "flac",
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            "1.0.0",
            ["audio/001.wav", "audio/002.wav"],
            "flac",
        ),
        (
            None,
            ["audio/001.wav"],
            None,
        ),
    ],
)
def test_load_media(cache, version, media, format):
    paths = audb.load_media(
        DB_NAME,
        media,
        version=version,
        format=format,
        verbose=False,
    )
    expected_paths = [os.path.join(cache, p) for p in paths]
    if format is not None:
        expected_paths = [
            audeer.replace_file_extension(p, format) for p in expected_paths
        ]
    assert paths == expected_paths

    # Clear cache to force loading from other cache
    if version is None:
        version = audb.latest_version(DB_NAME)
    cache_root = audb.core.load.database_cache_root(
        DB_NAME,
        version,
        cache,
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


@pytest.mark.parametrize("pickle_tables", [True, False])
@pytest.mark.parametrize("name, version, table", [(DB_NAME, "1.0.0", "emotion")])
class TestLoadPickle:
    r"""Test storing tables as pickle files in cache.

    When tables are first downloaded from a backend,
    they are stored in their original format in the cache,
    and dependent on the ``pickle_tables`` argument
    stored as pickle as well.

    """

    def assert_table_exist_in_cache(
        self, db_root, table, storage_format, pickle_tables
    ):
        """Assert table exists in original format and maybe as pickle.

        Args:
            db_root: database root folder
            table: table ID
            storage_format: storage format of table, ``"csv"`` or ``"parquet"``
            pickle_tables: if ``True``,
                table is asserted to exist as pickle file as well,
                otherwise to not exist

        """
        original_table = audeer.path(db_root, f"db.{table}.{storage_format}")
        assert os.path.exists(original_table)

        pickled_table = audeer.path(db_root, f"db.{table}.pkl")
        if pickle_tables:
            assert os.path.exists(pickled_table)
        else:
            assert not os.path.exists(pickled_table)

    def test_load_pickle(self, storage_format, name, version, table, pickle_tables):
        """Test storing tables with audb.load().

        Args:
            storage_format: storage_format fixture
            name: database name
            version: database version
            table: table ID
            pickle_tables: if ``True``,
                tables are stored as pickle files as well in cache

        """
        db = audb.load(
            name,
            version=version,
            tables=table,
            pickle_tables=pickle_tables,
            only_metadata=True,
            verbose=False,
        )
        self.assert_table_exist_in_cache(db.root, table, storage_format, pickle_tables)

    def test_load_table_pickle(
        self, cache, storage_format, name, version, table, pickle_tables
    ):
        r"""Test storing tables with audb.load_table().

        Args:
            cache: cache fixture
            storage_format: storage_format fixture
            name: database name
            version: database version
            table: table ID
            pickle_tables: if ``True``,
                tables are stored as pickle files as well in cache

        """
        audb.load_table(
            name,
            table,
            version=version,
            pickle_tables=pickle_tables,
            verbose=False,
        )
        db_root = audeer.path(cache, name, version)
        self.assert_table_exist_in_cache(db_root, table, storage_format, pickle_tables)


@pytest.mark.parametrize(
    "version, table",
    [
        (
            "1.0.0",
            "emotion",
        ),
        pytest.param(
            "1.0.0",
            "non-existing-table",
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            None,
            "emotion",
        ),
    ],
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
            "audio/001.wav",
            "audio/002.wav",
            "audio/003.wav",
            "audio/004.wav",
        ]
    elif version == "1.0.0":
        expected_files = [
            "audio/001.wav",
            "audio/002.wav",
            "audio/003.wav",
            "audio/004.wav",
            "audio/005.wav",
        ]
    files = sorted(list(set(df.index.get_level_values("file"))))
    assert files == expected_files


@pytest.mark.parametrize(
    "version, table, map, expected",
    [
        (
            "1.0.0",
            "files",
            None,
            pd.concat(
                [
                    pd.Series(
                        index=audformat.filewise_index(
                            [f"audio/00{n + 1}.wav" for n in range(5)]
                        ),
                        name="speaker",
                        data=["adam", "adam", "eve", "eve", None],
                        dtype=pd.CategoricalDtype(["adam", "eve"]),
                    ),
                    pd.Series(
                        index=audformat.filewise_index(
                            [f"audio/00{n + 1}.wav" for n in range(5)]
                        ),
                        name="misc",
                        data=[0, 1, 1, 2, np.nan],
                        dtype=pd.CategoricalDtype([0, 1, 2]),
                    ),
                ],
                axis=1,
            ),
        ),
        (
            "1.0.0",
            "files",
            {"misc": "emotion"},
            pd.concat(
                [
                    pd.Series(
                        index=audformat.filewise_index(
                            [f"audio/00{n + 1}.wav" for n in range(5)]
                        ),
                        name="speaker",
                        data=["adam", "adam", "eve", "eve", None],
                        dtype=pd.CategoricalDtype(["adam", "eve"]),
                    ),
                    pd.Series(
                        index=audformat.filewise_index(
                            [f"audio/00{n + 1}.wav" for n in range(5)]
                        ),
                        name="emotion",
                        data=["positive", "positive", "positive", "negative", None],
                        dtype=pd.CategoricalDtype(["positive", "neutral", "negative"]),
                    ),
                ],
                axis=1,
            ),
        ),
    ],
)
def test_load_table_map(version, table, map, expected):
    df = audb.load_table(
        DB_NAME,
        table,
        version=version,
        map=map,
        verbose=False,
    )
    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "version",
    [
        None,
        "1.0.0",
        "1.1.0",
        "1.1.1",
        "2.0.0",
        "3.0.0",
        pytest.param(
            "4.0.0",
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ],
)
@pytest.mark.parametrize("only_metadata", [True, False])
def test_load_to(tmpdir, dbs, version, only_metadata):
    db_root = audeer.path(tmpdir, "raw")

    db = audb.load_to(
        db_root,
        DB_NAME,
        version=version,
        only_metadata=only_metadata,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert db.root == db_root

    # Load original database from folder (expected database)
    resolved_version = version or audb.latest_version(DB_NAME)
    db_original = audformat.Database.load(dbs[resolved_version])

    # Assert media files are identical and do (not) exist
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        if only_metadata:
            assert not os.path.exists(os.path.join(db_root, file))
        else:
            assert os.path.exists(os.path.join(db_root, file))

    # Assert tables are identical and exist as files
    for table in db:
        assert os.path.exists(os.path.join(db_root, f"db.{table}.csv"))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )

    # Assert attachments are identical and files exist
    assert db.attachments == db_original.attachments
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if only_metadata:
            assert not os.path.exists(path)
        else:
            assert os.path.exists(path)
            for attachment_file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, attachment_file))


@pytest.mark.parametrize("only_metadata", [True, False])
def test_load_to_update(tmpdir, dbs, only_metadata):
    # Use version 1.0.0 as this contains two attachments,
    # one file and one folder
    # which is needed to reach full code coverage
    version = "1.0.0"

    db_root = audeer.path(tmpdir, "raw")

    db = audb.load_to(
        db_root,
        DB_NAME,
        version=version,
        only_metadata=only_metadata,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert db.root == db_root

    # Remove some files
    if not only_metadata:
        media = audeer.path(db_root, db.files[0])
        os.remove(media)
    table = audeer.path(db_root, f"db.{list(db)[0]}.csv")
    os.remove(table)

    # Change some files
    if not only_metadata:
        for attachment_id in list(db.attachments):
            attachment = audeer.path(
                db_root,
                db.attachments[attachment_id].path,
            )
            if os.path.isdir(attachment):
                audeer.touch(attachment, "other-file.txt")
            else:
                with open(attachment, "a") as fp:
                    fp.write("next")

    # Load again to force restoring to original state
    db = audb.load_to(
        db_root,
        DB_NAME,
        version=version,
        only_metadata=only_metadata,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    assert db.root == db_root

    # Load original database from folder (expected database)
    resolved_version = version or audb.latest_version(DB_NAME)
    db_original = audformat.Database.load(dbs[resolved_version])

    # Assert media files are identical and do (not) exist
    pd.testing.assert_index_equal(db.files, db_original.files)
    for file in db.files:
        if only_metadata:
            assert not os.path.exists(os.path.join(db_root, file))
        else:
            assert os.path.exists(os.path.join(db_root, file))

    # Assert tables are identical and exist as CSV files
    for table in db:
        assert os.path.exists(os.path.join(db_root, f"db.{table}.csv"))
        pd.testing.assert_frame_equal(
            db_original[table].df,
            db[table].df,
        )

    # Assert attachments are identical and files exist
    assert db.attachments == db_original.attachments
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if only_metadata:
            assert not os.path.exists(path)
        else:
            assert os.path.exists(path)
            for attachment_file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, attachment_file))


@pytest.mark.parametrize(
    "name, version, error, error_msg",
    [
        (DB_NAME, "1.0.0", None, None),
        pytest.param(  # database does not exist
            "does-not-exist",
            "1.0.0",
            RuntimeError,
            "Cannot find database 'does-not-exist'.",
        ),
        pytest.param(  # version does not exist
            DB_NAME,
            "999.9.9",
            RuntimeError,
            f"Cannot find version '999.9.9' for database '{DB_NAME}'.",
        ),
    ],
)
def test_repository(persistent_repository, name, version, error, error_msg):
    if error is not None:
        with pytest.raises(error, match=error_msg):
            repository = audb.repository(name, version)
    else:
        repository = audb.repository(name, version)
        assert repository == persistent_repository
