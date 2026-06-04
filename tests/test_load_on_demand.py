import os

import pandas as pd
import pytest

import audeer
import audformat
import audformat.testing

import audb


DB_NAME = "test_load_on_demand"
DB_VERSION = "1.0.0"


@pytest.fixture(
    scope="module",
    autouse=True,
)
def dbs(tmpdir_factory, persistent_repository, storage_format):
    # Collect single database paths
    # and return them in the end
    paths = {}

    # publish 1.0.0

    db_root = tmpdir_factory.mktemp(DB_VERSION)
    paths[DB_VERSION] = db_root

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes["scheme"] = audformat.Scheme()
    audformat.testing.add_table(
        db,
        "table1",
        "filewise",
        num_files=[0, 1, 2],
    )
    audformat.testing.add_table(
        db,
        "table2",
        "filewise",
        num_files=[1, 2, 3],
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
    db.schemes["misc"] = audformat.Scheme(
        "int",
        labels="misc-in-scheme",
    )
    db.attachments["file"] = audformat.Attachment("file.txt")
    db.attachments["folder"] = audformat.Attachment("folder/")
    audeer.mkdir(db_root, "folder")
    audeer.touch(db_root, "file.txt")
    audeer.touch(db_root, "folder/file1.txt")
    audeer.touch(db_root, "folder/file2.txt")
    db.save(db_root, storage_format=storage_format)
    audformat.testing.create_audio_files(db)

    audb.publish(
        db_root,
        DB_VERSION,
        persistent_repository,
        verbose=False,
    )

    return paths


def test_load_only_metadata(dbs, storage_format):
    db_original = audformat.Database.load(dbs[DB_VERSION])

    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

    assert db["table1"] == db_original["table1"]
    assert db["table2"] == db_original["table2"]
    assert db["misc-in-scheme"] == db_original["misc-in-scheme"]
    assert db["misc-not-in-scheme"] == db_original["misc-not-in-scheme"]
    pd.testing.assert_index_equal(db.files, db_original.files)
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        assert not os.path.exists(path)
    for file in list(db.files):
        path = audeer.path(db.root, file)
        assert not os.path.exists(path)
    assert not db.meta["audb"]["complete"]

    # Delete table1
    # to force downloading from backend again
    os.remove(os.path.join(db.meta["audb"]["root"], f"db.table1.{storage_format}"))
    os.remove(os.path.join(db.meta["audb"]["root"], "db.table1.pkl"))
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )
    assert db["table1"] == db_original["table1"]
    assert db["table2"] == db_original["table2"]
    assert db["misc-in-scheme"] == db_original["misc-in-scheme"]
    assert db["misc-not-in-scheme"] == db_original["misc-not-in-scheme"]
    pd.testing.assert_index_equal(db.files, db_original.files)
    assert not db.meta["audb"]["complete"]

    # Load whole database
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_original.meta = []
    db.meta = []
    assert db == db_original


@pytest.mark.parametrize(
    "attachments, "
    "media, "
    "tables, "
    "expected_attachments, "
    "expected_media, "
    "expected_tables, "
    "complete",
    [
        (
            None,
            None,
            None,
            ["file", "folder"],
            [
                "audio/000.wav",
                "audio/001.wav",
                "audio/002.wav",
                "audio/003.wav",
            ],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            True,
        ),
        (
            [],
            None,
            None,
            [],
            [
                "audio/000.wav",
                "audio/001.wav",
                "audio/002.wav",
                "audio/003.wav",
            ],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            False,
        ),
        (
            None,
            [],
            None,
            ["file", "folder"],
            [],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            False,
        ),
        (
            None,
            None,
            [],
            ["file", "folder"],
            [],
            ["misc-in-scheme"],  # misc table used in scheme is still loaded
            False,
        ),
        (
            [],
            [],
            [],
            [],
            [],
            ["misc-in-scheme"],
            False,
        ),
        (
            [],
            [],
            None,
            [],
            [],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            False,
        ),
        (
            [],
            None,
            [],
            [],
            [],
            ["misc-in-scheme"],
            False,
        ),
        (
            None,
            [],
            [],
            ["file", "folder"],
            [],
            ["misc-in-scheme"],
            False,
        ),
        (
            "folder",
            None,
            None,
            ["folder"],
            [
                "audio/000.wav",
                "audio/001.wav",
                "audio/002.wav",
                "audio/003.wav",
            ],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            False,
        ),
        (
            ["file"],
            None,
            ["table1"],
            ["file"],
            [
                "audio/000.wav",
                "audio/001.wav",
                "audio/002.wav",
            ],
            ["misc-in-scheme", "table1"],
            False,
        ),
        (
            ["file", "folder"],
            None,
            ".*1",
            ["file", "folder"],
            [
                "audio/000.wav",
                "audio/001.wav",
                "audio/002.wav",
            ],
            ["misc-in-scheme", "table1"],
            False,
        ),
        (
            None,
            ".3.wav",
            None,
            ["file", "folder"],
            [
                "audio/003.wav",
            ],
            ["misc-in-scheme", "misc-not-in-scheme", "table1", "table2"],
            False,
        ),
    ],
)
def test_load_filter(
    tmpdir,
    attachments,
    media,
    tables,
    expected_attachments,
    expected_media,
    expected_tables,
    complete,
):
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        attachments=attachments,
        media=media,
        tables=tables,
        full_path=False,
        cache_root=tmpdir,
        verbose=False,
    )
    assert list(db) == expected_tables
    assert list(db.files) == expected_media
    assert list(db.attachments) == ["file", "folder"]
    for attachment in db.attachments:
        path = audeer.path(db.root, db.attachments[attachment].path)
        if attachment in expected_attachments:
            assert os.path.exists(path)
            for file in db.attachments[attachment].files:
                assert os.path.exists(audeer.path(db.root, file))
        else:
            assert not os.path.exists(path)
    assert db.meta["audb"]["complete"] == complete


def test_complete_file(dbs):
    r"""A completely loaded database is marked by a ``.complete`` file."""
    # Partial load does not mark the database as complete
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        tables=[],
        media=[],
        full_path=False,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]
    complete_file = os.path.join(db_root, audb.core.define.COMPLETE_FILE)
    assert not os.path.exists(complete_file)
    assert not db.meta["audb"]["complete"]

    # Full load creates the ``.complete`` file
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    assert os.path.exists(complete_file)
    assert db.meta["audb"]["complete"]

    # Completeness is no longer stored in the database header
    header = audformat.Database.load(db_root, load_data=False)
    assert "complete" not in header.meta["audb"]


def test_complete_skips_lock(dbs):
    r"""The cache folder is locked only for an incomplete database.

    A complete database is loaded without acquiring the lock,
    whereas an incomplete database still requires it.

    """
    # Partially load the database
    # -> no `.complete` file
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]
    lock_file = os.path.join(db_root, audb.core.define.LOCK_FILE)
    assert not os.path.exists(os.path.join(db_root, audb.core.define.COMPLETE_FILE))

    # Simulate a lock held by another process
    audeer.touch(lock_file)
    try:
        # Loading acquires the lock for an incomplete database,
        # hence it cannot be loaded with ``timeout=0``
        with pytest.warns(UserWarning, match=audb.core.define.TIMEOUT_MSG):
            db = audb.load(
                DB_NAME,
                version=DB_VERSION,
                full_path=False,
                timeout=0,
                verbose=False,
            )
        assert db is None
    finally:
        os.remove(lock_file)

    # Completely load the database
    # -> `.complete` file is created
    audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    assert os.path.exists(os.path.join(db_root, audb.core.define.COMPLETE_FILE))

    # Simulate a lock held by another process
    audeer.touch(lock_file)
    try:
        # Loading does not acquire the lock for a complete database,
        # hence it succeeds even with ``timeout=0``
        db = audb.load(
            DB_NAME,
            version=DB_VERSION,
            full_path=False,
            timeout=0,
            verbose=False,
        )
        assert db is not None
        assert db.meta["audb"]["complete"]
    finally:
        os.remove(lock_file)


def test_complete_legacy_header(dbs):
    r"""Fall back to completeness stored in the header.

    Databases cached with a version of audb
    before the introduction of the ``.complete`` file
    store the completeness information
    in the database header instead.

    """
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]
    complete_file = os.path.join(db_root, audb.core.define.COMPLETE_FILE)
    assert os.path.exists(complete_file)

    # Emulate an old cache:
    # remove the ``.complete`` file
    # and store the completeness in the header
    os.remove(complete_file)
    header = audformat.Database.load(db_root, load_data=False)
    header.meta["audb"]["complete"] = True
    header.save(db_root, header_only=True)

    # Loading recognizes the database as complete
    # and recreates the ``.complete`` file
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    assert os.path.exists(complete_file)
    assert db.meta["audb"]["complete"]


def test_complete_other_loaders(dbs):
    r"""On-demand loaders use no lock for a complete database."""
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]
    assert os.path.exists(os.path.join(db_root, audb.core.define.COMPLETE_FILE))
    media = list(db.files)[0]

    # All on-demand loaders work on a complete database without locking
    audb.info.header(DB_NAME, version=DB_VERSION)
    audb.load_table(DB_NAME, "table1", version=DB_VERSION, verbose=False)
    audb.load_media(DB_NAME, media, version=DB_VERSION, verbose=False)
    audb.load_attachment(DB_NAME, "file", version=DB_VERSION, verbose=False)
    next(
        audb.stream(
            DB_NAME,
            "table1",
            version=DB_VERSION,
            batch_size=1,
            verbose=False,
        )
    )


def test_complete_legacy_header_load_media(dbs):
    r"""``load_media`` falls back to completeness stored in the header."""
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        full_path=False,
        verbose=False,
    )
    db_root = db.meta["audb"]["root"]
    complete_file = os.path.join(db_root, audb.core.define.COMPLETE_FILE)
    media = list(db.files)[0]

    # Emulate an old cache:
    # remove the ``.complete`` file
    # and store the completeness in the header
    os.remove(complete_file)
    header = audformat.Database.load(db_root, load_data=False)
    header.meta["audb"]["complete"] = True
    header.save(db_root, header_only=True)

    # ``load_media`` recognizes the database as complete from the header,
    # and recreates the ``.complete`` file
    audb.load_media(DB_NAME, media, version=DB_VERSION, verbose=False)
    assert os.path.exists(complete_file)
