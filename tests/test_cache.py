import os

import pytest

import audeer
import audformat.testing

import audb


DB_NAMES = [
    "test_cache-0",
    "test_cache-1",
]


@pytest.fixture(
    scope="module",
    autouse=True,
)
def dbs(tmpdir_factory, persistent_repository):
    # create dbs

    for name in DB_NAMES:
        db_root = tmpdir_factory.mktemp(name)
        db = audformat.testing.create_db(minimal=True)
        db.name = name
        db["files"] = audformat.Table(audformat.filewise_index(["f1.wav"]))

        db.save(db_root)
        audformat.testing.create_audio_files(db)
        audb.publish(
            db_root,
            "1.0.0",
            persistent_repository,
            verbose=False,
        )


@pytest.mark.parametrize("shared", [False, True])
def test_cache_root(cache, shared_cache, shared):
    if shared:
        assert audb.default_cache_root(shared=shared) == shared_cache
    else:
        assert audb.default_cache_root(shared=shared) == cache


def test_empty_shared_cache(shared_cache):
    # Handle non-existing cache folder
    # See https://github.com/audeering/audb/issues/125
    audeer.rmdir(shared_cache)
    assert not os.path.exists(shared_cache)
    df = audb.cached(shared=True)
    assert len(df) == 0
    # Handle empty shared cache folder
    # See https://github.com/audeering/audb/issues/126
    audeer.mkdir(shared_cache)
    df = audb.cached(shared=True)
    assert "name" in df.columns


def test_cached_name(cache):
    # Empty cache
    df = audb.cached()
    assert len(df) == 0
    df = audb.cached(name=DB_NAMES[0])
    assert len(df) == 0
    # Load first database
    audb.load(DB_NAMES[0], verbose=False)
    df = audb.cached()
    assert len(df) == 1
    assert set(df["name"]) == {DB_NAMES[0]}
    df = audb.cached(name=DB_NAMES[0])
    assert len(df) == 1
    assert set(df["name"]) == {DB_NAMES[0]}
    df = audb.cached(name=DB_NAMES[1])
    assert len(df) == 0
    # Load second database
    audb.load(DB_NAMES[1], verbose=False)
    df = audb.cached()
    assert len(df) == 2
    assert set(df["name"]) == set(DB_NAMES)
    df = audb.cached(name=DB_NAMES[0])
    assert len(df) == 1
    assert set(df["name"]) == {DB_NAMES[0]}
    df = audb.cached(name=DB_NAMES[1])
    assert len(df) == 1
    assert set(df["name"]) == {DB_NAMES[1]}
    df = audb.cached(name="non-existent")
    assert len(df) == 0


def test_database_tmp_root_cleanup(tmpdir):
    """Test that database_tmp_root cleans up leftover tmp folders."""
    db_root = str(tmpdir / "db_root")
    tmp_root = db_root + "~"

    # Create tmp folder with some leftover files
    # (simulating an interrupted download)
    audeer.mkdir(tmp_root)
    leftover_file = os.path.join(tmp_root, "leftover.txt")
    with open(leftover_file, "w") as f:
        f.write("leftover content")
    leftover_dir = os.path.join(tmp_root, "leftover_dir")
    audeer.mkdir(leftover_dir)
    nested_file = os.path.join(leftover_dir, "nested.txt")
    with open(nested_file, "w") as f:
        f.write("nested content")

    # Verify leftover files exist
    assert os.path.exists(tmp_root)
    assert os.path.exists(leftover_file)
    assert os.path.exists(leftover_dir)
    assert os.path.exists(nested_file)

    # Call database_tmp_root - should clean up old tmp folder
    from audb.core.cache import database_tmp_root

    result_tmp_root = database_tmp_root(db_root)

    # Verify tmp folder was cleaned and recreated empty
    assert result_tmp_root == tmp_root
    assert os.path.exists(tmp_root)
    assert not os.path.exists(leftover_file)
    assert not os.path.exists(leftover_dir)
    assert not os.path.exists(nested_file)
    # Verify tmp folder is empty
    assert len(os.listdir(tmp_root)) == 0

    # Test calling it again - should handle already empty tmp folder
    result_tmp_root2 = database_tmp_root(db_root)
    assert result_tmp_root2 == tmp_root
    assert os.path.exists(tmp_root)
    assert len(os.listdir(tmp_root)) == 0

    # Test when tmp folder doesn't exist at all
    audeer.rmdir(tmp_root)
    assert not os.path.exists(tmp_root)
    result_tmp_root3 = database_tmp_root(db_root)
    assert result_tmp_root3 == tmp_root
    assert os.path.exists(tmp_root)
    assert len(os.listdir(tmp_root)) == 0
