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
