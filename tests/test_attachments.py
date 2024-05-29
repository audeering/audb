import os

import pytest

import audeer
import audformat

import audb


@pytest.fixture(scope="function", autouse=False)
def same_cache():
    r"""Set shared cache to same folder as user cache."""
    current_shared_cache = audb.config.SHARED_CACHE_ROOT
    audb.config.SHARED_CACHE_ROOT = audb.config.CACHE_ROOT

    yield

    audb.config.SHARED_CACHE_ROOT = current_shared_cache


def test_loading_attachments_from_cache(tmpdir, repository, same_cache):
    r"""Ensures loading attachments from cache works.

    As reported in https://github.com/audeering/audb/issues/314,
    loading an attachment for a database
    fails to acquire a lock,
    if the database has a previous version in the cache,
    and ``audb.config.CACHE_ROOT`` and ``audb.config.SHARED_CACHE_ROOT``
    point to the same folder.

    """
    # Create version 1.0.0 of database,
    # publish and load
    db_name = "db"
    db_version = "1.0.0"
    db_root = audeer.mkdir(audeer.path(tmpdir, db_name))
    db = audformat.Database(db_name)
    db.description = f"Version {db_version} of database."
    filename = "file.txt"
    with open(audeer.path(db_root, filename), "w") as file:
        file.write(f"{filename}\n")
    db.attachments[filename] = audformat.Attachment(filename)
    db.save(db_root)

    audb.publish(db_root, db_version, repository)

    db = audb.load(db_name, version=db_version, verbose=False)

    # Create version 2.0.0 of database,
    # and publish and load
    db = audb.load_to(db_root, db_name, version=db_version)
    db_version = "2.0.0"
    db.description = f"Version {db_version} of database."
    db.save(db_root)

    audb.publish(db_root, db_version, repository)

    db = audb.load(db_name, version=db_version, verbose=False)

    # Ensure attachment file is loaded
    assert list(db.attachments) == [filename]
    for attachment in list(db.attachments):
        for file in db.attachments[attachment].files:
            assert os.path.exists(audeer.path(db.root, file))
