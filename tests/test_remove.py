import os

import pytest

import audeer
import audformat.testing

import audb


DB_NAME = "test_remove"
DB_FILES = {
    "1.0.0": [
        "audio/bundle1.wav",
        "audio/bundle2.wav",
        "audio/single.wav",
    ],
    "2.0.0": [
        "audio/new.wav",
    ],
}


@pytest.fixture(
    scope="module",
    autouse=True,
)
def dbs(tmpdir_factory, persistent_repository):
    r"""Publish databases.

    This publishes a database with the name ``DB_NAME``
    and the versions 1.0.0 and 2.0.0
    to a module wide repository.

    """
    db_root = tmpdir_factory.mktemp("db")

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db["files"] = audformat.Table(audformat.filewise_index(DB_FILES["1.0.0"]))

    # publish 1.0.0

    db.save(db_root)
    audformat.testing.create_audio_files(db)
    archives = {
        db.files[0]: "bundle",
        db.files[1]: "bundle",
    }
    audb.publish(
        db_root,
        "1.0.0",
        persistent_repository,
        archives=archives,
        verbose=False,
    )

    # publish 2.0.0

    db["files"].extend_index(
        audformat.filewise_index(DB_FILES["2.0.0"]),
        inplace=True,
    )
    db.save(db_root)
    audformat.testing.create_audio_files(db)
    audb.publish(
        db_root,
        "2.0.0",
        persistent_repository,
        verbose=False,
    )


@pytest.mark.parametrize(
    "format",
    [
        None,
        "wav",
        "flac",
    ],
)
def test_remove(cache, format):
    for remove in (
        DB_FILES["1.0.0"][0],  # bundle1
        DB_FILES["1.0.0"][1],  # bundle2
        DB_FILES["1.0.0"][2],  # single
        DB_FILES["2.0.0"][0],  # new
    ):
        # remove db cache to ensure we always get a fresh copy
        audeer.rmdir(cache)

        audb.remove_media(DB_NAME, remove)

        for removed_media in [False, True]:
            for version in audb.versions(DB_NAME):
                if remove in DB_FILES[version]:
                    if format is not None:
                        name, _ = os.path.splitext(remove)
                        removed_file = f"{name}.{format}"
                    else:
                        removed_file = remove

                    db = audb.load(
                        DB_NAME,
                        version=version,
                        format=format,
                        removed_media=removed_media,
                        full_path=False,
                        num_workers=pytest.NUM_WORKERS,
                        verbose=False,
                    )

                    if removed_media:
                        assert removed_file in db.files
                    else:
                        assert removed_file not in db.files
                    assert removed_file not in audeer.list_file_names(
                        os.path.join(db.meta["audb"]["root"], "audio"),
                    )

        # Make sure calling it again doesn't raise error
        audb.remove_media(DB_NAME, remove)
