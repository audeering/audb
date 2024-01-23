import os

import numpy as np
import pytest

import audeer
import audformat.testing
import audiofile

import audb


DB_NAME = "test_convert"


@pytest.fixture(
    scope="module",
    autouse=True,
)
def db_root(tmpdir_factory, persistent_repository):
    r"""Publish single database.

    Returns:
        path to original database root

    """
    version = "1.0.0"
    db_root = tmpdir_factory.mktemp(version)

    # define audio files and metadata

    db_files = {
        "audio/file1.wav": {
            "bit_depth": 16,
            "channels": 1,
            "format": "wav",
            "sampling_rate": 8000,
        },
        "audio/file2.wav": {
            "bit_depth": 24,
            "channels": 2,
            "format": "wav",
            "sampling_rate": 16000,
        },
        "audio/file3.flac": {
            "bit_depth": 8,
            "channels": 3,
            "format": "flac",
            "sampling_rate": 44100,
        },
    }

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME

    db["files"] = audformat.Table(audformat.filewise_index(list(db_files)))
    db["files"]["original"] = audformat.Column()
    db["files"]["original"].set(list(db_files))
    for file in db_files:
        signal = np.zeros(
            (
                db_files[file]["channels"],
                db_files[file]["sampling_rate"],
            ),
            dtype=np.float32,
        )
        path = os.path.join(db_root, file)
        audeer.mkdir(os.path.dirname(path))
        audiofile.write(
            path,
            signal,
            db_files[file]["sampling_rate"],
            bit_depth=db_files[file]["bit_depth"],
        )
    db["segments"] = audformat.Table(
        audformat.segmented_index(
            [list(db_files)[0]] * 3,
            starts=["0s", "1s", "2s"],
            ends=["1s", "2s", "3s"],
        )
    )
    db.save(db_root)

    # publish db

    audb.publish(
        db_root,
        version,
        persistent_repository,
        verbose=False,
    )

    return db_root


@pytest.mark.parametrize(
    "bit_depth",
    [
        None,
        16,
    ],
)
def test_bit_depth(db_root, bit_depth):
    db = audb.load(
        DB_NAME,
        bit_depth=bit_depth,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db["files"]["original"].get()

    df = audb.cached()
    assert df["bit_depth"].values[0] == bit_depth

    for converted_file, original_file in zip(db.files, original_files):
        converted_file = os.path.join(db.meta["audb"]["root"], converted_file)
        original_file = os.path.join(db_root, original_file)

        if bit_depth is None:
            assert audiofile.bit_depth(converted_file) == audiofile.bit_depth(
                original_file
            )
        else:
            assert audiofile.bit_depth(converted_file) == bit_depth


@pytest.mark.parametrize(
    "channels",
    [
        None,
        1,
        [0, -1],
        range(5),
    ],
)
def test_channels(db_root, channels):
    db = audb.load(
        DB_NAME,
        channels=channels,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db["files"]["original"].get()

    df = audb.cached()

    for converted_file, original_file in zip(db.files, original_files):
        converted_file = os.path.join(db.meta["audb"]["root"], converted_file)
        original_file = os.path.join(db_root, original_file)

        if channels is None:
            assert audiofile.channels(converted_file) == audiofile.channels(
                original_file
            )
            assert df["channels"].values[0] == channels
        elif isinstance(channels, int):
            assert audiofile.channels(converted_file) == 1
            assert df["channels"].values[0] == [1]
        else:
            assert audiofile.channels(converted_file) == len(channels)


@pytest.mark.parametrize(
    "format",
    [
        None,
        audb.core.define.Format.WAV,
        audb.core.define.Format.FLAC,
    ],
)
def test_format(db_root, format):
    db = audb.load(
        DB_NAME,
        format=format,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db["files"]["original"].get()

    df = audb.cached()
    assert df["format"].values[0] == format

    for converted_file, original_file in zip(db.files, original_files):
        converted_file = os.path.join(db.meta["audb"]["root"], converted_file)
        original_file = os.path.join(db_root, original_file)

        if format is None:
            assert converted_file[-4:] == original_file[-4:]
        else:
            assert converted_file.endswith(format)


@pytest.mark.parametrize(
    "mixdown",
    [False, True],
)
def test_mixdown(db_root, mixdown):
    db = audb.load(
        DB_NAME,
        mixdown=mixdown,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db["files"]["original"].get()

    df = audb.cached()
    assert df["mixdown"].values[0] == mixdown

    for converted_file, original_file in zip(db.files, original_files):
        converted_file = os.path.join(db.meta["audb"]["root"], converted_file)
        original_file = os.path.join(db_root, original_file)

        if mixdown:
            assert audiofile.channels(converted_file) == 1
        else:
            assert audiofile.channels(converted_file) == audiofile.channels(
                original_file
            )


@pytest.mark.parametrize(
    "sampling_rate",
    [None, 16000],
)
def test_sampling_rate(db_root, sampling_rate):
    db = audb.load(
        DB_NAME,
        sampling_rate=sampling_rate,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db["files"]["original"].get()

    df = audb.cached()
    assert df["sampling_rate"].values[0] == sampling_rate

    for converted_file, original_file in zip(db.files, original_files):
        converted_file = os.path.join(db.meta["audb"]["root"], converted_file)
        original_file = os.path.join(db_root, original_file)

        if sampling_rate is None:
            assert audiofile.sampling_rate(converted_file) == audiofile.sampling_rate(
                original_file
            )
        else:
            assert audiofile.sampling_rate(converted_file) == sampling_rate


def test_mixed_cache(cache, shared_cache):
    # Avoid failing searching for other versions
    # if databases a stored across private and shared cache
    # and the private one is empty, see
    # https://github.com/audeering/audb/issues/101

    # First load to shared cache
    audb.load(
        DB_NAME,
        sampling_rate=8000,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
        only_metadata=True,
        tables="files",
        cache_root=shared_cache,
    )
    # Now try to load same version to private cache
    # to force audb.cached() to return empty dataframe
    audeer.rmdir(cache)
    audb.load(
        DB_NAME,
        sampling_rate=8000,
        full_path=False,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
        only_metadata=True,
        tables="segments",
    )
