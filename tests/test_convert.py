import os
import shutil

import numpy as np
import pandas as pd
import pytest

import audformat.testing
import audeer
import audiofile

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.REPOSITORIES = [
    (
        audb2.config.FILE_SYSTEM_REGISTRY_NAME,
        pytest.HOST,
        pytest.REPOSITORY
    )
]
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = f'test_convert-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
BACKEND = audb2.backend.FileSystem(pytest.HOST)

DB_FILES = {
    os.path.join('audio', 'file1.wav'): {
        'bit_depth': 16,
        'channels': 1,
        'format': 'wav',
        'sampling_rate': 8000,
    },
    os.path.join('audio', 'file2.wav'): {
        'bit_depth': 24,
        'channels': 2,
        'format': 'wav',
        'sampling_rate': 16000,
    },
    os.path.join('audio', 'file3.flac'): {
        'bit_depth': 8,
        'channels': 3,
        'format': 'flac',
        'sampling_rate': 44100,
    },
}


def clear_root(root: str):
    root = audeer.safe_path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db['files'] = audformat.Table(audformat.filewise_index(list(DB_FILES)))
    db['files']['original'] = audformat.Column()
    db['files']['original'].set(list(DB_FILES))
    for file in DB_FILES:
        signal = np.zeros(
            (
                DB_FILES[file]['channels'],
                DB_FILES[file]['sampling_rate'],
            ),
            dtype=np.float32,
        )
        path = os.path.join(DB_ROOT, file)
        audeer.mkdir(os.path.dirname(path))
        audiofile.write(
            path, signal, DB_FILES[file]['sampling_rate'],
            bit_depth=DB_FILES[file]['bit_depth']
        )
    db['segments'] = audformat.Table(
        audformat.segmented_index(
            [list(DB_FILES)[0]] * 3,
            starts=['0s', '1s', '2s'],
            ends=['1s', '2s', '3s'],
        )
    )
    db.save(DB_ROOT)

    # publish db

    audb2.publish(
        DB_ROOT,
        '1.0.0',
        pytest.REPOSITORY,
        backend=BACKEND,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_clear_cache():
    clear_root(pytest.CACHE_ROOT)
    yield
    clear_root(pytest.CACHE_ROOT)


@pytest.mark.parametrize(
    'bit_depth',
    [
        None, 16,
    ],
)
def test_bit_depth(bit_depth):

    db = audb2.load(
        DB_NAME,
        bit_depth=bit_depth,
        full_path=False,
        backend=BACKEND,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db['files']['original'].get()

    for converted_file, original_file in zip(db.files, original_files):

        converted_file = os.path.join(db.meta['audb']['root'], converted_file)
        original_file = os.path.join(DB_ROOT, original_file)

        if bit_depth is None:
            assert audiofile.bit_depth(converted_file) == \
                   audiofile.bit_depth(original_file)
        else:
            assert audiofile.bit_depth(converted_file) == bit_depth


@pytest.mark.parametrize(
    'channels',
    [
        None, 1, [0, -1], range(5),
    ],
)
def test_channels(channels):

    db = audb2.load(
        DB_NAME,
        channels=channels,
        full_path=False,
        backend=BACKEND,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db['files']['original'].get()

    for converted_file, original_file in zip(db.files, original_files):

        converted_file = os.path.join(db.meta['audb']['root'], converted_file)
        original_file = os.path.join(DB_ROOT, original_file)

        if channels is None:
            assert audiofile.channels(converted_file) == \
                   audiofile.channels(original_file)
        elif isinstance(channels, int):
            assert audiofile.channels(converted_file) == 1
        else:
            assert audiofile.channels(converted_file) == len(channels)


@pytest.mark.parametrize(
    'format',
    [
        None, audb2.define.Format.WAV, audb2.define.Format.FLAC
    ],
)
def test_format(format):

    db = audb2.load(
        DB_NAME,
        format=format,
        full_path=False,
        backend=BACKEND,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db['files']['original'].get()

    for converted_file, original_file in zip(db.files, original_files):

        converted_file = os.path.join(db.meta['audb']['root'], converted_file)
        original_file = os.path.join(DB_ROOT, original_file)

        if format is None:
            assert converted_file[-4:] == original_file[-4:]
        else:
            assert converted_file.endswith(format)


@pytest.mark.parametrize(
    'mixdown',
    [
        False, True
    ],
)
def test_mixdown(mixdown):

    db = audb2.load(
        DB_NAME,
        mixdown=mixdown,
        full_path=False,
        backend=BACKEND,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db['files']['original'].get()

    for converted_file, original_file in zip(db.files, original_files):

        converted_file = os.path.join(db.meta['audb']['root'], converted_file)
        original_file = os.path.join(DB_ROOT, original_file)

        if mixdown:
            assert audiofile.channels(converted_file) == 1
        else:
            assert audiofile.channels(converted_file) == \
                   audiofile.channels(original_file)


@pytest.mark.parametrize(
    'sampling_rate',
    [
        None, 16000
    ],
)
def test_sampling_rate(sampling_rate):

    db = audb2.load(
        DB_NAME,
        sampling_rate=sampling_rate,
        full_path=False,
        backend=BACKEND,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )
    original_files = db['files']['original'].get()

    for converted_file, original_file in zip(db.files, original_files):

        converted_file = os.path.join(db.meta['audb']['root'], converted_file)
        original_file = os.path.join(DB_ROOT, original_file)

        if sampling_rate is None:
            assert audiofile.sampling_rate(converted_file) == \
                   audiofile.sampling_rate(original_file)
        else:
            assert audiofile.sampling_rate(converted_file) == sampling_rate
