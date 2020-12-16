import os
import shutil

import numpy as np
import pytest

import audata.testing
import audeer
import audiofile

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORY_PUBLIC = pytest.REPOSITORY_PUBLIC
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = 'test_convert'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
BACKEND = audb2.backend.FileSystem(DB_NAME, pytest.HOST)

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

    db = audata.testing.create_db(minimal=True)
    db.name = DB_NAME
    db['files'] = audata.Table(list(DB_FILES))
    db['files']['original'] = audata.Column()
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
    db.save(DB_ROOT)

    # publish db

    audb2.publish(DB_ROOT, '1.0.0', backend=BACKEND)

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
    [None, 16],
)
@pytest.mark.parametrize(
    'format',
    [None, audb2.define.Format.WAV, audb2.define.Format.FLAC],
)
@pytest.mark.parametrize(
    'mix',
    [None, audb2.define.Mix.MONO_ONLY, audb2.define.Mix.MONO,
     audb2.define.Mix.RIGHT, audb2.define.Mix.LEFT, audb2.define.Mix.STEREO,
     audb2.define.Mix.STEREO_ONLY, 1, [0, -1]],
)
@pytest.mark.parametrize(
    'sampling_rate',
    [None, 16000],
)
def test_convert(bit_depth, format, mix, sampling_rate):

    db = audb2.load(
        DB_NAME,
        bit_depth=bit_depth,
        format=format,
        mix=mix,
        full_path=False,
        sampling_rate=sampling_rate,
        backend=BACKEND,
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

        if format is None:
            assert converted_file[-4:] == original_file[-4:]
        else:
            assert converted_file.endswith(format)

        if mix is None:
            assert audiofile.channels(converted_file) == \
                   audiofile.channels(original_file)
        else:
            if mix == audb2.define.Mix.MONO:
                assert audiofile.channels(converted_file) == 1
            elif mix == audb2.define.Mix.MONO_ONLY:
                assert audiofile.channels(converted_file) == \
                       audiofile.channels(original_file) == 1
            elif mix in (
                audb2.define.Mix.LEFT,
                audb2.define.Mix.RIGHT,
            ):
                assert audiofile.channels(converted_file) == 1
                assert audiofile.channels(original_file) == 2
            elif mix == audb2.define.Mix.STEREO:
                assert audiofile.channels(converted_file) == 2
            elif mix == audb2.define.Mix.STEREO_ONLY:
                assert audiofile.channels(converted_file) == \
                       audiofile.channels(original_file) == 2
            elif isinstance(mix, int):
                assert audiofile.channels(converted_file) == 1
            else:
                assert audiofile.channels(converted_file) == len(mix)
