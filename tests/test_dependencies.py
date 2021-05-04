import os

import pandas as pd
import pytest

import audb


ENTRIES = {
    'db.files.csv': [
        'archive1',
        0,
        0,
        '7c1f6b568f7221ab968a705fd5e7477b',
        0,
        'csv',
        0,
        0,
        0,
        '1.0.0',
    ],
    'file.wav': [
        'archive2',
        16,
        2,
        '917338b854ad9c72f76bc9a68818dcd8',
        1.23,
        'wav',
        0,
        16000,
        1,
        '1.0.0',
    ],
}


def get_entries(index):
    return [v[index] for k, v in ENTRIES.items()]


def test_get_entries():
    assert get_entries(0) == ['archive1', 'archive2']


@pytest.fixture(
    scope='function',
)
def deps():
    deps = audb.Dependencies()
    deps._df = pd.DataFrame(
        data=list(ENTRIES.values()),
        columns=audb.core.define.DEPEND_FIELD_NAMES.values(),
        index=list(ENTRIES.keys()),
    )
    return deps


def test_init():
    deps = audb.Dependencies()
    expected_columns = audb.core.define.DEPEND_FIELD_NAMES.values()
    assert list(deps._df.columns) == list(expected_columns)


def test_call(deps):
    pd.testing.assert_frame_equal(deps(), deps._df)


def test_contains(deps):
    assert 'db.files.csv' in deps
    assert 'file.wav' in deps
    assert 'not.csv' not in deps


def test_get_item(deps):
    assert deps['db.files.csv'] == ENTRIES['db.files.csv']
    assert deps['file.wav'] == ENTRIES['file.wav']


def test_archives(deps):
    assert deps.archives == get_entries(audb.core.define.DependField.ARCHIVE)


def test_files(deps):
    assert deps.files == list(ENTRIES.keys())


def test_media(deps):
    assert deps.media == [list(ENTRIES.keys())[1]]


def test_removed_media(deps):
    assert deps.removed_media == []


def test_table_ids(deps):
    assert deps.table_ids == ['files']


def test_tables(deps):
    assert deps.tables == ['db.files.csv']


def test_archive(deps):
    assert deps.archive('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.ARCHIVE
    ]
    assert type(deps.archive('file.wav')) == str


def test_bit_depth(deps):
    assert deps.bit_depth('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.BIT_DEPTH
    ]
    assert type(deps.bit_depth('file.wav')) == int


def test_channels(deps):
    assert deps.channels('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.CHANNELS
    ]
    assert type(deps.channels('file.wav')) == int


def test_checksum(deps):
    assert deps.checksum('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.CHECKSUM
    ]
    assert type(deps.checksum('file.wav')) == str


def test_duration(deps):
    assert deps.duration('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.DURATION
    ]
    assert type(deps.duration('file.wav')) == float


def test_format(deps):
    assert deps.format('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.FORMAT
    ]
    assert type(deps.format('file.wav')) == str


def test_removed(deps):
    assert not deps.removed('file.wav')
    assert type(deps.removed('file.wav')) == bool


def test_load_save(deps):
    deps_file = 'deps.csv'
    deps.save(deps_file)
    deps2 = audb.Dependencies()
    deps2.load(deps_file)
    pd.testing.assert_frame_equal(deps(), deps2())
    os.remove(deps_file)
    # Wrong extension or file missng
    with pytest.raises(ValueError, match=r".*'txt'.*"):
        deps2.load('deps.txt')
    with pytest.raises(FileNotFoundError):
        deps.load(deps_file)


def test_remove(deps):
    deps._remove('file.wav')
    assert 'file.wav' in deps.files
    assert deps.removed('file.wav')


def test_sampling_rate(deps):
    assert deps.sampling_rate('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.SAMPLING_RATE
    ]
    assert type(deps.sampling_rate('file.wav')) == int


def test_type(deps):
    assert deps.type('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.TYPE
    ]
    assert type(deps.type('file.wav')) == int


def test_version(deps):
    assert deps.version('file.wav') == ENTRIES['file.wav'][
        audb.core.define.DependField.VERSION
    ]
    assert type(deps.version('file.wav')) == str


def test_len(deps):
    assert len(deps) == len(ENTRIES)


def test_str(deps):
    assert str(deps) == deps._df.to_string()
