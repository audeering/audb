import pandas as pd
import pytest

import audb


ENTRIES = {
    'db.csv': [
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
    assert deps._df.columns == expected_columns


def test_call(deps):
    assert deps() == deps._df


def test_contains(deps):
    assert 'db.csv' in deps
    assert 'file.wav' in deps
    assert 'not.csv' not in deps


def test_get_item(deps):
    assert deps['db.csv'] == ENTRIES['db.csv']
    assert deps['file.wav'] == ENTRIES['file.wav']


def test_archives(deps):
    assert deps.archives == get_entries(audb.core.define.DependField.ARCHIVE)


def test_data(deps):
    assert deps.data == ENTRIES


def test_files(deps):
    assert deps.files == list(ENTRIES.values())


def test_media(deps):
    assert deps.media == [list(ENTRIES.keys())[1]]


def test_removed_media(deps):
    assert deps.removed_media == [
        get_entries(audb.core.define.DependField.REMOVED)[1]
    ]
