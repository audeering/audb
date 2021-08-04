import os
import pytest

import audeer

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


@pytest.mark.parametrize(
    'shared, expected',
    [
        (False, audeer.safe_path(pytest.CACHE_ROOT)),
        (True, audeer.safe_path(pytest.SHARED_CACHE_ROOT)),
    ]
)
def test_cache_root(shared, expected):
    assert audb.default_cache_root(shared=shared) == expected


def test_empty_shared_cache():
    # Handle non-existing cache folder
    # See https://github.com/audeering/audb/issues/125
    assert not os.path.exists(audeer.safe_path(pytest.SHARED_CACHE_ROOT))
    df = audb.cached(shared=True)
    assert len(df) == 0
    # Handle empty shared cache folder
    # See https://github.com/audeering/audb/issues/126
    audeer.mkdir(pytest.SHARED_CACHE_ROOT)
    df = audb.cached(shared=True)
    assert 'name' in df.columns


def test_cached_name():
    # Here we only have emodb available.
    # A test using more published databases
    # is executed in test_publish.py
    df = audb.cached(name='emodb')
    assert len(df) > 0
    assert set(df['name']) == set(['emodb'])
    df = audb.cached(name='non-existent')
    assert len(df) == 0
