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
