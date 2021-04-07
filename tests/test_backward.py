import os

import pytest


import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


def test_get_default_cache_root():
    with pytest.warns(UserWarning):
        cache = audb.get_default_cache_root()
    assert cache == audb.default_cache_root()
