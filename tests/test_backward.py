import os

import pytest


import audb2


os.environ['AUDB2_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB2_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


def test_get_default_cache_root():
    with pytest.warns(UserWarning):
        cache = audb2.get_default_cache_root()
    assert cache == audb2.default_cache_root()
