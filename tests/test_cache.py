import pytest

import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORIES = [pytest.REPOSITORY]
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


@pytest.mark.parametrize(
    'shared, expected',
    [
        (False, audeer.safe_path(audb2.config.CACHE_ROOT)),
        (True, audeer.safe_path(audb2.config.SHARED_CACHE_ROOT)),
    ]
)
def test_cache_root(shared, expected):
    assert audb2.default_cache_root(shared=shared) == expected
