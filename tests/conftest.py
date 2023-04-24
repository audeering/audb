import glob
import os

import pytest

import audb
import audeer


pytest.ID = audeer.uid()
pytest.NUM_WORKERS = 5


@pytest.fixture(scope='session', autouse=True)
def cleanup_coverage_files():
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '.coverage.*',
    )
    for file in glob.glob(path):
        os.remove(file)


@pytest.fixture(scope='session', autouse=True)
def cleanup_environment_variables():
    env_cache = os.environ.get('AUDB_CACHE_ROOT', None)
    env_shared_cache = os.environ.get('AUDB_SHARED_CACHE_ROOT', None)
    if env_cache is not None:
        del os.environ['AUDB_CACHE_ROOT']
    if env_shared_cache is not None:
        del os.environ['AUDB_SAHRED_CACHE_ROOT']

    yield

    if env_cache is not None:
        os.environ['AUDB_CACHE_ROOT'] = env_cache
    if env_shared_cache is not None:
        os.environ['AUDB_SHARED_CACHE_ROOT'] = env_shared_cache


# === CACHE ===
#
# Provide two fixtures that create tmp folders
# holding the cache and shared cache folder.
# A fresh tmp folder is used for each test.
#
@pytest.fixture(scope='function', autouse=True)
def cache(tmp_path):
    cache = tmp_path / 'cache'
    cache.mkdir()
    cache = str(cache)
    audb.config.CACHE_ROOT = cache
    return cache


@pytest.fixture(scope='function', autouse=True)
def shared_cache(tmp_path):
    cache = tmp_path / 'shared_cache'
    cache.mkdir()
    cache = str(cache)
    audb.config.SHARED_CACHE_ROOT = cache
    return cache


# === REPOSITORIES ===
#
# Provide two fixtures that create tmp folders
# holding a repository on a file-system backend.
# One fixture provides a fresh repository
# for each test,
# the other fixture allows to reuse the same repository
# across all tests in a module.
#
@pytest.fixture(scope='module', autouse=False)
def persistent_repository(tmp_path_factory):
    host = tmp_path_factory.mktemp('host').as_posix()
    repository = audb.Repository(
        name='data-unittests-local',
        host=host,
        backend='file-system',
    )
    audb.config.REPOSITORIES = [repository]
    return repository


@pytest.fixture(scope='function', autouse=False)
def repository(tmpdir):
    repository = audb.Repository(
        name='data-unittests-local',
        host=audeer.path(tmpdir, 'host'),
        backend='file-system',
    )
    audb.config.REPOSITORIES += [repository]
    return repository
