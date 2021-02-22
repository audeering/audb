import glob
import os
import shutil

import pytest

import audeer

import audb2


pytest.ROOT = audeer.safe_path(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'tmp',
    )
)

pytest.BACKEND = 'file-system'
pytest.CACHE_ROOT = os.path.join(pytest.ROOT, 'cache')
pytest.FILE_SYSTEM_HOST = os.path.join(pytest.ROOT, 'repo')
pytest.ID = audeer.uid()
pytest.NUM_WORKERS = 5
pytest.REPOSITORY_NAME = 'data-unittests-local'
pytest.REPOSITORY = {
    'name': pytest.REPOSITORY_NAME,
    'backend': pytest.BACKEND,
    'host': pytest.FILE_SYSTEM_HOST,
}
pytest.REPOSITORIES = [pytest.REPOSITORY]
pytest.SHARED_CACHE_ROOT = os.path.join(pytest.ROOT, 'shared')


@pytest.fixture(scope='session', autouse=True)
def cleanup_session():
    path = os.path.join(
        pytest.ROOT,
        '..',
        '.coverage.*',
    )
    for file in glob.glob(path):
        os.remove(file)
    yield
    if os.path.exists(pytest.ROOT):
        shutil.rmtree(pytest.ROOT)
