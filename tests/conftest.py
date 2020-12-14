import glob
import os
import shutil

import pytest

import audeer


pytest.ROOT = os.path.dirname(os.path.realpath(__file__))
pytest.CACHE_ROOT = audeer.mkdir(os.path.join(pytest.ROOT, 'audb2'))


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
    if os.path.exists(pytest.CACHE_ROOT):
        shutil.rmtree(pytest.CACHE_ROOT)
