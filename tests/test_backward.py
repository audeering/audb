import os
import re
import shutil

import pytest

import audeer
import audformat.testing

import audb2


os.environ['AUDB2_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB2_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb2.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_publish-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')


def clear_root(root: str):
    root = audeer.safe_path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.save(DB_ROOT)

    audb2.publish(
        DB_ROOT,
        '1.0.0',
        pytest.PUBLISH_REPOSITORY,
        num_workers=pytest.NUM_WORKERS,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


@pytest.mark.parametrize(
    'mix',
    [
        'mono',
        'stereo',
        'left',
        'right',
    ],
)
def test_cached_databases(mix):
    with pytest.warns(UserWarning):
        db = audb2.load(DB_NAME, version='1.0.0', mix=mix)
    with pytest.warns(UserWarning):
        df = audb2.cached_databases()
    assert len(df) > 0
    assert list(df.columns) == [
        'name',
        'flavor_id',
        'version',
        'only_metadata',
        'format',
        'sampling_rate',
        'mix',
        'include',
        'exclude',
    ]
    assert df['name'].values[0] == DB_NAME
    assert df['version'].values[0] == '1.0.0'
    assert df['mix'].values[0] == mix
    # Clear cache root
    clear_root(db.meta['audb']['root'])


def test_get_default_cache_root():
    with pytest.warns(UserWarning):
        cache = audb2.get_default_cache_root()
    assert cache == audb2.default_cache_root()
