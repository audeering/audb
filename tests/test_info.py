import os
import shutil

import pandas as pd
import pytest

import audformat.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.REPOSITORIES = [
    (
        audb2.config.FILE_SYSTEM_REGISTRY_NAME,
        pytest.HOST,
        pytest.REPOSITORY
    )
]
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = f'test_info-{pytest.ID}'
DB = audformat.Database(
    DB_NAME,
    source='https://gitlab.audeering.com/tools/audb2',
    usage=audformat.define.Usage.UNRESTRICTED,
    languages=['de', 'English'],
    description='audb2.info unit test database',
    meta={'foo': 'bar'}
)
DB.media['media'] = audformat.Media()
DB.schemes['scheme'] = audformat.Scheme()
DB.splits['split'] = audformat.Split()
DB.raters['rater'] = audformat.Rater()
DB['table'] = audformat.Table(media_id='media', split_id='split')
DB['table']['column'] = audformat.Column(
    scheme_id='scheme', rater_id='rater',
)
DB_ROOT = os.path.join(pytest.ROOT, 'db')
BACKEND = audb2.backend.FileSystem(pytest.HOST)


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
    clear_root(pytest.HOST)

    # create db

    DB.save(DB_ROOT)

    # publish db

    audb2.publish(
        DB_ROOT,
        '1.0.0',
        pytest.REPOSITORY,
        backend=BACKEND,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_clear_cache():
    clear_root(pytest.CACHE_ROOT)
    yield
    clear_root(pytest.CACHE_ROOT)


def test_info():

    deps = audb2.dependencies(DB_NAME, backend=BACKEND,)

    assert str(audb2.info.header(DB_NAME, backend=BACKEND)) == str(DB)
    assert audb2.info.bit_depths(DB_NAME, backend=BACKEND) == set(
        [
            deps.bit_depth(file) for file in deps.media
            if deps.bit_depth(file)
        ]
    )
    assert audb2.info.channels(DB_NAME, backend=BACKEND) == set(
        [
            deps.channels(file) for file in deps.media
            if deps.channels(file)
        ]
    )
    assert audb2.info.description(DB_NAME, backend=BACKEND) == DB.description
    assert audb2.info.duration(DB_NAME, backend=BACKEND) == pd.to_timedelta(
        sum(
            [
                deps.duration(file) for file in deps.media
            ]
        ),
        unit='s',
    )
    assert audb2.info.formats(DB_NAME, backend=BACKEND) == set(
        [
            deps.format(file) for file in deps.media
        ]
    )
    assert audb2.info.languages(DB_NAME, backend=BACKEND) == DB.languages
    assert str(audb2.info.media(DB_NAME, backend=BACKEND)) == str(DB.media)
    assert audb2.info.meta(DB_NAME, backend=BACKEND) == DB.meta
    assert str(audb2.info.raters(DB_NAME, backend=BACKEND)) == str(DB.raters)
    assert audb2.info.sampling_rates(DB_NAME, backend=BACKEND) == set(
        [
            deps.sampling_rate(file) for file in deps.media
            if deps.sampling_rate(file)
        ]
    )
    assert str(audb2.info.schemes(DB_NAME, backend=BACKEND)) == str(DB.schemes)
    assert str(audb2.info.splits(DB_NAME, backend=BACKEND)) == str(DB.splits)
    assert audb2.info.source(DB_NAME, backend=BACKEND) == DB.source
    assert str(audb2.info.tables(DB_NAME, backend=BACKEND)) == str(DB.tables)
    assert audb2.info.usage(DB_NAME, backend=BACKEND) == DB.usage
