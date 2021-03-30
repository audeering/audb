import os
import shutil

import pandas as pd
import pytest

import audformat.testing
import audeer

import audb2


os.environ['AUDB2_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB2_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb2.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_info-{pytest.ID}'
DB_VERSION = '1.0.0'
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

    DB.save(DB_ROOT)

    # publish db

    audb2.publish(
        DB_ROOT,
        DB_VERSION,
        pytest.PUBLISH_REPOSITORY,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)


@pytest.fixture(
    scope='function',
    autouse=True,
)
def fixture_clear_cache():
    clear_root(pytest.CACHE_ROOT)
    yield
    clear_root(pytest.CACHE_ROOT)


def test_author():
    assert audb2.info.author(DB_NAME) == DB.author


def test_header():
    assert str(audb2.info.header(DB_NAME)) == str(DB)


def test_bit_depths():
    deps = audb2.dependencies(DB_NAME, version=DB_VERSION)
    assert audb2.info.bit_depths(DB_NAME) == set(
        [
            deps.bit_depth(file) for file in deps.media
            if deps.bit_depth(file)
        ]
    )


def test_channels():
    deps = audb2.dependencies(DB_NAME, version=DB_VERSION)
    assert audb2.info.channels(DB_NAME) == set(
        [
            deps.channels(file) for file in deps.media
            if deps.channels(file)
        ]
    )


def test_description():
    assert audb2.info.description(DB_NAME) == DB.description


def test_duration():
    deps = audb2.dependencies(DB_NAME, version=DB_VERSION)
    assert audb2.info.duration(DB_NAME) == pd.to_timedelta(
        sum(
            [
                deps.duration(file) for file in deps.media
            ]
        ),
        unit='s',
    )


def test_formats():
    deps = audb2.dependencies(DB_NAME, version=DB_VERSION)
    assert audb2.info.formats(DB_NAME) == set(
        [
            deps.format(file) for file in deps.media
        ]
    )


def test_languages():
    assert audb2.info.languages(DB_NAME) == DB.languages


def test_license():
    assert audb2.info.license(DB_NAME) == DB.license


def test_license_url():
    assert audb2.info.license_url(DB_NAME) == DB.license_url


def test_media():
    assert str(audb2.info.media(DB_NAME)) == str(DB.media)


def test_meta():
    assert audb2.info.meta(DB_NAME) == DB.meta


def test_raters():
    assert str(audb2.info.raters(DB_NAME)) == str(DB.raters)


def test_sampling_rates():
    deps = audb2.dependencies(DB_NAME, version=DB_VERSION)
    assert audb2.info.sampling_rates(DB_NAME) == set(
        [
            deps.sampling_rate(file) for file in deps.media
            if deps.sampling_rate(file)
        ]
    )


def test_schemes():
    assert str(audb2.info.schemes(DB_NAME)) == str(DB.schemes)


def test_splits():
    assert str(audb2.info.splits(DB_NAME)) == str(DB.splits)


def test_source():
    assert audb2.info.source(DB_NAME) == DB.source


def test_tables():
    assert str(audb2.info.tables(DB_NAME)) == str(DB.tables)


def test_usage():
    assert audb2.info.usage(DB_NAME) == DB.usage
