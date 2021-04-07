import os
import shutil

import pandas as pd
import pytest

import audformat.testing
import audeer

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT
audb.config.REPOSITORIES = pytest.REPOSITORIES


DB_NAME = f'test_info-{pytest.ID}'
DB_VERSION = '1.0.0'
DB = audformat.Database(
    DB_NAME,
    source='https://audeering.github.io/audb/',
    usage=audformat.define.Usage.UNRESTRICTED,
    languages=['de', 'English'],
    description='audb.info unit test database',
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

    audb.publish(
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
    assert audb.info.author(DB_NAME) == DB.author


def test_header():
    assert str(audb.info.header(DB_NAME)) == str(DB)


def test_bit_depths():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.bit_depths(DB_NAME) == set(
        [
            deps.bit_depth(file) for file in deps.media
            if deps.bit_depth(file)
        ]
    )


def test_channels():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.channels(DB_NAME) == set(
        [
            deps.channels(file) for file in deps.media
            if deps.channels(file)
        ]
    )


def test_description():
    assert audb.info.description(DB_NAME) == DB.description


def test_duration():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.duration(DB_NAME) == pd.to_timedelta(
        sum(
            [
                deps.duration(file) for file in deps.media
            ]
        ),
        unit='s',
    )


def test_formats():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.formats(DB_NAME) == set(
        [
            deps.format(file) for file in deps.media
        ]
    )


def test_languages():
    assert audb.info.languages(DB_NAME) == DB.languages


def test_license():
    assert audb.info.license(DB_NAME) == DB.license


def test_license_url():
    assert audb.info.license_url(DB_NAME) == DB.license_url


def test_media():
    assert str(audb.info.media(DB_NAME)) == str(DB.media)


def test_meta():
    assert audb.info.meta(DB_NAME) == DB.meta


def test_organization():
    assert audb.info.organization(DB_NAME) == DB.organization


def test_raters():
    assert str(audb.info.raters(DB_NAME)) == str(DB.raters)


def test_sampling_rates():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.sampling_rates(DB_NAME) == set(
        [
            deps.sampling_rate(file) for file in deps.media
            if deps.sampling_rate(file)
        ]
    )


def test_schemes():
    assert str(audb.info.schemes(DB_NAME)) == str(DB.schemes)


def test_splits():
    assert str(audb.info.splits(DB_NAME)) == str(DB.splits)


def test_source():
    assert audb.info.source(DB_NAME) == DB.source


def test_tables():
    assert str(audb.info.tables(DB_NAME)) == str(DB.tables)


def test_usage():
    assert audb.info.usage(DB_NAME) == DB.usage
