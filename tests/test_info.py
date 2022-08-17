import os
import shutil

import numpy as np
import pandas as pd
import pytest

import audformat.testing
import audeer
import audiofile

import audb


os.environ['AUDB_CACHE_ROOT'] = pytest.CACHE_ROOT
os.environ['AUDB_SHARED_CACHE_ROOT'] = pytest.SHARED_CACHE_ROOT


@pytest.fixture(
    scope='session',
    autouse=True,
)
def fixture_set_repositories():
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
DB.schemes['scheme1'] = audformat.Scheme()
DB.splits['split'] = audformat.Split()
DB.raters['rater'] = audformat.Rater()
DB['table1'] = audformat.Table(
    audformat.filewise_index(
        ['f11.wav', 'f12.wav', 'f13.wav'],
    ),
    media_id='media',
    split_id='split',
)
DB['table1']['column'] = audformat.Column(
    scheme_id='scheme1',
    rater_id='rater',
)
DB['table2'] = audformat.Table(
    audformat.segmented_index(
        ['f21.wav', 'f22.wav', 'f22.wav'],
        [0, 0, .5],
        [1, .5, 1],
    ),
    media_id='media',
    split_id='split',
)
DB['table2']['column'] = audformat.Column(
    scheme_id='scheme1',
    rater_id='rater',
)
DB['misc-in-scheme'] = audformat.MiscTable(
    pd.Index([0, 1], name='idx')
)
DB['misc-not-in-scheme'] = audformat.MiscTable(
    pd.Index([0, 1], name='idx')
)
DB.schemes['scheme2'] = audformat.Scheme(
    'int',
    labels='misc-in-scheme',
)

DB_ROOT = os.path.join(pytest.ROOT, 'db')


def clear_root(root: str):
    root = audeer.path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.FILE_SYSTEM_HOST)

    # create db + audio files
    sampling_rate = 8000
    audeer.mkdir(DB_ROOT)
    for table in list(DB.tables):
        for file in DB[table].files:
            audiofile.write(
                os.path.join(DB_ROOT, file),
                np.zeros((1, sampling_rate)),
                sampling_rate,
            )

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
    # Load header without loading misc tables
    db = audb.info.header(DB_NAME, load_tables=False)
    assert str(db) == str(DB)
    error_msg = 'No file found for table with path'
    with pytest.raises(RuntimeError, match=error_msg):
        assert 0 in db.schemes['scheme2']
    # Load header with tables
    db = audb.info.header(DB_NAME)
    assert str(db) == str(DB)
    assert 0 in db.schemes['scheme2']


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


@pytest.mark.parametrize(
    'tables, media',
    [
        (None, None),
        ([], None),
        (None, []),
        ('', ''),
        ('table1', None),
        ('misc-in-scheme', None),
        ('misc-not-in-scheme', None),
        (None, ['f11.wav', 'f12.wav']),
        ('table1', ['f11.wav', 'f12.wav']),
        # Error as tables and media do not overlap
        pytest.param(
            'table2',
            ['f11.wav', 'f12.wav'],
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ]
)
def test_duration(tables, media):
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    duration = audb.info.duration(DB_NAME, tables=tables, media=media)
    db = audb.load(
        DB_NAME,
        version=DB_VERSION,
        tables=tables,
        media=media,
        full_path=False,
        verbose=False,
    )
    expected_duration = pd.to_timedelta(
        sum(
            [
                deps.duration(file) for file in db.files
            ]
        ),
        unit='s',
    )
    assert duration == expected_duration


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


def test_misc_tables():
    assert str(audb.info.misc_tables(DB_NAME)) == str(DB.misc_tables)


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
    # Load schemes without loading misc tables
    schemes = audb.info.schemes(DB_NAME, load_tables=False)
    assert str(schemes) == str(DB.schemes)
    error_msg = 'No file found for table with path'
    with pytest.raises(RuntimeError, match=error_msg):
        assert 0 in schemes['scheme2']
    # Load header with tables
    schemes = audb.info.schemes(DB_NAME)
    str(schemes) == str(DB.schemes)
    assert 0 in schemes['scheme2']


def test_splits():
    assert str(audb.info.splits(DB_NAME)) == str(DB.splits)


def test_source():
    assert audb.info.source(DB_NAME) == DB.source


def test_tables():
    assert str(audb.info.tables(DB_NAME)) == str(DB.tables)


def test_usage():
    assert audb.info.usage(DB_NAME) == DB.usage
