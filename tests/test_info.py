import os

import numpy as np
import pandas as pd
import pytest

import audeer
import audformat.testing
import audiofile

import audb


DB_NAME = "test_info"
DB_VERSION = "1.0.0"


@pytest.fixture(
    scope="module",
    autouse=True,
)
def db(tmpdir_factory, persistent_repository):
    r"""Publish a single database.

    Returns:
        database object

    """
    # creat db

    db = audformat.Database(
        DB_NAME,
        source="https://audeering.github.io/audb/",
        usage=audformat.define.Usage.UNRESTRICTED,
        languages=["de", "English"],
        description="audb.info unit test database",
        meta={"foo": "bar"},
    )
    db.media["media"] = audformat.Media()
    db.schemes["scheme1"] = audformat.Scheme()
    db.splits["split"] = audformat.Split()
    db.raters["rater"] = audformat.Rater()
    db.attachments["attachment"] = audformat.Attachment("file.txt")
    db["table1"] = audformat.Table(
        audformat.filewise_index(
            ["f11.wav", "f12.wav", "f13.wav"],
        ),
        media_id="media",
        split_id="split",
    )
    db["table1"]["column"] = audformat.Column(
        scheme_id="scheme1",
        rater_id="rater",
    )
    db["table2"] = audformat.Table(
        audformat.segmented_index(
            ["f21.wav", "f22.wav", "f22.wav"],
            [0, 0, 0.5],
            [1, 0.5, 1],
        ),
        media_id="media",
        split_id="split",
    )
    db["table2"]["column"] = audformat.Column(
        scheme_id="scheme1",
        rater_id="rater",
    )
    db["misc-in-scheme"] = audformat.MiscTable(pd.Index([0, 1], name="idx"))
    db["misc-not-in-scheme"] = audformat.MiscTable(pd.Index([0, 1], name="idx"))
    db.schemes["scheme2"] = audformat.Scheme(
        "int",
        labels="misc-in-scheme",
    )

    # create db + audio files

    db_root = tmpdir_factory.mktemp(DB_VERSION)
    sampling_rate = 8000
    audeer.touch(db_root, db.attachments["attachment"].path)
    for table in list(db.tables):
        for file in db[table].files:
            audiofile.write(
                os.path.join(db_root, file),
                np.zeros((1, sampling_rate)),
                sampling_rate,
            )

    db.save(db_root)

    # publish db

    audb.publish(
        db_root,
        DB_VERSION,
        persistent_repository,
        verbose=False,
    )

    return db


def test_attachemnts(db):
    assert str(audb.info.attachments(DB_NAME)) == str(db.attachments)


def test_author(db):
    assert audb.info.author(DB_NAME) == db.author


def test_header(db):
    # Load header without loading misc tables
    header = audb.info.header(DB_NAME, load_tables=False)
    assert str(header) == str(db)
    error_msg = "No file found for table with path"
    with pytest.raises(RuntimeError, match=error_msg):
        assert 0 in header.schemes["scheme2"]
    # Load header with tables
    header = audb.info.header(DB_NAME)
    assert str(header) == str(db)
    assert 0 in header.schemes["scheme2"]


def test_bit_depths():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.bit_depths(DB_NAME) == set(
        [deps.bit_depth(file) for file in deps.media if deps.bit_depth(file)]
    )


def test_channels():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.channels(DB_NAME) == set(
        [deps.channels(file) for file in deps.media if deps.channels(file)]
    )


def test_description(db):
    assert audb.info.description(DB_NAME) == db.description


@pytest.mark.parametrize(
    "tables, media",
    [
        (None, None),
        ([], None),
        (None, []),
        ("", ""),
        ("table1", None),
        ("misc-in-scheme", None),
        ("misc-not-in-scheme", None),
        (None, ["f11.wav", "f12.wav"]),
        ("table1", ["f11.wav", "f12.wav"]),
        # Error as tables and media do not overlap
        pytest.param(
            "table2",
            ["f11.wav", "f12.wav"],
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ],
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
        sum([deps.duration(file) for file in db.files]),
        unit="s",
    )
    assert duration == expected_duration


def test_formats():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.formats(DB_NAME) == set([deps.format(file) for file in deps.media])


def test_languages(db):
    assert audb.info.languages(DB_NAME) == db.languages


def test_license(db):
    assert audb.info.license(DB_NAME) == db.license


def test_license_url(db):
    assert audb.info.license_url(DB_NAME) == db.license_url


def test_media(db):
    assert str(audb.info.media(DB_NAME)) == str(db.media)


def test_meta(db):
    assert audb.info.meta(DB_NAME) == db.meta


def test_misc_tables(db):
    assert str(audb.info.misc_tables(DB_NAME)) == str(db.misc_tables)


def test_organization(db):
    assert audb.info.organization(DB_NAME) == db.organization


def test_raters(db):
    assert str(audb.info.raters(DB_NAME)) == str(db.raters)


def test_sampling_rates():
    deps = audb.dependencies(DB_NAME, version=DB_VERSION)
    assert audb.info.sampling_rates(DB_NAME) == set(
        [deps.sampling_rate(file) for file in deps.media if deps.sampling_rate(file)]
    )


def test_schemes(db):
    # Load schemes without loading misc tables
    schemes = audb.info.schemes(DB_NAME, load_tables=False)
    assert str(schemes) == str(db.schemes)
    error_msg = "No file found for table with path"
    with pytest.raises(RuntimeError, match=error_msg):
        assert 0 in schemes["scheme2"]
    # Load header with tables
    schemes = audb.info.schemes(DB_NAME)
    str(schemes) == str(db.schemes)
    assert 0 in schemes["scheme2"]


def test_splits(db):
    assert str(audb.info.splits(DB_NAME)) == str(db.splits)


def test_source(db):
    assert audb.info.source(DB_NAME) == db.source


def test_tables(db):
    assert str(audb.info.tables(DB_NAME)) == str(db.tables)


def test_usage(db):
    assert audb.info.usage(DB_NAME) == db.usage
