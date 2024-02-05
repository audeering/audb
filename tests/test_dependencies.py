import os

import pandas as pd
import pytest

import audb


ROWS = [
    {
        "file": "db.files.csv",
        "archive": "archive1",
        "bit_depth": 0,
        "channels": 0,
        "checksum": "7c1f6b568f7221ab968a705fd5e7477b",
        "duration": 0.0,
        "format": "csv",
        "removed": 0,
        "sampling_rate": 0,
        "type": 0,
        "version": "1.0.0",
    },
    {
        "file": "file.wav",
        "archive": "archive2",
        "bit_depth": 16,
        "channels": 2,
        "checksum": "917338b854ad9c72f76bc9a68818dcd8",
        "duration": 1.23,
        "format": "wav",
        "removed": 0,
        "sampling_rate": 16000,
        "type": 1,
        "version": "1.0.0",
    },
]


def get_entries(column):
    return [row[column] for row in ROWS]


def test_get_entries():
    assert get_entries("archive") == ["archive1", "archive2"]


@pytest.fixture(
    scope="function",
)
def deps():
    deps = audb.Dependencies()
    deps._table_add_rows(ROWS)
    # # Ensure correct dtype
    # for name, dtype in zip(
    #     audb.core.define.DEPEND_FIELD_NAMES.values(),
    #     audb.core.define.DEPEND_FIELD_DTYPES.values(),
    # ):
    #     deps._df[name] = deps._df[name].astype(dtype)
    return deps


def test_init(deps):
    expected_columns = [
        "file",
        "archive",
        "bit_depth",
        "channels",
        "checksum",
        "duration",
        "format",
        "removed",
        "sampling_rate",
        "type",
        "version",
    ]
    assert deps._table.column_names == expected_columns
    df = deps()
    assert list(df.columns) == expected_columns[1:]


def test_call(deps):
    expected_df = pd.DataFrame.from_records(ROWS).set_index("file")
    expected_df.index.name = ""
    print(f"{expected_df=}")
    df = deps()
    print(f"{df=}")
    # TODO: fix dtypes
    # pd.testing.assert_frame_equal(df, expected_df)


def test_contains(deps):
    assert "db.files.csv" in deps
    assert "file.wav" in deps
    assert "not.csv" not in deps


def test_get_item(deps):
    assert deps["db.files.csv"] == list(ROWS[0].values())[1:]
    assert deps["file.wav"] == list(ROWS[1].values())[1:]
    with pytest.raises(KeyError, match="non.existing"):
        deps["non.existing"]


def test_archives(deps):
    assert deps.archives == get_entries("archive")


def test_files(deps):
    assert deps.files == get_entries("file")


def test_media(deps):
    assert deps.media == ["file.wav"]


def test_removed_media(deps):
    assert deps.removed_media == []


def test_table_ids(deps):
    assert deps.table_ids == ["files"]


def test_tables(deps):
    assert deps.tables == ["db.files.csv"]


def test_archive(deps):
    files = get_entries("file")
    archives = get_entries("archive")
    for file, archive in zip(files, archives):
        assert deps.archive(file) == archive
        assert isinstance(deps.archive(file), str)
    with pytest.raises(KeyError, match="non.existing"):
        deps.archive("non.existing")


def test_bit_depth(deps):
    files = get_entries("file")
    bit_depths = get_entries("bit_depth")
    for file, bit_depth in zip(files, bit_depths):
        assert deps.bit_depth(file) == bit_depth
        assert isinstance(deps.bit_depth(file), int)
    with pytest.raises(KeyError, match="non.existing"):
        deps.bit_depth("non.existing")


def test_channels(deps):
    files = get_entries("file")
    channels = get_entries("channels")
    for file, channel in zip(files, channels):
        assert deps.channels(file) == channel
        assert isinstance(deps.channels(file), int)
    with pytest.raises(KeyError, match="non.existing"):
        deps.channels("non.existing")


def test_checksum(deps):
    files = get_entries("file")
    checksums = get_entries("checksum")
    for file, checksum in zip(files, checksums):
        assert deps.checksum(file) == checksum
        assert isinstance(deps.checksum(file), str)
    with pytest.raises(KeyError, match="non.existing"):
        deps.checksum("non.existing")


def test_duration(deps):
    files = get_entries("file")
    durations = get_entries("duration")
    for file, duration in zip(files, durations):
        assert deps.duration(file) == duration
        assert isinstance(deps.duration(file), float)
    with pytest.raises(KeyError, match="non.existing"):
        deps.duration("non.existing")


def test_format(deps):
    files = get_entries("file")
    formats = get_entries("format")
    for file, format in zip(files, formats):
        assert deps.format(file) == format
        assert isinstance(deps.format(file), str)
    with pytest.raises(KeyError, match="non.existing"):
        deps.format("non.existing")


def test_removed(deps):
    files = get_entries("file")
    removeds = get_entries("removed")
    for file, removed in zip(files, removeds):
        assert deps.removed(file) == removed
        assert isinstance(deps.removed(file), bool)
    with pytest.raises(KeyError, match="non.existing"):
        deps.removed("non.existing")


def test_load_save(deps):
    deps_file = "deps.csv"
    deps.save(deps_file)
    deps2 = audb.Dependencies()
    deps2.load(deps_file)
    pd.testing.assert_frame_equal(deps(), deps2())
    os.remove(deps_file)
    # Wrong extension or file missng
    with pytest.raises(ValueError, match=r".*'txt'.*"):
        deps2.load("deps.txt")
    with pytest.raises(FileNotFoundError):
        deps.load(deps_file)


def test_sampling_rate(deps):
    files = get_entries("file")
    sampling_rates = get_entries("sampling_rate")
    for file, sampling_rate in zip(files, sampling_rates):
        assert deps.sampling_rate(file) == sampling_rate
        assert isinstance(deps.sampling_rate(file), int)
    with pytest.raises(KeyError, match="non.existing"):
        deps.sampling_rate("non.existing")


def test_type(deps):
    files = get_entries("file")
    types = get_entries("type")
    for file, type in zip(files, types):
        assert deps.type(file) == type
        assert isinstance(deps.type(file), int)
    with pytest.raises(KeyError, match="non.existing"):
        deps.type("non.existing")


def test_version(deps):
    files = get_entries("file")
    versions = get_entries("version")
    for file, version in zip(files, versions):
        assert deps.version(file) == version
        assert isinstance(deps.version(file), str)
    with pytest.raises(KeyError, match="non.existing"):
        deps.version("non.existing")


def test_len(deps):
    assert len(deps) == len(ROWS)


def test_str(deps):
    expected_str = (
        "               archive  bit_depth  channels                          checksum  duration format  removed  sampling_rate  type version\n"  # noqa: E501
        "file                                                                                                                                \n"  # noqa: E501
        "db.files.csv  archive1          0         0  7c1f6b568f7221ab968a705fd5e7477b      0.00    csv        0              0     0   1.0.0\n"  # noqa: E501
        "file.wav      archive2         16         2  917338b854ad9c72f76bc9a68818dcd8      1.23    wav        0          16000     1   1.0.0"  # noqa: E501
    )
    assert str(deps) == expected_str


# === Test hidden methods ===
@pytest.mark.parametrize(
    "file, version, archive, checksum",
    [
        ("attachment.txt", "1.0.0", "andhfner", "asndmsknfporkrgfk3l"),
    ],
)
def test_add_attachment(deps, file, version, archive, checksum):
    deps._add_attachment(file, version, archive, checksum)
    assert len(deps) == 3
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    assert deps.version(file) == version
    assert deps.archive(file) == archive
    assert deps.checksum(file) == checksum


@pytest.mark.parametrize(
    "values",
    [
        [
            (
                "file1.wav",
                "archive1",
                16,
                1,
                "jsdfjioergjiergnmo",
                2.3,
                16000,
                "1.1.0",
            ),
            (
                "file2.wav",
                "archive2",
                24,
                1,
                "masdfmiosedascrf34",
                5.6,
                44100,
                "1.2.0",
            ),
        ],
    ],
)
def test_add_media(deps, values):
    deps._add_media(values)
    assert len(deps) == 4
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    for (
        file,
        archive,
        bit_depth,
        channels,
        checksum,
        duration,
        sampling_rate,
        version,
    ) in values:
        assert deps.archive(file) == archive
        assert deps.bit_depth(file) == bit_depth
        assert deps.channels(file) == channels
        assert deps.checksum(file) == checksum
        assert deps.duration(file) == duration
        assert deps.sampling_rate(file) == sampling_rate
        assert deps.version(file) == version


@pytest.mark.parametrize(
    "file, version, archive, checksum",
    [
        ("db.table1.csv", "2.1.0", "table1", "asddfnfpork45rgfl"),
    ],
)
def test_add_meta(deps, file, version, archive, checksum):
    deps._add_meta(file, version, archive, checksum)
    assert len(deps) == 3
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    assert deps.version(file) == version
    assert deps.archive(file) == archive
    assert deps.checksum(file) == checksum


@pytest.mark.parametrize(
    "file",
    [
        ("file.wav"),
        ("db.files.csv"),
    ],
)
def test_drop(deps, file):
    assert file in deps
    deps._drop(file)
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    assert len(deps) == 1
    assert file not in deps


@pytest.mark.parametrize(
    "file",
    [
        ("file.wav"),
        ("db.files.csv"),
    ],
)
def test_remove(deps, file):
    assert not deps.removed(file)
    deps._remove(file)
    assert len(deps) == 2
    assert deps.removed(file)


@pytest.mark.parametrize(
    "values",
    [
        [
            (
                "file.wav",
                "archive1",
                16,
                1,
                "jsdfjioergjiergnmo",
                2.3,
                16000,
                "1.1.0",
            ),
        ],
    ],
)
def test_update_media(deps, values):
    # TODO:
    # this should raise an error
    # if a file that should be updated
    # is not available
    # (at the moment it is silently added)
    deps._update_media(values)
    assert len(deps) == 2
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    for (
        file,
        archive,
        bit_depth,
        channels,
        checksum,
        duration,
        sampling_rate,
        version,
    ) in values:
        assert deps.archive(file) == archive
        assert deps.bit_depth(file) == bit_depth
        assert deps.channels(file) == channels
        assert deps.checksum(file) == checksum
        assert deps.duration(file) == duration
        assert deps.sampling_rate(file) == sampling_rate
        assert deps.version(file) == version


@pytest.mark.parametrize(
    "files, version",
    [
        (["file.wav"], "3.0.0"),
        (["file.wav", "db.files.csv"], "4.0.0"),
    ],
)
def test_update_media_version(deps, files, version):
    # TODO:
    # this should raise an error
    # if a file that should be updated
    # is not available
    # (at the moment it is silently added)
    deps._update_media_version(files, version)
    assert len(deps) == 2
    # Ensure we only have one chunk in table
    assert len(deps._table.column("file").chunks) == 1
    for file in files:
        assert deps.version(file) == version
