import pandas as pd
import pytest

import audeer

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
    df = pd.DataFrame.from_records(ROWS)
    df.set_index("file", inplace=True)
    # Ensure correct dtype
    df.index = df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
    df.index.name = None
    for name, dtype in zip(
        audb.core.define.DEPEND_FIELD_NAMES.values(),
        audb.core.define.DEPEND_FIELD_DTYPES.values(),
    ):
        df[name] = df[name].astype(dtype)
    deps._df = df
    return deps


def test_instantiation():
    r"""Test instantiation of audb.Dependencies.

    During instantiation of ``audb.Dependencies``
    an empty dataframe is created under ``self._df``,
    that stores the dependency table.
    This test ensures,
    that the dataframe
    contains the correct column names and data types,
    and the correct name and data type of its index.

    """
    deps = audb.Dependencies()
    expected_columns = [
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
    expected_df = pd.DataFrame(columns=expected_columns)
    expected_df.index = expected_df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
    for name, dtype in zip(
        audb.core.define.DEPEND_FIELD_NAMES.values(),
        audb.core.define.DEPEND_FIELD_DTYPES.values(),
    ):
        expected_df[name] = expected_df[name].astype(dtype)
    pd.testing.assert_frame_equal(deps._df, expected_df)
    assert list(deps._df.columns) == expected_columns
    df = deps()
    assert list(df.columns) == expected_columns


def test_call(deps):
    expected_df = pd.DataFrame.from_records(ROWS).set_index("file")
    expected_df.index = expected_df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
    expected_df.index.name = None
    for name, dtype in zip(
        audb.core.define.DEPEND_FIELD_NAMES.values(),
        audb.core.define.DEPEND_FIELD_DTYPES.values(),
    ):
        expected_df[name] = expected_df[name].astype(dtype)
    df = deps()
    pd.testing.assert_frame_equal(df, expected_df)


def test_contains(deps):
    assert "db.files.csv" in deps
    assert "file.wav" in deps
    assert "not.csv" not in deps


def test_equals(deps):
    # empty table vs. empty table
    _deps = audb.Dependencies()
    assert _deps == _deps
    assert _deps == audb.Dependencies()
    # empty table vs. example table
    assert deps != audb.Dependencies()
    # example table vs. example table
    assert deps == deps
    _deps._df = deps._df.copy()
    assert deps == _deps
    # example table vs. different table
    _deps._df.loc["db.files.csv", "channels"] = 4
    assert deps != _deps


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


@pytest.mark.parametrize("file", ["deps.csv", "deps.pkl", "deps.parquet"])
def test_load_save(tmpdir, deps, file):
    """Test consistency of dependency table after save/load cycle.

    Dependency values and data types
    should remain identical
    when first storing and then loading from a file.
    This should hold for all possible file formats.

    """
    deps_file = audeer.path(tmpdir, file)
    deps.save(deps_file)
    deps2 = audb.Dependencies()
    deps2.load(deps_file)
    pd.testing.assert_frame_equal(deps(), deps2())
    assert list(deps2._df.dtypes) == list(audb.core.define.DEPEND_FIELD_DTYPES.values())


def test_load_save_backward_compatibility(tmpdir, deps):
    """Test backward compatibility with old pickle cache files.

    As the dtype of the index has changed,
    we need to make sure this is corrected
    when loading old cache files.

    """
    deps_file = audeer.path(tmpdir, "deps.pkl")
    # Change dtype of index from object to string
    # to mimic previous behavior
    deps._df.index = deps._df.index.astype("string")
    deps.save(deps_file)
    deps2 = audb.Dependencies()
    deps2.load(deps_file)
    assert deps2._df.index.dtype == audb.core.define.DEPEND_INDEX_DTYPE


def test_load_save_errors(deps):
    """Test possible errors when loading/saving."""
    # Wrong file extension
    with pytest.raises(ValueError, match=r".*'txt'.*"):
        deps.load("deps.txt")
    # File missing
    with pytest.raises(FileNotFoundError):
        deps.load("deps.csv")


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
        "               archive  bit_depth  channels  ... sampling_rate  type version\n"  # noqa: E501
        "db.files.csv  archive1          0         0  ...             0     0   1.0.0\n"  # noqa: E501
        "file.wav      archive2         16         2  ...         16000     1   1.0.0\n"  # noqa: E501
        "\n"
        "[2 rows x 10 columns]"
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
                "wav",
                0,
                16000,
                audb.core.define.DependType.MEDIA,
                "1.1.0",
            ),
            (
                "file2.wav",
                "archive2",
                24,
                1,
                "masdfmiosedascrf34",
                5.6,
                "wav",
                0,
                44100,
                audb.core.define.DependType.MEDIA,
                "1.2.0",
            ),
        ],
    ],
)
def test_add_media(deps, values):
    deps._add_media(values)
    assert len(deps) == 4
    for (
        file,
        archive,
        bit_depth,
        channels,
        checksum,
        duration,
        format,
        removed,
        sampling_rate,
        type,
        version,
    ) in values:
        assert deps.archive(file) == archive
        assert deps.bit_depth(file) == bit_depth
        assert deps.channels(file) == channels
        assert deps.checksum(file) == checksum
        assert deps.duration(file) == duration
        assert deps.format(file) == format
        assert deps.removed(file) == removed
        assert deps.sampling_rate(file) == sampling_rate
        assert deps.type(file) == type
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
    assert deps.version(file) == version
    assert deps.archive(file) == archive
    assert deps.checksum(file) == checksum


@pytest.mark.parametrize(
    "files, expected_length",
    [
        (["file.wav"], 1),
        (["db.files.csv"], 1),
        (["file.wav", "db.files.csv"], 0),
    ],
)
def test_drop(deps, files, expected_length):
    deps._drop(files)
    assert len(deps) == expected_length
    for file in files:
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
                "wav",
                0,
                16000,
                audb.core.define.DependType.MEDIA,
                "1.1.0",
            ),
        ],
        pytest.param(
            [
                (
                    "non-existent.wav",
                    "archive1",
                    16,
                    1,
                    "jsdfjioergjiergnmo",
                    2.3,
                    "wav",
                    0,
                    16000,
                    audb.core.define.DependType.MEDIA,
                    "1.1.0",
                ),
            ],
            marks=pytest.mark.xfail(raises=KeyError),
        ),
    ],
)
def test_update_media(deps, values):
    deps._update_media(values)
    assert len(deps) == 2
    for (
        file,
        archive,
        bit_depth,
        channels,
        checksum,
        duration,
        format,
        removed,
        sampling_rate,
        type,
        version,
    ) in values:
        assert deps.archive(file) == archive
        assert deps.bit_depth(file) == bit_depth
        assert deps.channels(file) == channels
        assert deps.checksum(file) == checksum
        assert deps.duration(file) == duration
        assert deps.format(file) == format
        assert deps.removed(file) == removed
        assert deps.sampling_rate(file) == sampling_rate
        assert deps.type(file) == type
        assert deps.version(file) == version


@pytest.mark.parametrize(
    "files, version",
    [
        (["file.wav"], "3.0.0"),
        (["file.wav", "db.files.csv"], "4.0.0"),
        pytest.param(
            ["non-existent.wav"],
            "3.0.0",
            marks=pytest.mark.xfail(raises=KeyError),
        ),
    ],
)
def test_update_media_version(deps, files, version):
    deps._update_media_version(files, version)
    assert len(deps) == 2
    for file in files:
        assert deps.version(file) == version
