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


def test_init(deps):
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


def test_get_item(deps):
    assert deps["db.files.csv"] == list(ROWS[0].values())[1:]
    assert deps["file.wav"] == list(ROWS[1].values())[1:]
    with pytest.raises(KeyError, match="non.existing"):
        deps["non.existing"]


def test_archives(deps):
    assert deps.archives == ["archive1", "archive2"]


def test_files(deps):
    assert deps.files == ["db.files.csv", "file.wav"]


def test_media(deps):
    assert deps.media == ["file.wav"]


def test_removed_media(deps):
    assert deps.removed_media == []


def test_table_ids(deps):
    assert deps.table_ids == ["files"]


def test_tables(deps):
    assert deps.tables == ["db.files.csv"]


@pytest.mark.parametrize(
    "files",
    [
        "",
        "non-existing",
        [""],
        ["non-existing"],
    ],
)
@pytest.mark.parametrize(
    "method, expected_error",
    [
        ("archive", KeyError),
        ("bit_depth", KeyError),
        ("channels", KeyError),
        ("checksum", KeyError),
        ("duration", KeyError),
        ("format", KeyError),
        ("removed", KeyError),
        ("sampling_rate", KeyError),
        ("type", KeyError),
        ("version", KeyError),
    ],
)
def test_error_file_based_methods(deps, files, method, expected_error):
    """Test errors for all file based methods of audb.Dependencies.

    Test all methods that have ``files`` as argument,
    and return an entry from a column
    of the dependency table
    for the selected files.

    """
    deps_method = getattr(deps, method)
    with pytest.raises(expected_error):
        deps_method(files)


@pytest.mark.parametrize(
    "files",
    [
        "db.files.csv",
        "file.wav",
        ["db.files.csv"],
        ["db.files.csv", "file.wav"],
    ],
)
@pytest.mark.parametrize(
    "method, expected_dtype",
    [
        ("archive", str),
        ("bit_depth", int),
        ("channels", int),
        ("checksum", str),
        ("duration", float),
        ("format", str),
        ("removed", bool),
        ("sampling_rate", int),
        ("type", int),
        ("version", str),
    ],
)
def test_file_bases_methods(deps, files, method, expected_dtype):
    """Test all file based methods of audb.Dependencies.

    Test all methods that have ``files`` as argument,
    and return an entry from a column
    of the dependency table
    for the selected files.

    """
    deps_method = getattr(deps, method)
    result = deps_method(files)
    if not isinstance(files, list):
        for row in ROWS:
            if row["file"] == files:
                assert result == row[method]
                assert isinstance(result, expected_dtype)
                break
    else:
        expected = []
        for file in files:
            for row in ROWS:
                if row["file"] == file:
                    expected.append(row[method])
                    break
        assert result == expected
        for result in result:
            assert isinstance(result, expected_dtype)


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


def test_load_save_errors(deps):
    # Wrong extension or file missng
    with pytest.raises(ValueError, match=r".*'txt'.*"):
        deps.load("deps.txt")
    with pytest.raises(FileNotFoundError):
        deps.load("deps.csv")


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
