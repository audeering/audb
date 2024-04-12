import os


# Configuration files
CONFIG_FILE = os.path.join("etc", "audb.yaml")
USER_CONFIG_FILE = "~/.audb.yaml"

# Database
DB = "db"
HEADER_FILE = f"{DB}.yaml"

# Dependencies
DEPENDENCIES_FILE = f"{DB}.parquet"
CACHED_DEPENDENCIES_FILE = f"{DB}.pkl"
LEGACY_DEPENDENCIES_FILE = f"{DB}.csv"

# Cache lock
CACHED_VERSIONS_TIMEOUT = 10  # Timeout to acquire access to cached versions
LOCK_FILE = ".lock"
TIMEOUT_MSG = "Lock could not be acquired. Timeout exceeded."


class DependField:
    r"""Fields stored in dependency table."""

    ARCHIVE = 0
    BIT_DEPTH = 1
    CHANNELS = 2
    CHECKSUM = 3
    DURATION = 4
    FORMAT = 5
    REMOVED = 6
    SAMPLING_RATE = 7
    TYPE = 8
    VERSION = 9


DEPEND_FIELD_NAMES = {
    DependField.ARCHIVE: "archive",
    DependField.BIT_DEPTH: "bit_depth",
    DependField.CHANNELS: "channels",
    DependField.CHECKSUM: "checksum",
    DependField.DURATION: "duration",
    DependField.FORMAT: "format",
    DependField.REMOVED: "removed",
    DependField.SAMPLING_RATE: "sampling_rate",
    DependField.TYPE: "type",
    DependField.VERSION: "version",
}

DEPEND_FIELD_DTYPES = {
    DependField.ARCHIVE: "string[pyarrow]",
    DependField.BIT_DEPTH: "int32[pyarrow]",
    DependField.CHANNELS: "int32[pyarrow]",
    DependField.CHECKSUM: "string[pyarrow]",
    DependField.DURATION: "float64[pyarrow]",
    DependField.FORMAT: "string[pyarrow]",
    DependField.REMOVED: "int32[pyarrow]",
    DependField.SAMPLING_RATE: "int32[pyarrow]",
    DependField.TYPE: "int32[pyarrow]",
    DependField.VERSION: "string[pyarrow]",
}

DEPEND_INDEX_DTYPE = "object"


class DependType:
    r"""Dependency file types."""

    META = 0
    MEDIA = 1
    ATTACHMENT = 2


DEPEND_TYPE_NAMES = {
    DependType.META: "meta",
    DependType.MEDIA: "media",
    DependType.ATTACHMENT: "attachment",
}


# Flavors
class Format:
    r"""Media formats.

    Media formats :meth:`audb.load` can convert to on request.

    """

    WAV = "wav"
    FLAC = "flac"


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 22500, 44100, 48000]
