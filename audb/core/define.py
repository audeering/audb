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
DEPENDENCY_TABLE = {
    # Column name: column dtype
    "archive": "string[pyarrow]",
    "bit_depth": "int32[pyarrow]",
    "channels": "int32[pyarrow]",
    "checksum": "string[pyarrow]",
    "duration": "float64[pyarrow]",
    "format": "string[pyarrow]",
    "removed": "int32[pyarrow]",
    "sampling_rate": "int32[pyarrow]",
    "type": "int32[pyarrow]",
    "version": "string[pyarrow]",
}
r"""Column names and data types of dependency table.

The dependency table is stored in a dataframe
at ``audb.Dependencies._df``,
and contains the specified column names
and data types.

"""
DEPEND_INDEX_DTYPE = "object"


# Cache lock
CACHED_VERSIONS_TIMEOUT = 10  # Timeout to acquire access to cached versions
LOCK_FILE = ".lock"
TIMEOUT_MSG = "Lock could not be acquired. Timeout exceeded."


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

# Progress bar
MAXIMUM_REFRESH_TIME = 1  # force progress bar to update every second
