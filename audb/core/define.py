import os


# Configuration files
CONFIG_FILE = os.path.join("etc", "audb.yaml")
USER_CONFIG_FILE = "~/.config/audb.yaml"
DEPRECATED_USER_CONFIG_FILE = "~/.audb.yaml"

# Database
DB = "db"
HEADER_FILE = f"{DB}.yaml"

# Dependencies
DEPENDENCY_FILE = f"{DB}.parquet"
r"""Filename and extension of dependency table file."""

CACHED_DEPENDENCY_FILE = f"{DB}.pkl"
r"""Filename and extension of cached dependency table file.

As loading from a pickle file is still faster
than loading from a parquet file,
we are storing the dependency table
as a pickle file in cache.

"""

LEGACY_DEPENDENCY_FILE = f"{DB}.csv"
r"""Filename and extension of legacy dependency table file.

In ``audb`` versions smaller than 1.7.0,
the dependency table was stored in a csv file.

"""

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

DEPENDENCY_INDEX_DTYPE = "object"
r"""Data type of the dependency table index."""

DEPENDENCY_TYPE = {
    "meta": 0,
    "media": 1,
    "attachment": 2,
}
r"""Types of files stored in a database.

Currently, a database can contain the following files:

* ``"meta"``: tables and misc tables
* ``"media"``: media files, e.g. audio, video, text
* ``"attachment"``: files included as attachments

"""

# Cache lock
TIMEOUT = 86400  # 24 h
CACHED_VERSIONS_TIMEOUT = 10  # Timeout to acquire access to cached versions
LOCK_FILE = ".lock"
TIMEOUT_MSG = "Lock could not be acquired. Timeout exceeded."


# Flavors
class Format:
    r"""Media formats.

    Media formats :meth:`audb.load` can convert to on request.

    """

    WAV = "wav"
    FLAC = "flac"


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 22050, 24000, 44100, 48000]

# Progress bar
MAXIMUM_REFRESH_TIME = 1  # force progress bar to update every second
