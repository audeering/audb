import os

import pyarrow as pa


# Configuration files
CONFIG_FILE = os.path.join("etc", "audb.yaml")
USER_CONFIG_FILE = "~/.audb.yaml"

# Database
DB = "db"
HEADER_FILE = f"{DB}.yaml"

# Dependencies
DEPENDENCIES_FILE = f"{DB}.csv"
CACHED_DEPENDENCIES_FILE = f"{DB}.pkl"

# Cache lock
CACHED_VERSIONS_TIMEOUT = 10  # Timeout to acquire access to cached versions
LOCK_FILE = ".lock"
TIMEOUT_MSG = "Lock could not be acquired. Timeout exceeded."


DEPEND_FIELDS = {
    "file": pa.string(),
    "archive": pa.string(),
    "bit_depth": pa.int32(),
    "channels": pa.int32(),
    "checksum": pa.string(),
    "duration": pa.float64(),
    "format": pa.string(),
    "removed": pa.int32(),
    "sampling_rate": pa.int32(),
    "type": pa.int32(),
    "version": pa.string(),
}


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
