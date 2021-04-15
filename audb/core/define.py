import os


# Configuration files
CONFIG_FILE = os.path.join('etc', 'audb.yaml')
USER_CONFIG_FILE = '~/.audb.yaml'

# Database
DB = 'db'
HEADER_FILE = f'{DB}.yaml'

# Dependencies
DEPENDENCIES_FILE = f'{DB}.csv'
CACHED_DEPENDENCIES_FILE = f'{DB}.pkl'


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
    DependField.ARCHIVE: 'archive',
    DependField.BIT_DEPTH: 'bit_depth',
    DependField.CHANNELS: 'channels',
    DependField.CHECKSUM: 'checksum',
    DependField.DURATION: 'duration',
    DependField.FORMAT: 'format',
    DependField.REMOVED: 'removed',
    DependField.SAMPLING_RATE: 'sampling_rate',
    DependField.TYPE: 'type',
    DependField.VERSION: 'version',
}

DEPEND_FIELD_DTYPES = {
    DependField.ARCHIVE: str,
    DependField.BIT_DEPTH: int,
    DependField.CHANNELS: int,
    DependField.CHECKSUM: str,
    DependField.DURATION: float,
    DependField.FORMAT: str,
    DependField.REMOVED: int,
    DependField.SAMPLING_RATE: int,
    DependField.TYPE: int,
    DependField.VERSION: str,
}


class DependType:
    r"""Dependency file types."""
    META = 0
    MEDIA = 1


DEPEND_TYPE_NAMES = {
    DependType.META: 'meta',
    DependType.MEDIA: 'media',
}


# Flavors
class Format:
    r"""Media formats.

    Media formats :meth:`audb.load` can convert to on request.

    """
    WAV = 'wav'
    FLAC = 'flac'


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 22500, 44100, 48000]
