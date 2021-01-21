BACKEND_ALLOWED_CHARS = '[A-Za-z0-9/._-]+'
DB = 'db'
DEPENDENCIES_FILE = f'{DB}.csv'
HEADER_FILE = f'{DB}.yaml'


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


class DependType:
    r"""Dependency file types."""
    META = 0
    MEDIA = 1


DEPEND_TYPE_NAMES = {
    DependType.META: 'meta',
    DependType.MEDIA: 'media',
}


class Format:
    r"""Media formats.

    Media formats :meth:`audb2.load` can convert to on request.

    """
    WAV = 'wav'
    FLAC = 'flac'


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 2250, 44100, 48000]
