import re


ARCHIVE_NAME_PATTERN = re.compile('[A-Za-z0-9._-]+')
DB_HEADER = 'db.yaml'
DB_DEPEND = 'db.csv'


class DependField:
    r"""Fields stored in dependency table."""
    ARCHIVE = 0
    CHANNELS = 1
    CHECKSUM = 2
    REMOVED = 3
    TYPE = 4
    VERSION = 5


DEPEND_FIELD_NAMES = {
    DependField.ARCHIVE: 'archive',
    DependField.CHANNELS: 'channels',
    DependField.CHECKSUM: 'checksum',
    DependField.REMOVED: 'removed',
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
    r"""Supported media formats."""
    WAV = 'wav'
    FLAC = 'flac'


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 2250, 44100, 48000]


class Mix:
    r"""Supported mix strings."""
    MONO = 'mono'
    MONO_ONLY = 'mono-only'
    LEFT = 'left'
    RIGHT = 'right'
    STEREO = 'stereo'
    STEREO_ONLY = 'stereo-only'


MIXES = [
    Mix.MONO,
    Mix.MONO_ONLY,
    Mix.LEFT,
    Mix.RIGHT,
    Mix.STEREO,
    Mix.STEREO_ONLY,
]
