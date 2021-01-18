BACKEND_ALLOWED_CHARS = '[A-Za-z0-9/._-]+'
DB = 'db'
DB_HEADER = f'{DB}.yaml'
DB_DEPEND = f'{DB}.csv'


class DependField:
    r"""Fields stored in dependency table."""
    ARCHIVE = 0
    CHANNELS = 1
    CHECKSUM = 2
    DURATION = 3
    REMOVED = 4
    TYPE = 5
    VERSION = 6


DEPEND_FIELD_NAMES = {
    DependField.ARCHIVE: 'archive',
    DependField.CHANNELS: 'channels',
    DependField.CHECKSUM: 'checksum',
    DependField.DURATION: 'duration',
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
    r"""Media formats.

    Media formats :meth:`audb2.load` can convert to on request.

    """
    WAV = 'wav'
    FLAC = 'flac'


FORMATS = [Format.WAV, Format.FLAC]
BIT_DEPTHS = [16, 24, 32]
SAMPLING_RATES = [8000, 16000, 2250, 44100, 48000]
