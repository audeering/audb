import os

import numpy as np
import pytest

import audiofile
import audobject

import audb2


@pytest.mark.parametrize(
    'only_metadata, bit_depth, channels, format, mixdown, sampling_rate',
    [
        (
            False, None, None, None, False, None,
        ),
        (
            False, 16, None, audb2.define.Format.WAV, True, 16000,
        ),
        (
            True, 16, None, audb2.define.Format.WAV, True, 16000,
        ),
        pytest.param(
            False, 0, None, audb2.define.Format.WAV, True, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            False, 16, None, 'bad', True, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            False, 16, None, audb2.define.Format.WAV, True, 0,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ]
)
def test_init(only_metadata, bit_depth, channels, format, mixdown,
              sampling_rate):
    flavor = audb2.Flavor(
        only_metadata=only_metadata,
        bit_depth=bit_depth,
        channels=channels,
        format=format,
        mixdown=mixdown,
        sampling_rate=sampling_rate,
    )
    if only_metadata:
        assert flavor.bit_depth is None
        assert flavor.channels is None
        assert flavor.format is None
        assert not flavor.mixdown
        assert flavor.sampling_rate is None
    flavor_s = flavor.to_yaml_s()
    flavor_2 = audobject.Object.from_yaml_s(flavor_s)
    assert isinstance(flavor_2, audb2.Flavor)
    assert flavor.id == flavor_2.id


@pytest.mark.parametrize(
    'format',
    [
        audb2.define.Format.WAV,
        audb2.define.Format.FLAC,
    ],
)
def test_destination(format):
    flavor = audb2.Flavor(format=format)
    filename = 'wav/audio1.wav'
    expected_filename = f'{filename[:-4]}.{format}'
    assert flavor.destination(filename) == expected_filename


@pytest.mark.parametrize(
    'bit_depth_in, channels_in, format_in, sampling_rate_in, flavor, '
    'bit_depth_out, channels_out, format_out, sampling_rate_out',
    [
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(bit_depth=32),
            32, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(format=audb2.define.Format.FLAC),
            16, 1, audb2.define.Format.FLAC, 16000,
        ),
        pytest.param(
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(format=audb2.define.Format.FLAC),
            16, 2, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mixdown=True),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mixdown=True),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(channels=0),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(channels=1),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(channels=[0, 1]),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(channels=[0, -1]),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(channels=[0, 2]),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(sampling_rate=8000),
            16, 1, audb2.define.Format.WAV, 8000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(
                bit_depth=24,
                format=audb2.define.Format.FLAC,
                sampling_rate=8000,
            ),
            24, 1, audb2.define.Format.FLAC, 8000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(
                bit_depth=24,
                format=audb2.define.Format.FLAC,
                sampling_rate=8000,
            ),
            24, 1, audb2.define.Format.FLAC, 8000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(
                bit_depth=24,
                format=audb2.define.Format.FLAC,
                sampling_rate=8000,
            ),
            24, 1, audb2.define.Format.FLAC, 8000,
        ),
        (
            16, 1, 'mp3', 16000,
            audb2.Flavor(format=audb2.define.Format.WAV),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        # Cannot convert MP3 files
        pytest.param(
            16, 1, 'mp3', 16000,
            audb2.Flavor(bit_depth=24),
            24, 1, 'mp3', 16000,
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        pytest.param(
            16, 1, 'mp3', 16000,
            audb2.Flavor(channels=[0, 0]),
            16, 2, 'mp3', 16000,
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
        pytest.param(
            16, 1, 'mp3', 16000,
            audb2.Flavor(sampling_rate=8000),
            16, 1, 'mp3', 8000,
            marks=pytest.mark.xfail(raises=RuntimeError),
        ),
    ],
)
def test_call(tmpdir, bit_depth_in, channels_in, format_in, sampling_rate_in,
              flavor, bit_depth_out, channels_out, format_out,
              sampling_rate_out):

    file_in = os.path.join(tmpdir, 'in.' + format_in)
    file_out = os.path.join(tmpdir, 'out.' + format_out)

    signal = np.zeros((channels_in, sampling_rate_in), np.float32)
    if format_in == 'mp3':
        audiofile.write(
            f'{file_in[:-4]}.wav',
            signal,
            sampling_rate_in,
            bit_depth_in,
        )
        os.rename(f'{file_in[:-4]}.wav', file_in)
    else:
        audiofile.write(
            file_in,
            signal,
            sampling_rate_in,
            bit_depth_in,
        )

    flavor(file_in, file_out)
    assert audiofile.bit_depth(file_out) == bit_depth_out
    assert audiofile.channels(file_out) == channels_out
    assert audiofile.sampling_rate(file_out) == sampling_rate_out
