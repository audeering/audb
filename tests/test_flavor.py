import os

import numpy as np
import pytest

import audiofile
import audobject

import audb2


@pytest.mark.parametrize(
    'only_metadata, bit_depth, format, mix, sampling_rate',
    [
        (
            False, None, None, None, None,
        ),
        (
            False, 16, audb2.define.Format.WAV, audb2.define.Mix.MONO, 16000,
        ),
        (
            True, 16, audb2.define.Format.WAV, audb2.define.Mix.MONO, 16000,
        ),
        pytest.param(
            False, 0, audb2.define.Format.WAV, audb2.define.Mix.MONO, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            False, 16, 'bad', audb2.define.Mix.MONO, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            False, 16, audb2.define.Format.WAV, 'bad', 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        pytest.param(
            False, 16, audb2.define.Format.WAV, audb2.define.Mix.MONO, 0,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ]
)
def test_init(only_metadata, bit_depth, format, mix, sampling_rate):
    flavor = audb2.Flavor(
        only_metadata=only_metadata,
        bit_depth=bit_depth,
        format=format,
        mix=mix,
        sampling_rate=sampling_rate,
    )
    if only_metadata:
        assert flavor.bit_depth is None
        assert flavor.format is None
        assert flavor.mix is None
        assert flavor.sampling_rate is None
    flavor_s = flavor.to_yaml_s()
    flavor_2 = audobject.Object.from_yaml_s(flavor_s)
    assert isinstance(flavor_2, audb2.Flavor)
    assert flavor.id == flavor_2.id


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
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.MONO),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.MONO_ONLY),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.MONO_ONLY),
            16, 1, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.LEFT),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.LEFT),
            16, 1, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.RIGHT),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.RIGHT),
            16, 1, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.STEREO),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.STEREO),
            16, 2, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.STEREO_ONLY),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 1, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=audb2.define.Mix.STEREO_ONLY),
            16, 2, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=ValueError),
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
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=0),
            16, 1, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=[0, 1]),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        (
            16, 3, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=[0, -1]),
            16, 2, audb2.define.Format.WAV, 16000,
        ),
        pytest.param(
            16, 2, audb2.define.Format.WAV, 16000,
            audb2.Flavor(mix=[0, 2]),
            16, 2, audb2.define.Format.WAV, 16000,
            marks=pytest.mark.xfail(raises=RuntimeError)
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
    ],
)
def test_call(tmpdir, bit_depth_in, channels_in, format_in, sampling_rate_in,
              flavor, bit_depth_out, channels_out, format_out,
              sampling_rate_out):

    file_in = os.path.join(tmpdir, 'in.' + format_in)
    file_out = os.path.join(tmpdir, 'out.' + format_out)

    signal = np.zeros((channels_in, sampling_rate_in), np.float32)
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
