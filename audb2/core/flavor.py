import os
import typing

import numpy as np

import audiofile
import audobject
import audresample

from audb2.core import define


def check_channels(
        channels: int,
        expected: typing.Union[int, typing.Sequence[int]],
):
    r"""Raises an error if ``channels`` does not matches ``expected``."""
    if isinstance(expected, int):
        if channels != expected:
            raise ValueError(
                f'Got {channels} channels, but expected {expected}.'
            )
    else:
        if channels not in expected:
            raise ValueError(
                f'Got {channels} channels, but expected one of {expected}.'
            )


class Flavor(audobject.Object):
    r"""Database flavor.

    Use :meth:`audb2.Flavor.__call__` to convert a file to the flavor.

    Args:
        only_metadata: only metadata is stored
        format: file format, one of ``'flac'``, ``'wav'``
        mix: mixing strategy, one of
            ``'left'``, ``'right'``, ``'mono'``, ``'stereo'`` or
            a list with channels numbers
        bit_depth: sample precision, one of ``16``, ``24``, ``32``
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``
        include: regexp pattern specifying data artifacts to include
        exclude: regexp pattern specifying data artifacts to exclude

    """
    def __init__(
            self,
            *,
            only_metadata: bool = False,
            bit_depth: int = None,
            format: str = None,
            mix: typing.Union[str, int, typing.Sequence[int]] = None,
            sampling_rate: int = None,
            tables: typing.Union[str, typing.Sequence[str]] = None,
            exclude: typing.Union[str, typing.Sequence[str]] = None,
            include: typing.Union[str, typing.Sequence[str]] = None,
    ):
        if only_metadata:
            bit_depth = format = sampling_rate = mix = None

        if mix is not None and not isinstance(mix, str):
            if isinstance(mix, int):
                mix = [mix]
            else:
                mix = list(mix)

        if format is not None:
            if format not in define.FORMATS:
                raise ValueError(
                    f'Format has to be one of '
                    f"{define.FORMATS}, not '{format}'."
                )
        if mix is not None:
            if isinstance(mix, str) and mix not in define.MIXES:
                raise ValueError(
                    f'Mix has to be one of '
                    f"{define.MIXES}, not '{mix}'."
                )
        if bit_depth is not None:
            if bit_depth not in define.BIT_DEPTHS:
                raise ValueError(
                    f'Bit depth has to be one of '
                    f"{define.BIT_DEPTHS}, not {bit_depth}."
                )
        if sampling_rate is not None:
            if sampling_rate not in define.SAMPLING_RATES:
                raise ValueError(
                    f'Sampling_rate has to be one of '
                    f'{define.SAMPLING_RATES}, not {sampling_rate}.'
                )

        self.exclude = exclude
        r"""Filter for excluding media."""
        self.format = format
        r"""File format."""
        self.include = include
        r"""Filter for including media."""
        self.mix = mix
        r"""Mixing strategy."""
        self.only_metadata = only_metadata
        r"""Sample precision."""
        self.bit_depth = bit_depth
        r"""Only metadata is stored."""
        self.sampling_rate = sampling_rate
        r"""Sampling rate in Hz."""
        self.tables = tables
        r"""Table filter."""

    def destination(
            self,
            file: str,
    ) -> str:
        r"""Return converted file path.

        The file path will only change if the file is converted to a different
        format.

        Args:
            file: path to input file

        Returns:
            path to output file

        """
        if self.format is not None:
            name, format = os.path.splitext(file)
            if format[1:].lower() != self.format:
                file = name + '.' + self.format
        return file

    def _check_convert(
            self,
            file: str,
    ) -> bool:
        r"""Check if file needs to be converted to flavor."""

        # format change
        if file != self.destination(file):
            return True

        # mix change
        if self.mix is not None:
            channels = audiofile.channels(file)
            if isinstance(self.mix, str):
                if self.mix == define.Mix.MONO_ONLY:
                    check_channels(channels, 1)
                elif self.mix == define.Mix.MONO and channels != 1:
                    return True
                elif self.mix in (define.Mix.LEFT, define.Mix.RIGHT):
                    check_channels(channels, 2)
                    return True
                elif self.mix == define.Mix.STEREO and channels != 2:
                    check_channels(channels, [1, 2])
                    return True
                elif self.mix == define.Mix.STEREO_ONLY:
                    check_channels(channels, 2)
            else:
                if list(range(channels)) != self.mix:
                    return True

        # sampling rate change
        if self.sampling_rate is not None:
            sampling_rate = audiofile.sampling_rate(file)
            if self.sampling_rate != sampling_rate:
                return True

        # precision change
        if self.bit_depth is not None:
            bit_depth = audiofile.bit_depth(file)
            if self.bit_depth != bit_depth:
                return True

        return False

    def _remix(
            self,
            signal: np.ndarray,
    ) -> np.ndarray:
        r"""Remix signal to flavor."""

        if self.mix is None:
            return signal

        channels = signal.shape[0]

        if isinstance(self.mix, str):
            if self.mix == define.Mix.MONO:
                # mixdown
                signal = audresample.remix(signal, mixdown=True)
            elif self.mix == define.Mix.LEFT:
                # input 2 channels, output left
                signal = signal[0, :]
            elif self.mix == define.Mix.RIGHT:
                # input 2 channels, output right
                signal = signal[1, :]
            elif self.mix == define.Mix.STEREO:
                # input either 1 or 2 channels,
                # output always 2 channels
                if channels == 1:
                    signal = np.repeat(signal, 2, axis=0)
        else:
            signal = audresample.remix(signal, channels=self.mix)

        return signal

    def _resample(
            self,
            signal: np.ndarray,
            sampling_rate: int,
    ) -> (np.ndarray, int):
        r"""Resample signal to flavor."""

        if (self.sampling_rate is not None) and \
                (sampling_rate != self.sampling_rate):
            signal = audresample.resample(
                signal, sampling_rate, self.sampling_rate,
            )
            sampling_rate = self.sampling_rate
        return signal, sampling_rate

    def __call__(
            self,
            file: str,
    ) -> str:
        r"""Convert file to flavor.

        Depending on the type of conversion,
        the input file will be replaced or deleted.

        Args:
            file: path to input file

        Returns:
            path to converted file

        """
        if not self._check_convert(file):
            return file

        signal, sampling_rate = audiofile.read(file, always_2d=True)
        signal = self._remix(signal)
        signal, sampling_rate = self._resample(signal, sampling_rate)
        destination = self.destination(file)
        bit_depth = self.bit_depth or audiofile.bit_depth(file)
        audiofile.write(
            destination,
            signal,
            sampling_rate,
            bit_depth=bit_depth,
        )
        if file != destination:
            os.remove(file)
        return destination
