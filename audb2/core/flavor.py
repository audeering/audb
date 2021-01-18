import os
import shutil
import typing

import numpy as np

import audeer
import audiofile
import audobject
import audresample

from audb2.core import define


def split_ext(file: str) -> str:
    r"""File extension without . and lower case."""
    _, ext = os.path.splitext(file.lower())
    return ext[1:]


class Flavor(audobject.Object):
    r"""Database flavor.

    Helper class used by :meth:`audb2.load`
    to convert media files to the desired format.
    It stores the meta information about a flavor
    and offers a convenient way to convert files to it.

    As the following example shows,
    it can also be used to convert files that are not part of database:

    .. code-block::  python

        original_file = '/org/path/file.flac'
        converted_file = '/new/path/file.wav'

        # convert file to 16 kHz
        flavor = Flavor(sampling_rate=16000)
        flavor(original_file, converted_file)

    Args:
        only_metadata: only metadata is stored
        bit_depth: sample precision, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down on selection
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
            channels: typing.Union[int, typing.Sequence[int]] = None,
            format: str = None,
            mixdown: bool = False,
            sampling_rate: int = None,
            tables: typing.Union[str, typing.Sequence[str]] = None,
            exclude: typing.Union[str, typing.Sequence[str]] = None,
            include: typing.Union[str, typing.Sequence[str]] = None,
    ):
        if only_metadata:
            bit_depth = channels = format = sampling_rate = None
            mixdown = False

        if bit_depth is not None:
            if bit_depth not in define.BIT_DEPTHS:
                raise ValueError(
                    f'Bit depth has to be one of '
                    f"{define.BIT_DEPTHS}, not {bit_depth}."
                )

        if channels is not None:
            if isinstance(channels, int):
                channels = [channels]
            channels = list(channels)

        if format is not None:
            if format not in define.FORMATS:
                raise ValueError(
                    f'Format has to be one of '
                    f"{define.FORMATS}, not '{format}'."
                )

        if sampling_rate is not None:
            if sampling_rate not in define.SAMPLING_RATES:
                raise ValueError(
                    f'Sampling_rate has to be one of '
                    f'{define.SAMPLING_RATES}, not {sampling_rate}.'
                )

        self.bit_depth = bit_depth
        r"""Sample precision."""
        self.channels = channels
        r"""Selected channels."""
        self.exclude = exclude
        r"""Filter for excluding media."""
        self.format = format
        r"""File format."""
        self.include = include
        r"""Filter for including media."""
        self.mixdown = mixdown
        r"""Apply mixdown."""
        self.only_metadata = only_metadata
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

    def path(
            self,
            name: str,
            version: str,
            repository: str,
    ) -> str:
        r"""Flavor path.

        Args:
            name: database name
            version: version string
            repository: repository

        Returns:
            relative path

        """
        return os.path.join(repository, name, self.id, version)

    def _check_convert(
            self,
            file: str,
            bit_depth: typing.Optional[int],
            channels: typing.Optional[int],
            sampling_rate: typing.Optional[int],
    ) -> bool:
        r"""Check if file needs to be converted to flavor."""

        # format change
        if self.format is not None:
            ext = split_ext(file)
            if self.format != ext:
                return True

        # precision change
        if self.bit_depth is not None:
            bit_depth = bit_depth or audiofile.bit_depth(file)
            if self.bit_depth != bit_depth:
                return True

        # mixdown and channel selection
        if self.mixdown or self.channels is not None:
            channels = channels or audiofile.channels(file)
            if self.mixdown and channels != 1:
                return True
            elif list(range(channels)) != self.channels:
                return True

        # sampling rate change
        if self.sampling_rate is not None:
            sampling_rate = sampling_rate or audiofile.sampling_rate(file)
            if self.sampling_rate != sampling_rate:
                return True

        return False

    def _remix(
            self,
            signal: np.ndarray,
    ) -> np.ndarray:

        if (self.channels is not None) or self.mixdown:
            if self.channels is not None:
                max_channel = max(self.channels)
                if 0 < max_channel >= signal.shape[0]:
                    signal_ex = np.zeros(
                        (max_channel + 1, signal.shape[1]),
                        dtype=signal.dtype,
                    )
                    signal_ex[:signal.shape[0], :] = signal
                    signal = signal_ex
            signal = audresample.remix(signal, self.channels, self.mixdown)

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
            src_path: str,
            dst_path: str,
            *,
            bit_depth: int = None,
            channels: int = None,
            sampling_rate: int = None,
    ):
        r"""Convert file to flavor.

        Args:
            src_path: path to input file
            dst_path: path to output file
            bit_depth: bit depth
            channels: number of channels
            sampling_rate: sampling rate in Hz

        Raises:
            ValueError: if extension of output file does not match the
                format of the flavor

        """
        src_path = audeer.safe_path(src_path)
        dst_path = audeer.safe_path(dst_path)

        # verify that extension matches the output format
        src_ext = split_ext(src_path)
        dst_ext = split_ext(dst_path)
        expected_ext = self.format or src_ext
        if expected_ext != dst_ext:
            raise ValueError(
                f"Extension of output file is '{dst_ext}', "
                f"but should be '{expected_ext} "
                "to match the format of the converted file."
            )

        if not self._check_convert(
                src_path, bit_depth, channels, sampling_rate
        ):

            # file already satisfies flavor
            if src_path != dst_path:
                shutil.copy(src_path, dst_path)

        else:

            # convert file to flavor
            signal, sampling_rate = audiofile.read(src_path, always_2d=True)
            signal = self._remix(signal)
            signal, sampling_rate = self._resample(signal, sampling_rate)
            if self.bit_depth:
                bit_depth = self.bit_depth
            else:
                bit_depth = bit_depth or audiofile.bit_depth(src_path)
            audiofile.write(
                dst_path,
                signal,
                sampling_rate,
                bit_depth=bit_depth,
            )
