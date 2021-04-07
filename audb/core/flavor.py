import os
import shutil
import typing

import numpy as np

import audeer
import audiofile
import audobject
import audresample

from audb.core import define


class Flavor(audobject.Object):
    r"""Database flavor.

    Helper class used by :meth:`audb.load`
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
        bit_depth: sample precision, one of ``16``, ``24``, ``32``
        channels: channel selection, see :func:`audresample.remix`
        format: file format, one of ``'flac'``, ``'wav'``
        mixdown: apply mono mix-down on selection
        sampling_rate: sampling rate in Hz, one of
            ``8000``, ``16000``, ``22500``, ``44100``, ``48000``

    """
    def __init__(
            self,
            *,
            bit_depth: int = None,
            channels: typing.Union[int, typing.Sequence[int]] = None,
            format: str = None,
            mixdown: bool = False,
            sampling_rate: int = None,
    ):
        if bit_depth is not None:
            bit_depth = int(bit_depth)
            if bit_depth not in define.BIT_DEPTHS:
                raise ValueError(
                    f'Bit depth has to be one of '
                    f"{define.BIT_DEPTHS}, not {bit_depth}."
                )

        if channels is not None:
            channels = audeer.to_list(channels)
            if len(channels) < 2:
                mixdown = False

        if format is not None:
            if format not in define.FORMATS:
                raise ValueError(
                    f'Format has to be one of '
                    f"{define.FORMATS}, not '{format}'."
                )

        if sampling_rate is not None:
            sampling_rate = int(sampling_rate)
            if sampling_rate not in define.SAMPLING_RATES:
                raise ValueError(
                    f'Sampling_rate has to be one of '
                    f'{define.SAMPLING_RATES}, not {sampling_rate}.'
                )

        self.bit_depth = bit_depth
        r"""Sample precision."""
        self.channels = channels
        r"""Selected channels."""
        self.format = format
        r"""File format."""
        self.mixdown = mixdown
        r"""Apply mixdown."""
        self.sampling_rate = sampling_rate
        r"""Sampling rate in Hz."""

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
            format = audeer.file_extension(file).lower()
            if format != self.format:
                file = f'{file[:-len(format)]}{self.format}'
        return file

    def path(
            self,
            name: str,
            version: str,
    ) -> str:
        r"""Flavor path.

        Args:
            name: database name
            version: version string

        Returns:
            relative path

        """
        return os.path.join(name, version, self.short_id)

    @property
    def short_id(
            self,
    ) -> str:
        r"""Short flavor ID.

        This just truncates the ID
        to its last eight characters.

        """
        return self.id[-8:]

    def _check_convert(
            self,
            file: str,
            bit_depth: typing.Optional[int],
            channels: typing.Optional[int],
            sampling_rate: typing.Optional[int],
    ) -> bool:
        r"""Check if file needs to be converted to flavor."""
        format = audeer.file_extension(file).lower()

        # format change
        if self.format is not None:
            if self.format != format:
                return True

        convert = False

        # precision change
        if not convert and self.bit_depth is not None:
            bit_depth = bit_depth or audiofile.bit_depth(file)
            if self.bit_depth != bit_depth:
                convert = True

        # mixdown and channel selection
        if not convert and self.mixdown or self.channels is not None:
            channels = channels or audiofile.channels(file)
            if self.mixdown and channels != 1:
                convert = True
            elif list(range(channels)) != self.channels:
                convert = True

        # sampling rate change
        if not convert and self.sampling_rate is not None:
            sampling_rate = sampling_rate or audiofile.sampling_rate(file)
            if self.sampling_rate != sampling_rate:
                convert = True

        if convert and format not in define.FORMATS:
            raise RuntimeError(
                f"You have to specify the 'format' argument "
                f"to convert '{file}' "
                f"to the specified flavor "
                f"as we cannot write to {format.upper()} files."
            )

        return convert

    def _remix(
            self,
            signal: np.ndarray,
    ) -> np.ndarray:

        if self.channels is None and not self.mixdown:
            return signal
        else:
            return audresample.remix(
                signal,
                self.channels,
                self.mixdown,
                upmix='repeat',
            )

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
            src_bit_depth: int = None,
            src_channels: int = None,
            src_sampling_rate: int = None,
    ):
        r"""Convert file to flavor.

        If ``bit_depth``, ``channels`` or ``sampling_rate``
        of source signal are known, they can be provided.
        Otherwise, they will be computed using :mod:`audiofile`.

        Args:
            src_path: path to input file
            dst_path: path to output file
            src_bit_depth: bit depth
            src_channels: number of channels
            src_sampling_rate: sampling rate in Hz

        Raises:
            ValueError: if extension of output file does not match the
                format of the flavor
            RuntimeError: if a conversion is requested,
                but no output format is specified,
                and the input format is not WAV or FLAC

        """
        src_path = audeer.safe_path(src_path)
        dst_path = audeer.safe_path(dst_path)

        # verify that extension matches the output format
        src_ext = audeer.file_extension(src_path).lower()
        dst_ext = audeer.file_extension(dst_path).lower()
        expected_ext = self.format or src_ext
        if expected_ext != dst_ext:
            raise ValueError(
                f"Extension of output file is '{dst_ext}', "
                f"but should be '{expected_ext}' "
                "to match the format of the converted file."
            )

        if not self._check_convert(
                src_path, src_bit_depth, src_channels, src_sampling_rate
        ):

            # file already satisfies flavor
            if src_path != dst_path:
                shutil.copy(src_path, dst_path)

        else:

            # convert file to flavor
            signal, sampling_rate = audiofile.read(src_path, always_2d=True)
            signal = self._remix(signal)
            signal, sampling_rate = self._resample(signal, sampling_rate)
            bit_depth = (
                self.bit_depth
                or src_bit_depth
                or audiofile.bit_depth(src_path)
                or 16
            )
            audiofile.write(
                dst_path,
                signal,
                sampling_rate,
                bit_depth=bit_depth,
            )
