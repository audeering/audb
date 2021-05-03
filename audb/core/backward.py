import re
import typing
import warnings

import pandas as pd

import audeer

from audb.core.api import default_cache_root
from audb.core.dependencies import Dependencies
from audb.core.utils import mix_mapping


@audeer.deprecated(
    removal_version='1.2.0',
    alternative='default_cache_root',
)
def get_default_cache_root() -> str:
    return default_cache_root()


def include_exclude_mapping(
        deps: Dependencies,
        include: typing.Optional[typing.Union[str, typing.Sequence[str]]],
        exclude: typing.Optional[typing.Union[str, typing.Sequence[str]]],
) -> typing.Sequence[str]:
    r"""Map include and exclude to media argument."""
    media = None

    if include is not None:
        archives = set([deps.archive(f) for f in deps.media])
        if isinstance(include, str):
            pattern = re.compile(include)
            include = [a for a in archives if pattern.search(a)]
        media = [x for x in deps.media if deps.archive(x) in include]

    if media is None:
        media = deps.media

    if exclude is not None:
        archives = set([deps.archive(f) for f in deps.media])
        if isinstance(exclude, str):
            pattern = re.compile(exclude)
            exclude = [a for a in archives if pattern.search(a)]
        media = [x for x in media if deps.archive(x) not in exclude]

    return media


def parse_deprecated_load_arguments(
        channels: typing.Union[int, typing.Sequence[int]],
        mixdown: bool,
        media: typing.Union[str, typing.Sequence[str]],
        deps: Dependencies,
        kwargs,
) -> typing.Tuple[
    typing.Optional[typing.List[int]],
    bool,
    typing.Optional[typing.List[str]],
]:
    r"""Reassign deprecated audb.load arguments

    It maps the deprecated argument ``'mix'``
    to ``'channels'`` and ``'mixdown'``.
    It maps the deprecated arguments
    ``'include'``
    and ``'exclude'``
    to the selection of media files.

    If some of the original arguments contain already a setting
    they will be overwritten.

    Args:
        channels: channel selection, see :func:`audresample.remix`
        mixdown: apply mono mix-down
        media: include only media matching the regular expression or
            provided in the list
        deps: database dependencies
        kwargs: keyword arguments containing possibly deprecated arguments

    Returns:
        channels: updated channel argument
        mixdown: updated mixdown argument
        media: updated media argument

    """
    # Map 'mix'
    if (
            channels is None
            and not mixdown
            and 'mix' in kwargs
    ):  # pragma: no cover
        mix = kwargs['mix']
        channels, mixdown = mix_mapping(mix)

    # Map 'include' and 'exclude'
    if 'include' in kwargs or 'exclude' in kwargs:  # pragma: no cover
        include = None
        if 'include' in kwargs:
            include = kwargs['include']
            warnings.warn(
                "Argument 'include' is deprecated "
                "and will be removed with version '1.2.0'. "
                "Use 'media' instead.",
                category=UserWarning,
                stacklevel=2,
            )
        exclude = None
        if 'exclude' in kwargs:  # pragma: no cover
            exclude = kwargs['exclude']
            warnings.warn(
                "Argument 'exclude' is deprecated "
                "and will be removed with version '1.2.0'. "
                "Use 'media' instead.",
                category=UserWarning,
                stacklevel=2,
            )
        if include is not None or exclude is not None:
            media = include_exclude_mapping(deps, include, exclude)

    return channels, mixdown, media
