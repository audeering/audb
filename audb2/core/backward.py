import pandas as pd

import audeer

from audb2.core.api import (
    cached,
    default_cache_root,
)


@audeer.deprecated(
    removal_version='1.1.0',
    alternative='cached',
)
def cached_databases(
        cache_root: str = None,
) -> pd.DataFrame:
    df = cached(cache_root)
    df['exclude'] = None
    df['include'] = None
    df['only_metadata'] = None

    for idx in df[['channels', 'mixdown']].index:
        mix = None
        channels = df.loc[idx, 'channels']
        mixdown = df.loc[idx, 'mixdown']
        if channels is None and mixdown:
            mix = 'mono'
        elif channels == [0]:
            mix = 'left'
        elif channels == [1]:
            mix = 'right'
        elif channels == [0, 1]:
            mix = 'stereo'
        df.loc[idx, 'mix'] = mix

    columns = [
        'name',
        'flavor_id',
        'version',
        'only_metadata',
        'format',
        'sampling_rate',
        'mix',
        'include',
        'exclude',
    ]
    return df[columns]


@audeer.deprecated(
    removal_version='1.1.0',
    alternative='default_cache_root',
)
def get_default_cache_root() -> str:
    return default_cache_root()
