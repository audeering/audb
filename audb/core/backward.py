import pandas as pd

import audeer

from audb.core.api import default_cache_root


@audeer.deprecated(
    removal_version='1.1.0',
    alternative='default_cache_root',
)
def get_default_cache_root() -> str:
    return default_cache_root()
