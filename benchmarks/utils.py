import errno
import os
import re
import tempfile
import typing

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet
import polars as pl
from pprint import pprint as pr

import audbackend
import audeer
import collections


from audb.core import define

polars_mappings = pl.datatypes.convert._DataTypeMappings()
pl2arrow_mappings = polars_mappings.PY_TYPE_TO_ARROW_TYPE
arrow2pl_mapping = dict(zip(pl2arrow_mappings.values(), pl2arrow_mappings.keys()))

DEPEND_FIELD_DTYPES = dict(
    zip(
        define.DEPEND_FIELD_DTYPES.keys(),
        [
            pl.datatypes.String,
            pl.datatypes.Int32,
            pl.datatypes.Int32,
            pl.datatypes.String,
            pl.datatypes.Float64,
            pl.datatypes.String,
            pl.datatypes.Int32,
            pl.datatypes.Int32,
            pl.datatypes.Int32,
            pl.datatypes.String,
        ],
    )
)

# do NOT use strings to designate types
# DEPEND_INDEX_DTYPE = str(pl.datatypes.Object)
# we cannot convert it anyway!
DEPEND_INDEX_DTYPE = pl.datatypes.Object
DEPEND_INDEX_COLNAME = "file"


def pascheme_to_plscheme(schema: pa.lib.Schema) -> collections.OrderedDict:
    r"""Convert pyarrow schema to polars schema.

    Args:
        schema a pyarrow scheme

    Returns:
        polars scheme

    Polars schemes are simple Ordered Dicts

    """
    pl_schema = {x.name: pl.from_arrow(pa.array([None], x.type)).dtype for x in schema}
    pl_schema = collections.OrderedDict(pl_schema)
    return pl_schema
