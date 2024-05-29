import hashlib
import os
import pickle
import random
import string
import time

import pandas as pd
import polars as pl
import tabulate
import utils

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")

CACHE_EXT: str = "pkl"
CACHE_EXT: str = "parquet"
PARQUET_SAVE_OPTS: dict = {"engine": "pyarrow"}
# dtypes : list = ["string", "object", "pyarrow"]
dtypes: list = [
    "string",
]


def set_dependency_module():
    # audb.core.define.DEPEND_INDEX_DTYPE
    audb.core.define.DEPEND_INDEX_COLNAME = utils.DEPEND_INDEX_COLNAME
    audb.core.define.DEPEND_FIELD_DTYPES_PANDAS = audb.core.define.DEPEND_FIELD_DTYPES
    audb.core.define.DEPEND_FIELD_DTYPES = utils.DEPEND_FIELD_DTYPES
    audb.core.define.DEPEND_INDEX_DTYPE_PANDAS = audb.core.define.DEPEND_INDEX_DTYPE
    audb.core.define.DEPEND_INDEX_DTYPE = utils.DEPEND_INDEX_DTYPE

    import dependencies_polars

    audb.Dependencies = dependencies_polars.Dependencies


def astype(df, dtype):
    """Convert to desired dataframe dtypes."""
    if dtype == "object":
        # Use `object` to represent strings
        df["archive"] = df["archive"].astype("object")
        df["bit_depth"] = df["bit_depth"].astype("int32")
        df["channels"] = df["channels"].astype("int32")
        df["checksum"] = df["checksum"].astype("object")
        df["duration"] = df["duration"].astype("float64")
        df["format"] = df["format"].astype("object")
        df["removed"] = df["removed"].astype("int32")
        df["sampling_rate"] = df["sampling_rate"].astype("int32")
        df["type"] = df["type"].astype("int32")
        df["version"] = df["version"].astype("object")
        df.index = df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
        # Set dtypes in library
        audb.core.define.DEPEND_FIELD_DTYPES = {
            "archive": "object",
            "bit_depth": "int32",
            "channels": "int32",
            "checksum": "object",
            "duration": "float64",
            "format": "object",
            "removed": "int32",
            "sampling_rate": "int32",
            "type": "int32",
            "version": "object",
        }
    elif dtype == "string":
        # Uses `string` to represent strings
        audb.core.define.DEPEND_INDEX_COLNAME

    elif dtype == "pyarrow":
        # Use `pyarrow` to represent all dtypes
        df["archive"] = df["archive"].astype("string[pyarrow]")
        df["bit_depth"] = df["bit_depth"].astype("int32[pyarrow]")
        df["channels"] = df["channels"].astype("int32[pyarrow]")
        df["checksum"] = df["checksum"].astype("string[pyarrow]")
        df["duration"] = df["duration"].astype("float64[pyarrow]")
        df["format"] = df["format"].astype("string[pyarrow]")
        df["removed"] = df["removed"].astype("int32[pyarrow]")
        df["sampling_rate"] = df["sampling_rate"].astype("int32[pyarrow]")
        df["type"] = df["type"].astype("int32[pyarrow]")
        df["version"] = df["version"].astype("string[pyarrow]")
        df.index = df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
        # Set dtypes in library
        audb.core.define.DEPEND_FIELD_DTYPES = {
            "archive": "string[pyarrow]",
            "bit_depth": "int32[pyarrow]",
            "channels": "int32[pyarrow]",
            "checksum": "string[pyarrow]",
            "duration": "float64[pyarrow]",
            "format": "string[pyarrow]",
            "removed": "int32[pyarrow]",
            "sampling_rate": "int32[pyarrow]",
            "type": "int32[pyarrow]",
            "version": "string[pyarrow]",
        }
    return df


# === Dependencies pandas.DataFrame ===
data_cache = audeer.path(cache, f"df.{CACHE_EXT}")

num_rows = 1000000

if not os.path.exists(data_cache):
    bit_depths = [0, 16, 24]
    channels = [0, 1, 2]
    formats = ["csv", "wav", "txt"]
    sampling_rates = [0, 16000, 44100]
    types = [0, 1, 2]
    versions = ["1.0.0", "1.1.0"]
    records = [
        {
            "file": f"file-{n}.wav",
            "archive": f"archive-{n}",
            "bit_depth": random.choices(bit_depths, weights=[0.1, 0.8, 0.1])[0],
            "channels": random.choices(channels, weights=[0.1, 0.8, 0.1])[0],
            "checksum": hashlib.md5(
                pickle.dumps(random.choice(string.ascii_letters))
            ).hexdigest(),
            "duration": 10 * random.random(),
            "format": random.choices(formats, weights=[0.1, 0.8, 0.1])[0],
            "removed": random.choices([0, 1], weights=[0.1, 0.9])[0],
            "sampling_rate": random.choices(sampling_rates, weights=[0.1, 0.8, 0.1])[0],
            "type": random.choices(types, weights=[0.1, 0.8, 0.1])[0],
            "version": random.choices(versions, weights=[0.2, 0.8])[0],
        }
        for n in range(num_rows)
    ]
    df = pd.DataFrame.from_records(records)
    for column, dtype in zip(
        audb.core.define.DEPEND_FIELD_NAMES.values(),
        audb.core.define.DEPEND_FIELD_DTYPES.values(),
    ):
        df[column] = df[column].astype(dtype)
    df.set_index("file", inplace=True)
    df.index.name = None

    df.index = df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
    df.to_pickle(data_cache)

    # df.index = df.index.astype("string")
    if CACHE_EXT == "pkl":
        df.to_pickle(data_cache)
    elif CACHE_EXT == "parquet":
        df.to_parquet(**PARQUET_SAVE_OPTS)


# ===== Benchmark audb.Dependencies =====
# deps = audb.Dependencies()
# deps.load(data_cache)
file = "file-10.wav"
n_files = 10000
results = pd.DataFrame(columns=dtypes)
results.index.name = "method"
set_dependency_module()

for dtype in dtypes:
    # load them
    deps = audb.Dependencies()
    deps.load(data_cache)
    deps._df = astype(deps._df, dtype)

    _files = deps._df["file"][:n_files].to_list()

    # only string meanningful
    expected_dtype = pl.String

    assert deps._df["archive"].dtype == expected_dtype

    method = "Dependencies.__call__()"
    t0 = time.time()
    # deps()
    t = time.time() - t0
    results.at[method, dtype] = t

    # Access the index one time.
    # Further calls will be faster
    file in deps

    method = f"Dependencies.__contains__({n_files} files)"
    t0 = time.time()
    [file in deps for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.__get_item__({n_files} files)"
    t0 = time.time()
    # [deps[file] for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = pd.NaT

    method = "Dependencies.__len__()"
    t0 = time.time()
    len(deps)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.__str__()"
    t0 = time.time()
    str(deps)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.archives"
    t0 = time.time()
    deps.archives
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.attachments"
    t0 = time.time()
    deps.attachments
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.attachment_ids"
    t0 = time.time()
    deps.attachment_ids
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.files"
    t0 = time.time()
    deps.files
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.media"
    t0 = time.time()
    deps.media
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.removed_media"
    t0 = time.time()
    deps.removed_media
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.table_ids"
    t0 = time.time()
    deps.table_ids
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies.tables"
    t0 = time.time()
    deps.tables
    t = time.time() - t0
    results.at[method, dtype] = t

    # slow but cannot vectorize as such
    # method = f"Dependencies.archive({n_files} files)"
    # t0 = time.time()
    # [deps.archive(file) for file in _files]
    # t = time.time() - t0
    # results.at[method, dtype] = t

    method = f"Dependencies.bit_depth({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.bit_depth(_files)
    t = time.time() - t0
    t_frame = t
    results.at[method, dtype] = t

    method = f"Dependencies.channels({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.channels(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.checksum({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.checksum(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.duration({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.duration(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.format({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.format(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.removed({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.removed(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.sampling_rate({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.sampling_rate(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.type({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.type(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.version({n_files} files) / vectorized"
    t0 = time.time()
    res_v = deps.version(_files)
    t = time.time() - t0
    results.at[method, dtype] = t

    # -------------------------------------------------------------------------

    method = "Dependencies._add_attachment()"
    t0 = time.time()
    deps._add_attachment("attachment.txt", "1.0.0", "archive", "checksum")
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies._add_media({n_files} files)"
    values = [
        (
            f"file-new-{n}.wav",  # file
            f"archive-new-{n}",  # archive
            16,  # bit_depth
            1,  # channels
            f"checksum-{n}",  # checksum
            0.4,  # duration
            "wav",  # format
            0,  # removed
            16000,  # sampling_rate
            1,  # type
            "1.0.0",  # version
        )
        for n in range(n_files)
    ]
    t0 = time.time()
    deps._add_media(values)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies._add_meta()"
    t0 = time.time()
    deps._add_meta("db.new-table.csv", "1.0.0", "archive", "checksum")
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies._drop()"
    t0 = time.time()
    deps._drop(["file-90000.wav"])
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies._remove()"
    t0 = time.time()
    deps._remove(file)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = "Dependencies._update_media()"
    t0 = time.time()
    deps._update_media(values)
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies._update_media_version({n_files} files)"
    t0 = time.time()
    deps._update_media_version([f"file-{n}.wav" for n in range(n_files)], "version")
    t = time.time() - t0
    results.at[method, dtype] = t

# ===== Print results =====
table = tabulate.tabulate(results, headers="keys", tablefmt="github", floatfmt=".3f")
fp_results = audeer.path(cache, "results_polars.csv")
results.to_csv(fp_results)

print(table)
