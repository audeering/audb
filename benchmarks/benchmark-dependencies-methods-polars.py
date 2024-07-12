import random
import time

import pandas as pd
import tabulate

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")

CACHE_EXT: str = "pkl"
CACHE_EXT: str = "parquet"
PARQUET_SAVE_OPTS: dict = {"engine": "pyarrow"}
# dtypes : list = ["string", "object", "pyarrow"]
dtypes: list = [
    "polars",
]


def set_dependency_module():
    r"""Monkeypatch dependency modult to use polars module."""
    import polars as pl

    from audb.core import define

    depend_index_colname = "file"
    depend_index_dtype = pl.datatypes.Object
    depend_field_dtypes = dict(
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

    audb.core.define.DEPEND_INDEX_COLNAME = depend_index_colname
    audb.core.define.DEPEND_FIELD_DTYPES_PANDAS = audb.core.define.DEPEND_FIELD_DTYPES
    audb.core.define.DEPEND_FIELD_DTYPES = depend_field_dtypes
    audb.core.define.DEPEND_INDEX_DTYPE_PANDAS = audb.core.define.DEPEND_INDEX_DTYPE
    audb.core.define.DEPEND_INDEX_DTYPE = depend_index_dtype

    import dependencies_polars

    audb.Dependencies = dependencies_polars.Dependencies


# === Dependencies load via pickle before monkey_patching ===
data_cache = audeer.path(cache, "df.pkl")
deps = audb.Dependencies()
deps.load(data_cache)

# save cache in parquet format as the polars load method depends on it
parquet_cache = audeer.path(cache, "df.parquet")
deps.save(parquet_cache)

file = "file-10.wav"
n_files = 10000
results = pd.DataFrame(columns=["polars"])
results.index.name = "method"
set_dependency_module()
dtype = "polars"

for dtype in dtypes:
    # load them
    deps = audb.Dependencies()
    deps.load(parquet_cache)
    _files = deps._df["file"][:n_files].to_list()

    # only string meanningful
    # expected_dtype = pl.String

    # assert deps._df["archive"].dtype == expected_dtype

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
    [deps[file] for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

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

    method = f"Dependencies.archive({n_files} files)"
    t0 = time.time()
    [deps.archive(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.bit_depth({n_files} files)"
    t0 = time.time()
    [deps.bit_depth(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.channels({n_files} files)"
    t0 = time.time()
    [deps.channels(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.checksum({n_files} files)"
    t0 = time.time()
    [deps.checksum(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.duration({n_files} files)"
    t0 = time.time()
    [deps.duration(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.format({n_files} files)"
    t0 = time.time()
    [deps.format(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.removed({n_files} files)"
    t0 = time.time()
    [deps.removed(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.sampling_rate({n_files} files)"
    t0 = time.time()
    [deps.sampling_rate(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.type({n_files} files)"
    t0 = time.time()
    [deps.type(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    method = f"Dependencies.version({n_files} files)"
    t0 = time.time()
    [deps.version(file) for file in _files]
    t = time.time() - t0
    results.at[method, dtype] = t

    # -------------------------------------------------------------------------

    # TODO: Reimplement
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
