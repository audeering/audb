import hashlib
import os
import pickle
import random
import string
import time

import pandas as pd
import tabulate

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")


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
        # Use `string` to represent strings
        df["archive"] = df["archive"].astype("string")
        df["bit_depth"] = df["bit_depth"].astype("int32")
        df["channels"] = df["channels"].astype("int32")
        df["checksum"] = df["checksum"].astype("string")
        df["duration"] = df["duration"].astype("float64")
        df["format"] = df["format"].astype("string")
        df["removed"] = df["removed"].astype("int32")
        df["sampling_rate"] = df["sampling_rate"].astype("int32")
        df["type"] = df["type"].astype("int32")
        df["version"] = df["version"].astype("string")
        df.index = df.index.astype(audb.core.define.DEPEND_INDEX_DTYPE)
        # Set dtypes in library
        audb.core.define.DEPEND_FIELD_DTYPES = {
            "archive": "string",
            "bit_depth": "int32",
            "channels": "int32",
            "checksum": "string",
            "duration": "float64",
            "format": "string",
            "removed": "int32",
            "sampling_rate": "int32",
            "type": "int32",
            "version": "string",
        }
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
data_cache = audeer.path(cache, "df.pkl")
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


# ===== Benchmark audb.Dependencies =====
deps = audb.Dependencies()
deps.load(data_cache)
file = "file-10.wav"
n_files = 10000
_files = deps._df.index[:n_files].tolist()
dtypes = ["string", "object", "pyarrow"]
results = pd.DataFrame(columns=dtypes)
results.index.name = "method"

for dtype in dtypes:
    deps.load(data_cache)
    deps._df = astype(deps._df, dtype)

    # Check we have the expected dtypes
    # in dependency table
    # and library
    if dtype == "pyarrow":
        expected_dtype = "string[pyarrow]"
    else:
        expected_dtype = dtype
    assert deps._df.archive.dtype == expected_dtype
    assert audb.core.define.DEPEND_FIELD_DTYPES["archive"] == expected_dtype

    method = "Dependencies.__call__()"
    t0 = time.time()
    deps()
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
print(table)
