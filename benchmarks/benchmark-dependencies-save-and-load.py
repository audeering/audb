import hashlib
import os
import pickle
import random
import string
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet
import tabulate

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")


def astype(df, dtype):
    r"""Convert dataframe to desired format."""
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
        df.index = df.index.astype("object")
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
        df.index = df.index.astype("string")
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
        df.index = df.index.astype("string[pyarrow]")
    return df


# Create dependency dataframe
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
    df.index = df.index.astype("string")
    df.to_pickle(data_cache)
else:
    df = pd.read_pickle(data_cache)

# ===== Data type mappings =====
dtype_mapping = {
    # pandas string as `string`
    "string": {
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
    },
    # pandas string as `object`
    "object": {
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
    },
    # pandas pyarrow dtypes
    "pyarrow": {
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
    },
}
# pyarrow dtype schema
pyarrow_schema = pa.schema(
    [
        ("file", pa.string()),
        ("archive", pa.string()),
        ("bit_depth", pa.int32()),
        ("channels", pa.int32()),
        ("checksum", pa.string()),
        ("duration", pa.float64()),
        ("format", pa.string()),
        ("removed", pa.int32()),
        ("sampling_rate", pa.int32()),
        ("type", pa.int32()),
        ("version", pa.string()),
    ]
)

# ===== Benchmark storing data =====
folder = audeer.path(cache, "files")
audeer.rmdir(folder)
audeer.mkdir(folder)

dtypes = ["object", "string", "pyarrow"]

# ===== WRITING ===========================================================
results = pd.DataFrame(columns=["csv", "pickle", "parquet"])
results.index.name = "method"

print()
print("* Write to CSV file.", end="", flush=True)
file = audeer.path(folder, "df.csv")

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    t0 = time.time()
    df.to_csv(file)
    t = time.time() - t0
    method = f"pd.DataFrame[{dtype}]"
    results.at[method, "csv"] = t
    print(".", end="", flush=True)

# Use object and copy to pyarrow before writing
_df = df.copy()
_df = astype(_df, "object")
t0 = time.time()
_df = astype(_df, "pyarrow")
_df.to_csv(file)
t = time.time() - t0
method = "pd.DataFrame[object] -> pd.DataFrame[pyarrow]"
results.at[method, "csv"] = t
print(".", end="", flush=True)

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    t0 = time.time()
    _df.index = _df.index.rename("file")
    _df = _df.reset_index()
    table = pa.Table.from_pandas(_df, preserve_index=False, schema=pyarrow_schema)
    _columns = table.column_names
    _columns = ["" if c == "file" else c for c in _columns]
    _table = table.rename_columns(_columns)
    csv.write_csv(
        _table,
        file,
        write_options=csv.WriteOptions(quoting_style="none"),
    )
    t = time.time() - t0
    method = f"pd.DataFrame[{dtype}] -> pa.Table"
    results.at[method, "csv"] = t
    print(".", end="", flush=True)

# Use object and copy to pyarrow before writing
_df = df.copy()
_df = astype(_df, "object")
t0 = time.time()
_df = astype(_df, "pyarrow")
_df.index = _df.index.rename("file")
_df = _df.reset_index()
table = pa.Table.from_pandas(_df, preserve_index=False, schema=pyarrow_schema)
_columns = table.column_names
_columns = ["" if c == "file" else c for c in _columns]
_table = table.rename_columns(_columns)
csv.write_csv(
    _table,
    file,
    write_options=csv.WriteOptions(quoting_style="none"),
)
t = time.time() - t0
method = "pd.DataFrame[object] -> pd.DataFrame[pyarrow] -> pa.Table"
results.at[method, "csv"] = t
print(".", end="", flush=True)

t0 = time.time()
_table = table.rename_columns(_columns)
csv.write_csv(
    _table,
    file,
    write_options=csv.WriteOptions(quoting_style="none"),
)
t = time.time() - t0
method = "pa.Table"
results.at[method, "csv"] = t
print(".")

# -------------------------------------------------------------------------
print("* Write to PKL file.", end="", flush=True)
file = audeer.path(folder, "df.pkl")

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    t0 = time.time()
    _df.to_pickle(file, protocol=4)
    t = time.time() - t0
    method = f"pd.DataFrame[{dtype}]"
    results.at[method, "pickle"] = t
    print(".", end="", flush=True)

# Use object and copy to pyarrow before writing
_df = df.copy()
_df = astype(_df, "object")
t0 = time.time()
_df = astype(_df, "pyarrow")
_df.to_pickle(file, protocol=4)
t = time.time() - t0
method = "pd.DataFrame[object] -> pd.DataFrame[pyarrow]"
results.at[method, "pickle"] = t
print(".")

# -------------------------------------------------------------------------
print("* Write to PARQUET file.", end="", flush=True)
file = audeer.path(folder, "df.parquet")

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    t0 = time.time()
    _df.to_parquet(file, engine="pyarrow")
    t = time.time() - t0
    method = f"pd.DataFrame[{dtype}]"
    results.at[method, "parquet"] = t
    print(".", end="", flush=True)

t0 = time.time()
parquet.write_table(table, file)
t = time.time() - t0
method = "pa.Table"
results.at[method, "parquet"] = t
print(".")
print()

# ===== Print results =====
print(f"Results for writing {num_rows} lines.")
print()
results = results.replace(np.nan, None)
results = tabulate.tabulate(results, headers="keys", tablefmt="github", floatfmt=".3f")
print(results)


# ===== READING ===========================================================
results = pd.DataFrame(columns=["csv", "pickle", "parquet"])
results.index.name = "method"

print()
print("* Read CSV file.", end="", flush=True)
file = audeer.path(folder, "df.csv")

for engine in [None, "c", "pyarrow"]:
    if engine is None:
        arrow = "---->"
    elif engine == "pyarrow":
        arrow = "-pa->"
    elif engine == "c":
        arrow = "-c-->"

    for dtype in dtypes:
        t0 = time.time()
        index_col = 0
        _df = pd.read_csv(
            file,
            index_col=index_col,
            na_filter=False,
            dtype=dtype_mapping[dtype],
            header=0,
            engine=engine,
        )
        t = time.time() - t0
        method = f"{arrow} pd.DataFrame[{dtype}]"
        results.at[method, "csv"] = t
        if dtype == "pyarrow":
            dtype = "string[pyarrow]"
        _df.index = _df.index.astype(dtype)
        assert _df.archive.dtype == dtype
        print(".", end="", flush=True)

    # Convert pyarrow dtypes to object
    t0 = time.time()
    index_col = 0
    _df = pd.read_csv(
        file,
        index_col=index_col,
        na_filter=False,
        dtype=dtype_mapping["pyarrow"],
        header=0,
        engine=engine,
    )
    _df = astype(_df, "object")
    t = time.time() - t0
    method = f"{arrow} pd.DataFrame[pyarrow] -> pd.DataFrame[object]"
    results.at[method, "csv"] = t
    print(".", end="", flush=True)


for dtype in dtypes:
    t0 = time.time()
    _table = csv.read_csv(
        file,
        read_options=csv.ReadOptions(
            column_names=table.column_names,
            skip_rows=1,
        ),
        convert_options=csv.ConvertOptions(column_types=pyarrow_schema),
    )
    if dtype == "object":
        types_mapper = None
    elif dtype == "string":
        types_mapper = {pa.string(): pd.StringDtype()}.get
    elif dtype == "pyarrow":
        types_mapper = pd.ArrowDtype
    _df = _table.to_pandas(
        # Speed up conversion,
        # increase memory usage by ~20 MB
        deduplicate_objects=False,
        types_mapper=types_mapper,
    )
    _df.set_index("file", inplace=True)
    _df.index.name = None
    t = time.time() - t0
    method = f"----> pa.Table -> pd.DataFrame[{dtype}]"
    results.at[method, "csv"] = t
    if dtype == "pyarrow":
        dtype = "string[pyarrow]"
    assert _df.archive.dtype == dtype
    print(".", end="", flush=True)

# Convert pyarrow dtypes to object
t0 = time.time()
_table = csv.read_csv(
    file,
    read_options=csv.ReadOptions(
        column_names=table.column_names,
        skip_rows=1,
    ),
    convert_options=csv.ConvertOptions(column_types=pyarrow_schema),
)
_df = _table.to_pandas(
    # Speed up conversion,
    # increase memory usage by ~20 MB
    deduplicate_objects=False,
    types_mapper=pd.ArrowDtype,
)
_df.set_index("file", inplace=True)
_df.index.name = None
_df = astype(_df, "object")
t = time.time() - t0
method = "----> pa.Table -> pd.DataFrame[pyarrow] -> pd.DataFrame[object]"
results.at[method, "csv"] = t
print(".", end="", flush=True)

t0 = time.time()
_table = csv.read_csv(
    file,
    read_options=csv.ReadOptions(
        column_names=table.column_names,
        skip_rows=1,
    ),
    convert_options=csv.ConvertOptions(column_types=pyarrow_schema),
)
_index = {file: n for n, file in enumerate(_table.column("file"))}
t = time.time() - t0
method = "----> pa.Table"
results.at[method, "csv"] = t
print(".")

# -------------------------------------------------------------------------
print("* Read PKL file.", end="", flush=True)
file = audeer.path(folder, "df.pkl")

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    _df.to_pickle(file)
    t0 = time.time()
    _df = pd.read_pickle(file)
    t = time.time() - t0
    method = f"----> pd.DataFrame[{dtype}]"
    results.at[method, "pickle"] = t
    print(".", end="", flush=True)

# Convert pyarrow dtypes to object
_df = df.copy()
_df = astype(_df, "pyarrow")
_df.to_pickle(file)
t0 = time.time()
_df = pd.read_pickle(file)
_df = astype(_df, "object")
t = time.time() - t0
method = "----> pd.DataFrame[pyarrow] -> pd.DataFrame[object]"
results.at[method, "pickle"] = t
print(".")

# -------------------------------------------------------------------------
print("* Read PARQUET file.", end="", flush=True)
file = audeer.path(folder, "df.parquet")

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    _df.index.rename("file", inplace=True)
    _df = _df.reset_index()
    _df.to_parquet(file, index=False, engine="pyarrow")
    t0 = time.time()
    _df = pd.read_parquet(file, engine="pyarrow")
    t = time.time() - t0
    method = f"----> pd.DataFrame[{dtype}]"
    results.at[method, "parquet"] = t
    print(".", end="", flush=True)

for dtype in dtypes:
    _df = df.copy()
    _df = astype(_df, dtype)
    _df.index.rename("file", inplace=True)
    _df = _df.reset_index()
    _df.to_parquet(file, index=False, engine="pyarrow")
    t0 = time.time()
    _table = parquet.read_table(file)
    if dtype == "object":
        types_mapper = None
    elif dtype == "string":
        types_mapper = {pa.string(): pd.StringDtype()}.get
    elif dtype == "pyarrow":
        types_mapper = pd.ArrowDtype
    _df = _table.to_pandas(
        # Speed up conversion,
        # increase memory usage by ~20 MB
        deduplicate_objects=False,
        types_mapper=types_mapper,
    )
    _df.set_index("file", inplace=True)
    _df.index.name = None
    t = time.time() - t0
    method = f"----> pa.Table -> pd.DataFrame[{dtype}]"
    results.at[method, "parquet"] = t
    print(".", end="", flush=True)

t0 = time.time()
_table = parquet.read_table(file)
_index = {file: n for n, file in enumerate(_table.column("file"))}
t = time.time() - t0
method = "----> pa.Table"
results.at[method, "parquet"] = t
print(".")
print()

# ===== Print results =====
print(f"Results for reading {num_rows} lines.")
print()
results = results.replace(np.nan, None)
results = tabulate.tabulate(results, headers="keys", tablefmt="github", floatfmt=".3f")
print(results)
