import hashlib
import os
import pickle
import random
import string
import time

import pandas as pd
import polars
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet

import audeer


random.seed(1)

cache = audeer.mkdir("./cache")

# Create dependency dataframe
data_cache = audeer.path(cache, "df.pkl")
num_rows = 1000000
dtypes = [str, str, int, int, str, float, str, int, int, int, str]
columns = [
    "file",
    "archive",
    "bit_depth",
    "channels",
    "checksum",
    "duration",
    "format",
    "removed",
    "sampling_rate",
    "type",
    "version",
]
if not os.path.exists(data_cache):
    records = [
        {
            "file": f"file-{n}.wav",
            "archive": f"archive-{n}",
            "bit_depth": random.choices([0, 16, 24], weights=[0.1, 0.8, 0.1])[0],
            "channels": random.choices([0, 1, 2], weights=[0.1, 0.8, 0.1])[0],
            "checksum": hashlib.md5(
                pickle.dumps(random.choice(string.ascii_letters))
            ).hexdigest(),
            "duration": 10 * random.random(),
            "format": random.choices(["csv", "wav", "txt"], weights=[0.1, 0.8, 0.1])[0],
            "removed": random.choices([0, 1], weights=[0.1, 0.9])[0],
            "sampling_rate": random.choices(
                [0, 16000, 44100],
                weights=[0.1, 0.8, 0.1],
            )[0],
            "type": random.choices([0, 1, 2], weights=[0.1, 0.8, 0.1])[0],
            "version": random.choices(["1.0.0", "1.1.0"], weights=[0.2, 0.8])[0],
        }
        for n in range(num_rows)
    ]
    df = pd.DataFrame.from_records(records)
    for column, dtype in zip(df.columns, dtypes):
        df[column] = df[column].astype(dtype)
    df.set_index("file", inplace=True)
    df.index.name = ""
    df.to_pickle(data_cache)
else:
    df = pd.read_pickle(data_cache)

# Corresponding pyarrow schema
schema = pa.schema(
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
polars_dtype = {
    "file": polars.String(),
    "archive": polars.String(),
    "bit_depth": polars.Int32(),
    "channels": polars.Int32(),
    "checksum": polars.String(),
    "duration": polars.Float64(),
    "format": polars.String(),
    "removed": polars.Int32(),
    "sampling_rate": polars.Int32(),
    "type": polars.Int32(),
    "version": polars.String(),
}

# ===== Benchmark storing data =====
folder = audeer.path(cache, "files")
audeer.rmdir(folder)
audeer.mkdir(folder)

# -------------------------------------------------------------------------
print("=== Write to CSV file ===")
file = audeer.path(folder, "df.csv")

t = time.time()
df.to_csv(file)
print(f"pandas.DataFrame -> CSV: {time.time() -t:.2f} s")

_df = df.copy()
t = time.time()
_df.index = _df.index.rename("file")
_df = _df.reset_index()
table = pa.Table.from_pandas(_df, preserve_index=False, schema=schema)
_columns = table.column_names
_columns = ["" if c == "file" else c for c in _columns]
_table = table.rename_columns(_columns)
csv.write_csv(
    _table,
    file,
    write_options=csv.WriteOptions(quoting_style="none"),
)
print(f"pandas.DataFrame -> pyarrow.Table -> CSV: {time.time() -t:.2f} s")

t = time.time()
_table = table.rename_columns(_columns)
csv.write_csv(
    _table,
    file,
    write_options=csv.WriteOptions(quoting_style="none"),
)
print(f"pyarrow.Table -> CSV: {time.time() -t:.2f} s")

# _df = _df.rename(columns={"file": ""})
# _df = polars.from_pandas(_df)
# t = time.time()
# _df.write_csv(file, quote_style="never")
# print(f"polars.DataFrame -> CSV: {time.time() -t:.2f} s")

# -------------------------------------------------------------------------
print("=== Write to PKL file ===")
file = audeer.path(folder, "df.pkl")

t = time.time()
df.to_pickle(file, protocol=4)
print(f"pandas.DataFrame -> PKL: {time.time() -t:.2f} s")

t = time.time()
_df = table.to_pandas()
_df.set_index("file", inplace=True)
_df.index.name = ""
_df.to_pickle(file, protocol=4)
print(f"pyarrow.Table -> pandas.DataFrame -> PKL: {time.time() -t:.2f} s")

# -------------------------------------------------------------------------
print("=== Write to PARQUET file ===")
file = audeer.path(folder, "df.parquet")

t = time.time()
df.to_parquet(file, engine="pyarrow")
print(f"pandas.DataFrame -> PARQUET: {time.time() -t:.2f} s")

t = time.time()
parquet.write_table(table, file)
print(f"pyarrow.Table -> PARQUET: {time.time() -t:.2f} s")

# -------------------------------------------------------------------------
print()
print("=== Read CSV file ===")
file = audeer.path(folder, "df.csv")

t = time.time()
# dtype_mapping = {column: dtype for column, dtype in zip(columns, dtypes)}
# dtype_mapping = {
#     # "": "string[pyarrow]",
#     "archive": "string[pyarrow]",
#     "bit_depth": "int32[pyarrow]",
#     "channels": "int32[pyarrow]",
#     "checksum": "string[pyarrow]",
#     "duration": "float64[pyarrow]",
#     "format": "string[pyarrow]",
#     "removed": "int32[pyarrow]",
#     "sampling_rate": "int32[pyarrow]",
#     "type": "int32[pyarrow]",
#     "version": "string[pyarrow]",
# }
dtype_mapping = {
    # "": "string",
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
index_col = 0
_df = pd.read_csv(
    file,
    index_col=index_col,
    na_filter=False,
    dtype=dtype_mapping,
    header=0,
)
_df.index = _df.index.astype("string")
print(f"CSV -> pandas.DataFrame: {time.time() -t:.2f} s")
print(_df.dtypes)

t = time.time()
# dtype_mapping = {column: dtype for column, dtype in zip(columns, dtypes)}
dtype_mapping = {
    # "": "string[pyarrow]",
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
# dtype_mapping = {
#     # "": "string",
#     "archive": "string",
#     "bit_depth": "int32",
#     "channels": "int32",
#     "checksum": "string",
#     "duration": "float64",
#     "format": "string",
#     "removed": "int32",
#     "sampling_rate": "int32",
#     "type": "int32",
#     "version": "string",
# }
index_col = 0
# dtype_mapping[index_col] = str
_df = pd.read_csv(
    file,
    index_col=index_col,
    na_filter=False,
    dtype=dtype_mapping,
    header=0,
    engine="pyarrow",
    # dtype_backend="pyarrow",
)
_df.index = _df.index.astype("string")
print(f"CSV -[pyarrow]> pandas.DataFrame: {time.time() -t:.2f} s")
print(_df.dtypes)

t = time.time()
_table = csv.read_csv(
    file,
    read_options=csv.ReadOptions(
        column_names=table.column_names,
        skip_rows=1,
    ),
    convert_options=csv.ConvertOptions(column_types=schema),
)
_df = _table.to_pandas()
_df.set_index("file", inplace=True)
_df.index.name = ""
print(f"CSV -> pyarrow.Table -> pd.DataFrame: {time.time() -t:.2f} s")

t = time.time()
_table = csv.read_csv(
    file,
    read_options=csv.ReadOptions(
        column_names=table.column_names,
        skip_rows=1,
    ),
    convert_options=csv.ConvertOptions(column_types=schema),
)
print(f"CSV -> pyarrow.Table: {time.time() -t:.2f} s")

t = time.time()
_df = polars.read_csv(
    file,
    dtypes=polars_dtype,
)
print(f"CSV -> polars.DataFrame: {time.time() -t:.2f} s")

# -------------------------------------------------------------------------
print("=== Read PKL file ===")
file = audeer.path(folder, "df.pkl")

t = time.time()
_df = pd.read_pickle(file)
print(f"PKL -> pd.DataFrame: {time.time() -t:.2f} s")

t = time.time()
_df = pd.read_pickle(file)
_df.index = _df.index.rename("file")
_df = _df.reset_index()
_table = pa.Table.from_pandas(_df, preserve_index=False, schema=schema)
_columns = _table.column_names
_columns = ["" if c == "file" else c for c in _columns]
_table = _table.rename_columns(_columns)
print(f"PKL -> pd.DataFrame -> pyarrow.Table: {time.time() -t:.2f} s")

# -------------------------------------------------------------------------
print("=== Read PARQUET file ===")
file = audeer.path(folder, "df.parquet")

t = time.time()
_df = pd.read_parquet(file, engine="pyarrow")
print(f"PARQUET -> pandas.DataFrame: {time.time() -t:.2f} s")

t = time.time()
_table = parquet.read_table(file)
_df = _table.to_pandas()
_df.set_index("file", inplace=True)
_df.index.name = ""
print(f"PARQUET -> pyarrow.Table -> pandas.DataFrame: {time.time() -t:.2f} s")

t = time.time()
_table = parquet.read_table(file)
print(f"PARQUET -> pyarrow.Table: {time.time() -t:.2f} s")
