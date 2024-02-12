# Benchmarks

This folder contains benchmarks
used to evaluate design decisions
in `audb`.

The reported results were calculated
on the following machine:

* CPU: 13th Gen Intel Core i7-1355U, 10-core (2-mt/8-st)
* RAM: 15.29 GiB
* Hard drive: KIOXIA KXG8AZNV1T02
* Linux: Ubuntu 22.04
* Python: 3.10

Before running any benchmark,
make sure to install missing requirements with:

```bash
$ cd benchmarks/
$ pip install -r requirements.txt
```


## audb.Dependencies methods

Benchmarks all methods of `audb.Dependencies`
besides `audb.Dependencies.load()`
and `audb.Dependencies.save()`.
This benchmark provides insights
how to best represent
the dependency table internally.

To run the benchmark execute:

```bash
$ python benchmark-dependencies-methods.py
```

Execution times in seconds
of `audb.Dependencies` methods
for a dependency table
containing 1,000,000 files
stored as a `pandas.DataFrame`
using different dtype representations
(storing string as `string`,
storing string as `object`,
using `pyarrow` dtypes).

| method                                          |   string |   object |   pyarrow |
|-------------------------------------------------|----------|----------|-----------|
| Dependencies.\_\_call__()                       |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_contains__(10000 files)        |    0.005 |    0.004 |     0.004 |
| Dependencies.\_\_get_item__(10000 files)        |    0.317 |    0.224 |     0.909 |
| Dependencies.\_\_len__()                        |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_str__()                        |    0.006 |    0.005 |     0.006 |
| Dependencies.archives                           |    0.143 |    0.115 |     0.148 |
| Dependencies.attachments                        |    0.029 |    0.018 |     0.017 |
| Dependencies.attachment_ids                     |    0.029 |    0.018 |     0.017 |
| Dependencies.files                              |    0.031 |    0.011 |     0.045 |
| Dependencies.media                              |    0.135 |    0.072 |     0.091 |
| Dependencies.removed_media                      |    0.123 |    0.068 |     0.086 |
| Dependencies.table_ids                          |    0.036 |    0.026 |     0.026 |
| Dependencies.tables                             |    0.029 |    0.017 |     0.017 |
| Dependencies.archive(10000 files)               |    0.046 |    0.045 |     0.066 |
| Dependencies.bit_depth(10000 files)             |    0.024 |    0.024 |     0.045 |
| Dependencies.channels(10000 files)              |    0.024 |    0.024 |     0.043 |
| Dependencies.checksum(10000 files)              |    0.026 |    0.023 |     0.047 |
| Dependencies.duration(10000 files)              |    0.023 |    0.024 |     0.044 |
| Dependencies.format(10000 files)                |    0.026 |    0.024 |     0.049 |
| Dependencies.removed(10000 files)               |    0.024 |    0.023 |     0.045 |
| Dependencies.sampling_rate(10000 files)         |    0.024 |    0.023 |     0.044 |
| Dependencies.type(10000 files)                  |    0.024 |    0.023 |     0.045 |
| Dependencies.version(10000 files)               |    0.026 |    0.023 |     0.049 |
| Dependencies._add_attachment()                  |    0.060 |    0.063 |     0.231 |
| Dependencies._add_media(10000 files)            |    0.059 |    0.064 |     0.080 |
| Dependencies._add_meta()                        |    0.050 |    0.068 |     0.066 |
| Dependencies._drop()                            |    0.077 |    0.085 |     0.138 |
| Dependencies._remove()                          |    0.063 |    0.066 |     0.069 |
| Dependencies._update_media()                    |    0.089 |    0.087 |     0.172 |
| Dependencies._update_media_version(10000 files) |    0.011 |    0.011 |     0.025 |


## audb.Dependencies loading/writing to file

Benchmarks how fast a dependency table
can be written to disk
or read from disk
using different file formats
(csv, pickle, parquet)
and different internal representations
(`pandas.DataFrame` with different dtypes,
and `pyarrow.Table`).

To run the benchmark execute:

```bash
$ python benchmark-dependencies-save-and-load.py
```

**Writing**

Execution times in seconds
for a dependency table
containing 1,000,000 files.

| method                                                    |   csv |   pickle |   parquet |
|-----------------------------------------------------------|-------|----------|-----------|
| pd.DataFrame[object]                                      | 2.022 |    0.641 |     0.469 |
| pd.DataFrame[string]                                      | 2.032 |    0.660 |     0.597 |
| pd.DataFrame[pyarrow]                                     | 2.031 |    0.222 |     0.928 |
| pd.DataFrame[object] -> pd.DataFrame[pyarrow]             | 2.248 |    0.446 |           |
| pd.DataFrame[object] -> pa.Table                          | 0.515 |          |           |
| pd.DataFrame[string] -> pa.Table                          | 0.652 |          |           |
| pd.DataFrame[pyarrow] -> pa.Table                         | 0.234 |          |           |
| pd.DataFrame[object] -> pd.DataFrame[pyarrow] -> pa.Table | 0.481 |          |           |
| pa.Table                                                  | 0.252 |          |     0.236 |

**Reading**

Execution times in seconds
for a dependency table
containing 1,000,000 files.
-c\-\-> stands for using the `c` engine
and -pa-> for using the `pyarrow` engine
with `pandas.read_csv()`.

| method                                                              |   csv |   pickle |   parquet |
|---------------------------------------------------------------------|-------|----------|-----------|
| \-\-\-\-> pd.DataFrame[object]                                      | 1.115 |    0.232 |     0.351 |
| \-\-\-\-> pd.DataFrame[string]                                      | 1.126 |    0.212 |     0.325 |
| \-\-\-\-> pd.DataFrame[pyarrow]                                     | 2.611 |    0.027 |     0.288 |
| \-\-\-\-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]             | 2.759 |    0.262 |           |
| -c\--> pd.DataFrame[object]                                         | 1.144 |          |           |
| -c\--> pd.DataFrame[string]                                         | 1.097 |          |           |
| -c\--> pd.DataFrame[pyarrow]                                        | 2.578 |          |           |
| -c\--> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                | 2.763 |          |           |
| -pa-> pd.DataFrame[object]                                          | 0.448 |          |           |
| -pa-> pd.DataFrame[string]                                          | 0.398 |          |           |
| -pa-> pd.DataFrame[pyarrow]                                         | 0.557 |          |           |
| -pa-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                 | 0.723 |          |           |
| \-\-\-\-> pa.Table -> pd.DataFrame[object]                          | 0.315 |          |     0.215 |
| \-\-\-\-> pa.Table -> pd.DataFrame[string]                          | 0.316 |          |     0.314 |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow]                         | 0.109 |          |     0.069 |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow] -> pd.DataFrame[object] | 0.335 |          |           |
| \-\-\-\-> pa.Table                                                  | 0.049 |          |     0.051 |


### File sizes

Storing a dependency table with 1,000,000 entries resulted in:

* 102 MB for csv
* 131 MB for pickle
* 20 MB for parquet

When zipped all files can be further reduced by 50%.


### Memory consumption

Besides the execution time,
memory consumption might also be considered.
We use [memray](https://github.com/bloomberg/memray) v1.11.0,
to measure it.
As the evaluation of the results cannot be easily automated,
the investigation was done manually
by creating single Python scripts
containing code for the desired operations,
running `memray`
and inspecting the results.

**Writing**

When writing to files
there is no memory overhead
when converting a `pandas.DataFrame`
first to `pyarrow.Table`.
Hence, we don't have to compare results.

**Reading**

Peak memory consumption when reading a dependency table containing 1,000,000 files.

| method                                      |     csv | pickle | parquet |
| ------------------------------------------- | ------- | ------ | ------- |
| \-\-\-\-> pd.DataFrame[object]              |  391 MB | 275 MB |  754 MB |
| \-\-\-\-> pd.DataFrame[string]              |  356 MB | 275 MB |  874 MB |
| \-\-\-\-> pd.DataFrame[pyarrow]             |  696 MB | 161 MB |  903 MB |
| \-c--> pd.DataFrame[object]                 |  390 MB |        |         |
| \-c--> pd.DataFrame[string]                 |  356 MB |        |         |
| \-c--> pd.DataFrame[pyarrow]                |  696 MB |        |         |
| \-pa-> pd.DataFrame[object]                 | 1295 MB |        |         |
| \-pa-> pd.DataFrame[string]                 | 1333 MB |        |         |
| \-pa-> pd.DataFrame[pyarrow]                | 1420 MB |        |         |
| \-\-\-\-> pa.Table                          |  530 MB |        |  381 MB |
| \-\-\-\-> pa.Table -> pd.DataFrame[object]  |  994 MB |        |  897 MB |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow] |  541 MB |        |  437 MB |
