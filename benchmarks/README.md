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
using `pyarrow` dtypes)
as of commit 91528e4.

| method                                          |   string |   object |   pyarrow |
|-------------------------------------------------|----------|----------|-----------|
| Dependencies.__call__()                         |    0.000 |    0.000 |     0.000 |
| Dependencies.__contains__(10000 files)          |    0.005 |    0.005 |     0.004 |
| Dependencies.__get_item__(10000 files)          |    0.311 |    0.223 |     0.907 |
| Dependencies.__len__()                          |    0.000 |    0.000 |     0.000 |
| Dependencies.__str__()                          |    0.006 |    0.005 |     0.006 |
| Dependencies.archives                           |    0.145 |    0.112 |     0.144 |
| Dependencies.attachments                        |    0.029 |    0.018 |     0.017 |
| Dependencies.attachment_ids                     |    0.028 |    0.018 |     0.016 |
| Dependencies.files                              |    0.031 |    0.011 |     0.042 |
| Dependencies.media                              |    0.132 |    0.072 |     0.088 |
| Dependencies.removed_media                      |    0.118 |    0.063 |     0.081 |
| Dependencies.table_ids                          |    0.035 |    0.025 |     0.022 |
| Dependencies.tables                             |    0.028 |    0.017 |     0.016 |
| Dependencies.archive(10000 files)               |    0.046 |    0.043 |     0.064 |
| Dependencies.bit_depth(10000 files)             |    0.042 |    0.042 |     0.060 |
| Dependencies.channels(10000 files)              |    0.041 |    0.042 |     0.060 |
| Dependencies.checksum(10000 files)              |    0.043 |    0.041 |     0.064 |
| Dependencies.duration(10000 files)              |    0.042 |    0.042 |     0.059 |
| Dependencies.format(10000 files)                |    0.044 |    0.042 |     0.064 |
| Dependencies.removed(10000 files)               |    0.041 |    0.042 |     0.059 |
| Dependencies.sampling_rate(10000 files)         |    0.043 |    0.043 |     0.061 |
| Dependencies.type(10000 files)                  |    0.043 |    0.042 |     0.060 |
| Dependencies.version(10000 files)               |    0.044 |    0.041 |     0.066 |
| Dependencies._add_attachment()                  |    0.068 |    0.057 |     0.222 |
| Dependencies._add_media(10000 files)            |    0.057 |    0.057 |     0.068 |
| Dependencies._add_meta()                        |    0.121 |    0.138 |     0.148 |
| Dependencies._drop()                            |    0.077 |    0.076 |     0.117 |
| Dependencies._remove()                          |    0.061 |    0.065 |     0.066 |
| Dependencies._update_media()                    |    0.087 |    0.087 |     0.149 |
| Dependencies._update_media_version(10000 files) |    0.011 |    0.011 |     0.026 |


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
