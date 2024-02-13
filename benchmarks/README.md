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
| Dependencies.\_\_contains__(10000 files)        |    0.004 |    0.005 |     0.004 |
| Dependencies.\_\_get_item__(10000 files)        |    0.319 |    0.225 |     0.871 |
| Dependencies.\_\_len__()                        |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_str__()                        |    0.005 |    0.005 |     0.006 |
| Dependencies.archives                           |    0.142 |    0.112 |     0.140 |
| Dependencies.attachments                        |    0.024 |    0.018 |     0.017 |
| Dependencies.attachment_ids                     |    0.027 |    0.018 |     0.019 |
| Dependencies.files                              |    0.011 |    0.011 |     0.010 |
| Dependencies.media                              |    0.105 |    0.070 |     0.064 |
| Dependencies.removed_media                      |    0.102 |    0.062 |     0.063 |
| Dependencies.table_ids                          |    0.032 |    0.026 |     0.023 |
| Dependencies.tables                             |    0.024 |    0.017 |     0.017 |
| Dependencies.archive(10000 files)               |    0.028 |    0.026 |     0.050 |
| Dependencies.archive([10000 files])             |    0.008 |    0.008 |     0.008 |
| Dependencies.bit_depth(10000 files)             |    0.024 |    0.024 |     0.044 |
| Dependencies.bit_depth([10000 files])           |    0.002 |    0.002 |     0.004 |
| Dependencies.channels(10000 files)              |    0.024 |    0.024 |     0.045 |
| Dependencies.channels([10000 files])            |    0.002 |    0.002 |     0.005 |
| Dependencies.checksum(10000 files)              |    0.025 |    0.024 |     0.048 |
| Dependencies.checksum([10000 files])            |    0.002 |    0.002 |     0.002 |
| Dependencies.duration(10000 files)              |    0.024 |    0.025 |     0.044 |
| Dependencies.duration([10000 files])            |    0.003 |    0.003 |     0.004 |
| Dependencies.format(10000 files)                |    0.026 |    0.023 |     0.049 |
| Dependencies.format([10000 files])              |    0.002 |    0.002 |     0.002 |
| Dependencies.removed(10000 files)               |    0.024 |    0.024 |     0.044 |
| Dependencies.removed([10000 files])             |    0.002 |    0.002 |     0.004 |
| Dependencies.sampling_rate(10000 files)         |    0.024 |    0.024 |     0.046 |
| Dependencies.sampling_rate([10000 files])       |    0.002 |    0.002 |     0.004 |
| Dependencies.type(10000 files)                  |    0.024 |    0.024 |     0.044 |
| Dependencies.type([10000 files])                |    0.002 |    0.002 |     0.004 |
| Dependencies.version(10000 files)               |    0.026 |    0.024 |     0.049 |
| Dependencies.version([10000 files])             |    0.002 |    0.002 |     0.002 |
| Dependencies._add_attachment()                  |    0.055 |    0.055 |     0.178 |
| Dependencies._add_media(10000 files)            |    0.055 |    0.055 |     0.066 |
| Dependencies._add_meta()                        |    0.120 |    0.124 |     0.122 |
| Dependencies._drop()                            |    0.076 |    0.076 |     0.118 |
| Dependencies._remove()                          |    0.068 |    0.069 |     0.065 |
| Dependencies._update_media()                    |    0.081 |    0.079 |     0.138 |
| Dependencies._update_media_version(10000 files) |    0.012 |    0.011 |     0.021 |


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
| pd.DataFrame[object]                                      | 2.002 |    0.638 |     0.485 |
| pd.DataFrame[string]                                      | 2.026 |    0.649 |     0.505 |
| pd.DataFrame[pyarrow]                                     | 2.023 |    0.294 |     0.273 |
| pd.DataFrame[object] -> pd.DataFrame[pyarrow]             | 2.187 |    0.498 |           |
| pd.DataFrame[object] -> pa.Table                          | 0.509 |          |           |
| pd.DataFrame[string] -> pa.Table                          | 0.576 |          |           |
| pd.DataFrame[pyarrow] -> pa.Table                         | 0.277 |          |           |
| pd.DataFrame[object] -> pd.DataFrame[pyarrow] -> pa.Table | 0.459 |          |           |
| pa.Table                                                  | 0.249 |          |     0.239 |

**Reading**

Execution times in seconds
for a dependency table
containing 1,000,000 files.
-c\-\-> stands for using the `c` engine
and -pa-> for using the `pyarrow` engine
with `pandas.read_csv()`.

| method                                                              |   csv |   pickle |   parquet |
|---------------------------------------------------------------------|-------|----------|-----------|
| \-\-\-\-> pd.DataFrame[object]                                      | 1.110 |    0.240 |     0.376 |
| \-\-\-\-> pd.DataFrame[string]                                      | 1.158 |    0.255 |     0.399 |
| \-\-\-\-> pd.DataFrame[pyarrow]                                     | 2.493 |    0.092 |     0.408 |
| \-\-\-\-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]             | 2.673 |    0.289 |           |
| -c\--> pd.DataFrame[object]                                         | 1.154 |          |           |
| -c\--> pd.DataFrame[string]                                         | 1.101 |          |           |
| -c\--> pd.DataFrame[pyarrow]                                        | 2.472 |          |           |
| -c\--> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                | 2.655 |          |           |
| -pa-> pd.DataFrame[object]                                          | 0.460 |          |           |
| -pa-> pd.DataFrame[string]                                          | 0.436 |          |           |
| -pa-> pd.DataFrame[pyarrow]                                         | 0.575 |          |           |
| -pa-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                 | 0.791 |          |           |
| \-\-\-\-> pa.Table -> pd.DataFrame[object]                          | 0.271 |          |     0.225 |
| \-\-\-\-> pa.Table -> pd.DataFrame[string]                          | 0.308 |          |     0.318 |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow]                         | 0.113 |          |     0.085 |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow] -> pd.DataFrame[object] | 0.384 |          |           |
| \-\-\-\-> pa.Table                                                  | 0.050 |          |     0.065 |


### File sizes

Storing a dependency table with 1,000,000 entries resulted in:

* 102 MB for csv
* 131 MB for pickle
* 20 MB for parquet

When zipped all files can be further reduced by 50%.


### Memory consumption

Besides the execution time,
memory consumption is also measured
when running the benchmark script.
We use [memray](https://github.com/bloomberg/memray) v1.11.0,
to measure it.
It stores its evaluation results in binary files
in the folder ``results``.
Those files are then manually evaluated
using ``memray tree <file>``.

**Writing**

When writing to files
there is no memory overhead
when converting a `pandas.DataFrame`
first to `pyarrow.Table`.
Hence, we don't have to compare results.

**Reading**

Peak memory consumption when reading a dependency table containing 1,000,000 files.

| method                                                              |     csv |   pickle |   parquet |
|---------------------------------------------------------------------|---------|----------|-----------|
| \-\-\-\-> pd.DataFrame[object]                                      |  357 MB |   291 MB |    354 MB |
| \-\-\-\-> pd.DataFrame[string]                                      |  323 MB |   334 MB |    465 MB |
| \-\-\-\-> pd.DataFrame[pyarrow]                                     |  495 MB |   240 MB |    400 MB |
| \-\-\-\-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]             |  502 MB |   340 MB |           |
| -c\--> pd.DataFrame[object]                                         |  359 MB |          |           |
| -c\--> pd.DataFrame[string]                                         |  277 MB |          |           |
| -c\--> pd.DataFrame[pyarrow]                                        |  501 MB |          |           |
| -c\--> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                |  459 MB |          |           |
| -pa-> pd.DataFrame[object]                                          | 1029 MB |          |           |
| -pa-> pd.DataFrame[string]                                          |  353 MB |          |           |
| -pa-> pd.DataFrame[pyarrow]                                         |  396 MB |          |           |
| -pa-> pd.DataFrame[pyarrow] -> pd.DataFrame[object]                 |  528 MB |          |           |
| \-\-\-\-> pa.Table -> pd.DataFrame[object]                          |  443 MB |          |    360 MB |
| \-\-\-\-> pa.Table -> pd.DataFrame[string]                          |  325 MB |          |    305 MB |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow]                         |   29 MB |          |     15 MB |
| \-\-\-\-> pa.Table -> pd.DataFrame[pyarrow] -> pd.DataFrame[object] |  418 MB |          |           |
| \-\-\-\-> pa.Table                                                  |   16 MB |          |      1 MB |
