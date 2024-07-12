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
| Dependencies.\_\_get_item__(10000 files)        |    0.322 |    0.224 |     0.900 |
| Dependencies.\_\_len__()                        |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_str__()                        |    0.006 |    0.005 |     0.006 |
| Dependencies.archives                           |    0.144 |    0.116 |     0.152 |
| Dependencies.attachments                        |    0.030 |    0.018 |     0.018 |
| Dependencies.attachment_ids                     |    0.029 |    0.018 |     0.018 |
| Dependencies.files                              |    0.030 |    0.011 |     0.046 |
| Dependencies.media                              |    0.129 |    0.073 |     0.095 |
| Dependencies.removed_media                      |    0.117 |    0.070 |     0.087 |
| Dependencies.table_ids                          |    0.037 |    0.026 |     0.023 |
| Dependencies.tables                             |    0.029 |    0.017 |     0.017 |
| Dependencies.archive(10000 files)               |    0.045 |    0.042 |     0.065 |
| Dependencies.bit_depth(10000 files)             |    0.024 |    0.024 |     0.045 |
| Dependencies.channels(10000 files)              |    0.023 |    0.023 |     0.045 |
| Dependencies.checksum(10000 files)              |    0.026 |    0.023 |     0.047 |
| Dependencies.duration(10000 files)              |    0.023 |    0.023 |     0.043 |
| Dependencies.format(10000 files)                |    0.026 |    0.023 |     0.047 |
| Dependencies.removed(10000 files)               |    0.023 |    0.023 |     0.043 |
| Dependencies.sampling_rate(10000 files)         |    0.023 |    0.023 |     0.043 |
| Dependencies.type(10000 files)                  |    0.023 |    0.023 |     0.043 |
| Dependencies.version(10000 files)               |    0.026 |    0.023 |     0.047 |
| Dependencies._add_attachment()                  |    0.055 |    0.062 |     0.220 |
| Dependencies._add_media(10000 files)            |    0.057 |    0.057 |     0.066 |
| Dependencies._add_meta()                        |    0.117 |    0.129 |     0.145 |
| Dependencies._drop()                            |    0.075 |    0.078 |     0.121 |
| Dependencies._remove()                          |    0.061 |    0.069 |     0.064 |
| Dependencies._update_media()                    |    0.087 |    0.086 |     0.145 |
| Dependencies._update_media_version(10000 files) |    0.011 |    0.011 |     0.020 |

## audb.Dependencies methods using polars

Handling of the dependency table with `pandas`
was further compared to handling it with `polars`,
by reimplementing all methods of `audb.Dependencies` using `polars`.

This benchmark was executed on:

* CPU: 12th Gen Intel Core i7-1255U
* RAM: 15.66 GB
* Hard Drive: KBG5AZNT1T02 LA KIOXIA
* Linux: Ubuntu 22.04.4 LTS
* Python 3.11.9

To run the benchmark execute:

```bash
$ python benchmark-dependencies-methods-polars.py
```

The data were compared to
the results from `benchmark-dependencies-methods.py` using

```bash
python compare_dependency_methods_polars.py
```
Both steps require that `benchmark-dependencies-methods.py`
has been run previously in order to create the test data
and results. The comparison in the `pandas` column is
based on the pyarrow column in the tabulation in the previous
section.


| method                                          |   pandas |   polars | winner   |   factor |
|-------------------------------------------------|----------|----------|----------|----------|
| Dependencies.\_\_call__()                         |    0.000 |    0.000 | polars   |    2.667 |
| Dependencies.\_\_contains__(10000 files)          |    0.003 |    0.002 | polars   |    2.005 |
| Dependencies.\_\_get_item__(10000 files)          |    0.648 |    0.013 | polars   |   50.382 |
| Dependencies.\_\_len__()                          |    0.000 |    0.000 | pandas   |    1.300 |
| Dependencies.\_\_str__()                          |    0.004 |    0.000 | polars   |   24.677 |
| Dependencies._add_attachment()                  |    0.171 |    0.104 | polars   |    1.645 |
| Dependencies._add_media(10000 files)            |    0.073 |    0.008 | polars   |    9.589 |
| Dependencies._add_meta()                        |    0.127 |    0.100 | polars   |    1.260 |
| Dependencies._drop()                            |    0.118 |    0.021 | polars   |    5.628 |
| Dependencies._remove()                          |    0.067 |    0.002 | polars   |   39.324 |
| Dependencies._update_media()                    |    0.142 |    0.066 | polars   |    2.148 |
| Dependencies._update_media_version(10000 files) |    0.021 |    0.016 | polars   |    1.341 |
| Dependencies.archive(10000 files)               |    0.045 |    0.014 | polars   |    3.250 |
| Dependencies.archives                           |    0.145 |    0.151 | pandas   |    1.045 |
| Dependencies.attachment_ids                     |    0.018 |    0.008 | polars   |    2.375 |
| Dependencies.attachments                        |    0.017 |    0.008 | polars   |    2.194 |
| Dependencies.bit_depth(10000 files)             |    0.029 |    0.014 | polars   |    2.031 |
| Dependencies.channels(10000 files)              |    0.030 |    0.013 | polars   |    2.224 |
| Dependencies.checksum(10000 files)              |    0.030 |    0.014 | polars   |    2.201 |
| Dependencies.duration(10000 files)              |    0.028 |    0.014 | polars   |    2.066 |
| Dependencies.files                              |    0.012 |    0.011 | polars   |    1.040 |
| Dependencies.format(10000 files)                |    0.033 |    0.014 | polars   |    2.345 |
| Dependencies.media                              |    0.068 |    0.040 | polars   |    1.702 |
| Dependencies.removed(10000 files)               |    0.029 |    0.014 | polars   |    2.118 |
| Dependencies.removed_media                      |    0.068 |    0.038 | polars   |    1.809 |
| Dependencies.sampling_rate(10000 files)         |    0.029 |    0.014 | polars   |    2.102 |
| Dependencies.table_ids                          |    0.025 |    0.013 | polars   |    1.927 |
| Dependencies.tables                             |    0.017 |    0.008 | polars   |    2.166 |
| Dependencies.type(10000 files)                  |    0.028 |    0.014 | polars   |    2.063 |
| Dependencies.version(10000 files)               |    0.032 |    0.013 | polars   |    2.372 |


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
