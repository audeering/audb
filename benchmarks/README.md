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

| method                                         |   string |   object |   pyarrow |
|------------------------------------------------|----------|----------|-----------|
| Dependencies.\_\_call__()                      |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_contains__()                  |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_get_item__()                  |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_len__()                       |    0.000 |    0.000 |     0.000 |
| Dependencies.\_\_str__()                       |    0.006 |    0.005 |     0.007 |
| Dependencies.archives                          |    0.141 |    0.116 |     0.144 |
| Dependencies.attachments                       |    0.029 |    0.018 |     0.017 |
| Dependencies.attachment_ids                    |    0.029 |    0.018 |     0.017 |
| Dependencies.files                             |    0.030 |    0.012 |     0.043 |
| Dependencies.media                             |    0.127 |    0.072 |     0.086 |
| Dependencies.removed_media                     |    0.117 |    0.069 |     0.081 |
| Dependencies.table_ids                         |    0.037 |    0.026 |     0.023 |
| Dependencies.tables                            |    0.028 |    0.017 |     0.017 |
| Dependencies.archive(1000 files)               |    0.005 |    0.005 |     0.007 |
| Dependencies.bit_depth(1000 files)             |    0.004 |    0.004 |     0.006 |
| Dependencies.channels(1000 files)              |    0.004 |    0.004 |     0.006 |
| Dependencies.checksum(1000 files)              |    0.004 |    0.004 |     0.006 |
| Dependencies.duration(1000 files)              |    0.004 |    0.004 |     0.006 |
| Dependencies.format(1000 files)                |    0.004 |    0.004 |     0.006 |
| Dependencies.removed(1000 files)               |    0.004 |    0.004 |     0.006 |
| Dependencies.sampling_rate(1000 files)         |    0.004 |    0.004 |     0.006 |
| Dependencies.type(1000 files)                  |    0.004 |    0.004 |     0.006 |
| Dependencies.version(1000 files)               |    0.004 |    0.004 |     0.006 |
| Dependencies._add_attachment()                 |    0.055 |    0.056 |     0.207 |
| Dependencies._add_media(1000 files)            |    0.049 |    0.050 |     0.060 |
| Dependencies._add_meta()                       |    0.120 |    0.128 |     0.138 |
| Dependencies._drop()                           |    0.075 |    0.075 |     0.117 |
| Dependencies._remove()                         |    0.068 |    0.068 |     0.064 |
| Dependencies._update_media()                   |    0.071 |    0.072 |     0.125 |
| Dependencies._update_media_version(1000 files) |    0.008 |    0.008 |     0.017 |


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

Reading times in seconds
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

Writing times in seconds
for a dependency table
containing 1,000,000 files.

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
