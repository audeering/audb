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
| Dependencies.__call__()                        |    0.000 |    0.000 |     0.000 |
| Dependencies.__contains__()                    |    0.000 |    0.000 |     0.000 |
| Dependencies.__get_item__()                    |    0.000 |    0.000 |     0.001 |
| Dependencies.__len__()                         |    0.000 |    0.000 |     0.000 |
| Dependencies.__str__()                         |    0.012 |    0.010 |     0.013 |
| Dependencies.archives                          |    0.278 |    0.223 |     0.284 |
| Dependencies.attachments                       |    0.053 |    0.029 |     0.034 |
| Dependencies.attachment_ids                    |    0.052 |    0.029 |     0.033 |
| Dependencies.files                             |    0.056 |    0.026 |     0.084 |
| Dependencies.media                             |    0.273 |    0.156 |     0.166 |
| Dependencies.removed_media                     |    0.246 |    0.140 |     0.154 |
| Dependencies.table_ids                         |    0.070 |    0.045 |     0.044 |
| Dependencies.tables                            |    0.053 |    0.029 |     0.034 |
| Dependencies.archive(1000 files)               |    0.014 |    0.013 |     0.025 |
| Dependencies.bit_depth(1000 files)             |    0.013 |    0.014 |     0.022 |
| Dependencies.channels(1000 files)              |    0.013 |    0.014 |     0.022 |
| Dependencies.checksum(1000 files)              |    0.014 |    0.013 |     0.025 |
| Dependencies.duration(1000 files)              |    0.014 |    0.013 |     0.022 |
| Dependencies.format(1000 files)                |    0.014 |    0.013 |     0.024 |
| Dependencies.removed(1000 files)               |    0.013 |    0.013 |     0.022 |
| Dependencies.sampling_rate(1000 files)         |    0.014 |    0.014 |     0.022 |
| Dependencies.type(1000 files)                  |    0.013 |    0.013 |     0.021 |
| Dependencies.version(1000 files)               |    0.014 |    0.013 |     0.025 |
| Dependencies._add_attachment()                 |    0.114 |    0.112 |     0.387 |
| Dependencies._add_media(1000 files)            |    0.114 |    0.111 |     0.147 |
| Dependencies._add_meta()                       |    0.224 |    0.238 |     0.283 |
| Dependencies._drop()                           |    0.166 |    0.166 |     0.243 |
| Dependencies._remove()                         |    0.109 |    0.114 |     0.113 |
| Dependencies._update_media()                   |    0.156 |    0.155 |     0.276 |
| Dependencies._update_media_version(1000 files) |    0.010 |    0.010 |     0.044 |
