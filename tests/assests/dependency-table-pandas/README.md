# Dependency table pandas compatibility

Since version 1.7.0 of `audb`,
we use `pyarrow` dtypes
inside the dependency table
(`audb.Dependencies._df`).
The dependency table
is still stored in cache
as a pickle file.
When loading the pickle file
with a different `pandas` version,
than the one used to store the file,
an error related to the `pyarrow` dtypes
might be raised.

To test this,
we store an example dependency table
from the `emodb` dataset
as pickle file
using different `pandas` versions
as test assests.

The pickle files,
stored in this folder,
where created by running:

```bash
$ bash store_dependency_tables.sh
```
