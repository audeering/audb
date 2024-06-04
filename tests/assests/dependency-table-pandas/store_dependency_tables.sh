#!/bin/bash
#
# This stores dependency tables of emodb
# as pickle files
# for different versions of pandas
# to test compatibility
audb_version="1.7.2"
python_version="3.10"
for pandas_version in 2.0.3 2.1.4 2.2.2; do
    rm -rf venv
    virtualenv -p "python${python_version}" venv
    source venv/bin/activate
    pip install "audb==${audb_version}"
    pip install "pandas==${pandas_version}"
    python store_dependency_table.py ${pandas_version}
    deactivate
done
