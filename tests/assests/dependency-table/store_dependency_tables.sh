#!/bin/bash
#
# This stores dependency tables of emodb
# for different versions of audb
# to test backward compatibility
for python_version in 3.8 3.9 3.10 3.11; do
    for audb_version in 1.0.4 1.1.9 1.2.6 1.3.0 1.4.2 1.5.2; do
        rm -rf venv
        virtualenv -p "python${python_version}" venv
        source venv/bin/activate
        pip install "audb==${audb_version}"
        pip install "audbackend<1.0.0"
        python store_dependency_table.py db-python-${python_version}-audb-${audb_version}.pkl
        deactivate
    done
    for audb_version in 1.6.5; do
        rm -rf venv
        virtualenv -p "python${python_version}" venv
        source venv/bin/activate
        pip install "audb==${audb_version}"
        pip install "audbackend<2.0.0"
        python store_dependency_table.py db-python-${python_version}-audb-${audb_version}.pkl
        deactivate
    done
done
