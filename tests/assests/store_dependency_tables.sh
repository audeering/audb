#!/bin/bash
#
# This stores dependency tables of emodb
# for different versions of audb
# to test backward compatibility
for version in 1.0.4 1.1.9 1.2.6 1.3.0 1.4.2 1.5.2; do
    rm -rf venv
    virtualenv venv
    source venv/bin/activate
    pip install "audb==${version}"
    pip install "audbackend<1.0.0"
    python store_dependency_table.py emodb-audb-${version}.pkl
    deactivate
done
for version in 1.6.5; do
    rm -rf venv
    virtualenv venv
    source venv/bin/activate
    pip install "audb==${version}"
    pip install "audbackend<2.0.0"
    python store_dependency_table.py emodb-audb-${version}.pkl
    deactivate
done
