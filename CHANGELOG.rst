Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_,
and this project adheres to `Semantic Versioning`_.


Version 1.1.3 (2021-05-18)
--------------------------

* Added: discussion of needed system packages for handling audio files
  in the documentation
* Changed: allow only to publish portable databases
* Fixed: macOS support by relying on new ``audresample`` version


Version 1.1.2 (2021-05-06)
--------------------------

* Added: ``audb.load_media()``
* Added: ``audb.load_table()``
* Added: documentation on how to configure access rights
  for shared cache folder
* Changed: speedup ``audb.Dependencies`` methods
* Changed: speedup ``audb.info`` functions
* Changed: ``audb.info`` uses cache as well
* Changed: use emodb 1.1.1 in documentation
* Changed: depend on ``audformat>=0.11.0``
* Fixed: allow ``audb.load()`` to work offline if database is cached


Version 1.1.1 (2021-04-30)
--------------------------

* Fixed: update removal version of deprecated stuff to 1.2.0


Version 1.1.0 (2021-04-29)
--------------------------

* Added: ``audb.Dependencies._remove()``
* Changed: ``audb.Dependencies`` internally uses ``pd.DataFrame`` instead of ``dict``
* Changed: store dependencies with pickle to speed up loading
* Changed: versions of the same flavor share dependency file
* Changed: if possible ``audb.load()`` copies tables and media files from other versions in the cache
* Changed: ``audb.Dependencies._add_media()`` is now private
* Changed: ``audb.Dependencies._add_meta()`` is now private
* Changed: ``audb.Dependencies.is_removed`` renamed to ``audb.Dependencies.removed``
* Fixed: ``audb.load()`` considers format when searching the cache
* Fixed: ``audb.load()`` considers format when resolving missing media
* Fixed: ``audb.available()`` correctly returns versions of the same database from multiple repositories
* Fixed: add missing link to ``emodb`` example repository
* Removed: ``audb.Dependencies.data``


Version 1.0.4 (2021-04-09)
--------------------------

* Changed: ``audb.Dependencies.bit_depth()`` now always returns an integer
* Changed: ``audb.Dependencies.channels()`` now always returns an integer
* Changed: ``audb.Dependencies.duration()`` now always returns a float
* Changed: ``audb.Dependencies.sampling_rate()`` now always returns an integer
* Fixed: ``audb.info.duration()`` for databases that contain files with a
  duration of 0s
* Fixed: remove dependency to ``fire`` package


Version 1.0.3 (2021-04-08)
--------------------------

* Fixed: docstring of ``audb.exists()`` falsely claimed that it was not
  returning a boolean
* Fixed: several typos in documentation


Version 1.0.2 (2021-04-07)
--------------------------

* Fixed: renamed ``latest_only`` argument of ``audb.available()``
  to ``only_latest`` as it was before


Version 1.0.1 (2021-04-07)
--------------------------

* Fixed: appearance of documentation TOC by requirering ``docutils<0.17``


Version 1.0.0 (2021-04-07)
--------------------------

* Added: first public release
* Added: ``audb.info.author()``
* Added: ``audb.info.license()``
* Added: ``audb.info.license_url()``
* Added: ``audb.info.organization()``
* Added: ``audb.Dependencies.archives`` property
* Added: section on publication in the documentation
* Added: introduction texts to documentation
* Changed: raise error for conversion of non-supported format
* Changed: ``audb.exists()`` to return bool
* Changed: rename ``audb.lookup_repository()`` to ``audb.repository()``
* Changed: one combined section on load in the documentation
* Fixed: data types in dataframe returned by ``audb.cached()``
* Fixed: support files stored in archives with nested folders
* Fixed: listing of cache entries
* Removed: command line interface
* Removed: ``audb.cached_databases()``
* Removed: ``audb.define`` module


Version 0.93.0 (2021-03-29)
---------------------------

* Added: ``complete`` column in ``audb.cached()``
* Added: ``previous_version`` argument to ``audb.publish()``
* Added: backward compatibility with ``audb <0.90``
* Changed: cache flavor path to name/version/flavor_id
* Changed: use open source releases of ``audbackend``,
  ``audobject``,
  and ``audresample``
* Changed: require ``audformat>=0.10.0``
* Changed: rename ``audb.load_original_to()`` to ``audb.load_to()``
* Changed: shorten flavor ID in cache
* Changed: filter operations and ``only_metadata`` no longer part
  of ``audb.Flavor``
* Deprecated: ``include`` and ``excldue`` arguments
* Fixed: looking for latest version across repositories
* Fixed: ``Flavor.destination`` for nested paths
* Fixed: allow for cross-backend dependencies for ``audb.publish()``
* Fixed: ``audb.remove_media()`` can now be called several times


Version 0.92.1 (2021-03-19)
---------------------------

* Changed: enforce ``mixdown=False`` for mono file flavors
* Fixed: global config file was missing in PyPI package


Version 0.92.0 (2021-03-09)
---------------------------

* Added: configuration file
* Changed: use external package for backend implementations


Version 0.91.0 (2021-02-19)
---------------------------

* Added: ``audb.Backend.latest_version()``
* Added: ``audb.Backend.create()``
* Added: ``audb.Backend.register()``
* Added: ``audb.lookup_repository()``
* Added: ``config.REPOSITORY_PUBLISH``
* Fixed: update ``fire`` dependency
* Fixed: remove ``config.GROUP_ID``
* Fixed: use ``sphinx>=3.5.1`` to fix inherited attributes
  in documentation


Version 0.90.3 (2021-02-01)
---------------------------

* Changed: define data types when reading dependency file


Version 0.90.2 (2021-01-28)
---------------------------

* Added: ``data-provate-local`` to the default repositories


Version 0.90.1 (2021-01-25)
---------------------------

* Fixed: CHANGELOG


Version 0.90.0 (2021-01-22)
---------------------------

* Added: initial release


.. _Keep a Changelog:
    https://keepachangelog.com/en/1.0.0/
.. _Semantic Versioning:
    https://semver.org/spec/v2.0.0.html
