Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_,
and this project adheres to `Semantic Versioning`_.


Version 1.11.4 (2025-07-22)
---------------------------

* Changed: restrict publishing of a new version of a database
  with dependencies to a previous version
  to the same repository
  as the previous version
* Changed: skip media files without audio
  when requesting a flavor,
  e.g. don't raise an error for a JSON file
* Fixed: ``audb.stream()`` for ``pyarrow>=21.0.0``


Version 1.11.3 (2025-02-20)
---------------------------

* Changed: reduce the timeout for acquiring a lock for a cache folder
  from infinity to 24h
* Changed: show a user warning
  if a lock for a cache folder cannot be acquired within 2s.
  The warning message suggests how to check
  whether the existing lock file needs to be deleted manually


Version 1.11.2 (2025-02-03)
---------------------------

* Added: support for Artifactory backend in Python 3.12
* Changed: depend on ``audbackend>=2.2.2``


Version 1.11.1 (2025-01-06)
---------------------------

* Added: ``repositories`` argument to ``audb.available()``
  to limit it to selected repositories
* Changed: ensure ``audb.Repository`` is hashable
* Fixed: default value for ``audb.config.REPOSITORIES``
  in documentation


Version 1.11.0 (2024-12-06)
---------------------------

* Added: a sampling rate of 24000 Hz as flavor
* Added: a sampling rate of 22050 Hz as flavor
* Added: support for Python 3.13
  (without Artifactory backend)
* Added: support for Python 3.12
  (without Artifactory backend)
* Changed: switch default repository
  to ``audb-public``,
  hosted on S3
* Changed: ``audb.Repository.create_backend_interface()``
  and ``audb.publish()``
  now raise a ``ValueError``
  for a repository with non-registered backends,
  or an Artifactory backend under Python>=3.12
* Changed: skip non-registered backends
  without raising an error
  in all functions with read-only access to repositories
* Changed: simplify quickstart section
  of the documentation
* Changed: depend on ``audbackend>=2.2.1``
* Changed: depend on ``audeer>=2.2.0``
* Deprecated: a sampling rate of 22500 Hz as flavor
* Removed: file-system repository from default configuration
* Fixed: handle an empty configuration file


Version 1.10.2 (2024-11-18)
---------------------------

* Fixed: remove extra ``"/"`` at end of dataset names
  in ``audb.available()`` for S3 and Minio backends


Version 1.10.1 (2024-11-18)
---------------------------

* Added: ``"s3"`` as a registered backend name
* Changed: depend on ``audbackend>=2.2.0``
* Changed: make Artifactory backend optional,
  to allow importing ``audb`` in Python>=3.12
* Fixed:  speedup ``audb.available()`` for S3 and Minio backends


Version 1.10.0 (2024-11-04)
---------------------------

* Added: support for repositories
  on S3 and MinIO servers,
  using the ``minio`` backend
  of ``audbackend``
* Changed: depend on ``audbackend>=2.1.0``


Version 1.9.0 (2024-08-22)
--------------------------

* Added: pseudo-streaming support with ``audb.stream()``,
  which returns the new ``audb.DatabaseIterator`` object.
  In each iteration
  it will load a few rows from a requested table
  and downloads corresponding media files
* Added: ``map`` argument to ``audb.load_table()``,
  which behaves identical to the ``map`` argument
  of ``audformat.Database.get()``
* Added: ``pickle_tables`` argument
  to ``audb.load()``,
  ``audb.load_to()``
  and ``audb.load_table()``
  with default value of ``True``.
  It can be used to disable
  storing tables as pickle files
  in cache/root folder
* Fixed: ``audb.load_table()`` now only loads additional misc tables,
  that are used as scheme labels inside the requested table,
  and not in the whole database


Version 1.8.0 (2024-07-10)
--------------------------

* Added: support for publishing tables as parquet files
* Changed: depend on ``audeer >=2.1.0``
* Changed: depend on ``audformat >=1.2.0``
* Changed: depend on ``pandas >=2.1.0``
* Fixed: update progress bar at least every second
  in ``audb.load()``,
  ``audb.load_attachment()``,
  ``audb.load_media()``,
  ``audb.load_tables()``,
  ``audb.load_to()``,
  ``audb.publish()``
* Removed: support for Python 3.8


Version 1.7.4 (2024-06-25)
--------------------------

* Fixed: ensure correct data types
  in dependency table
  when loaded from a version in cache,
  stored by ``audb<=1.6.3``


Version 1.7.3 (2024-06-04)
--------------------------

* Fixed: ensure correct data types
  in dependency table
  when loaded from cache
* Fixed: publishing an update of a database
  when the previous version
  was stored in cache
  by an older version of ``audb``
* Fixed: loading of database attachments
  when ``audb.config.CACHE_ROOT``
  and ``audb.config.SHARED_CACHE_ROOT``
  point to the same folder
* Fixed: ensure ``audb.versions()``
  does not fail
  when database is not available
  in a repository
* Fixed: loading of dependency table from cache
  when the previous version
  was stored in cache
  by a different ``pandas`` version


Version 1.7.2 (2024-05-16)
--------------------------

* Fixed: loading of dependency table from cache
  under Python 3.8,
  when stored by an older version of ``audb``


Version 1.7.1 (2024-05-14)
--------------------------

* Fixed: require ``pandas>=2.0.1``
  for ``pyarrow`` based data types


Version 1.7.0 (2024-05-10)
--------------------------

* Added: experimental support for text files
  as media files
* Added: dependency on ``pyarrow``
* Added: ``audb.Repository.backend_registry``
  that maps repository names like ``artifactory``
  to corresponding backend classes,
  e.g. ``audbackend.backend.Artifactory``
* Added: ``audb.Repository.register()``
  to add an entry to ``audb.Repository.backend_registry``
* Added: ``audb.Repository.create_backend_interface()``
  returns a backend interface
  to access files in the repository
* Changed: improve speed
  of loading dependency table to the cache.
  E.g. for version 1.0.0 of the database musan
  loading time is reduced by 35%
* Changed: improve speed
  of downloading a database to the cache.
  E.g. for version 1.0.0 of the database musan
  loading time is reduced by 40%
  when using 8 threads
* Changed: depend on ``audbackend>=2.0.0``
* Changed: dependency table dataframe
  returned by ``audb.Dependencies.__call__()``
  now uses ``pyarrow`` based data types
* Changed: dependency table
  is now stored as a PARQUET file
  on the backend,
  instead as a CSV file
* Fixed: ``audb.versions()``
  for non-existing repositories
* Fixed: documentation of ``audb.Repository.__eq__()``


Version 1.6.5 (2024-03-28)
--------------------------

* Added: ``audb.Dependencies.__eq__()``
  to compare two dependency tables
* Fixed: let ``audb.available()``
  skip incomplete datasets
  instead of raising an error


Version 1.6.4 (2024-02-23)
--------------------------

* Fixed: in ``audb.publish()``
  updating of multi-file archives
  that have been published
  before the version
  given by the ``previous_version`` argument
* Fixed: speed up most methods
  of ``audb.Dependencies``
* Fixed: dtype of the index
  of the data frame
  returned by ``audb.Dependencies.__call__()``
  is now ``string``
  instead of ``object``


Version 1.6.3 (2024-01-30)
--------------------------

* Fixed: ``audb.versions()``
  when ``audb.config.REPOSITORIES``
  includes non-existing Artifactory repositories
  or Artifactory repositories without read access


Version 1.6.2 (2024-01-25)
--------------------------

* Changed: depend on ``audeer>=2.0.0``
* Changed: speed up ``audb.versions()``
* Fixed: ``pandas`` deprecation warnings
* Fixed: make documentation independent
  of the number of public datasets


Version 1.6.1 (2023-11-16)
--------------------------

* Fixed: accessing a database in any repository
  listed after a repository with access restrictions
  or a non-existing repository
  in ``audb.config.REPOSITORIES``


Version 1.6.0 (2023-10-17)
--------------------------

* Added: support for new backend API
* Changed: depend on ``audbackend>=1.0.0``


Version 1.5.2 (2023-09-26)
--------------------------

* Added: BibTeX reference to README
* Fixed: link to Artifactory anonymous access
  in the documentation
* Fixed: enforce reproducible order
  of media files entries in dependency table
  during publication


Version 1.5.1 (2023-05-04)
--------------------------

* Changed: require ``audeer>=1.20.0``
* Fixed: ``audb.load()``,
  ``audb.load_to()``,
  ``audb.load_media()``,
  and ``audb.remove_media()``
  were failing with ``audeer==1.20.0``
  under Windows


Version 1.5.0 (2023-04-27)
--------------------------

* Added: support loading and publishing
  of database attachments
  (``audformat.Attachment``)
* Added: ``audb.load_attachment()``
  to load a single attachment of a database
* Added: ``audb.info.attachments()``
  to return the attachments entry
  of a database header
* Added: ``attachments`` argument to ``audb.load()``
  to load only specific
  attachments of a database
* Changed: raise ``RuntimeError`` in ``audb.publish()``
  if the file extension of a media file
  contains uppercase letters
* Changed: raise ``RuntimeError`` in ``audb.publish()``
  if a table ID or attachment ID
  contains a character not in ``[A-Za-z0-9._-]``
* Changed: raise ``ValueError`` in ``audb.publish()``
  if ``version`` or ``previous_version``
  are not conform to ``audeer.StrictVersion``
* Changed: use emodb v1.4.1 for documentation examples
* Changed: require ``audbackend<1.0.0``
  as ``audbackend`` will introduce breaking changes
* Fixed: speed up ``audb.load_to()``
  when loading databases with large tables
  using ``only_metadata=True``


Version 1.4.2 (2023-02-13)
--------------------------

* Added: support for Python 3.10
* Added: document optional needed overwrite permissions
  for ``audb.publish()``
  when continuing a canceled publishing command
* Changed: require ``audbackend>=0.3.17``
* Changed: split API documentation into sub-pages
  for each function


Version 1.4.1 (2022-10-17)
--------------------------

* Changed: ``audb.load()`` and ``audb.load_to()``
  extract archives in the corresponding database folder
  inside the ``audb`` cache
  instead of the system-wide temporary folder


Version 1.4.0 (2022-08-18)
--------------------------

* Added: support for ``audformat``'s newly introduced misc tables
* Added: ``audb.info.misc_tables()``
* Added: ``load_tables=True`` argument to
  ``audb.info.header()``
  and ``audb.info.schemes()``
  specifying if misc tables
  used as labels
  in a scheme
  should be downloaded
* Changed: require ``audformat >=0.15.2``
* Changed: use version 1.3.0 of emodb
  in the documentation examples
* Removed: support for Python 3.7


Version 1.3.0 (2022-07-14)
--------------------------

* Added: lock cache folder with a lock file
  when modifying it
* Added: ``verbose`` argument to ``audb.dependencies()``
* Added: ``audb.info.files()``
* Added: ``media`` and ``tables`` arguments
  to appropriate functions
  in ``audb.info`` sub-module
* Added: ``only_metadata`` argument to ``audb.load_to()``
* Added: ``audb.publish()`` raises ``ValueError``
  if ``previous_version``
  is not smaller than ``version``
* Changed: ``audb.publish()`` does not require unchanged media files
  to exists in database folder
* Changed: ``audb.load()`` raises ``ValueError``
  if a table or media file is requested
  that is not part of the database
* Fixed: add missing exceptions to docstrings


Version 1.2.6 (2022-04-01)
--------------------------

* Changed: use emodb v1.2.0 for examples and tests
* Changed: depend on ``audobject>=0.5.0``
* Changed: depend on ``audformat>=0.14.0``
* Changed: depend on ``audeer>=1.18.0``
* Fixed: depend on ``audbackend>=0.3.15``
  to avoid the possibility of an error
  when requesting versions of a database
* Fixed: add full Windows support and tests
* Fixed: only create tmp folder when needed in ``audb.load()``
* Removed: ``include``/``exclude`` keyword arguments
* Removed: ``audb.get_default_cache_root()``


Version 1.2.5 (2022-02-23)
--------------------------

* Fixed: make moving of local files Windows compatible
* Fixed: create folder tree more efficiently when loading to cache


Version 1.2.4 (2022-02-07)
--------------------------

* Changed: depend on ``audformat>=0.13.3``
* Fixed: conversion of pickle protocol 5 files to pickle protocol 4 in cache


Version 1.2.3 (2022-02-01)
--------------------------

* Added: more examples to the API docstrings
* Changed: depend on ``audformat>=0.13.2``
* Changed: use pickle protocol-4 for caching dependencies


Version 1.2.2 (2022-01-03)
--------------------------

* Fixed: small improvements to API documentation
* Fixed: speed up ``audb.load_to()`` storing of CSV files


Version 1.2.1 (2021-11-18)
--------------------------

* Fixed: build documentation inside the release process with Python 3.8


Version 1.2.0 (2021-11-18)
--------------------------

* Added: support for Python 3.9
* Added: store file duration of the database
  in the duration cache of ``audformat.Database``
* Changed: ``audb.publish()`` now raises an error
  if a table contains duplicated index entries
* Fixed: several speed ups when loading or publishing a database
* Fixed: the ``root`` attribute of the returned database object
  from ``audb.load_to()`` does now point to the correct folder
  and not the temporal folder
* Removed: support for Python 3.6


Version 1.1.9 (2021-08-05)
--------------------------

* Added: ``name`` argument to ``audb.cached()``
  to limit search to given database name
* Changed: speedup ``audb.available()`` by 100%
* Changed: use ``audiofile.duration(..., sloppy=True)``
  for estimating durations for dependency files
* Fixed: ``audb.cached()`` for empty or missing shared cache


Version 1.1.8 (2021-08-03)
--------------------------

* Fixed: set ``bit_depth`` to ``0`` instead of ``None``
  for non SND formats in the dependency table


Version 1.1.7 (2021-08-03)
--------------------------

* Fixed: store metadata in dependency table for non SND formats
  like MP3 and MP4 files


Version 1.1.6 (2021-07-29)
--------------------------

* Added: documentation sub-section on database duration info
* Fixed: made compatible with future versions of ``pandas``
* Fixed: missing ``audb.Repository`` documentation


Version 1.1.5 (2021-05-26)
--------------------------

* Fixed: ``audb.load()`` raises now error for wrong keyword argument
* Fixed: look also in shared cache for partial loaded databases


Version 1.1.4 (2021-05-19)
--------------------------

* Fixed: version number shown in the documentation table of content


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
