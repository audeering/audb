Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_,
and this project adheres to `Semantic Versioning`_.


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
