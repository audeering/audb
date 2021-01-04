audb2.backend
=============

.. automodule:: audb2.backend

A backend is an interface
to a repository where meta and media
files of a database are stored
in the original format.
Users can implement their own
backend by deriving from
:class:`audb2.backend.Backend`.


Artifactory
-----------

.. autoclass:: Artifactory
    :members:
    :inherited-members:

Backend
-------

.. autoclass:: Backend

FileSystem
----------

.. autoclass:: FileSystem
    :members:
    :inherited-members:
