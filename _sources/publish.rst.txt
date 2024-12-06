.. _publish:

Publish a database
==================

To publish a database we need to first create
and store a database in :mod:`audformat`.
Afterwards we publish the database to a :class:`audb.Repository`.
Finally,
we add more files
and release a new version.


Create a database
-----------------

We can create an example database
with the :mod:`audformat.testing` module.

.. code-block:: python

    import random

    import audeer
    import audformat.testing

    random.seed(1)
    build_dir = audeer.mkdir("./age-test-1.0.0")

    db = audformat.testing.create_db(minimal=True)
    db.name = "age-test"
    db.license = "CC0-1.0"
    db.schemes["age"] = audformat.Scheme("int", minimum=20, maximum=90)
    audformat.testing.add_table(
        db,
        table_id="age",
        index_type="filewise",
        columns="age",
        num_files=3,
    )
    db.save(build_dir)
    audformat.testing.create_audio_files(db)

This results in the following database,
stored under :file:`build_dir`.

>>> db
name: age-test
source: internal
usage: unrestricted
languages: [deu, eng]
license: CC0-1.0
schemes:
  age: {dtype: int, minimum: 20, maximum: 90}
tables:
  age:
    type: filewise
    columns:
      age: {scheme_id: age}

Containing a few random annotations.

>>> db["age"].get()
               age
file
audio/001.wav   37
audio/002.wav   28
audio/003.wav   52


Publish the first version
-------------------------

We define a repository on the local file system
to publish the database to.

.. code-block:: python

    audeer.mkdir("./data", "data-local")
    repository = audb.Repository(
        name="data-local",
        host="./data",
        backend="file-system",
    )

Then we select the folder,
where the database is stored,
and pick a version for publishing it.

.. code-block:: python

    deps = audb.publish(build_dir, "1.0.0", repository, verbose=False)

It returns a :class:`audb.Dependencies` object
that specifies
which files are part of the database
in which archives they are stored,
and information about audio metadata.

>>> deps()
                                             archive  bit_depth  ...  type version
db.age.parquet                                                0  ...     0   1.0.0
audio/001.wav   436c65ec-1e42-f9de-2708-ecafe07e827e         16  ...     1   1.0.0
audio/002.wav   fda7e4d6-f2b2-4cff-cab5-906ef5d57607         16  ...     1   1.0.0
audio/003.wav   e26ef45d-bdc1-6153-bdc4-852d83806e4a         16  ...     1   1.0.0
<BLANKLINE>
[4 rows x 10 columns]

We can compare this with the files stored in the repository.

.. code-block:: python

    import os

    def list_files(path):
        for root, _, files in sorted(os.walk(path)):
            level = root.replace(path, "").count(os.sep)
            indent = " " * 2 * (level)
            print(f"{indent}{os.path.basename(root)}/")
            subindent = " " * 2 * (level + 1)
            for f in sorted(files):
                print(f"{subindent}{f}")

>>> list_files(repository.host)
data/
  data-local/
    age-test/
      1.0.0/
        db.parquet
        db.yaml
      media/
        1.0.0/
          436c65ec-1e42-f9de-2708-ecafe07e827e.zip
          e26ef45d-bdc1-6153-bdc4-852d83806e4a.zip
          fda7e4d6-f2b2-4cff-cab5-906ef5d57607.zip
      meta/
        1.0.0/
          age.parquet

As you can see all media files are stored
inside the ``media/`` folder,
all tables inside the ``meta/`` folder,
the database header in the file ``db.yaml``,
and the database dependencies
in the file ``db.parquet``.
Note,
that the structure of the folders
used for versioning
:meth:`depends on the backend <audb.Repository.create_backend_interface>`,
and differs slightly
for an Artifactory backend.

To load the database,
or see which databases are available in your repository,
we need to tell :mod:`audb` that it should use our repository
instead of its default ones.

>>> audb.config.REPOSITORIES = [repository]
>>> audb.available()
              backend    host  repository version
name
age-test  file-system  ./data  data-local   1.0.0


Update a database
-----------------

In a next step we will add another file with age annotation
to the database.
As a first step we load
the metadata of the
previous version
of the database
to a new folder.

.. code-block:: python

    build_dir = audeer.mkdir("./age-test-1.1.0")
    db = audb.load_to(
        build_dir,
        "age-test",
        version="1.0.0",
        only_metadata=True,
        verbose=False,
    )

Then we extend the age table by another file (:file:`audio/004.wav`)
and add the age annotation of 22 to it.

.. code-block:: python

    index = audformat.filewise_index(["audio/004.wav"])
    db["age"].extend_index(index, inplace=True)
    db["age"]["age"].set([22], index=index)

>>> db["age"].get()
               age
file
audio/001.wav   37
audio/002.wav   28
audio/003.wav   52
audio/004.wav   22

We save it to the database build folder,
overwrite the old table,
and add a new audio file.

.. code-block:: python

    db.save(build_dir)
    audformat.testing.create_audio_files(db)

Publishing works as before,
but this time we have to specify a version where our update should be based on.
:func:`audb.publish` will then automatically figure out
which files have changed
and will only publish those.

.. code-block:: python

    deps = audb.publish(
        build_dir,
        "1.1.0",
        repository,
        previous_version="1.0.0",
        verbose=False,
    )

>>> deps()
                                             archive  bit_depth  ...  type version
db.age.parquet                                                0  ...     0   1.1.0
audio/001.wav   436c65ec-1e42-f9de-2708-ecafe07e827e         16  ...     1   1.0.0
audio/002.wav   fda7e4d6-f2b2-4cff-cab5-906ef5d57607         16  ...     1   1.0.0
audio/003.wav   e26ef45d-bdc1-6153-bdc4-852d83806e4a         16  ...     1   1.0.0
audio/004.wav   ef4d1e81-6488-95cf-a165-604d1e47d575         16  ...     1   1.1.0
<BLANKLINE>
[5 rows x 10 columns]

It has just uploaded a new version of the table,
and the new media files.
For the other media files,
it just :ref:`depends on the previous published version <database-dependencies>`.
We can again inspect the repository.

>>> list_files(repository.host)
data/
  data-local/
    age-test/
      1.0.0/
        db.parquet
        db.yaml
      1.1.0/
        db.parquet
        db.yaml
      media/
        1.0.0/
          436c65ec-1e42-f9de-2708-ecafe07e827e.zip
          e26ef45d-bdc1-6153-bdc4-852d83806e4a.zip
          fda7e4d6-f2b2-4cff-cab5-906ef5d57607.zip
        1.1.0/
          ef4d1e81-6488-95cf-a165-604d1e47d575.zip
      meta/
        1.0.0/
          age.parquet
        1.1.0/
          age.parquet

And check which databases are available.

>>> audb.available()
              backend    host  repository version
name
age-test  file-system  ./data  data-local   1.0.0
age-test  file-system  ./data  data-local   1.1.0


As you can even `update one database by another one`_,
you could automate the update step
and let a database grow every day.


Real world example
------------------

We published a version of a small German acted emotional speech databases
called emodb_
in the default Artifactory repository of :mod:`audb`.
You can find the example code at
https://github.com/audeering/emodb
and can continue at :ref:`load`
to see how to load and use a database.


.. _update one database by another one: https://audeering.github.io/audformat/update-database.html
.. _emodb: http://emodb.bilderbar.info/start.html
