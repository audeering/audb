.. Specify pandas format output in cells
.. jupyter-execute::
    :hide-code:

    import pandas as pd

    pd.set_option('display.max_columns', 7)

.. Make sure we have no left-overs
.. jupyter-execute::
    :hide-code:

    import os
    import shutil

    folders = [
        './age-test-1.0.0',
        './age-test-1.1.0',
        './data',
    ]
    for folder in folders:
        if os.path.exists(folder):
            shutil.rmtree(folder)


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

.. jupyter-execute::

    import audformat.testing

    build_dir = './age-test-1.0.0'

    db = audformat.testing.create_db(minimal=True)
    db.name = 'age-test'
    db.license = 'CC0-1.0'
    db.schemes['age'] = audformat.Scheme('int', minimum=20, maximum=90)
    audformat.testing.add_table(
        db,
        table_id='age',
        index_type='filewise',
        columns='age',
        num_files=3,
    )
    db.save(build_dir)
    audformat.testing.create_audio_files(db)

This results in the following database,
stored under :file:`build_dir`.

.. jupyter-execute::

    db

Containing a few random annotations.

.. jupyter-execute::

    db['age'].get()


Publish the first version
-------------------------

We define a repository on the :class:`audbackend.FileSystem` backend
to publish the database to.

.. jupyter-execute::

    import audb

    repository = audb.Repository(
        name='data-local',
        host='./data',
        backend='file-system',
    )

Then we select the folder,
where the database is stored,
and pick a version for publishing it.

.. jupyter-execute::

    deps = audb.publish(build_dir, '1.0.0', repository, verbose=False)

It returns a :class:`audb.Dependencies` object
that specifies
which files are part of the database
in which archives they are stored,
and information about audio metadata.

.. jupyter-execute::

    deps()

We can compare this with the files stored in the repository.

.. jupyter-execute::

    import os

    def list_files(path):
        for root, dirs, files in os.walk(path):
            level = root.replace(path, '').count(os.sep)
            indent = ' ' * 2 * (level)
            print(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 2 * (level + 1)
            for f in files:
                print(f'{subindent}{f}')

    list_files(repository.host)

As you can see all media files are stored inside the :file:`media/` folder,
all tables inside the :file:`meta/` folder,
the database header inside the :file:`db/` folder
as :file:`db-1.0.0.yaml`,
and the database dependency file inside the :file:`db/` folder
inside :file:`db-1.0.0.zip`.

To load the database,
or see which databases are available in your repository,
we need to tell :mod:`audb` that it should use our repository
instead of its default ones.

.. jupyter-execute::

    audb.config.REPOSITORIES = [repository]
    audb.available()


Update a database
-----------------

In a next step we will add another file with age annotation
to the database.
As a first step we load
the previous version
of the database
to a new folder.

.. jupyter-execute::

    build_dir = './age-test-1.1.0'
    db = audb.load_to(build_dir, 'age-test', version='1.0.0', verbose=False)

Then we extend the age table by another file (:file:`audio/004.wav`)
and add the age annotation of 22 to it.

.. jupyter-execute::

    index = audformat.filewise_index(['audio/004.wav'])
    db['age'].extend_index(index, inplace=True)
    db['age']['age'].set([22], index=index)

    db['age'].get()

We save it to the database build folder,
overwrite the old table,
and add a new audio file.

.. jupyter-execute::

    db.save(build_dir)
    audformat.testing.create_audio_files(db)

Publishing works as before,
but this time we have to specify a version where our update should be based on.
:func:`audb.publish` will then automatically figure out
which files have changed
and will only publish those.

.. jupyter-execute::

    deps = audb.publish(
        build_dir,
        '1.1.0',
        repository,
        previous_version='1.0.0',
        verbose=False,
    )
    deps()

It has just uploaded a new version of the table,
and the new media files.
For the other media files,
it just :ref:`depends on the previous published version <database-dependencies>`.
We can again inspect the repository.

.. jupyter-execute::

    list_files(repository.host)

And check which databases are available.

.. jupyter-execute::

    audb.available()

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


.. Clean up
.. jupyter-execute::
    :hide-code:

    for folder in folders: 
        if os.path.exists(folder):
            shutil.rmtree(folder)
