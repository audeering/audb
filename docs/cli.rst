Command-line
============

The methods provided by :mod:`audb2` and :mod:`audb2.info`
are also available from the command line.
In addition, a command line tool for getting
tables as CSV is provided.


audb2
~~~~~

Command-line interface for methods provided by :mod:`audb2`.

.. code-block:: bash

    $ audb2 <method> <parameter> ... --<argument> value ...

To see a list of available methods, please do:

.. code-block:: bash

    $ audb2 <method> --help

And to get help on a specific method, please do:

.. code-block:: bash

    $ audb2 --help

Load version ``1.0.1`` of ``emodb`` as stereo FLAC files at 44100 Hz.
The database is stored in your :ref:`cache root <cache-root>` and
its header is printed to standard out:

.. command-output:: audb2 load emodb --version 1.0.1 --format flac --sampling_rate 44100

audb2get
~~~~~~~~

Command-line interface to get tables of a database as CSV.

.. code-block:: bash

    $ audb2get <database> <table> --columns <columns> --<argument> value ...

Use the additional keyword arguments to specify
the flavor of the database (see ``$ audb2 load --help`` for a list of
available options).

.. code-block:: bash

    $ audb2get --help

``audb2get`` works like ``audb2 load``,
but instead of returning the header of the database
you specify a table and optional some columns to be printed to standard out.
The following returns the `emotion` table:

.. command-output:: audb2get emodb emotion --version 1.0.1 --format flac --sampling_rate 44100
    :ellipsis: 10

audb2info
~~~~~~~~~

Command-line interface for methods provided by :mod:`audb2info`.

.. code-block:: bash

    $ audb2info <method> <parameter> ... --<argument> value ...

To see a list of available methods, please do:

.. code-block:: bash

    $ audb2info <method> --help

And to get help on a specific method, please do:

.. code-block:: bash

    $ audb2info --help

Show schemes in version ``1.0.1`` of ``emodb``:

.. command-output:: audb2info schemes emodb --version 1.0.1
