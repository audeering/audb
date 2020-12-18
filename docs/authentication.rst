Authentication
==============

Users have to store their credentials in :file:`~/.artifactory_python.cfg`:

.. code-block:: cfg

    [artifactory.audeering.com/artifactory]
    username = MY_USERNAME
    password = MY_API_KEY

Alternatively, they can export them as environment variables:

.. code-block:: bash

    export ARTIFACTORY_USERNAME="MY_USERNAME"
    export ARTIFACTORY_API_KEY="MY_API_KEY"
