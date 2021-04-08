Authentication
==============

If you want to use an Artifactory backend,
users need to authenticate.
You could use `anonymous access`_,
but we would only recommend it for downloading public data.

To authenticate
users have to store their credentials in :file:`~/.artifactory_python.cfg`.

.. code-block:: cfg

    [your-organization.jfrog.io/artifactory]
    username = MY_USERNAME
    password = MY_API_KEY

Alternatively, they can export them as environment variables.

.. code-block:: bash

    export ARTIFACTORY_USERNAME="MY_USERNAME"
    export ARTIFACTORY_API_KEY="MY_API_KEY"


.. _anonymous access: https://jfrog.com/knowledge-base/how-to-grant-an-anonymous-user-access-to-specific-repositories/
