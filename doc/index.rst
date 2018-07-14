.. Python Client for Pilosa documentation master file, created by
   sphinx-quickstart on Wed May 10 12:08:03 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Python Client for Pilosa's documentation!
====================================================

Python client for `Pilosa <https://www.pilosa.com>`_ high performance distributed row index.


.. toctree::
   :maxdepth: 4
   :caption: Contents:

   pilosa

Requirements
------------

-  Python 2.6 and higher or Python 3.3 and higher

Install
-------

Pilosa client is on `PyPI <https://pypi.python.org/pypi/pilosa>`__. You
can install the library using ``pip``:

::

    pip install pilosa

Quick overview
--------------

Assuming `Pilosa <https://github.com/pilosa/pilosa>`__ server is running
at ``localhost:10101`` (the default):

.. code:: python

    import pilosa

    # Create the default client
    client = pilosa.Client()

    # Retrieve the schema
    schema = client.schema()

    # Create an Index object
    myindex = schema.index("myindex")

    # Create a Field object
    myfield = myindex.field("myfield")

    # make sure the index and field exists on the server
    client.sync_schema(schema)

    # Send a SetBit query. PilosaError is thrown if execution of the query fails.
    client.query(myfield.set(5, 42))

    # Send a Bitmap query. PilosaError is thrown if execution of the query fails.
    response = client.query(myfield.row(5))

    # Get the result
    result = response.result

    # Act on the result
    if result:
        columns = result.row.columns
        print("Got columns: ", columns)

    # You can batch queries to improve throughput
    response = client.query(
        myindex.batch_query(
            myfield.row(5),
            myfield.row(10),
        )
    )
    for result in response.results:
        # Act on the result
        print(result)



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
