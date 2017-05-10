.. Python Client for Pilosa documentation master file, created by
   sphinx-quickstart on Wed May 10 12:08:03 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Python Client for Pilosa's documentation!
====================================================

Python client for `Pilosa <https://www.pilosa.com>`_ high performance distributed bitmap index.


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

    # Create an Index object
    myindex = pilosa.Index("myindex")

    # Make sure the index exists on the server
    client.ensure_index(myindex)

    # Create a Frame object
    myframe = myindex.frame("myframe")

    # Make sure the frame exists on the server
    client.ensure_frame(myframe)

    # Send a SetBit query. PilosaError is thrown if execution of the query fails.
    client.query(myframe.setbit(5, 42))

    # Send a Bitmap query. PilosaError is thrown if execution of the query fails.
    response = client.query(myframe.bitmap(5))

    # Get the result
    result = response.result

    # Act on the result
    if result:
        bits = result.bitmap.bits
        print("Got bits: ", bits)

    # You can batch queries to improve throughput
    response = client.query(
        myindex.batch_query(
            myframe.bitmap(5),
            myframe.bitmap(10),
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
