================================
Python Client Library for Pilosa
================================

.. image:: https://travis-ci.com/pilosa/python-pilosa.svg?token=Peb4jvQ3kLbjUEhpU5aR&branch=master
    :target: https://travis-ci.com/pilosa/python-pilosa

------------
Installation
------------

Installation through pip is recommended:

    $ pip install pilosa-driver


-----
Usage
-----

Creating a Connection
---------------------

The first step in accessing Pilosa is to create a client object.

.. code:: python

    from pilosa import Client
    client = Client()

    # You may also specify the hosts when instanciating the client. The default host is 127.0.0.1:15000.
    client = Client(['my.custom.hostname:15000'])

Query
-----

Queries to Pilosa require sending a POST request where the query itself is sent as POST data.
You specify the database on which to perform the query with a URL argument ``db=database-name``.

A query sent to database ``exampleDB`` will have the following format:

.. code:: python

    from pilosa import Setbit
    results = client.query("exampleDB", Query())

The ``Query()`` object referenced above should be made up of one or more of the query types listed below.
So for example, a SetBit() query would look like this:

.. code:: python

    result = client.query("exampleDB", SetBit(id=10, frame="foo", profileID=1))

Query results have the format ``{"results":[]}``, where ``results`` is a list of results for each ``Query()``. This
means that you can provide multiple ``Query()`` objects with each request and ``results`` will contain
the results of all of the queries.

.. code:: python

    results = client.query("exampleDB", [Query(), Query(), Query()])

SetBit()
--------

.. code:: python

    from pilosa import SetBit
    results = client.query("exampleDB", SetBit(id=10, frame="foo", profileID=1))

A return value of ``{"results":[true]}`` indicates that the bit was toggled from 0 to 1.
A return value of ``{"results":[false]}`` indicates that the bit was already set to 1 and therefore nothing changed.

ClearBit()
----------

.. code:: python

    from pilosa import ClearBit
    results = client.query("exampleDB", ClearBit(id=10, frame="foo", profileID=1))

A return value of ``{"results":[true]}`` indicates that the bit was toggled from 1 to 0.
A return value of ``{"results":[false]}`` indicates that the bit was already set to 0 and therefore nothing changed.

SetBitmapAttrs()
----------------

.. code:: python

    from pilosa import SetBitmapAttrs
    SetBitmapAttrs(id=10, frame="foo", category=123, color="blue", happy=true)

Returns ``{"results":[null]}``

Bitmap()
--------

.. code:: python

    from pilosa import Bitmap
    results = client.query("exampleDB", Bitmap(id=10, frame="foo"))

Returns ``{"results":[{"attrs":{"category":123,"color":"blue","happy":true},"bits":[1,2]}]}`` where ``attrs`` are the
attributes set using ``SetBitmapAttrs()`` and ``bits`` are the bits set using ``SetBit()``.

Union()
-------

.. code:: python

    from pilosa import Union
    results = client.query("exampleDB", Union(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo"))))

Returns a result set similar to that of a ``Bitmap()`` query, only the ``attrs`` dictionary will be empty: ``{"results":[{"attrs":{},"bits":[1,2]}]}``.
Note that a ``Union()`` query can be nested within other queries anywhere that you would otherwise provide a ``Bitmap()``.

Intersect()
-----------

.. code:: python

from pilosa import Intersect
results = client.query("exampleDB", Intersect(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))

Returns a result set similar to that of a ``Bitmap()`` query, only the ``attrs`` dictionary will be empty: ``{"results":[{"attrs":{},"bits":[1]}]}``.
Note that an ``Intersect()`` query can be nested within other queries anywhere that you would otherwise provide a ``Bitmap()``.

Difference()
------------

.. code:: python

from pilosa import Difference
results = client.query("exampleDB", Difference(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))

``Difference()`` represents all of the bits that are set in the first ``Bitmap()`` but are not set in the second ``Bitmap()``.  It returns a result set similar to that of a ``Bitmap()`` query, only the ``attrs`` dictionary will be empty: ``{"results":[{"attrs":{},"bits":[2]}]}``.
Note that a ``Difference()`` query can be nested within other queries anywhere that you would otherwise provide a ``Bitmap()``.

Count()
-------

.. code:: python

    from pilosa import Count
    results = client.query(exampleDB,Count(Bitmap(id=10, frame="foo")))

Returns the count of the number of bits set in ``Bitmap()``: ``{"results":[28]}``

Range()
-------

.. code:: python

    from pilosa import Range
    results = client.query(exampleDB,Range(id=10, frame="foo", start="1970-01-01T00:00", end="2000-01-02T03:04"))

TopN()
------

.. code:: python

    from pilosa import TopN
    results = client.query("exampleDB", TopN(frame="bar", n=20))

Returns the top 20 Bitmaps from frame ``bar``.

.. code:: python

    results = client.query("exampleDB", TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20))

Returns the top 20 Bitmaps from ``bar`` sorted by the count of bits in the intersection with ``Bitmap(id=10)``.

.. code:: python

    results = client.query("exampleDB", TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20, field="category", [81,82]))

Returns the top 20 Bitmaps from ``bar`` in attribute ``category`` with values ``81 or 82``
sorted by the count of bits in the intersection with ``Bitmap(id=10)``.
