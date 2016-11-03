Python Driver for Pilosa
========================

Installation
------------
Installation through pip is recommended:

    $ pip install pilosa-driver


Usage
-----
#### Creating a Connection

The first step in accessing pilosa is to create a connection to the cluster

```sh
from pilosa import Cluster
cluster = Cluster(settings=SETTINGS)
```

Support to connect to pilosa hosts or kinesis.

Pilosa Hosts:
```
SETTINGS = {"hosts: :[]}
```
Kinesis:
```
SETTINGS = {"kinesis_firehose_stream": "name",
            "kinesis_encode_type": 1,
            "kinesis_region_name": "us-east-1",
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY}
```

#### Query

Queries to Pilosa require sending a POST request where the query itself is sent as POST data.
You specify the database on which to perform the query with a URL argument `db=database-name`.

A query sent to database `exampleDB` will have the following format:

```sh
from pilosa import Setbit
results = cluster.execute(exampleDB, Query())
```

The `Query()` object referenced above should be made up of one or more of the query types listed below.
So for example, a SetBit() query would look like this:
```sh
result = cluster.execute(exampleDB, SetBit(id=10, frame="foo", profileID=1))
```

Query results have the format `{"results":[]}`, where `results` is a list of results for each `Query()`. This
means that you can provide multiple `Query()` objects with each request and `results` will contain
the results of all of the queries.

```sh
results = cluster.execute(exampleDB, [Query(), Query(), Query() )
```

---
#### SetBit()
```
from pilosa import SetBit
results = cluster.execute(exampleDB, SetBit(id=10, frame="foo", profileID=1))
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 0 to 1.
A return value of `{"results":[false]}` indicates that the bit was already set to 1 and therefore nothing changed.

---
#### ClearBit()
```
from pilosa import ClearBit
results = cluster.execute(exampleDB, ClearBit(id=10, frame="foo", profileID=1))
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 1 to 0.
A return value of `{"results":[false]}` indicates that the bit was already set to 0 and therefore nothing changed.

---
#### SetBitmapAttrs()
```
from pilosa import SetBitmapAttrs
SetBitmapAttrs(id=10, frame="foo", category=123, color="blue", happy=true)
```
Returns `{"results":[null]}`

---
#### Bitmap()
```
from pilosa import Bitmap
results = cluster.execute(exampleDB, Bitmap(id=10, frame="foo"))
```
Returns `{"results":[{"attrs":{"category":123,"color":"blue","happy":true},"bits":[1,2]}]}` where `attrs` are the
attributes set using `SetBitmapAttrs()` and `bits` are the bits set using `SetBit()`.

---
#### Union()
```
from pilosa import Union
results = cluster.execute(exampleDB, Union(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo"))))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1,2]}]}`.
Note that a `Union()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Intersect()
```
from pilosa import Intersect
results = cluster.execute(exampleDB,Intersect(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1]}]}`.
Note that an `Intersect()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Difference()
```
from pilosa import Difference
results = cluster.execute(exampleDB,Difference(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))
```
`Difference()` represents all of the bits that are set in the first `Bitmap()` but are not set in the second `Bitmap()`.  It returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[2]}]}`.
Note that a `Difference()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Count()
```
from pilosa import Count
results = cluster.execute(exampleDB,Count(Bitmap(id=10, frame="foo")))
```
Returns the count of the number of bits set in `Bitmap()`: `{"results":[28]}`

---
#### Range()
```
from pilosa import Range
results = cluster.execute(exampleDB,Range(id=10, frame="foo", start="1970-01-01T00:00", end="2000-01-02T03:04"))
```

---
#### TopN()
```
from pilosa import TopN
results = cluster.execute(exampleDB,TopN(frame="bar", n=20))
```
Returns the top 20 Bitmaps from frame `bar`.

```
results = cluster.execute(exampleDB,TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20))
```
Returns the top 20 Bitmaps from `bar` sorted by the count of bits in the intersection with `Bitmap(id=10)`.


```
results = cluster.execute(exampleDB,TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20, field="category", [81,82]))
```

Returns the top 20 Bitmaps from `bar`in attribute `category` with values `81 or
82` sorted by the count of bits in the intersection with `Bitmap(id=10)`.
