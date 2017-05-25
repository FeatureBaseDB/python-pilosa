# Python Client for Pilosa

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-v0.3.2-blue.svg"></a>
<a href="https://pypi.python.org/pypi/pilosa"><img src="https://img.shields.io/pypi/v/pilosa.svg?maxAge=2592000&updated=1"></a>
<a href="http://pilosa.readthedocs.io/en/latest/?badge=latest"><img src="https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat"></A>
<a href="https://travis-ci.org/pilosa/python-pilosa"><img src="https://travis-ci.com/pilosa/python-pilosa.svg?token=Peb4jvQ3kLbjUEhpU5aR&branch=master"></a>
<a href="https://coveralls.io/github/pilosa/python-pilosa?branch=master"><img src="https://coveralls.io/repos/github/pilosa/python-pilosa/badge.svg?branch=master"></a>

<img src="https://www.pilosa.com/img/ce.svg" style="float: right" align="right" height="301">

Python client for Pilosa high performance distributed bitmap index.

## Change Log

* **v0.3.3**
    * This version has the updated documentation.
    * Some light refactoring which shouldn't affect any user code.
    * Updated the accepted values for index, frame names and labels to match with the Pilosa server.
    * `Union` queries accept 0 or more arguments. `Union`, `Intersect` and `Difference` queries accept 1 or more arguments.

* **v0.3.2** (2017-05-03):
    * Fixes a bug with getting the version of the package.
    
* **v0.3.1** (2017-05-01):
    * Initial version.
    * Supports Pilosa Server v0.3.1.

## Requirements

* Python 2.6 and higher or Python 3.3 and higher

## Install

Pilosa client is on [PyPI](https://pypi.python.org/pypi/pilosa). You can install the library using `pip`:

```
pip install pilosa
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```python
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
```

### Data Model and Queries

#### Indexes and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`Index` constructor is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```python
repository = pilosa.Index("repository")
```

Indexes support changing the column label and time quantum (*resolution*). You can pass these additional arguments to the `Index` constructor:

```python
repository = pilosa.Index("repository",
    column_label="repo_id", time_quantum=pilosa.TimeQuantum.YEAR_MONTH)
```

Frames are created with a call to `index.frame` method:

```python
stargazer = repository.frame("stargazer")
```

Similar to index objects, you can pass custom options to the `index.frame` method:

```python
stargazer = repository.frame("stargazer",
    row_label="stargazer_id", time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY)
```

#### Queries

Once you have indexes and frame objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```python
bitmap_query = stargazer.bitmap(1, 100)  # corresponds to PQL: Bitmap(frame='stargazer', stargazer_id=1)
```

`Union` queries work on columns; use the index object to create them:

```python
query = repository.union(bitmap_query1, bitmap_query2)
```

In order to increase througput, you may want to batch queries sent to the Pilosa server. The `index.batch_query` method is used for that purpose:

```python
query = repository.batch_query(
    stargazer.bitmap(1, 100),
    repository.union(stargazer.bitmap(100, 200), stargazer.bitmap(5, 100))
)
```

The recommended way of creating query objects is, using dedicated methods attached to index and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use the `index.raw_query` method for that. Note that, query string is not validated before sending to the server:

```python
query = repository.raw_query("Bitmap(frame='stargazer', stargazer_id=5)")
```

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `union(self, *bitmaps)`
* `intersect(self, *bitmaps)`
* `difference(self, *bitmaps)`
* `count(self, bitmap)`
* `set_column_attrs(self, column_id, attrs)`

Frame:

* `bitmap(self, row_id)`
* `setbit(self, row_id, column_id, timestamp=None)`
* `clearbit(self, row_id, column_id)`
* `topn(self, n, bitmap=None, field="", *values)`
* `range(self, row_id, start, end)`
* `set_row_attrs(self, row_id, attrs)`

### Pilosa URI

A Pilosa URI has the `${SCHEME}://${HOST}:${PORT}` format:
* **Scheme**: Protocol of the URI. Default: `http`.
* **Host**: Hostname or ipv4/ipv6 IP address. Default: localhost.
* **Port**: Port number. Default: `10101`.

All parts of the URI are optional, but at least one of them must be specified. The following are equivalent:

* `http://localhost:10101`
* `http://localhost`
* `http://:10101`
* `localhost:10101`
* `localhost`
* `:10101`

A Pilosa URI is represented by the `pilosa.URI` class. Below are a few ways to create `URI` objects:

```python
# create the default URI: http://localhost:10101
uri1 = pilosa.URI()

# create a URI from string address
uri2 = pilosa.URI.address("db1.pilosa.com:20202")

# create a URI with the given host and port
URI uri3 = pilosa.URI(host="db1.pilosa.com", port=20202);
``` 

### Pilosa Client

In order to interact with a Pilosa server, an instance of `pilosa.Client` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and share it with other objects when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the default client with default options using:

```python
client = pilosa.Client()
```

To use a a custom server address, pass the address in the first argument:

```python
client = pilosa.Client("http://db1.pilosa.com:15000")
```

If you are running a cluster of Pilosa servers, you can create a `pilosa.Cluster` object that keeps addresses of those servers:

```python
cluster = pilosa.Cluster(
    pilosa.URI.address(":10101"),
    pilosa.URI.address(":10110"),
    pilosa.URI.address(":10111"),
);

# Create a client with the cluster
client = pilosa.Client(cluster)
```

It is possible to customize the behaviour of the underlying HTTP client by passing client options to the `Client` constructor:

```python
client = pilosa.Client(cluster,
    connect_timeout=1000,  # if can't connect in  a second, close the connection
    socket_timeout=10000,  # if no response received in 10 seconds, close the connection
    pool_size_per_route=3,  # number of connections in the pool per host
    rety_count=5,  # number of retries before failing the request
)
```

Once you create a client, you can create indexes, frames and start sending queries.

Here is how you would create a index and frame:

```python
# materialize repository index instance initialized before
client.create_index(repository)

# materialize stargazer frame instance initialized before
client.create_frame(stargazer)
```

If the index or frame exists on the server, you will receive a `PilosaError`. You can use `ensure_index` and `ensure_frame` methods to ignore existing indexes and frames.

You can send queries to a Pilosa server using the `query` method of client objects:

```python
response = client.query(frame.bitmap(5))
```

`query` method accepts optional `columns` argument:

```python
response = client.query(frame.bitmap(5),
    columns=True  # return column data in the response
)
```

### Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, `PilosaError` is thrown, otherwise a `QueryResponse` object is returned.

A `QueryResponse` object may contain zero or more results of `QueryResult` type. You can access all results using the `results` property of `QueryResponse` (which returns a list of `QueryResult` objects) or you can use the `result` property (which returns either the first result or `None` if there are no results):

```python
response = client.query(frame.bitmap(5))

# check that there's a result and act on it
result = response.result
if result:
    # act on the result
}

# iterate on all results
for result in response.results:
    # act on the result
```

Similarly, a `QueryResponse` object may include a number of profiles (column objects), if `profiles=True` query option was used:

```python
# check that there's a profile and act on it
profile = response.profile
if profile:
    # act on the profile

# iterate on all profiles
for profile in response.profiles:
    # act on the profile
```

`QueryResult` objects contain:

* `bitmap` property to retrieve a bitmap result,
* `count_items` property to retrieve column count per row ID entries returned from `topn` queries,
* `count` attribute to retrieve the number of rows per the given row ID returned from `count` queries.

```python
bitmap = response.bitmap
bits = bitmap.bits
attributes = bitmap.attributes

count_items = response.count_items

count = response.count
```

## Contribution

Please check our [Contributor's Guidelines](https://github.com/pilosa/pilosa/CONTRIBUTING.md).

1. Sign the [Developer Agreement](https://wwww.pilosa.com/developer-agreement) so we can include your contibution in our codebase.
2. Fork this repo and add it as upstream: `git remote add upstream git@github.com:pilosa/python-pilosa.git`.
3. Make sure all tests pass (use `make test-all`) and be sure that the tests cover all statements in your code (we aim for 100% test coverage).
4. Commit your code to a feature branch and send a pull request to the `master` branch of our repo.

The sections below assume your platform has `make`. Otherwise you can view the corresponding steps of the `Makefile`.

### Running tests

You can run unit tests with:
```
make test
```

And both unit and integration tests with:
```
make test-all
```

### Generating protobuf classes

Protobuf classes are already checked in to source control, so this step is only needed when the upstream `public.proto` changes.

Before running the following step, make sure you have the [Protobuf compiler](https://github.com/google/protobuf) installed:

```
make generate
```

## License

```
Copyright 2017 Pilosa Corp.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.
```
