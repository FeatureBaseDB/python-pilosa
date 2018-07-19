# Server Interaction

## Pilosa URI

A Pilosa URI has the `${SCHEME}://${HOST}:${PORT}` format:
* **Scheme**: Protocol of the URI, one of `http` or `https`. Default: `http`.
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
uri3 = pilosa.URI(host="db1.pilosa.com", port=20202)
```

## Pilosa Client

In order to interact with a Pilosa server, an instance of `pilosa.Client` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and share it with other objects when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the default client with default options using:

```python
client = pilosa.Client()
```

To use a a custom server address, pass the address in the first argument:

```python
client = pilosa.Client("http://node1.pilosa.com:15000")
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
    pool_size_total=50,  # total number of connections in the pool
    retry_count=5,  # number of retries before failing the request
)
```

Once you create a client, you can create indexes, fields and start sending queries.

Here is how you would create a index and field:

```python
schema = client.schema()
index = schema.index("repository")
field = index.field("stargazer")
client.sync_schema(schema)
```

You can send queries to a Pilosa server using the `query` method of client objects:

```python
response = client.query(field.row(5))
```

`query` method accepts optional arguments, including `column_attrs`, `exclude_columns`, `exclude_attrs` and `shards`.

```python
response = client.query(field.row(5),
    column_attrs=True  # return column data in the response
)
```

## Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, `PilosaError` is thrown, otherwise a `QueryResponse` object is returned.

A `QueryResponse` object may contain zero or more results of `QueryResult` type. You can access all results using the `results` property of `QueryResponse` (which returns a list of `QueryResult` objects) or you can use the `result` property (which returns either the first result or `None` if there are no results):

```python
response = client.query(field.row(5))

# check that there's a result and act on it
result = response.result
if result:
    # act on the result

# iterate on all results
for result in response.results:
    # act on the result
```

Similarly, a `QueryResponse` object may include a number of column objects, if `column_attrs=True` query option was used:

```python
# check that there's a column object and act on it
column = response.column
if column:
    # act on the column

# iterate on all columns
for column in response.columns:
    # act on the column
```

`QueryResult` objects contain:

* `row` property to retrieve a row result,
* `count_items` property to retrieve column count per row ID entries returned from `topn` queries,
* `count` attribute to retrieve the number of rows per the given row ID returned from `count` queries.
* `value` attribute to retrieve the result of `Min`, `Max` or `Sum` queries.
* `changed` attribute shows whether a `SetBit` or `ClearBit` query changed a bit.

```python
result = response.result
row = result.row
columns = row.columns
attributes = row.attributes

count_items = result.count_items

count = result.count

value = result.value

changed = result.changed
```

## SSL/TLS

Make sure the Pilosa server runs on a TLS address. [How To Set Up a Secure Cluster](https://www.pilosa.com/docs/latest/tutorials/#how-to-set-up-a-secure-cluster) tutorial explains how to do that.

In order to enable TLS support on the client side, the scheme of the address should be explicitly specified as `https`, e.g.: `https://01.pilosa.local:10501`

If you are using a self signed certificate, just pass `tls_skip_verify=True` to the `pilosa.Client` constructor:
```python
client = pilosa.Client("https://01.pilosa.local:10501", tls_skip_verify=True)
```

Otherwise, pass the path to server's TLS certificate in `tls_ca_certificate_path`:
```python
certificate_path = "/home/ubuntu/pilosa-tls-tutorial/pilosa.local.crt"
client = pilosa.Client("https://01.pilosa.local:10501", tls_ca_certificate_path=certificate_path)
```

If the certificate was signed by an authority which is not recognized by your operating system, you may have to pass the certificate key too. In that case, pass a tuple containing paths of both the certificate and key in `tls_ca_certificate_path`:
```python
certificate_path = "/home/ubuntu/pilosa-tls-tutorial/pilosa.local.crt"
key_path= "/home/ubuntu/pilosa-tls-tutorial/pilosa.local.key"
client = pilosa.Client("https://01.pilosa.local:10501", tls_ca_certificate_path=(certificate_path, key_path))
```

