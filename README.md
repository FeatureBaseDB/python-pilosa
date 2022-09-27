# Python Client for Pilosa

This repo archived Sept 2022 as part of the transition from Pilosa to FeatureBase.
Please contact community[at]featurebase[dot]com with any questions.

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-1.3-blue.svg"></a>
<a href="https://pypi.python.org/pypi/pilosa"><img src="https://img.shields.io/pypi/v/pilosa.svg?maxAge=2592&updated=2"></a>
<a href="http://pilosa.readthedocs.io/en/latest/?badge=latest"><img src="https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat"></A>
<a href="https://travis-ci.org/pilosa/python-pilosa"><img src="https://api.travis-ci.org/pilosa/python-pilosa.svg?branch=master"></a>
<a href="https://coveralls.io/github/pilosa/python-pilosa?branch=master"><img src="https://coveralls.io/repos/github/pilosa/python-pilosa/badge.svg?branch=master"></a>

<img src="https://www.pilosa.com/img/ce.svg" style="float: right" align="right" height="301">

Python client for Pilosa high performance distributed row index.

## What's New?

See: [CHANGELOG](https://github.com/pilosa/python-pilosa/blob/master/CHANGELOG.md)

## Requirements

* **Compatible with Pilosa 1.2 and Pilosa 1.3**
* Requires Python 2.7 and higher or Python 3.4 and higher.

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

# Retrieve the schema
schema = client.schema()

# Create an Index object
myindex = schema.index("myindex")

# Create a Field object
myfield = myindex.field("myfield")

# make sure the index and field exists on the server
client.sync_schema(schema)

# Send a Set query. PilosaError is thrown if execution of the query fails.
client.query(myfield.set(5, 42))

# Send a Row query. PilosaError is thrown if execution of the query fails.
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
    print(result.row.columns)
```

## Documentation

### Data Model and Queries

See: [Data Model and Queries](https://github.com/pilosa/python-pilosa/blob/master/docs/data-model-queries.md)

### Executing Queries

See: [Server Interaction](https://github.com/pilosa/python-pilosa/blob/master/docs/server-interaction.md)

### Importing and Exporting Data

See: [Importing and Exporting Data](https://github.com/pilosa/python-pilosa/blob/master/docs/imports.md)

### Other Documentation

* [Tracing](docs/tracing.md)

## Contributing

See: [CONTRIBUTING](https://github.com/pilosa/python-pilosa/blob/master/CONTRIBUTING.md)

## License

See: [LICENSE](https://github.com/pilosa/python-pilosa/blob/master/LICENSE)
