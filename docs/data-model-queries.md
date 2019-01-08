# Data Model and Queries

## Indexes and Fields

*Index* and *field*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`schema.index` method is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```python
schema = pilosa.Schema()
repository = schema.index("repository")
```

Fields are created with a call to `index.field` method:

```python
stargazer = repository.field("stargazer")
```

You can pass custom options to the `index.field` method:

```python
stargazer = repository.field("stargazer", time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY)
```

## Queries

Once you have indexes and field objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to fields.

For instance, `Row` queries work on rows; use a field object to create those queries:

```python
row_query = stargazer.row(1)  # corresponds to PQL: Row(stargazer=1)
```

`Union` queries work on columns; use the index object to create them:

```python
query = repository.union(row_query1, row_query2)
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.batch_query` method is used for that purpose:

```python
query = repository.batch_query(
    stargazer.row(1),
    repository.union(stargazer.row(100), stargazer.row(5)))
```

The recommended way of creating query objects is, using dedicated methods attached to index and field objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use the `index.raw_query` method for that. Note that, the query string is not validated before sending to the server. Also, raw queries may be less efficient than the corresponding ORM query, since they are only sent to the coordinator node.

```python
query = repository.raw_query("Row(stargazer=5)")
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a field should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that using this library:
```python
index = schema.index("animals")
traits = index.field("traits", int_min=0, int_max=956)
captivity = index.field("captivity")
client.sync_schema(schema)
```

If the field with the necessary field already exists on the server, you don't need to create the field instance, `client.sync_schema(schema)` would load that to `schema`. You can then add some data:
```python
# Add the captivity values to the field.
data = [3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318]
query = index.batch_query()
for i, x in enumerate(data):
    column = i + 1
    query.add(traits.setvalue(column, x))
client.query(query)
```

Let's write a range query:
```python
# Query for all animals with more than 100 specimens
response = client.query(traits.gt(100))
print(response.result.row.columns)

# Query for the total number of animals in captivity
response = client.query(traits.sum())
print(response.result.value)
```

It's possible to pass a row query to `sum`, so only columns where a row is set are filtered in:
```python
# Let's run a few set queries first
client.query(index.batch_query(
    field.set(42, 1),
    field.set(42, 6)
))
# Query for the total number of animals in captivity where row 42 is set
response = client.query(traits.sum(captivity.row(42)))
print(response.result.value)
```

See the *Field* functions further below for the list of functions that can be used with a `_RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `union(*rows)`
* `intersect(*rows)`
* `difference(*rows)`
* `count(row)`
* `set_column_attrs(column_id, attrs)`
* `xor(*rows)`
* `not_(row)`
* `options(row_query, column_attrs=False, exclude_columns=False, exclude_row_attrs=False, shards=[])`
* `group_by(*rows_queries, limit=0, filter=None)`

Field:

* `row(row_id)`
* `set(row_id, column_id, timestamp=None)`
* `clear(row_id, column_id)`
* `topn(n, row=None, field="", *values)`
* `range(row_id, start, end)`
* `set_row_attrs(row_id, attrs)`
* `lt(n)`
* `lte(n)`
* `gt(n)`
* `gte(n)`
* `between(a, b)`
* `sum(row=None)`
* `min(row=None)`
* `max(row=None)`
* `setvalue(column_id, value)`
* `store(row_query, row)`
* `clear_row(row)`
* `rows(prev_row=None, limit=0, column=None)`