# Data Model and Queries

## Indexes and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`schema.index` method is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```python
schema = pilosa.Schema()
repository = schema.index("repository")
```

Frames are created with a call to `index.frame` method:

```python
stargazer = repository.frame("stargazer")
```

Similar to index objects, you can pass custom options to the `index.frame` method:

```python
stargazer = repository.frame("stargazer",
    inverse_enabled=True, time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY)
```

## Queries

Once you have indexes and frame objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```python
bitmap_query = stargazer.bitmap(1)  # corresponds to PQL: Bitmap(frame='stargazer', row=1)
```

`Union` queries work on columns; use the index object to create them:

```python
query = repository.union(bitmap_query1, bitmap_query2)
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.batch_query` method is used for that purpose:

```python
query = repository.batch_query(
    stargazer.bitmap(1),
    repository.union(stargazer.bitmap(100), stargazer.bitmap(5)))
```

The recommended way of creating query objects is, using dedicated methods attached to index and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use the `index.raw_query` method for that. Note that, query string is not validated before sending to the server:

```python
query = repository.raw_query("Bitmap(frame='stargazer', row=5)")
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a frame should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that using this library:
```python
index = schema.index("animals")
frame = index.frame("traits", fields=[pilosa.IntField.int("captivity", min=0, max=956)])
client.sync_schema(schema)
```

If the frame with the necessary field already exists on the server, you don't need to create the field instance, `client.syncSchema(schema)` would load that to `schema`. You can then add some data:
```python
# Add the captivity values to the field.
captivity = frame.field("captivity")
data = [3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318]
query = index.batch_query()
for i, x in enumerate(data):
    column = i + 1
    query.add(captivity.set_value(column, x))
client.query(query)
```

Let's write a range query:
```python
# Query for all animals with more than 100 specimens
response = client.query(captivity.gt(100))
print(response.result.bitmap.bits)

# Query for the total number of animals in captivity
response = client.query(captivity.sum())
print(response.result.value)
```

It's possible to pass a bitmap query to `sum`, so only columns where a row is set are filtered in:
```python
# Let's run a few setbit queries first
client.query(index.batch_query(
    frame.setbit(42, 1),
    frame.setbit(42, 6)
))
# Query for the total number of animals in captivity where row 42 is set
response = client.query(captivity.sum(frame.bitmap(42)))
print(response.result.value)
```

See the *Field* functions further below for the list of functions that can be used with a `_RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `union(self, *bitmaps)`
* `intersect(self, *bitmaps)`
* `difference(self, *bitmaps)`
* `count(self, bitmap)`
* `set_column_attrs(self, column_id, attrs)`
* `xor(self, *bitmaps)`

Frame:

* `bitmap(self, row_id)`
* `inverse_bitmap(self, column_id)`
* `setbit(self, row_id, column_id, timestamp=None)`
* `clearbit(self, row_id, column_id)`
* `topn(self, n, bitmap=None, field="", *values)`
* `inverse_topn(self, n, bitmap=None, field="", *values)`
* `range(self, row_id, start, end)`
* `inverse_range(self, column_id, start, end)`
* `set_row_attrs(self, row_id, attrs)`
* (**deprecated**) `sum(self, bitmap, field)`
* (**deprecated**) `set_field_value(self, column_id, field, value)`

Field:

* `lt(self, n)`
* `lte(self, n)`
* `gt(self, n)`
* `gte(self, n)`
* `between(self, a, b)`
* `sum(self, bitmap=None)`
* `min(self, bitmap=None)`
* `max(self, bitmap=None)`
* `set_value(self, column_id, value)`
