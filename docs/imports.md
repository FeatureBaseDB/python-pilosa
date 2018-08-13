# Importing Data

If you have large amounts of data, it is more efficient to import it to Pilosa instead of several `Set` queries.

`pilosa.imports` module defines several format functions. Depending on the data, the following format is expected:
* `row_id_column_id`: `ROW_ID,COLUMN_ID`
* `row_id_column_key`: `ROW_ID,COLUMN_KEY`
* `row_key_column_id`: `ROW_KEY,COLUMN_ID`
* `row_key_column_key`: `ROW_KEY,COLUMN_KEY`


Optionally, a timestamp with GMT time zone can be added:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

Note that, each line corresponds to a single bit and the lines end with a new line (`\n` or `\r\n`).
The target index and field must have been created before hand.

Here's some sample code that uses `row_id_column_id` formatter:
```python
import pilosa
from pilosa.imports import csv_column_reader, row_id_column_id

try:
    # python 2.7 and 3
    from io import StringIO
except ImportError:
    # python 2.6 and 2.7
    from StringIO import StringIO

text = u"""
    1,10,683793200
    5,20,683793300
    3,41,683793385
    10,10485760,683793385
"""
reader = csv_column_reader(StringIO(text), row_id_column_id)
client = pilosa.Client()
schema = client.schema()
index = schema.index("sample-index")
field = index.field("sample-field", time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY_HOUR)
client.sync_schema(schema)
client.import_field(field, reader)
```
