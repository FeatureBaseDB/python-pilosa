# Copyright 2017 Pilosa Corp.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
# contributors may be used to endorse or promote products derived
# from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#

import json

from .exceptions import PilosaError, ValidationError
from .validator import validate_index_name, validate_field_name, validate_label, validate_key

__all__ = ("TimeQuantum", "CacheType", "Schema", "Index", "PQLQuery",
           "PQLBatchQuery", "Field")

_TIME_FORMAT = "%Y-%m-%dT%H:%M"

# Python 2-3 compatibility
# PyPy doesn't have __builtins__.get
_basestring = globals()["__builtins__"].basestring if hasattr(globals()["__builtins__"], "basestring") else str


class TimeQuantum:
    """Valid time quantum values.

    * See: `Data Model/Time Quantum <https://www.pilosa.com/docs/latest/data-model/#time-quantum>`_
    """

    NONE = None
    YEAR = None
    MONTH = None
    DAY = None
    HOUR = None
    YEAR_MONTH = None
    MONTH_DAY = None
    DAY_HOUR = None
    YEAR_MONTH_DAY = None
    MONTH_DAY_HOUR = None
    YEAR_MONTH_DAY_HOUR = None

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, TimeQuantum):
            return self.value == other.value
        return False

TimeQuantum.NONE = TimeQuantum("")
TimeQuantum.YEAR = TimeQuantum("Y")
TimeQuantum.MONTH = TimeQuantum("M")
TimeQuantum.DAY = TimeQuantum("D")
TimeQuantum.HOUR = TimeQuantum("H")
TimeQuantum.YEAR_MONTH = TimeQuantum("YM")
TimeQuantum.MONTH_DAY = TimeQuantum("MD")
TimeQuantum.DAY_HOUR = TimeQuantum("DH")
TimeQuantum.YEAR_MONTH_DAY = TimeQuantum("YMD")
TimeQuantum.MONTH_DAY_HOUR = TimeQuantum("MDH")
TimeQuantum.YEAR_MONTH_DAY_HOUR = TimeQuantum("YMDH")


class CacheType:
    """Cache type for set and mutex fields.

    * See: `Data Model/Ranked <https://www.pilosa.com/docs/latest/data-model/#ranked>`_
    """

    #: Use the default cache type for the server
    DEFAULT = None
    #: The LRU cache maintains the most recently accessed Rows. See: `Data Model/LRU <https://www.pilosa.com/docs/latest/data-model/#lru>`_
    LRU = None
    #: Ranked Fields maintain a sorted cache of column counts by Row ID. `Data Model/Ranked <https://www.pilosa.com/docs/latest/data-model/#ranked>`_
    RANKED = None

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, CacheType):
            return self.value == other.value
        return False

CacheType.DEFAULT = CacheType("")
CacheType.LRU = CacheType("lru")
CacheType.RANKED = CacheType("ranked")


class Schema:
    """Schema is a container for index objects"""

    def __init__(self):
        self._indexes = {}

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False
        return self._indexes == other._indexes

    def __ne__(self, other):
        return not self.__eq__(other)

    def index(self, name, keys=False, track_existence=False, shard_width=0):
        """Returns an index object with the given name and options.

        If the index didn't exist in the schema, it is added to the schema.

        :param str name: Index name
        :param bool keys: Whether the index uses string keys
        :param bool track_existence: Enables keeping track of existence which is required for Not query
        :return: Index object

        * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
        * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
        """
        index = self._indexes.get(name)
        if index is None:
            index = Index(name, keys=keys, track_existence=track_existence, shard_width=shard_width)
            self._indexes[name] = index
        return index

    def has_index(self, name):
        """Checks whether the schema has the given index."""
        return name in self._indexes

    def _diff(self, other):
        result = Schema()
        for index_name, index in self._indexes.items():
            if index_name not in other._indexes:
                # if the index doesn't exist in the other schema, simply copy it
                result._indexes[index_name] = index.copy()
            else:
                # the index exists in the other schema; check the fields
                result_index = index.copy(fields=False)
                for field_name, field in index._fields.items():
                    # if the field doesn't exist in the other scheme, copy it
                    if field_name not in result_index._fields:
                        result_index._fields[field_name] = field.copy()
                # check whether we modified result index
                if len(result_index._fields) > 0:
                    result._indexes[index_name] = result_index

        return result


class SerializedQuery:

    def __init__(self, query, has_keys):
        self.query = query
        self.has_keys = has_keys


class Index:
    """The purpose of the Index is to represent a data namespace.

    You cannot perform cross-index queries. Column-level attributes are global to the Index.

    :param str name: Index name
    :param bool keys: Whether the index uses string keys
    :param bool track_existence: Enables keeping track of existence which is required for Not query

    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
    """

    def __init__(self, name, keys=False, track_existence=False, shard_width=0):
        validate_index_name(name)
        self.name = name
        self.keys = keys
        self.track_existence = track_existence
        self.shard_width = shard_width
        self._fields = {}

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False
        return self._meta_eq(other) and \
               self._fields == other._fields

    def __ne__(self, other):
        return not self.__eq__(other)

    def _meta_eq(self, other):
        return self.name == other.name

    def copy(self, fields=True):
        index = Index(self.name, keys=self.keys, track_existence=self.track_existence)
        if fields:
            index._fields = dict((name, field.copy()) for name, field in self._fields.items())
        return index

    def field(self, name, time_quantum=TimeQuantum.NONE,
              cache_type=CacheType.DEFAULT, cache_size=0,
              int_min=0, int_max=0, keys=None, mutex=False, bool=False):
        """Creates a field object with the specified name and defaults.

        :param str name: field name
        :param pilosa.TimeQuantum time_quantum: Sets the time quantum for the field. If a Field has a time quantum, then Views are generated for each of the defined time segments.
        :param pilosa.CacheType cache_type: ``CacheType.DEFAULT``, ``CacheType.LRU`` or ``CacheType.RANKED``
        :param int cache_size: Values greater than 0 sets the cache size. Otherwise uses the default cache size
        :param int int_min: Minimum for the integer field
        :param int int_max: Maximum for the integer field
        :param bool keys: Whether the field uses string keys
        :param bool mutex: Whether the field is a mutex field
        :param bool bool: Whether the field is a bool field
        :return: Pilosa field
        :rtype: pilosa.Field
        """
        field = self._fields.get(name)
        if field is None:
            field = Field(self, name, time_quantum,
                          cache_type, cache_size, int_min, int_max, keys, mutex, bool)
            self._fields[name] = field
        return field

    def has_field(self, name):
        """Checks whether the field exists in the index."""
        return name in self._fields

    def raw_query(self, query):
        """Creates a raw query.

        Note that the query is not validated before sending to the server.

        Raw queries may be less efficient than the corresponding ORM query, since they are only sent to the coordinator node.

        :param str query:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        q = PQLQuery(query, self)
        # Raw queries are always assumed to include keys
        q.query.has_keys = True
        return q

    def batch_query(self, *queries):
        """Creates a batch query.

        Using batch queries is more efficient than sending each query individually.

        If you are sending a large amount of ``Set`` or ``Clear`` queries, it is more efficient to import them instead of using a batch query.

        :param pilosa.PQLQuery queries: the queries in the batch
        :return: Pilosa batch query
        :rtype: pilosa.PQLBatchQuery
        """
        q = PQLBatchQuery(self)
        q.add(*queries)
        return q

    def union(self, *rows):
        """Creates a ``Union`` query.

        ``Union`` performs a logical OR on the results of each ROW_CALL query passed to it.

        :param pilosa.PQLQuery rows: 0 or more row queries to union
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Union <https://www.pilosa.com/docs/latest/query-language/#union>`_
        """
        return self._row_op("Union", rows)

    def intersect(self, *rows):
        """Creates an ``Intersect`` query.

        ``Intersect`` performs a logical AND on the results of each ROW_CALL query passed to it.

        :param pilosa.PQLQuery rows: 1 or more row queries to intersect
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        :raise PilosaError: if the number of rows is less than 1

        * See `Query Language/Intersect <https://www.pilosa.com/docs/latest/query-language/#intersect>`_
        """
        if len(rows) < 1:
            raise PilosaError("Number of row queries should be greater than or equal to 1")
        return self._row_op("Intersect", rows)

    def difference(self, *rows):
        """Creates a ``Difference`` query.

        ``Difference`` returns all of the columns from the first ROW_CALL argument passed to it,
        without the columns from each subsequent ROW_CALL.

        :param pilosa.PQLQuery rows: 1 or more row queries to differentiate
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        :raise PilosaError: if the number of rows is less than 1

        * See `Query Language/Difference <https://www.pilosa.com/docs/latest/query-language/#difference>`_
        """
        if len(rows) < 1:
            raise PilosaError("Number of row queries should be greater than or equal to 1")
        return self._row_op("Difference", rows)

    def xor(self, *rows):
        """Creates an ``Xor`` query.

        ``Xor`` performs a logical XOR on the results of each ROW_CALL query passed to it.

        :param pilosa.PQLQuery rows: 2 or more row queries to xor
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        :raise PilosaError: if the number of rows is less than 2

        * See `Query Language/Xor <https://www.pilosa.com/docs/latest/query-language/#xor>`_
        """
        if len(rows) < 2:
            raise PilosaError("Number of row queries should be greater than or equal to 2")
        return self._row_op("Xor", rows)

    def not_(self, row):
        """Creates a ``Not`` query.

        ``Not`` returns the inverse of all of the bits from the ROW_CALL argument. The ``Not`` query requires that ``track_existence`` has been enabled on the Index (the default).

        :param pilosa.PQLQuery row: a row query
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Not <https://www.pilosa.com/docs/latest/query-language/#not>`_
        """
        return PQLQuery(u"Not(%s)" % row.serialize().query, self)

    def count(self, row):
        """Creates a Count query.

        ``Count`` returns the number of set columns in the ROW_CALL passed in.

        :param pilosa.PQLQuery row: the row query
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Count <https://www.pilosa.com/docs/latest/query-language/#count>`_
        """
        return PQLQuery(u"Count(%s)" % row.serialize().query, self)

    def set_column_attrs(self, col, attrs):
        """Creates a ``SetColumnAttrs`` query.

        ``SetColumnAttrs`` associates arbitrary key/value pairs with a column in an index.

        Following object types are accepted:

        * int
        * str
        * bool
        * float

        :param int col:
        :param dict attrs: column attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/SetColumnAttrs <https://www.pilosa.com/docs/latest/query-language/#setcolumnattrs>`_
        """
        col_str = idkey_as_str(col)
        attrs_str = _create_attributes_str(attrs)
        fmt = u"SetColumnAttrs(%s,%s)"
        q = PQLQuery(fmt % (col_str, attrs_str), self)
        q.query.has_keys = self.keys
        return q

    def options(self, row_query, column_attrs=False, exclude_columns=False, exclude_row_attrs=False, shards=None):
        """Creates an ``Options`` query.

        Modifies the given query as follows:
        * ``columnAttrs``: Include column attributes in the result (Default: false).
        * ``excludeColumns``: Exclude column IDs from the result (Default: false).
        * ``excludeRowAttrs``: Exclude row attributes from the result (Default: false).
        * ``shards``: Run the query using only the data from the given shards. By default, the entire data set (i.e. data from all shards) is used.

        :param bool column_attrs: Include column attributes in the result (Default: ``False``).
        :param bool exclude_columns: Exclude column IDs from the result (Default: ``False``).
        :param bool exclude_row_attrs: Exclude row attributes from the result (Default: ``False``).
        :param bool shards: Run the query using only the data from the given shards. By default, the entire data set (i.e. data from all shards) is used.
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Options <https://www.pilosa.com/docs/latest/query-language/#options>`_
        """

        make_bool = lambda b: "true" if b else "false"
        serialized_options = u"columnAttrs=%s,excludeColumns=%s,excludeRowAttrs=%s" % \
                             (make_bool(column_attrs), make_bool(exclude_columns), make_bool(exclude_row_attrs))
        if shards:
            serialized_options = "%s,shards=[%s]" % (serialized_options, ",".join(str(s) for s in shards))
        return PQLQuery("Options(%s,%s)" % (row_query.serialize().query, serialized_options), self)

    def group_by(self, *rows_queries, **kwargs):
        """Creates a ``GroupBy`` query.

        :param *PQLQuery rows_queries: List of at least one ``Rows`` queries.
        :param int limit: (Optional) limits the number of results returned.
        :param PQLQuery filter: (Optional) takes any type of `Row` query (e.g. Row, Union,
 Intersect, etc.) which will be intersected with each result prior to returning
 the count. This is analagous to a WHERE clause applied to a relational GROUP BY
 query.
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        if len(rows_queries) < 1:
            raise PilosaError("Number of rows queries should be greater than or equal to 1")
        q = [u",".join(q.serialize().query for q in rows_queries)]
        limit = kwargs.get("limit")
        if limit is not None:
            q.append("limit=%s" % limit)
        filter = kwargs.get("filter")
        if filter is not None:
            q.append("filter=%s" % filter.serialize().query)
        return PQLQuery(u"GroupBy(%s)" % u",".join(q), self)

    def _row_op(self, name, rows):
        return PQLQuery(u"%s(%s)" % (name, u", ".join(b.serialize().query for b in rows)), self)

    def _get_options_string(self):
        options = {}
        if self.keys:
            options["keys"] = True
        if self.track_existence:
            options["trackExistence"] = True
        if options:
            return json.dumps({"options": options})
        return ""


class Field:
    """Fields are used to segment and define different functional characteristics within your entire index.

    You can think of a Field as a table-like data partition within your Index.
    Row-level attributes are namespaced at the Field level.

    Do not create a Field object directly. Instead, use ``pilosa.Index.field`` method.

    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
    """

    def __init__(self, index, name, time_quantum,
                 cache_type, cache_size, int_min, int_max, keys, mutex, bool):
        validate_field_name(name)
        if int_max < int_min:
            raise ValidationError("Max should be greater than min for int fields")

        self.index = index
        self.name = name
        self.time_quantum = time_quantum
        self.cache_type = cache_type
        self.cache_size = cache_size
        self.int_min = int_min
        self.int_max = int_max
        self.keys = keys
        self.mutex = mutex
        self.bool = bool

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False

        # Note that we skip comparing the fields of the indexes by using index._meta_eq
        # in order to avoid a call cycle
        return self.name == other.name and \
               self.index._meta_eq(other.index)

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def field_type(self):
        if self.mutex:
            return "mutex"
        if self.bool:
            return "bool"
        if self.time_quantum != TimeQuantum.NONE:
            return "time"
        if self.int_min != 0 or self.int_max != 0:
            return "int"
        return "set"

    def copy(self):
        return Field(self.index, self.name, self.time_quantum,
                     self.cache_type, self.cache_size,
                     self.int_min, self.int_max, self.keys,
                     self.mutex, self.bool)

    def row(self, row_idkey, from_=None, to=None):
        """Creates a Row query.

        Row retrieves the indices of all the set columns in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.

        :param int row_idkey:
        :param datetime from_: (Optional) start of the time range
        :param datetime to: (Optional) end of the time range
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Row <https://www.pilosa.com/docs/latest/query-language/#row>`_
        """
        if from_ or to:
            # this is a row range query
            return self._row_range(row_idkey, from_, to)
        row_str = idkey_as_str(row_idkey)
        fmt = u"Row(%s=%s)"
        return PQLQuery(fmt % (self.name, row_str), self.index)

    def set(self, row, col, timestamp=None):
        """Creates a Set query.

        ``Set`` assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given field with the given column.

        :param int row:
        :param int col:
        :param datetime timestamp:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Set <https://www.pilosa.com/docs/latest/query-language/#set>`_
        """
        row_str = idkey_as_str(row)
        col_str = idkey_as_str(col)
        ts = ", %s" % timestamp.strftime(_TIME_FORMAT) if timestamp else ''
        fmt = u"Set(%s,%s=%s%s)"
        return PQLQuery(fmt % (col_str, self.name, row_str, ts), self.index)

    def clear(self, row, col):
        """Creates a Clear query.

        ``Clear`` assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given field from the given column.

        :param int row:
        :param int col:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Clear <https://www.pilosa.com/docs/latest/query-language/#clear>`_
        """
        row_str = idkey_as_str(row)
        col_str = idkey_as_str(col)
        fmt = u"Clear(%s,%s=%s)"
        return PQLQuery(fmt % (col_str, self.name, row_str), self.index)

    def topn(self, n, row=None, name="", *values):
        """Creates a TopN query.

        ``TopN`` returns the id and count of the top n rows (by count of columns) in the field.

        * see: `TopN Query <https://www.pilosa.com/docs/query-language/#topn>`_

        :param int n: number of items to return
        :param pilosa.PQLQuery row: a PQL Row query
        :param str name: only return rows which have the attribute specified by attribute name
        :param object values: filter values to be matched against the attribute name

        * See `Query Language/TopN <https://www.pilosa.com/docs/latest/query-language/#topn>`_
        """
        parts = [self.name]
        if row:
            parts.append(row.serialize().query)
        parts.append("n=%d" % n)
        if name:
            validate_label(name)
            values_str = json.dumps(values, separators=(',', ': '))
            parts.extend(["attrName='%s'" % name, "attrValues=%s" % values_str])
        qry = u"TopN(%s)" % ",".join(parts)
        return PQLQuery(qry, self.index)

    def range(self, row, start, end):
        """Creates a Range query.

        *Deprecated at Pilosa 1.3. Use `rowRange` instead.*

        Similar to ``Row``, but only returns columns which were set with timestamps between the given start and end timestamps.

        * see: `Range Query <https://www.pilosa.com/docs/query-language/#range>`_

        :param int row:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        row_str = idkey_as_str(row)
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        fmt = u"Range(%s=%s,%s,%s)"
        return PQLQuery(fmt % (self.name, row_str, start_str, end_str),
                        self.index)

    def _row_range(self, row, start, end):
        """Creates a Row query with timestamps."""
        row_str = idkey_as_str(row)
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        parts = ['%s=%s' % (self.name, row_str)]
        if start:
            parts.append("from='%s'" % start_str)
        if end:
            parts.append("to='%s'" % end_str)
        return PQLQuery(u"Row(%s)" % ','.join(parts), self.index)

    def set_row_attrs(self, row, attrs):
        """Creates a SetRowAttrs query.

        ``SetRowAttrs`` associates arbitrary key/value pairs with a row in a field.

        Following object types are accepted:

        * int
        * str
        * bool
        * float

        :param int row:
        :param dict attrs: row attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/SetRowAttrs <https://www.pilosa.com/docs/latest/query-language/#setrowattrs>`_
        """
        row_str = idkey_as_str(row)
        attrs_str = _create_attributes_str(attrs)
        fmt = u"SetRowAttrs(%s,%s,%s)"
        return PQLQuery(fmt % (self.name, row_str, attrs_str), self.index)

    def store(self, row_query, row):
        """Creates a Store query.

        ``Store`` writes the result of the row query to the specified row. If the row already exists, it will be replaced. The destination field must be of field type set.

        :param row_query:
        :param row: ID or key of the target row
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/Store <https://www.pilosa.com/docs/latest/query-language/#store>`_
        """
        row_str = idkey_as_str(row)
        fmt = u"Store(%s,%s=%s)"
        return PQLQuery(fmt % (row_query.serialize().query, self.name, row_str), self.index)

    def clear_row(self, row):
        """Creates a ClearRow query.

        ``ClearRow`` sets all bits to 0 in a given row of the binary matrix, thus disassociating the given row in the given field from all columns.

        :param row: ID or key of the target row
        :return: Pilosa query
        :rtype: pilosa.PQLQuery

        * See `Query Language/ClearRow <https://www.pilosa.com/docs/latest/query-language/#clearrow>`_
        """
        row_str = idkey_as_str(row)
        fmt = u"ClearRow(%s=%s)"
        return PQLQuery(fmt %  (self.name, row_str), self.index)

    def lt(self, n):
        """Creates a Range query with less than (<) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation("<", n)

    def lte(self, n):
        """Creates a Range query with less than or equal (<=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation("<=", n)

    def gt(self, n):
        """Creates a Range query with greater than (>) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation(">", n)

    def gte(self, n):
        """Creates a Range query with greater than or equal (>=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation(">=", n)

    def equals(self, n):
        """Creates a Range query with equals (==) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation("==", n)

    def not_equals(self, n):
        """Creates a Range query with not equals (!=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        return self._binary_operation("!=", n)

    def not_null(self):
        """Creates a Range query with not null (!= null) condition.

        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        q = u"Range(%s != null)" % self.name
        return PQLQuery(q, self.index)

    def between(self, a, b):
        """Creates a Range query with between (><) condition.

        :param a: Closed range start
        :param b: Closed range end
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Range <https://www.pilosa.com/docs/latest/query-language/#range>`_
        """
        q = u"Range(%s >< [%d,%d])" % (self.name, a, b)
        return PQLQuery(q, self.index)

    def sum(self, row=None):
        """Creates a Sum query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Sum <https://www.pilosa.com/docs/latest/query-language/#sum>`_
        """
        return self._value_query("Sum", row)

    def min(self, row=None):
        """Creates a Min query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Min <https://www.pilosa.com/docs/latest/query-language/#min>`_
        """
        return self._value_query("Min", row)

    def max(self, row=None):
        """Creates a Max query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/Max <https://www.pilosa.com/docs/latest/query-language/#max>`_
        """
        return self._value_query("Max", row)

    def setvalue(self, col, value):
        """Creates a SetValue query.

        :param col: column ID or key
        :param value: the value to assign to the field
        :return: a PQL query
        :rtype: PQLQuery

        * See `Query Language/SetValue <https://www.pilosa.com/docs/latest/query-language/#setvalue>`_
        """
        col_str = idkey_as_str(col)
        q = u"Set(%s,%s=%d)" % (col_str, self.name, value)
        return PQLQuery(q, self.index)

    def rows(self, prev_row=None, limit=0, column=None):
        """Creates a ``Rows`` query.

        :param *PQLQuery prev_row: (Optional) If given, rows prior to and including the specified row ID or
key will not be returned.
        :param int limit: (Optional) If given, the number of rowIDs returned will be less than or equal to
``limit``.
        :param int column: If given, only rows which have a set bit
in the given column will be returned.
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """

        parts = [u"field=%s" % self.name]
        if prev_row:
            parts.append(u"previous=%s" % idkey_as_str(prev_row))
        if limit > 0:
            parts.append(u"limit=%d" % limit)
        if column:
            parts.append(u"column=%s" % idkey_as_str(column))
        return PQLQuery(u"Rows(%s)" % u",".join(parts), self.index)

    def _binary_operation(self, op, n):
        q = u"Range(%s %s %d)" % (self.name, op, n)
        return PQLQuery(q, self.index)

    def _value_query(self, op, row):
        row_str = "%s, " % row.serialize().query if row else ""
        q = u"%s(%sfield='%s')" % (op, row_str, self.name)
        return PQLQuery(q, self.index)

    def _get_options_string(self):
        field_type = self.field_type
        data = {
            "type": field_type
        }
        if self.keys:
            data["keys"] = self.keys
        if self.time_quantum != TimeQuantum.NONE:
            data["timeQuantum"] = str(self.time_quantum)
        elif self.int_min != 0 or self.int_max != 0:
            data["min"] = self.int_min
            data["max"] = self.int_max
        elif field_type in ["set", "mutex"]:
            if self.cache_type != CacheType.DEFAULT:
                data["cacheType"] = str(self.cache_type)
            if self.cache_size > 0:
                data["cacheSize"] = self.cache_size
        return json.dumps({"options": data}, sort_keys=True)


class PQLQuery:

    def __init__(self, pql, index):
        self.query = SerializedQuery(pql, False)
        self.index = index

    def serialize(self):
        return self.query


def _create_attributes_str(attrs):
    kvs = []
    try:
        for k, v in attrs.items():
            # TODO: make key use its own validator
            validate_label(k)
            kvs.append("%s=%s" % (k, json.dumps(v)))
        return ",".join(sorted(kvs))
    except TypeError:
        raise PilosaError("Error while converting values")


class PQLBatchQuery:

    def __init__(self, index):
        self.index = index
        self.queries = []

    def add(self, *queries):
        self.queries.extend(queries)

    def serialize(self):
        has_keys = self.index.keys
        text_queries = []
        for q in self.queries:
            serialized_query = q.serialize()
            has_keys = has_keys or serialized_query.has_keys
            text_queries.append(serialized_query.query)
        text = u''.join(text_queries)
        return SerializedQuery(text, has_keys)


def idkey_as_str(id_key):
    if isinstance(id_key, bool):
        return "true" if id_key else "false"
    elif isinstance(id_key, int):
        return str(id_key)
    elif isinstance(id_key, _basestring):
        validate_key(id_key)
        return "'%s'" % id_key
    else:
        raise ValidationError("Rows/Columns must be integers, booleans or strings")
