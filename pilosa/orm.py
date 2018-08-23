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
    """Valid time quantum values for fields having support for that.
    
    * See: `Data Model <https://www.pilosa.com/docs/data-model/>`_
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

    DEFAULT = None
    LRU = None
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

    def index(self, name, keys=None):
        """Returns an index object with the given name and options.

        If the index didn't exist in the schema, it is added to the schema.

        :param str name: Index name
        :param bool keys: Whether the index uses string keys
        :return: Index object

        * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
        * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
        """
        index = self._indexes.get(name)
        if index is None:
            index = Index(name, keys=keys)
            self._indexes[name] = index
        return index

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

    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_    
    """

    def __init__(self, name, keys=False):
        validate_index_name(name)
        self.name = name
        self.keys = keys
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
        index = Index(self.name, keys=self.keys)
        if fields:
            index._fields = dict((name, field.copy()) for name, field in self._fields.items())
        return index

    def field(self, name, time_quantum=TimeQuantum.NONE,
              cache_type=CacheType.DEFAULT, cache_size=0,
              int_min=0, int_max=0, keys=None):
        """Creates a field object with the specified name and defaults.
        
        :param str name: field name
        :param pilosa.TimeQuantum time_quantum: Sets the time quantum for the field. If a Field has a time quantum, then Views are generated for each of the defined time segments.
        :param pilosa.CacheType cache_type: ``CacheType.DEFAULT``, ``CacheType.LRU`` or ``CacheType.RANKED``
        :param int cache_size: Values greater than 0 sets the cache size. Otherwise uses the default cache size
        :param int int_min: Minimum for the integer field
        :param int int_max: Maximum for the integer field
        :param bool keys: Whether the field uses string keys
        :return: Pilosa field
        :rtype: pilosa.Field
        """
        field = self._fields.get(name)
        if field is None:
            field = Field(self, name, time_quantum,
                          cache_type, cache_size, int_min, int_max, keys)
            self._fields[name] = field
        return field

    def raw_query(self, query):
        """Creates a raw query.
        
        Note that the query is not validated before sending to the server.
        
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
        """
        return self._row_op("Union", rows)

    def intersect(self, *rows):
        """Creates an ``Intersect`` query.

        ``Intersect`` performs a logical AND on the results of each ROW_CALL query passed to it.
        
        :param pilosa.PQLQuery rows: 1 or more row queries to intersect
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        :raise PilosaError: if the number of rows is less than 1
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
        """
        if len(rows) < 1:
            raise PilosaError("Number of row queries should be greater than or equal to 1")
        return self._row_op("Difference", rows)

    def xor(self, *rows):
        """Creates a ``Xor`` query.

        :param pilosa.PQLQuery rows: 2 or more row queries to xor
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        :raise PilosaError: if the number of rows is less than 2
        """
        if len(rows) < 2:
            raise PilosaError("Number of row queries should be greater than or equal to 2")
        return self._row_op("Xor", rows)

    def count(self, row):
        """Creates a Count query.
        
        ``Count`` returns the number of set columns in the ROW_CALL passed in.
        
        :param pilosa.PQLQuery row: the row query
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        return PQLQuery(u"Count(%s)" % row.serialize().query, self)

    def set_column_attrs(self, col, attrs):
        """Creates a SetColumnAttrs query.
        
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
        """
        col_str = idkey_as_str(col)
        attrs_str = _create_attributes_str(attrs)
        fmt = u"SetColumnAttrs(%s,%s)"
        return PQLQuery(fmt % (col_str, attrs_str), self)

    def _row_op(self, name, rows):
        return PQLQuery(u"%s(%s)" % (name, u", ".join(b.serialize().query for b in rows)), self)

    def _get_options_string(self):
        if self.keys:
            return '''{"options":{"keys":true}}'''
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
                 cache_type, cache_size, int_min, int_max, keys):
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

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False

        # Note that we skip comparing the fields of the indexes by using index._meta_eq
        # in order to avoid a call cycle
        return self.name == other.name and \
               self.index._meta_eq(other.index) and \
               self.time_quantum == other.time_quantum and \
               self.cache_type == other.cache_type and \
               self.cache_size == other.cache_size

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def field_type(self):
        if self.time_quantum != TimeQuantum.NONE:
            return "time"
        if self.int_min != 0 or self.int_max != 0:
            return "int"
        return "set"

    def copy(self):
        return Field(self.index, self.name, self.time_quantum,
                     self.cache_type, self.cache_size,
                     self.int_min, self.int_max, self.keys)

    def row(self, row_idkey):
        """Creates a Row query.
        
        Row retrieves the indices of all the set columns in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.
        
        This variant of Row query uses the row label.
        
        :param int row_idkey:
        :return: Pilosa row query
        :rtype: pilosa.PQLQuery
        """
        fmt = id_key_format("Row ID/Key", row_idkey,
                            u"Row(%s=%s)",
                            u"Row(%s='%s')")
        return PQLQuery(fmt % (self.name, row_idkey), self.index)

    def set(self, row, col, timestamp=None):
        """Creates a SetBit query.
        
        ``SetBit`` assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given field with the given column.
        
        :param int row:
        :param int col:
        :param pilosa.TimeStamp timestamp:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        row_str = idkey_as_str(row)
        col_str = idkey_as_str(col)
        ts = ", %s" % timestamp.strftime(_TIME_FORMAT) if timestamp else ''
        fmt = u"Set(%s,%s=%s%s)"
        return PQLQuery(fmt % (col_str, self.name, row_str, ts), self.index)

    def clear(self, row, col):
        """Creates a ClearBit query.
        
        ``ClearBit`` assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given field from the given column.
        
        :param int row:
        :param int col:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
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

        Similar to ``Row``, but only returns columns which were set with timestamps between the given start and end timestamps.

        * see: `Range Query <https://www.pilosa.com/docs/query-language/#range>`_

        :param int row:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp
        """
        row_str = idkey_as_str(row)
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        fmt = u"Range(%s=%s,%s,%s)"
        return PQLQuery(fmt % (self.name, row_str, start_str, end_str),
                        self.index)

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
        """
        row_str = idkey_as_str(row)
        attrs_str = _create_attributes_str(attrs)
        fmt = u"SetRowAttrs(%s,%s,%s)"
        return PQLQuery(fmt % (self.name, row_str, attrs_str), self.index)

    def lt(self, n):
        """Creates a Range query with less than (<) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation("<", n)

    def lte(self, n):
        """Creates a Range query with less than or equal (<=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation("<=", n)

    def gt(self, n):
        """Creates a Range query with greater than (>) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation(">", n)

    def gte(self, n):
        """Creates a Range query with greater than or equal (>=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation(">=", n)

    def equals(self, n):
        """Creates a Range query with equals (==) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation("==", n)

    def not_equals(self, n):
        """Creates a Range query with not equals (!=) condition.

        :param n: The value to compare
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._binary_operation("!=", n)

    def not_null(self):
        """Creates a Range query with not null (!= null) condition.

        :return: a PQL query
        :rtype: PQLQuery
        """
        q = u"Range(%s != null)" % self.name
        return PQLQuery(q, self.index)

    def between(self, a, b):
        """Creates a Range query with between (><) condition.

        :param a: Closed range start
        :param b: Closed range end
        :return: a PQL query
        :rtype: PQLQuery
        """
        q = u"Range(%s >< [%d,%d])" % (self.name, a, b)
        return PQLQuery(q, self.index)

    def sum(self, row=None):
        """Creates a Sum query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Sum", row)

    def min(self, row=None):
        """Creates a Min query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Min", row)

    def max(self, row=None):
        """Creates a Max query.

        :param row: The row query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Max", row)

    def setvalue(self, col, value):
        """Creates a SetValue query.

        :param col: column ID or key
        :param value: the value to assign to the field
        :return: a PQL query
        :rtype: PQLQuery
        """
        col_str = idkey_as_str(col)
        q = u"Set(%s,%s=%d)" % (col_str, self.name, value)
        return PQLQuery(q, self.index)

    def _binary_operation(self, op, n):
        q = u"Range(%s %s %d)" % (self.name, op, n)
        return PQLQuery(q, self.index)

    def _value_query(self, op, row):
        row_str = "%s, " % row.serialize().query if row else ""
        q = u"%s(%sfield='%s')" % (op, row_str, self.name)
        return PQLQuery(q, self.index)

    def _get_options_string(self):
        data = {}
        if self.keys:
            data["keys"] = self.keys
        if self.time_quantum != TimeQuantum.NONE:
            data["type"] = "time"
            data["timeQuantum"] = str(self.time_quantum)
        elif self.int_min != 0 or self.int_max != 0:
            data["type"] = "int"
            data["min"] = self.int_min
            data["max"] = self.int_max
        else:
            data["type"] = "set"
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


def id_key_format(name, id_key, id_fmt, key_fmt):
    if isinstance(id_key, int):
        return id_fmt
    elif isinstance(id_key, _basestring):
        validate_key(id_key)
        return key_fmt
    else:
        raise ValidationError("%s must be an integer or string" % name)


def idkey_as_str(idkey):
    if isinstance(idkey, int):
        row_idkey_str = str(idkey)
    elif isinstance(idkey, _basestring):
        validate_key(idkey)
        row_idkey_str = "'%s'" % idkey
    else:
        raise ValidationError("Rows/Columns must be integers or strings")
    return row_idkey_str
