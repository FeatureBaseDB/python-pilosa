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
from .validator import validate_index_name, validate_frame_name, validate_label, validate_key

__all__ = ("TimeQuantum", "CacheType", "Schema", "Index", "PQLQuery",
           "PQLBatchQuery", "IntField", "RangeField", "Frame")

_TIME_FORMAT = "%Y-%m-%dT%H:%M"

# Python 2-3 compatibility
# PyPy doesn't have __builtins__.get
_basestring = globals()["__builtins__"].basestring if hasattr(globals()["__builtins__"], "basestring") else str


class TimeQuantum:
    """Valid time quantum values for frames having support for that.
    
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

    def index(self, name):
        """Returns an index object with the given name and options.

        If the index didn't exist in the schema, it is added to the schema.

        :param str name: index name
        :return: Index object

        * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
        * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
        """
        index = self._indexes.get(name)
        if index is None:
            index = Index(name)
            self._indexes[name] = index
        return index

    def _diff(self, other):
        result = Schema()
        for index_name, index in self._indexes.items():
            if index_name not in other._indexes:
                # if the index doesn't exist in the other schema, simply copy it
                result._indexes[index_name] = index.copy()
            else:
                # the index exists in the other schema; check the frames
                result_index = index.copy(frames=False)
                for frame_name, frame in index._frames.items():
                    # if the frame doesn't exist in the other scheme, copy it
                    if frame_name not in result_index._frames:
                        result_index._frames[frame_name] = frame.copy()
                # check whether we modified result index
                if len(result_index._frames) > 0:
                    result._indexes[index_name] = result_index

        return result


class Index:
    """The purpose of the Index is to represent a data namespace.
    
    You cannot perform cross-index queries. Column-level attributes are global to the Index.
    
    :param str name: index name

    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_    
    """

    def __init__(self, name):
        validate_index_name(name)
        self.name = name
        self._frames = {}

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False
        return self._meta_eq(other) and \
               self._frames == other._frames

    def __ne__(self, other):
        return not self.__eq__(other)

    def _meta_eq(self, other):
        return self.name == other.name

    def copy(self, frames=True):
        index = Index(self.name)
        if frames:
            index._frames = dict((name, frame.copy()) for name, frame in self._frames.items())
        return index

    def frame(self, name, time_quantum=TimeQuantum.NONE,
              inverse_enabled=False, cache_type=CacheType.DEFAULT, cache_size=0, fields=None):
        """Creates a frame object with the specified name and defaults.
        
        :param str name: frame name
        :param pilosa.TimeQuantum time_quantum: Sets the time quantum for the frame. If a Frame has a time quantum, then Views are generated for each of the defined time segments.
        :param bool inverse_enabled:
        :param pilosa.CacheType cache_type: ``CacheType.DEFAULT``, ``CacheType.LRU`` or ``CacheType.RANKED``
        :param int cache_size: Values greater than 0 sets the cache size. Otherwise uses the default cache size
        :param list(IntField) fields: List of ``IntField`` objects. E.g.: ``[IntField.int("rate", 0, 100)]``
        :return: Pilosa frame
        :rtype: pilosa.Frame
        """
        frame = self._frames.get(name)
        if frame is None:
            frame = Frame(self, name, time_quantum,
                          inverse_enabled, cache_type, cache_size, fields or [])
            self._frames[name] = frame
        return frame

    def raw_query(self, query):
        """Creates a raw query.
        
        Note that the query is not validated before sending to the server.
        
        :param str query:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        return PQLQuery(query, self)

    def batch_query(self, *queries):
        """Creates a batch query.
        
        :param pilosa.PQLQuery queries: the queries in the batch
        :return: Pilosa batch query
        :rtype: pilosa.PQLBatchQuery
        """
        q = PQLBatchQuery(self)
        q.add(*queries)
        return q

    def union(self, *bitmaps):
        """Creates a ``Union`` query.
        
        ``Union`` performs a logical OR on the results of each BITMAP_CALL query passed to it.
        
        :param pilosa.PQLBitmapQuery bitmaps: 0 or more bitmap queries to union
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        """
        return self._bitmap_op("Union", bitmaps)

    def intersect(self, *bitmaps):
        """Creates an ``Intersect`` query.

        ``Intersect`` performs a logical AND on the results of each BITMAP_CALL query passed to it.
        
        :param pilosa.PQLBitmapQuery bitmaps: 1 or more bitmap queries to intersect
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        :raise PilosaError: if the number of bitmaps is less than 1
        """
        if len(bitmaps) < 1:
            raise PilosaError("Number of bitmap queries should be greater than or equal to 1")
        return self._bitmap_op("Intersect", bitmaps)

    def difference(self, *bitmaps):
        """Creates a ``Difference`` query.

        ``Difference`` returns all of the bits from the first BITMAP_CALL argument passed to it,
        without the bits from each subsequent BITMAP_CALL.
        
        :param pilosa.PQLBitmapQuery bitmaps: 1 or more bitmap queries to differentiate
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        :raise PilosaError: if the number of bitmaps is less than 1
        """
        if len(bitmaps) < 1:
            raise PilosaError("Number of bitmap queries should be greater than or equal to 1")
        return self._bitmap_op("Difference", bitmaps)

    def xor(self, *bitmaps):
        """Creates a ``Xor`` query.

        :param pilosa.PQLBitmapQuery bitmaps: 2 or more bitmap queries to xor
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        :raise PilosaError: if the number of bitmaps is less than 2
        """
        if len(bitmaps) < 2:
            raise PilosaError("Number of bitmap queries should be greater than or equal to 2")
        return self._bitmap_op("Xor", bitmaps)

    def count(self, bitmap):
        """Creates a Count query.
        
        ``Count`` returns the number of set bits in the BITMAP_CALL passed in.
        
        :param pilosa.PQLQuery bitmap: the bitmap query
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        return PQLQuery(u"Count(%s)" % bitmap.serialize(), self)

    def set_column_attrs(self, column_idkey, attrs):
        """Creates a SetColumnAttrs query.
        
        ``SetColumnAttrs`` associates arbitrary key/value pairs with a column in an index.
        
        Following object types are accepted:
        
        * int
        * str
        * bool
        * float
        
        :param int column_idkey:
        :param dict attrs: column attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery        
        """
        fmt = id_key_format("Column", column_idkey,
                            u"SetColumnAttrs(col=%s, %s)",
                            u"SetColumnAttrs(col='%s, %s)")
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(fmt % (column_idkey, attrs_str), self)

    def _bitmap_op(self, name, bitmaps):
        return PQLQuery(u"%s(%s)" % (name, u", ".join(b.serialize() for b in bitmaps)), self)


class Frame:
    """Frames are used to segment and define different functional characteristics within your entire index.
    
    You can think of a Frame as a table-like data partition within your Index.
    Row-level attributes are namespaced at the Frame level.
    
    Do not create a Frame object directly. Instead, use ``pilosa.Index.frame`` method.
        
    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_    
    """

    def __init__(self, index, name, time_quantum, inverse_enabled,
                 cache_type, cache_size, fields):
        validate_frame_name(name)
        self.index = index
        self.name = name
        self.time_quantum = time_quantum
        self.inverse_enabled = inverse_enabled
        self.cache_type = cache_type
        self.cache_size = cache_size
        self.fields = fields
        self.range_fields = {}

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False

        # Note that we skip comparing the frames of the indexes by using index._meta_eq
        # in order to avoid a call cycle
        return self.name == other.name and \
               self.index._meta_eq(other.index) and \
               self.time_quantum == other.time_quantum and \
               self.inverse_enabled == other.inverse_enabled and \
               self.cache_type == other.cache_type and \
               self.cache_size == other.cache_size and \
               self.fields == other.fields

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self):
        return Frame(self.index, self.name, self.time_quantum,
                     self.inverse_enabled, self.cache_type, self.cache_size, self.fields)

    def bitmap(self, row_idkey):
        """Creates a Bitmap query.
        
        Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.
        
        This variant of Bitmap query uses the row label.
        
        :param int row_idkey:
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        """
        fmt = id_key_format("Row ID/Key", row_idkey,
                            u"Bitmap(row=%s, frame='%s')",
                            u"Bitmap(row='%s', frame='%s')")
        return PQLQuery(fmt % (row_idkey, self.name), self.index)

    def inverse_bitmap(self, column_idkey):
        """Creates a Bitmap query.

        ``Bitmap`` retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.

        This variant of Bitmap query uses the column label.

        :param int column_idkey:
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        """
        fmt = id_key_format("Column", column_idkey,
                            u"Bitmap(col=%s, frame='%s')",
                            u"Bitmap(col='%s', frame='%s')")
        return PQLQuery(fmt % (column_idkey, self.name), self.index)

    def setbit(self, row_idkey, column_idkey, timestamp=None):
        """Creates a SetBit query.
        
        ``SetBit`` assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given frame with the given column.
        
        :param int row_idkey:
        :param int column_idkey:
        :param pilosa.TimeStamp timestamp:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        if isinstance(row_idkey, int) and isinstance(column_idkey, int):
            fmt = u"SetBit(row=%s, frame='%s', col=%s%s)"
        elif isinstance(row_idkey, _basestring) and isinstance(column_idkey, _basestring):
            fmt = u"SetBit(row='%s', frame='%s', col='%s'%s)"
            validate_key(row_idkey)
            validate_key(column_idkey)
        else:
            raise ValidationError("Both Row and Columns must be integers or strings")
        ts = ", timestamp='%s'" % timestamp.strftime(_TIME_FORMAT) if timestamp else ''
        return PQLQuery(fmt % (row_idkey, self.name, column_idkey, ts), self.index)

    def clearbit(self, row_idkey, column_idkey):
        """Creates a ClearBit query.
        
        ``ClearBit`` assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given frame from the given column.
        
        :param int row_idkey:
        :param int column_idkey:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        if isinstance(row_idkey, int) and isinstance(column_idkey, int):
            fmt = u"ClearBit(row=%s, frame='%s', col=%s)"
        elif isinstance(row_idkey, _basestring) and isinstance(column_idkey, _basestring):
            fmt = u"ClearBit(row='%s', frame='%s', col='%s')"
            validate_key(row_idkey)
            validate_key(column_idkey)
        else:
            raise ValidationError("Both Row and Columns must be integers or strings")
        return PQLQuery(fmt % (row_idkey, self.name, column_idkey), self.index)

    def topn(self, n, bitmap=None, field="", *values):
        """Creates a TopN query.

        ``TopN`` returns the id and count of the top n bitmaps (by count of bits) in the frame.

        * see: `TopN Query <https://www.pilosa.com/docs/query-language/#topn>`_

        :param int n: number of items to return
        :param pilosa.PQLBitmapQuery bitmap: a PQL Bitmap query
        :param field str field: field name
        :param object values: filter values to be matched against the field
        """
        return self._topn(n, bitmap, field, False, *values)

    def inverse_topn(self, n, bitmap=None, field="", *values):
        """Creates a TopN query.

        ``TopN`` returns the id and count of the top n bitmaps (by count of bits) in the frame.

        This version sets `inverse=true`.

        * see: `TopN Query <https://www.pilosa.com/docs/query-language/#topn>`_

        :param int n: number of items to return
        :param pilosa.PQLBitmapQuery bitmap: a PQL Bitmap query
        :param field str field: field name
        :param object values: filter values to be matched against the field
        """
        return self._topn(n, bitmap, field, True, *values)

    def _topn(self, n, bitmap=None, field="", inverse=False, *values):
        parts = ["frame='%s'" % self.name, "n=%d" % n, "inverse=%s" % ('true' if inverse else 'false')]
        if bitmap:
            parts.insert(0, bitmap.serialize())
        if field:
            validate_label(field)
            values_str = json.dumps(values, separators=(',', ': '))
            parts.extend(["field='%s'" % field, "filters=%s" % values_str])
        qry = u"TopN(%s)" % ", ".join(parts)
        return PQLQuery(qry, self.index)

    def range(self, row_idkey, start, end):
        """Creates a Range query.

        Similar to ``Bitmap``, but only returns bits which were set with timestamps between the given start and end timestamps.

        * see: `Range Query <https://www.pilosa.com/docs/query-language/#range>`_

        :param int row_idkey:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp
        """
        return self._range("row", row_idkey, start, end)

    def inverse_range(self, column_id, start, end):
        """Creates a Range query.

        Similar to ``Bitmap``, but only returns bits which were set with timestamps between the given start and end timestamps.


        :param int column_id:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp
        """
        return self._range("col", column_id, start, end)

    def _range(self, label, row_idkey, start, end):
        fmt = id_key_format("Row", row_idkey,
                            u"Range(%s=%s, frame='%s', start='%s', end='%s')",
                            u"Range(%s='%s', frame='%s', start='%s', end='%s')")
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        return PQLQuery(fmt % (label, row_idkey, self.name, start_str, end_str),
                        self.index)

    def set_row_attrs(self, row_idkey, attrs):
        """Creates a SetRowAttrs query.
        
        ``SetRowAttrs`` associates arbitrary key/value pairs with a row in a frame.
        
        Following object types are accepted:
        
        * int
        * str
        * bool
        * float
        
        :param int row_idkey:
        :param dict attrs: row attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery        
        """
        fmt = id_key_format("Row", row_idkey,
                            u"SetRowAttrs(row=%s, frame='%s', %s)",
                            u"SetRowAttrs(row='%s', frame='%s', %s)")
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(fmt % (row_idkey, self.name, attrs_str), self.index)

    def field(self, name):
        """Returns a _RangeField object with the given name.

        :param name: field name
        :return: _RangeField object
        :rtype: RangeField
        """
        field = self.range_fields.get(name)
        if not field:
            validate_label(name)
            field = RangeField(self, name)
            self.range_fields[name] = field
        return field

    def _get_options_string(self):
        data = {}
        if self.inverse_enabled:
            data["inverseEnabled"] = True
        if self.time_quantum != TimeQuantum.NONE:
            data["timeQuantum"] = str(self.time_quantum)
        if self.cache_type != CacheType.DEFAULT:
            data["cacheType"] = str(self.cache_type)
        if self.cache_size > 0:
            data["cacheSize"] = self.cache_size
        if self.fields:
            data["rangeEnabled"] = True
            data["fields"] = [f.attrs for f in self.fields]
        return json.dumps({"options": data}, sort_keys=True)


class PQLQuery:

    def __init__(self, pql, index):
        self.pql = pql
        self.index = index

    def serialize(self):
        return self.pql


def _create_attributes_str(attrs):
    kvs = []
    try:
        for k, v in attrs.items():
            # TODO: make key use its own validator
            validate_label(k)
            kvs.append("%s=%s" % (k, json.dumps(v)))
        return ", ".join(sorted(kvs))
    except TypeError:
        raise PilosaError("Error while converting values")


class PQLBatchQuery:

    def __init__(self, index):
        self.index = index
        self.queries = []

    def add(self, *queries):
        self.queries.extend(queries)

    def serialize(self):
        return u''.join(q.serialize() for q in self.queries)


class IntField:

    def __init__(self, attrs):
        self.attrs = attrs

    @classmethod
    def int(cls, name, min=0, max=100):
        validate_label(name)
        if max <= min:
            raise ValidationError("Max should be greater than min for int fields")
        return cls({
            "name": name,
            "type": "int",
            "min": min,
            "max": max
        })


class RangeField:

    def __init__(self, frame, name):
        self.frame_name = frame.name
        self.name = name
        self.index = frame.index

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
        q = u"Range(frame='%s', %s != null)" % (self.frame_name, self.name)
        return PQLQuery(q, self.index)

    def between(self, a, b):
        """Creates a Range query with between (><) condition.

        :param a: Closed range start
        :param b: Closed range end
        :return: a PQL query
        :rtype: PQLQuery
        """
        q = u"Range(frame='%s', %s >< [%d,%d])" % (self.frame_name, self.name, a, b)
        return PQLQuery(q, self.index)

    def sum(self, bitmap=None):
        """Creates a Sum query.

        :param bitmap: The bitmap query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Sum", bitmap)

    def min(self, bitmap=None):
        """Creates a Min query.

        :param bitmap: The bitmap query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Min", bitmap)

    def max(self, bitmap=None):
        """Creates a Max query.

        :param bitmap: The bitmap query to use.
        :return: a PQL query
        :rtype: PQLQuery
        """
        return self._value_query("Max", bitmap)

    def set_value(self, column_id, value):
        """Creates a SetFieldValue query.

        :param column_id: column ID
        :param value: the value to assign to the field
        :return: a PQL query
        :rtype: PQLQuery
        """
        q = u"SetFieldValue(frame='%s', col=%d, %s=%d)" % \
            (self.frame_name, column_id, self.name, value)
        return PQLQuery(q, self.index)

    def _binary_operation(self, op, n):
        q = u"Range(frame='%s', %s %s %d)" % (self.frame_name, self.name, op, n)
        return PQLQuery(q, self.index)

    def _value_query(self, op, bitmap):
        bitmap_str = "%s, " % bitmap.serialize() if bitmap else ""
        q = u"%s(%sframe='%s', field='%s')" % (op, bitmap_str, self.frame_name, self.name)
        return PQLQuery(q, self.index)



def id_key_format(name, id_key, id_fmt, key_fmt):
    if isinstance(id_key, int):
        return id_fmt
    elif isinstance(id_key, _basestring):
        validate_key(id_key)
        return key_fmt
    else:
        raise ValidationError("%s must be an integer or string" % name)
