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

from .exceptions import PilosaError
from .validator import validate_index_name, validate_frame_name, validate_label

__all__ = ("TimeQuantum", "CacheType", "Index", "Frame", "PQLQuery", "PQLBatchQuery")

_TIME_FORMAT = "%Y-%m-%dT%H:%M"


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


class Index:
    """The purpose of the Index is to represent a data namespace.
    
    You cannot perform cross-index queries. Column-level attributes are global to the Index.
    
    :param str name: index name
    :param str column_label: a valid column label
    :param pilosa.TimeQuantum time_quantum: Sets the time quantum

    * See `Data Model <https://www.pilosa.com/docs/data-model/>`_
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_    
    """

    def __init__(self, name, column_label="columnID", time_quantum=TimeQuantum.NONE):
        validate_index_name(name)
        validate_label(column_label)
        self.name = name
        self.column_label = column_label
        self.time_quantum = time_quantum

    def frame(self, name, row_label="rowID", time_quantum=TimeQuantum.NONE,
              inverse_enabled=False, cache_type=CacheType.DEFAULT, cache_size=0):
        """Creates a frame object with the specified name and defaults.
        
        :param str name: frame name
        :param str row_label: a valid row label
        :param pilosa.TimeQuantum time_quantum: Sets the time quantum for the frame. If a Frame has a time quantum, then Views are generated for each of the defined time segments.
        :param bool inverse_enabled:
        :param pilosa.CacheType cache_type: ``CacheType.DEFAULT``, ``CacheType.LRU`` or ``CacheType.RANKED``
        :param int cache_size: Values greater than 0 sets the cache size. Otherwise uses the default cache size
        :return: Pilosa frame
        :rtype: pilosa.Frame
        """
        return Frame(self, name, row_label, time_quantum, inverse_enabled,
                     cache_type, cache_size)

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
            raise PilosaError("Number of bitmap queries should be greater or equal to 1")
        return self._bitmap_op("Intersect", bitmaps)

    def difference(self, *bitmaps):
        """Creates a ``Difference`` query.

        ``Difference`` returns all of the bits from the first BITMAP_CALL argument passed to it,
        without the bits from each subsequent BITMAP_CALL.
        
        :param pilosa.PQLBitmapQuery bitmaps: 0 or more bitmap queries to differentiate
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        :raise PilosaError: if the number of bitmaps is less than 1
        """
        if len(bitmaps) < 1:
            raise PilosaError("Number of bitmap queries should be greater or equal to 1")
        return self._bitmap_op("Difference", bitmaps)

    def count(self, bitmap):
        """Creates a Count query.
        
        ``Count`` returns the number of set bits in the BITMAP_CALL passed in.
        
        :param pilosa.PQLQuery bitmap: the bitmap query
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        return PQLQuery(u"Count(%s)" % bitmap.serialize(), self)

    def set_column_attrs(self, column_id, attrs):
        """Creates a SetColumnAttrs query.
        
        ``SetColumnAttrs`` associates arbitrary key/value pairs with a column in an index.
        
        Following object types are accepted:
        
        * int
        * str
        * bool
        * float
        
        :param int column_id:
        :param dict attrs: column attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery        
        """
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetColumnAttrs(%s=%d, %s)" %
                        (self.column_label, column_id, attrs_str), self)

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

    def __init__(self, index, name, row_label, time_quantum, inverse_enabled,
                 cache_type, cache_size):
        validate_frame_name(name)
        validate_label(row_label)
        self.index = index
        self.name = name
        self.time_quantum = time_quantum
        self.inverse_enabled = inverse_enabled
        self.cache_type = cache_type
        self.cache_size = cache_size
        self.row_label = row_label
        self.column_label = index.column_label

    def bitmap(self, row_id):
        """Creates a Bitmap query.
        
        Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.
        
        This variant of Bitmap query uses the row label.
        
        :param int row_id:
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        """
        return PQLQuery(u"Bitmap(%s=%d, frame='%s')" % (self.row_label, row_id, self.name),
                        self.index)

    def inverse_bitmap(self, column_id):
        """Creates a Bitmap query.

        ``Bitmap`` retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query. It also retrieves any attributes set on that row or column.

        This variant of Bitmap query uses the column label.

        :param int column_id:
        :return: Pilosa bitmap query
        :rtype: pilosa.PQLBitmapQuery
        """
        return PQLQuery(u"Bitmap(%s=%d, frame='%s')" % (self.column_label, column_id, self.name),
                        self.index)

    def setbit(self, row_id, column_id, timestamp=None):
        """Creates a SetBit query.
        
        ``SetBit`` assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given frame with the given column.
        
        :param int row_id:
        :param int column_id:
        :param pilosa.TimeStamp timestamp:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        ts = ", timestamp='%s'" % timestamp.strftime(_TIME_FORMAT) if timestamp else ''
        return PQLQuery(u"SetBit(%s=%d, frame='%s', %s=%d%s)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id, ts),
                        self.index)

    def clearbit(self, row_id, column_id):
        """Creates a ClearBit query.
        
        ``ClearBit`` assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given frame from the given column.
        
        :param int row_id:
        :param int column_id:
        :return: Pilosa query
        :rtype: pilosa.PQLQuery
        """
        return PQLQuery(u"ClearBit(%s=%d, frame='%s', %s=%d)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id),
                        self.index)

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
            parts.extend(["field='%s'" % field, values_str])
        qry = u"TopN(%s)" % ", ".join(parts)
        return PQLQuery(qry, self.index)

    def range(self, row_id, start, end):
        """Creates a Range query.

        Similar to ``Bitmap``, but only returns bits which were set with timestamps between the given start and end timestamps.

        * see: `Range Query <https://www.pilosa.com/docs/query-language/#range>`_

        :param int row_id:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp
        """
        return self._range(self.row_label, row_id, start, end)

    def inverse_range(self, column_id, start, end):
        """Creates a Range query.

        Similar to ``Bitmap``, but only returns bits which were set with timestamps between the given start and end timestamps.


        :param int column_id:
        :param datetime.datetime start: start timestamp
        :param datetime.datetime end: end timestamp
        """
        return self._range(self.column_label, column_id, start, end)

    def _range(self, label, rowcol_id, start, end):
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        return PQLQuery(u"Range(%s=%d, frame='%s', start='%s', end='%s')" %
                        (label, rowcol_id, self.name, start_str, end_str),
                        self.index)

    def set_row_attrs(self, row_id, attrs):
        """Creates a SetRowAttrs query.
        
        ``SetRowAttrs`` associates arbitrary key/value pairs with a row in a frame.
        
        Following object types are accepted:
        
        * int
        * str
        * bool
        * float
        
        :param int row_id:
        :param dict attrs: row attributes
        :return: Pilosa query
        :rtype: pilosa.PQLQuery        
        """
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetRowAttrs(%s=%d, frame='%s', %s)" %
                        (self.row_label, row_id, self.name, attrs_str),
                        self.index)

    def _get_options_string(self):
        data = {"rowLabel": self.row_label}
        if self.inverse_enabled:
            data["inverseEnabled"] = True
        if self.time_quantum != TimeQuantum.NONE:
            data["timeQuantum"] = str(self.time_quantum)
        if self.cache_type != CacheType.DEFAULT:
            data["cacheType"] = str(self.cache_type)
        if self.cache_size > 0:
            data["cacheSize"] = self.cache_size
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
