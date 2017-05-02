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

    def __init__(self, name, column_label="columnID", time_quantum=TimeQuantum.NONE):
        validate_index_name(name)
        validate_label(column_label)
        self.name = name
        self.column_label = column_label
        self.time_quantum = time_quantum

    def frame(self, name, row_label="rowID", time_quantum=TimeQuantum.NONE,
              inverse_enabled=False, cache_type=CacheType.DEFAULT, cache_size=0):
        return Frame(self, name, row_label, time_quantum, inverse_enabled,
                     cache_type, cache_size)

    def raw_query(self, query):
        return PQLQuery(query, self)

    def batch_query(self, *queries):
        q = PQLBatchQuery(self)
        q.add(*queries)
        return q

    def union(self, *bitmaps):
        return self._bitmap_op("Union", bitmaps)

    def intersect(self, *bitmaps):
        return self._bitmap_op("Intersect", bitmaps)

    def difference(self, *bitmaps):
        return self._bitmap_op("Difference", bitmaps)

    def count(self, bitmap):
        return PQLQuery(u"Count(%s)" % bitmap.serialize(), self)

    def set_column_attrs(self, column_id, attrs):
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetColumnAttrs(%s=%d, %s)" %
                        (self.column_label, column_id, attrs_str), self)

    def _bitmap_op(self, name, bitmaps):
        if len(bitmaps) < 2:
            raise PilosaError("Number of bitmap queries should be greater or equal to 2")
        return PQLQuery(u"%s(%s)" % (name, u", ".join(b.serialize() for b in bitmaps)), self)


class Frame:

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
        return PQLQuery(u"Bitmap(%s=%d, frame='%s')" % (self.row_label, row_id, self.name),
                        self.index)

    def inverse_bitmap(self, column_id):
        if not self.inverse_enabled:
            raise PilosaError("Inverse bitmaps support was not enabled for this frame")
        return PQLQuery(u"Bitmap(%s=%d, frame='%s')" % (self.column_label, column_id, self.name),
                        self.index)

    def setbit(self, row_id, column_id, timestamp=None):
        ts = ", timestamp='%s'" % timestamp.strftime(_TIME_FORMAT) if timestamp else ''
        return PQLQuery(u"SetBit(%s=%d, frame='%s', %s=%d%s)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id, ts),
                        self.index)

    def clearbit(self, row_id, column_id):
        return PQLQuery(u"ClearBit(%s=%d, frame='%s', %s=%d)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id),
                        self.index)

    def topn(self, n, bitmap=None, field="", *values):
        if field and bitmap:
            validate_label(field)
            values_str = json.dumps(values, separators=(',', ': '))
            qry = u"TopN(%s, frame='%s', n=%d, field='%s', %s)" % \
                   (bitmap.serialize(), self.name, n, field, values_str)
        elif bitmap:
            qry = u"TopN(%s, frame='%s', n=%d)" % (bitmap.serialize(), self.name, n)
        else:
            qry = u"TopN(frame='%s', n=%d)" % (self.name, n)
        return PQLQuery(qry, self.index)

    def range(self, row_id, start, end):
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        return PQLQuery(u"Range(%s=%d, frame='%s', start='%s', end='%s')" %
                        (self.row_label, row_id, self.name, start_str, end_str),
                        self.index)

    def set_row_attrs(self, row_id, attrs):
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetRowAttrs(%s=%d, frame='%s', %s)" %
                        (self.row_label, row_id, self.name, attrs_str),
                        self.index)

    def get_options_string(self):
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
