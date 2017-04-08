import json

from .exceptions import PilosaError
from .validator import validate_database_name, validate_frame_name, validate_label

_TIME_FORMAT = "%Y-%m-%dT%H:%M"


class TimeQuantum:

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        return self.value == other.value

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


class Database:

    def __init__(self, name, column_label="col_id", time_quantum=TimeQuantum.NONE):
        validate_database_name(name)
        validate_label(column_label)
        self.name = name
        self.column_label = column_label
        self.time_quantum = time_quantum

    def frame(self, name, row_label="id", time_quantum=TimeQuantum.NONE):
        return Frame(self, name, row_label=row_label, time_quantum=time_quantum)

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

    def set_profile_attributes(self, column_id, attrs):
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetProfileAttrs(%s=%d, %s)" %
                        (self.column_label, column_id, attrs_str), self)

    def _bitmap_op(self, name, bitmaps):
        if len(bitmaps) < 2:
            raise PilosaError("Number of bitmap queries should be greater or equal to 2")
        return PQLQuery(u"%s(%s)" % (name, u", ".join(b.serialize() for b in bitmaps)), self)


class Frame:

    def __init__(self, database, name, row_label="id", time_quantum=TimeQuantum.NONE):
        validate_frame_name(name)
        validate_label(row_label)
        self.database = database
        self.name = name
        self.time_quantum = time_quantum
        self.row_label = row_label
        self.column_label = database.column_label

    def bitmap(self, row_id):
        return PQLQuery(u"Bitmap(%s=%d, frame='%s')" % (self.row_label, row_id, self.name),
                        self.database)

    def setbit(self, row_id, column_id):
        return PQLQuery(u"SetBit(%s=%d, frame='%s', %s=%d)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id),
                        self.database)

    def clearbit(self, row_id, column_id):
        return PQLQuery(u"ClearBit(%s=%d, frame='%s', %s=%d)" % \
                        (self.row_label, row_id, self.name, self.column_label, column_id),
                        self.database)

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
        return PQLQuery(qry, self.database)

    def range(self, row_id, start, end):
        start_str = start.strftime(_TIME_FORMAT)
        end_str = end.strftime(_TIME_FORMAT)
        return PQLQuery(u"Range(%s=%d, frame='%s', start='%s', end='%s')" %
                        (self.row_label, row_id, self.name, start_str, end_str),
                        self.database)

    def set_bitmap_attributes(self, row_id, attrs):
        attrs_str = _create_attributes_str(attrs)
        return PQLQuery(u"SetBitmapAttrs(%s=%d, frame='%s', %s)" %
                        (self.row_label, row_id, self.name, attrs_str),
                        self.database)


class PQLQuery:

    def __init__(self, pql, database):
        self.pql = pql
        self.database = database

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

    def __init__(self, database):
        self.database = database
        self.queries = []

    def add(self, *queries):
        self.queries.extend(queries)

    def serialize(self):
        return u''.join(q.serialize() for q in self.queries)
