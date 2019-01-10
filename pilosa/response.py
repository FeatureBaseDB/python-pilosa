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

from .exceptions import PilosaError
from .internal import public_pb2 as internal


__all__ = ("RowResult", "CountResultItem", "QueryResult", "ColumnItem", "QueryResponse")


QUERYRESULT_NONE, QUERYRESULT_ROW, QUERYRESULT_PAIRS, \
QUERYRESULT_VAL_COUNT, QUERYRESULT_INT, QUERYRESULT_BOOL, \
QUERYRESULT_ROW_IDS,\
QUERYRESULT_GROUP_COUNTS, QUERYRESULT_ROW_IDENTIFIERS = range(9)


class RowResult:
    """Represents a result from ``Row``, ``Union``, ``Intersect``, ``Difference`` and ``Range`` PQL calls.
    
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_
    """

    def __init__(self, columns=None, keys=None, attributes=None):
        self.columns = columns or []
        self.keys = keys or []
        self.attributes = attributes or {}

    @classmethod
    def from_internal(cls, obj):
        return cls(list(obj.Columns), obj.Keys, _convert_protobuf_attrs_to_dict(obj.Attrs))


class CountResultItem:
    """Represents a result from ``TopN`` call.

    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_    
    """

    def __init__(self, id, key, count):
        self.id = id
        self.key = key
        self.count = count


class RowIdentifiersResult:

    def __init__(self, ids=None, keys=None):
        self.ids = ids or []
        self.keys = keys or []

    @classmethod
    def from_internal(cls, obj):
        return cls(list(obj.Rows), list(obj.Keys))


class QueryResult:
    """Represents one of the results in the response.
    
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_        
    """

    def __init__(self, row=None, count_items=None, count=0, value=0,
                 changed=False, group_counts=None, row_identifiers=None):
        self.row = row or RowResult()
        self.count_items = count_items or []
        self.count = count
        self.value = value
        self.changed = changed
        self.group_counts = group_counts or []
        self.row_identifiers = row_identifiers or RowIdentifiersResult()

    @classmethod
    def from_internal(cls, obj):
        row = None
        count_items = []
        count = 0
        value = 0
        changed = False
        group_counts = []
        row_identifiers = None

        if obj.Type == QUERYRESULT_ROW:
            row = RowResult.from_internal(obj.Row)
        elif obj.Type == QUERYRESULT_PAIRS:
            for pair in obj.Pairs:
                count_items.append(CountResultItem(pair.ID, pair.Key, pair.Count))
        elif obj.Type == QUERYRESULT_INT:
            count = obj.N
        elif obj.Type == QUERYRESULT_BOOL:
            changed = obj.Changed
        elif obj.Type == QUERYRESULT_VAL_COUNT:
            count = obj.ValCount.Count
            value = obj.ValCount.Val
        elif obj.Type == QUERYRESULT_NONE:
            pass
        elif obj.Type == QUERYRESULT_GROUP_COUNTS:
            group_counts = _group_counts_from_internal(obj.GroupCounts)
        elif obj.Type == QUERYRESULT_ROW_IDENTIFIERS:
            row_identifiers = RowIdentifiersResult.from_internal(obj.RowIdentifiers)
        else:
            raise PilosaError("Unknown type: %s" % obj.Type)

        return cls(row, count_items, count, value, changed, group_counts, row_identifiers)


class ColumnItem:
    """Contains data about a column.
    
    Column data is returned from ``QueryResponse.getColumns()`` method.
    They are only returned if ``Client.query`` was called with ``columns=True``.
 """

    def __init__(self, id, attributes):
        self.id = id
        self.attributes = attributes

    @classmethod
    def _from_internal(cls, obj):
        return cls(obj.ID, _convert_protobuf_attrs_to_dict(obj.Attrs))


class FieldRow:

    def __init__(self, field_name, id_key):
        self.field_name = field_name
        self.id_key = id_key

    def __hash__(self):
        return hash((self.field_name, self.id_key))

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.field_name == other.field_name and \
            self.id_key == other.id_key

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return u"FieldRow(%s,%s)" % (self.field_name,self.id_key)


class GroupCount:

    def __init__(self, groups, count):
        self.groups = groups
        self.count = count

    def __hash__(self):
        return hash((self.groups, self.count))

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.groups == other.groups and \
            self.count == other.count

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return u"GroupCount(%s,%s)" % (self.groups,self.count)


class QueryResponse(object):
    """Represents the response from a Pilosa query.

    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_        
    """

    def __init__(self, results=None, columns=None, error_message=""):
        self.results = results or []
        self.columns = columns or []
        self.error_message = error_message

    @classmethod
    def _from_protobuf(cls, bin):
        response = internal.QueryResponse()
        response.ParseFromString(bin)
        results = [QueryResult.from_internal(r) for r in response.Results]
        columns = [ColumnItem._from_internal(p) for p in response.ColumnAttrSets]
        error_message = response.Err
        return cls(results, columns, error_message)

    @property
    def result(self):
        return self.results[0] if self.results else None

    @property
    def column(self):
        return self.columns[0] if self.columns else None


def _convert_protobuf_attrs_to_dict(attrs):
    protobuf_attrs_to_dict = [
        None,
        lambda a: a.StringValue,
        lambda a: a.IntValue,
        lambda a: a.BoolValue,
        lambda a: a.FloatValue,
    ]
    d = {}
    attr = None  # to get the attr with invalid type
    try:
        for attr in attrs:
            value = protobuf_attrs_to_dict[attr.Type](attr)
            d[attr.Key] = value
    except (IndexError, TypeError):
        raise PilosaError("Invalid protobuf attribute type: %s" % attr.Type)
    return d


def _group_counts_from_internal(items):
    group_counts = []
    for item in items:
        groups = []
        for f in item.Group:
            field_row = FieldRow(f.Field, f.RowKey) if f.RowKey else FieldRow(f.Field, f.RowID)
            groups.append(field_row)
        group_counts.append(GroupCount(groups, item.Count))
    return group_counts