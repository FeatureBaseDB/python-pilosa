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

import itertools

from pilosa.exceptions import PilosaError

__all__ = ("Column", "csv_column_reader")


class Column:

    def __init__(self, row_id=0, column_id=0, row_key="", column_key="", timestamp=0):
        self.row_id = row_id
        self.column_id = column_id
        self.row_key = row_key
        self.column_key = column_key
        self.timestamp = timestamp

    def __hash__(self):
        return hash("%s:%s:%s:%s:%s" % (self.row_id, self.column_id,
                                        self.row_key, self.column_key,
                                        self.timestamp))

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.row_id == other.row_id and \
            self.column_id == other.column_id and \
            self.row_key == other.row_key and \
            self.column_key == other.column_key and \
            self.timestamp == other.timestamp

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return u"Column(row_id=%s, column_id=%s, row_key='%s', column_key='%s', timestamp=%s)" % \
            (self.row_id, self.column_id, self.row_key, self.column_key, self.timestamp)


class FieldValue:

    def __init__(self, column_id=0, column_key="", value=0):
        self.column_id = column_id
        self.column_key = column_key
        self.value = value

    def __hash__(self):
        return hash((self.column_id, self.column_key, self.value))

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.column_id == other.column_id and \
            self.column_key == other.column_key and \
            self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        if self.column_key:
            return u"FieldValue(column_key='%s', value=%s)" % \
                (self.column_key, self.value)
        return u"FieldValue(column_id=%s, value=%s)" % \
            (self.column_id, self.value)


def csv_row_id_column_id(parts, timestamp):
    return Column(row_id=int(parts[0]), column_id=int(parts[1]), timestamp=timestamp)


def csv_row_id_column_key(parts, timestamp):
    return Column(row_id=int(parts[0]), column_key=parts[1], timestamp=timestamp)


def csv_row_key_column_id(parts, timestamp):
    return Column(row_key=parts[0], column_id=int(parts[1]), timestamp=timestamp)


def csv_row_key_column_key(parts, timestamp):
    return Column(row_key=parts[0], column_key=parts[1], timestamp=timestamp)


def csv_column_id_value(parts, timestamp):
    return FieldValue(column_id=int(parts[0]), value=int(parts[1]))


def csv_column_key_value(parts, timestamp):
    return FieldValue(column_key=parts[0], value=int(parts[1]))


def csv_column_reader(file_obj, timefunc=int, formatfunc=csv_row_id_column_id):
    """
    Reads columns from the given file-like object.

    Each line of the file-like object should correspond to a single bit and must be in the following form:
    row,column[,timestamp]

    :param file_obj:
    :param timefunc: optional time parsing function, defaults to int
    :param formatfunc: optional format function, defaults to csv_row_id_column_id
    :return: a generator
    """
    for line in file_obj:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        try:
            if len(parts) == 2:
                bit = formatfunc(parts, 0)
            elif len(parts) == 3:
                bit = formatfunc(parts, timefunc(parts[2]))
            else:
                raise PilosaError("Invalid CSV line: %s", line)
        except ValueError:
            raise PilosaError("Invalid CSV line: %s", line)
        yield bit


def csv_field_value_reader(file_obj, formatfunc=csv_column_id_value):
    """
    Reads field values from the given file-like object.

    Each line of the file-like object should correspond to a column and must be in the following form:
    column,value

    :param file_obj:
    :param formatfunc: optional format function, defaults to csv_column_id_value

    :return: a generator
    """
    for line in file_obj:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) == 2:
            column = formatfunc(parts, 0)
        else:
            raise PilosaError("Invalid CSV line: %s", line)
        yield column


def batch_columns(reader, batch_size, shard_width):
    while 1:
        batch = list(itertools.islice(reader, batch_size))
        if not batch:
            break
        bit_groups = {}
        for bit in batch:
            bit_groups.setdefault(bit.column_id // shard_width, []).append(bit)
        for shard_bit_group in bit_groups.items():
            yield shard_bit_group
