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

import calendar
import datetime
import unittest

from pilosa.exceptions import PilosaError
from pilosa.imports import csv_column_reader, csv_field_value_reader, \
    batch_columns, Column, FieldValue, \
    csv_row_id_column_id, csv_row_id_column_key, csv_row_key_column_id, \
    csv_row_key_column_key, csv_column_id_value, csv_column_key_value

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO


class ImportsTestCase(unittest.TestCase):

    def test_csv_column_reader_row_id_column_id(self):
        reader = csv_column_reader(StringIO(u"""
            1,10,683793200
            5,20,683793300
            3,41,683793385        
            10,10485760,683793385        
        """))
        shard_bit_groups = list(batch_columns(reader, 2))
        self.assertEqual(3, len(shard_bit_groups))

        shard1, batch1 = shard_bit_groups[0]
        self.assertEqual(shard1, 0)
        self.assertEqual(2, len(list(batch1)))

        shard2, batch2 = shard_bit_groups[1]
        self.assertEqual(shard2, 0)
        self.assertEqual(1, len(list(batch2)))

        shard3, batch3 = shard_bit_groups[2]
        self.assertEqual(shard3, 10)
        self.assertEqual(1, len(list(batch3)))
    
    def test_csv_column_reader_row_id_column_key(self):
        reader = csv_column_reader(StringIO(u"""
            1,ten,683793200
            5,twenty,683793300
            3,forty-one,683793385
            10,a-big-number,683793385
        """), formatfunc=csv_row_id_column_key)

        ls = list(reader)
        target = [
            Column(row_id=1, column_key="ten", timestamp=683793200),
            Column(row_id=5, column_key="twenty", timestamp=683793300),
            Column(row_id=3, column_key="forty-one", timestamp=683793385),
            Column(row_id=10, column_key="a-big-number", timestamp=683793385)
        ]
        self.assertEqual(target, ls)

    def test_csv_column_reader_row_key_column_id(self):
        reader = csv_column_reader(StringIO(u"""
            one,10,683793200
            five,20,683793300
            three,41,683793385
            ten,10485760,683793385
        """), formatfunc=csv_row_key_column_id)

        ls = list(reader)
        target = [
            Column(row_key="one", column_id=10, timestamp=683793200),
            Column(row_key="five", column_id=20, timestamp=683793300),
            Column(row_key="three", column_id=41, timestamp=683793385),
            Column(row_key="ten", column_id=10485760, timestamp=683793385)
        ]
        self.assertEqual(target, ls)

    def test_csv_column_reader_row_key_column_key(self):
        reader = csv_column_reader(StringIO(u"""
            one,ten,683793200
            five,twenty,683793300
            three,forty-one,683793385
            ten,a-big-number,683793385
        """), formatfunc=csv_row_key_column_key)

        ls = list(reader)
        target = [
            Column(row_key="one", column_key="ten", timestamp=683793200),
            Column(row_key="five", column_key="twenty", timestamp=683793300),
            Column(row_key="three", column_key="forty-one", timestamp=683793385),
            Column(row_key="ten", column_key="a-big-number", timestamp=683793385)
        ]
        self.assertEqual(target, ls)

    def test_csv_field_value_reader_column_id(self):
        reader = csv_field_value_reader(StringIO(u"""
            1,10
            5,20
            3,41
            10,10485760
        """))
        shard_bit_groups = list(batch_columns(reader, 2))
        self.assertEqual(2, len(shard_bit_groups))

        shard1, batch1 = shard_bit_groups[0]
        self.assertEqual(shard1, 0)
        self.assertEqual(2, len(list(batch1)))

        shard2, batch2 = shard_bit_groups[1]
        self.assertEqual(shard2, 0)
        self.assertEqual(2, len(list(batch2)))

    def test_csv_field_value_column_key(self):
        reader = csv_column_reader(StringIO(u"""
            ten,1
            twenty,5
            forty-one,3
            a-big-number,10
        """), formatfunc=csv_column_key_value)

        ls = list(reader)
        target = [
            FieldValue(column_key="ten", value=1),
            FieldValue(column_key="twenty", value=5),
            FieldValue(column_key="forty-one", value=3),
            FieldValue(column_key="a-big-number", value=10)
        ]
        self.assertEqual(target, ls)

    def test_invalid_input(self):
        invalid_inputs = [
            # less than 2 columns
            u"155",
            # invalid row ID
            u"a5,155",
            # invalid column ID
            u"155,a5",
            # invalid timestamp
            u"155,255,a5",
        ]

        for text in invalid_inputs:
            reader = csv_column_reader(StringIO(text))
            self.assertRaises(PilosaError, list, reader)

    def test_csvbititerator_customtimefunc(self):
        class UtcTzinfo(datetime.tzinfo):
            ZERO = datetime.timedelta(0)
            def utcoffset(self, dt):
                return UtcTzinfo.ZERO
            def dst(self, dt):
                return UtcTzinfo.ZERO
            def tzname(self, dt):
                return "UTC"

        def timefunc_utcstr(timeval):
            dt = datetime.datetime.strptime(timeval, '%Y-%m-%dT%H:%M:%S')
            dt = dt.replace(tzinfo=UtcTzinfo())
            return calendar.timegm(dt.timetuple())

        reader = csv_column_reader(StringIO(u"""
            1,10,1991-09-02T06:33:20
            5,20,1991-09-02T06:35:00
            3,41,1991-09-02T06:36:25
            10,10485760,1991-09-02T06:36:25
        """), timefunc=timefunc_utcstr)

        rows = list(reader)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0], Column(row_id=1, column_id=10, timestamp=683793200))
        self.assertEqual(rows[1], Column(row_id=5, column_id=20, timestamp=683793300))
        self.assertEqual(rows[2], Column(row_id=3, column_id=41, timestamp=683793385))
        self.assertEqual(rows[3], Column(row_id=10, column_id=10485760, timestamp=683793385))

    def test_column_equals(self):
        c1 = Column(row_id=1, column_id=100, timestamp=123456)        
        self.assertEqual(c1, c1)
        self.assertNotEqual(c1, True)
        
        c2 = Column(row_id=1, column_id=100, timestamp=123456)
        self.assertEqual(c1, c2)
        
        c3 = Column(row_key="one", column_key="one-thousand", timestamp=123456)
        self.assertNotEqual(c1, c3)

        c4 = Column(row_key="one", column_key="one-thousand", timestamp=123456)
        self.assertEqual(c3, c4)
        
        targetRepr = "Column(row_id=1, column_id=100, row_key='', column_key='', timestamp=123456)"
        self.assertEqual(targetRepr, repr(c1))
    
    def test_column_hash(self):
        c1 = Column(row_id=1, column_id=100, timestamp=123456)
        c2 = Column(row_id=1, column_id=100, timestamp=123456)
        self.assertEqual(hash(c1), hash(c2))


