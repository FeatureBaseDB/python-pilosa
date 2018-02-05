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

import unittest
from datetime import datetime

from pilosa import PilosaError, Index, TimeQuantum, CacheType, IntField, ValidationError
from pilosa.orm import Schema

schema = Schema()
sampleIndex = schema.index("sample-db")
sampleFrame = sampleIndex.frame("sample-frame")
projectIndex = schema.index("project-db", column_label="user")
collabFrame = projectIndex.frame("collaboration", row_label="project")


class SchemaTestCase(unittest.TestCase):

    def test_diff(self):
        schema1 = Schema()
        index11 = schema1.index("diff-index1")
        index11.frame("frame1-1")
        index11.frame("frame1-2")
        index12 = schema1.index("diff-index2")
        index12.frame("frame2-1")

        schema2 = Schema()
        index21 = schema2.index("diff-index1")
        index21.frame("another-frame")

        target_diff12 = Schema()
        target_index1 = target_diff12.index("diff-index1")
        target_index1.frame("frame1-1")
        target_index1.frame("frame1-2")
        target_index2 = target_diff12.index("diff-index2")
        target_index2.frame("frame2-1")

        diff12 = schema1._diff(schema2)
        self.assertEqual(target_diff12, diff12)

    def test_same_equals(self):
        schema = Schema()
        self.assertEqual(schema, schema)

    def test_other_class_not_equals(self):
        schema = Schema()
        self.assertNotEqual(schema, projectIndex)


class IndexTestCase(unittest.TestCase):

    def test_create_index(self):
        index = schema.index("sample-index")
        self.assertEqual("sample-index", index.name)
        self.assertEqual("columnID", index.column_label)
        self.assertEqual(TimeQuantum.NONE, index.time_quantum)
        index2 = schema.index("sample-index")
        self.assertEqual(index, index2)

        index = schema.index("sample-index2",
                             column_label="col_id",
                             time_quantum=TimeQuantum.YEAR_MONTH)
        self.assertEqual("sample-index2", index.name)
        self.assertEqual("col_id", index.column_label)
        self.assertEqual(TimeQuantum.YEAR_MONTH, index.time_quantum)

    def test_same_equals(self):
        self.assertEqual(projectIndex, projectIndex)

    def test_other_class_not_equals(self):
        schema = Schema()
        self.assertNotEqual(projectIndex, schema)

    def test_raw_query(self):
        q = projectIndex.raw_query("No validation whatsoever for raw queries")
        self.assertEquals(
            "No validation whatsoever for raw queries",
            q.serialize())

    def test_union(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleIndex.union(b1, b2)
        self.assertEquals(
            "Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
            q1.serialize())

        q2 = sampleIndex.union(b1, b2, b3)
        self.assertEquals(
            "Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
            q2.serialize())

        q3 = sampleIndex.union(b1, b4)
        self.assertEquals(
            "Union(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            q3.serialize())

    def test_intersect(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleIndex.intersect(b1, b2)
        self.assertEquals(
            "Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
            q1.serialize())

        q2 = sampleIndex.intersect(b1, b2, b3)
        self.assertEquals(
            "Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
            q2.serialize())

        q3 = sampleIndex.intersect(b1, b4)
        self.assertEquals(
            "Intersect(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            q3.serialize())

    def test_difference(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleIndex.difference(b1, b2)
        self.assertEquals(
            "Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
            q1.serialize())

        q2 = sampleIndex.difference(b1, b2, b3)
        self.assertEquals(
            "Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'), Bitmap(rowID=42, frame='sample-frame'))",
            q2.serialize())

        q3 = sampleIndex.difference(b1, b4)
        self.assertEquals(
            "Difference(Bitmap(rowID=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            q3.serialize())

    def test_xor(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        q1 = sampleIndex.xor(b1, b2)

        self.assertEquals(
            "Xor(Bitmap(rowID=10, frame='sample-frame'), Bitmap(rowID=20, frame='sample-frame'))",
            q1.serialize())

    def test_union0(self):
        q = sampleIndex.union()
        self.assertEquals("Union()", q.serialize())

    def test_union1(self):
        q = sampleIndex.union(sampleFrame.bitmap(10))
        self.assertEquals("Union(Bitmap(rowID=10, frame='sample-frame'))", q.serialize())

    def test_intersect_invalid_bitmap_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.intersect)

    def test_difference_invalid_bitmap_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.difference)

    def test_xor_invalid_bitmap_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.xor, sampleFrame.bitmap(10))

    def test_count(self):
        b = collabFrame.bitmap(42)
        q = projectIndex.count(b)
        self.assertEquals(
            "Count(Bitmap(project=42, frame='collaboration'))",
            q.serialize())

    def test_set_column_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "happy": True
        }
        q = projectIndex.set_column_attrs(5, attrs_map)
        self.assertEquals(
            "SetColumnAttrs(user=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            q.serialize())

    def test_set_column_attributes_invalid_values(self):
        attrs_map = {
            "color": "blue",
            "dt": datetime.now()
        }
        self.assertRaises(PilosaError, projectIndex.set_column_attrs, 5, attrs_map)


class FrameTestCase(unittest.TestCase):

    def test_create_frame(self):
        db = Index("foo")
        frame = db.frame("sample-frame")
        self.assertEqual(db, frame.index)
        self.assertEqual("sample-frame", frame.name)
        self.assertEqual("rowID", frame.row_label)
        self.assertEqual(TimeQuantum.NONE, frame.time_quantum)

    def test_same_equals(self):
        self.assertEqual(sampleFrame, sampleFrame)

    def test_other_class_not_equals(self):
        schema = Schema()
        self.assertNotEqual(sampleFrame, schema)

    def test_bitmap(self):
        qry1 = sampleFrame.bitmap(5)
        self.assertEquals(
            "Bitmap(rowID=5, frame='sample-frame')",
            qry1.serialize())

        qry2 = collabFrame.bitmap(10)
        self.assertEquals(
            "Bitmap(project=10, frame='collaboration')",
            qry2.serialize())

        qry3 = sampleFrame.bitmap("b7feb014-8ea7-49a8-9cd8-19709161ab63")
        self.assertEquals(
            "Bitmap(rowID='b7feb014-8ea7-49a8-9cd8-19709161ab63', frame='sample-frame')",
            qry3.serialize())

    def test_bitmap_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.bitmap, {})

    def test_inverse_bitmap(self):
        f1 = projectIndex.frame("f1-inversable", row_label="row_label", inverse_enabled=True)
        qry = f1.inverse_bitmap(5)
        self.assertEquals(
            "Bitmap(user=5, frame='f1-inversable')",
            qry.serialize())

        qry2 = f1.inverse_bitmap("b7feb014-8ea7-49a8-9cd8-19709161ab63")
        self.assertEquals(
            "Bitmap(user='b7feb014-8ea7-49a8-9cd8-19709161ab63', frame='f1-inversable')",
            qry2.serialize())

    def test_setbit(self):
        qry1 = sampleFrame.setbit(5, 10)
        self.assertEquals(
            "SetBit(rowID=5, frame='sample-frame', columnID=10)",
            qry1.serialize())

        qry2 = collabFrame.setbit(10, 20)
        self.assertEquals(
            "SetBit(project=10, frame='collaboration', user=20)",

            qry2.serialize())

        qry3 = sampleFrame.setbit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            "SetBit(rowID='b7feb014-8ea7-49a8-9cd8-19709161ab63', frame='sample-frame', columnID='some_id')",
            qry3.serialize())

    def test_setbit_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.setbit, {}, 1)
        self.assertRaises(ValidationError, sampleFrame.setbit, 1, {})
        self.assertRaises(ValidationError, sampleFrame.setbit, 1, "zero")

    def test_setbit_with_timestamp(self):
        timestamp = datetime(2017, 4, 24, 12, 14)
        qry = collabFrame.setbit(10, 20, timestamp)
        self.assertEquals(
            "SetBit(project=10, frame='collaboration', user=20, timestamp='2017-04-24T12:14')",
            qry.serialize()
        )

    def test_clearbit(self):
        qry1 = sampleFrame.clearbit(5, 10)
        self.assertEquals(
            "ClearBit(rowID=5, frame='sample-frame', columnID=10)",
            qry1.serialize())

        qry2 = collabFrame.clearbit(10, 20)
        self.assertEquals(
            "ClearBit(project=10, frame='collaboration', user=20)",
            qry2.serialize())

        qry3 = sampleFrame.clearbit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            "ClearBit(rowID='b7feb014-8ea7-49a8-9cd8-19709161ab63', frame='sample-frame', columnID='some_id')",
            qry3.serialize())

    def test_clearbit_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.clearbit, {}, 1)
        self.assertRaises(ValidationError, sampleFrame.clearbit, 1, {})
        self.assertRaises(ValidationError, sampleFrame.clearbit, 1, "zero")

    def test_topn(self):
        q1 = sampleFrame.topn(27)
        self.assertEquals(
            "TopN(frame='sample-frame', n=27, inverse=false)",
            q1.serialize())

        q2 = sampleFrame.topn(10, collabFrame.bitmap(3))
        self.assertEquals(
            "TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10, inverse=false)",
            q2.serialize())

        q3 = sampleFrame.topn(12, collabFrame.bitmap(7), "category", 80, 81)
        self.assertEquals(
            "TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=false, field='category', filters=[80,81])",
            q3.serialize())

        q4 = sampleFrame.inverse_topn(12, collabFrame.bitmap(7), "category", 80, 81)
        self.assertEquals(
            "TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, inverse=true, field='category', filters=[80,81])",
            q4.serialize())

    def test_range(self):
        start = datetime(1970, 1, 1, 0, 0)
        end = datetime(2000, 2, 2, 3, 4)

        q1 = collabFrame.range(10, start, end)
        self.assertEquals(
            u"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            q1.serialize())

        q2 = collabFrame.inverse_range(10, start, end)
        self.assertEquals(
            u"Range(user=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            q2.serialize())

        q3 = sampleFrame.range("b7feb014-8ea7-49a8-9cd8-19709161ab63", start, end)
        self.assertEquals(
            u"Range(rowID='b7feb014-8ea7-49a8-9cd8-19709161ab63', frame='sample-frame', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            q3.serialize())

    def test_set_row_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "active": True
        }
        q = collabFrame.set_row_attrs(5, attrs_map)
        self.assertEquals(
            "SetRowAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            q.serialize())

    def test_field(self):
        # only a single instance of a field should exist
        field1 = sampleFrame.field("the-field")
        field2 = sampleFrame.field("the-field")
        self.assertTrue(id(field1) == id(field2))

    def test_field_lt(self):
        q = sampleFrame.field("foo").lt(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo < 10)",
            q.serialize())

    def test_field_lte(self):
        q = sampleFrame.field("foo").lte(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo <= 10)",
            q.serialize())

    def test_field_gt(self):
        q = sampleFrame.field("foo").gt(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo > 10)",
            q.serialize())

    def test_field_gte(self):
        q = sampleFrame.field("foo").gte(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo >= 10)",
            q.serialize())

    def test_field_equals(self):
        q = sampleFrame.field("foo").equals(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo == 10)",
            q.serialize())

    def test_field_not_equals(self):
        q = sampleFrame.field("foo").not_equals(10)
        self.assertEquals(
            "Range(frame='sample-frame', foo != 10)",
            q.serialize())

    def test_field_not_null(self):
        q = sampleFrame.field("foo").not_null()
        self.assertEquals(
            "Range(frame='sample-frame', foo != null)",
            q.serialize())

    def test_field_between(self):
        q = sampleFrame.field("foo").between(10, 20)
        self.assertEquals(
            "Range(frame='sample-frame', foo >< [10,20])",
            q.serialize())

    def test_field_set_value(self):
        q = sampleFrame.field("foo").set_value(10, 20)
        self.assertEquals(
            "SetFieldValue(frame='sample-frame', columnID=10, foo=20)",
            q.serialize())

    def test_field_sum(self):
        q = sampleFrame.field("foo").sum(sampleFrame.bitmap(10))
        self.assertEquals(
            "Sum(Bitmap(rowID=10, frame='sample-frame'), frame='sample-frame', field='foo')",
            q.serialize())
        q = sampleFrame.field("foo").sum()
        self.assertEquals(
            "Sum(frame='sample-frame', field='foo')",
            q.serialize())

    def test_get_options_string(self):
        frame = sampleIndex.frame("stargazer_id",
                                  time_quantum=TimeQuantum.DAY_HOUR,
                                  inverse_enabled=True,
                                  cache_type=CacheType.RANKED,
                                  cache_size=1000,
                                  fields=[IntField.int("foo"), IntField.int("bar", min=-1, max=1)])
        target = '{"options": {"cacheSize": 1000, "cacheType": "ranked", "fields": [{"max": 100, "min": 0, "name": "foo", "type": "int"}, {"max": 1, "min": -1, "name": "bar", "type": "int"}], "inverseEnabled": true, "rangeEnabled": true, "rowLabel": "rowID", "timeQuantum": "DH"}}'
        self.assertEquals(target, frame._get_options_string())


class TimeQuantumTestCase(unittest.TestCase):

    def test_value(self):
        tq = TimeQuantum.YEAR_MONTH_DAY_HOUR
        self.assertEqual("YMDH", str(tq))

    def test_equality(self):
        self.assertTrue(TimeQuantum.YEAR_MONTH_DAY_HOUR == TimeQuantum.YEAR_MONTH_DAY_HOUR)
        self.assertFalse(TimeQuantum.YEAR_MONTH_DAY_HOUR == TimeQuantum.YEAR)
        self.assertFalse(TimeQuantum.YEAR_MONTH_DAY_HOUR == "YMDH")


class CacheTypeTestCase(unittest.TestCase):

    def test_value(self):
        ct = CacheType.LRU
        self.assertEqual("lru", str(ct))

    def test_equality(self):
        self.assertTrue(CacheType.RANKED == CacheType.RANKED)
        self.assertFalse(CacheType.RANKED == CacheType.LRU)
        self.assertFalse(CacheType.RANKED == "ranked")


class RangeFieldTestCase(unittest.TestCase):

    def test_min_greater_equals_max_fails(self):
        self.assertRaises(ValidationError, IntField.int, "foo", min=10, max=9)