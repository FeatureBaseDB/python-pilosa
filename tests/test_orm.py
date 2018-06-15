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

from pilosa import PilosaError, Index, TimeQuantum, CacheType, ValidationError
from pilosa.orm import Schema

schema = Schema()
sampleIndex = schema.index("sample-db")
sampleFrame = sampleIndex.field("sample-field")
projectIndex = schema.index("project-db")
collabFrame = projectIndex.field("collaboration")


class SchemaTestCase(unittest.TestCase):

    def test_diff(self):
        schema1 = Schema()
        index11 = schema1.index("diff-index1")
        index11.field("field1-1")
        index11.field("field1-2")
        index12 = schema1.index("diff-index2")
        index12.field("field2-1")

        schema2 = Schema()
        index21 = schema2.index("diff-index1")
        index21.field("another-field")

        target_diff12 = Schema()
        target_index1 = target_diff12.index("diff-index1")
        target_index1.field("field1-1")
        target_index1.field("field1-2")
        target_index2 = target_diff12.index("diff-index2")
        target_index2.field("field2-1")

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
        index2 = schema.index("sample-index")
        self.assertEqual(index, index2)

        index = schema.index("sample-index2")
        self.assertEqual("sample-index2", index.name)

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
            "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
            q1.serialize())

        q2 = sampleIndex.union(b1, b2, b3)
        self.assertEquals(
            "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
            q2.serialize())

        q3 = sampleIndex.union(b1, b4)
        self.assertEquals(
            "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
            q3.serialize())

    def test_intersect(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleIndex.intersect(b1, b2)
        self.assertEquals(
            "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
            q1.serialize())

        q2 = sampleIndex.intersect(b1, b2, b3)
        self.assertEquals(
            "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
            q2.serialize())

        q3 = sampleIndex.intersect(b1, b4)
        self.assertEquals(
            "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
            q3.serialize())

    def test_difference(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleIndex.difference(b1, b2)
        self.assertEquals(
            "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
            q1.serialize())

        q2 = sampleIndex.difference(b1, b2, b3)
        self.assertEquals(
            "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
            q2.serialize())

        q3 = sampleIndex.difference(b1, b4)
        self.assertEquals(
            "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
            q3.serialize())

    def test_xor(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        q1 = sampleIndex.xor(b1, b2)

        self.assertEquals(
            "Xor(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
            q1.serialize())

    def test_union0(self):
        q = sampleIndex.union()
        self.assertEquals("Union()", q.serialize())

    def test_union1(self):
        q = sampleIndex.union(sampleFrame.bitmap(10))
        self.assertEquals("Union(Bitmap(row=10, field='sample-field'))", q.serialize())

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
            "Count(Bitmap(row=42, field='collaboration'))",
            q.serialize())

    def test_set_column_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "happy": True
        }
        q = projectIndex.set_column_attrs(5, attrs_map)
        self.assertEquals(
            u"SetColumnAttrs(col=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            q.serialize())

    def test_set_column_attributes_invalid_values(self):
        attrs_map = {
            "color": "blue",
            "dt": datetime.now()
        }
        self.assertRaises(PilosaError, projectIndex.set_column_attrs, 5, attrs_map)


class FrameTestCase(unittest.TestCase):

    def test_create_field(self):
        db = Index("foo")
        field = db.field("sample-field")
        self.assertEqual(db, field.index)
        self.assertEqual("sample-field", field.name)
        self.assertEqual(TimeQuantum.NONE, field.time_quantum)

    def test_same_equals(self):
        self.assertEqual(sampleFrame, sampleFrame)

    def test_other_class_not_equals(self):
        schema = Schema()
        self.assertNotEqual(sampleFrame, schema)

    def test_bitmap(self):
        qry1 = collabFrame.bitmap(5)
        self.assertEquals(
            "Bitmap(row=5, field='collaboration')",
            qry1.serialize())

        qry2 = collabFrame.bitmap(10)
        self.assertEquals(
            "Bitmap(row=10, field='collaboration')",
            qry2.serialize())

        qry3 = collabFrame.bitmap("b7feb014-8ea7-49a8-9cd8-19709161ab63")
        self.assertEquals(
            "Bitmap(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='collaboration')",
            qry3.serialize())

    def test_bitmap_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.bitmap, {})

    def test_setbit(self):
        qry1 = collabFrame.setbit(5, 10)
        self.assertEquals(
            "SetBit(row=5, field='collaboration', col=10)",
            qry1.serialize())

        qry2 = collabFrame.setbit(10, 20)
        self.assertEquals(
            "SetBit(row=10, field='collaboration', col=20)",

            qry2.serialize())

        qry3 = collabFrame.setbit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            "SetBit(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='collaboration', col='some_id')",
            qry3.serialize())

    def test_setbit_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.setbit, {}, 1)
        self.assertRaises(ValidationError, sampleFrame.setbit, 1, {})
        self.assertRaises(ValidationError, sampleFrame.setbit, 1, "zero")

    def test_setbit_with_timestamp(self):
        timestamp = datetime(2017, 4, 24, 12, 14)
        qry = collabFrame.setbit(10, 20, timestamp)
        self.assertEquals(
            "SetBit(row=10, field='collaboration', col=20, timestamp='2017-04-24T12:14')",
            qry.serialize()
        )

    def test_clearbit(self):
        qry1 = collabFrame.clearbit(5, 10)
        self.assertEquals(
            "ClearBit(row=5, field='collaboration', col=10)",
            qry1.serialize())

        qry2 = collabFrame.clearbit(10, 20)
        self.assertEquals(
            "ClearBit(row=10, field='collaboration', col=20)",
            qry2.serialize())

        qry3 = collabFrame.clearbit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            "ClearBit(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='collaboration', col='some_id')",
            qry3.serialize())

    def test_clearbit_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleFrame.clearbit, {}, 1)
        self.assertRaises(ValidationError, sampleFrame.clearbit, 1, {})
        self.assertRaises(ValidationError, sampleFrame.clearbit, 1, "zero")

    def test_topn(self):
        q1 = collabFrame.topn(27)
        self.assertEquals(
            "TopN(field='collaboration', n=27)",
            q1.serialize())

        q2 = collabFrame.topn(10, collabFrame.bitmap(3))
        self.assertEquals(
            u"TopN(Bitmap(row=3, field='collaboration'), field='collaboration', n=10)",
            q2.serialize())

        q3 = sampleFrame.topn(12, collabFrame.bitmap(7), "category", 80, 81)
        self.assertEquals(
            "TopN(Bitmap(row=7, field='collaboration'), field='sample-field', n=12, field='category', filters=[80,81])",
            q3.serialize())

    def test_range(self):
        start = datetime(1970, 1, 1, 0, 0)
        end = datetime(2000, 2, 2, 3, 4)

        q1 = collabFrame.range(10, start, end)
        self.assertEquals(
            "Range(row=10, field='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            q1.serialize())

        q3 = sampleFrame.range("b7feb014-8ea7-49a8-9cd8-19709161ab63", start, end)
        self.assertEquals(
            u"Range(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='sample-field', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            q3.serialize())

    def test_set_row_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "active": True
        }
        q = collabFrame.set_row_attrs(5, attrs_map)
        self.assertEquals(
            "SetRowAttrs(row=5, field='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            q.serialize())

    def test_field_lt(self):
        q = collabFrame.lt(10)
        self.assertEquals(
            "Range(collaboration < 10)",
            q.serialize())

    def test_field_lte(self):
        q = collabFrame.lte(10)
        self.assertEquals(
            "Range(collaboration <= 10)",
            q.serialize())

    def test_field_gt(self):
        q = collabFrame.gt(10)
        self.assertEquals(
            "Range(collaboration > 10)",
            q.serialize())

    def test_field_gte(self):
        q = collabFrame.gte(10)
        self.assertEquals(
            "Range(collaboration >= 10)",
            q.serialize())

    def test_field_equals(self):
        q = collabFrame.equals(10)
        self.assertEquals(
            "Range(collaboration == 10)",
            q.serialize())

    def test_field_not_equals(self):
        q = collabFrame.not_equals(10)
        self.assertEquals(
            "Range(collaboration != 10)",
            q.serialize())

    def test_field_not_null(self):
        q = collabFrame.not_null()
        self.assertEquals(
            "Range(collaboration != null)",
            q.serialize())

    def test_field_between(self):
        q = collabFrame.between(10, 20)
        self.assertEquals(
            "Range(collaboration >< [10,20])",
            q.serialize())

    def test_field_sum(self):
        q = collabFrame.sum(collabFrame.bitmap(10))
        self.assertEquals(
            "Sum(Bitmap(row=10, field='collaboration'), field='collaboration')",
            q.serialize())
        q = collabFrame.sum()
        self.assertEquals(
            "Sum(field='collaboration')",
            q.serialize())

    def test_field_set_value(self):
        q = collabFrame.set_value(10, 20)
        self.assertEquals(
            "SetValue(col=10, collaboration=20)",
            q.serialize())

    def test_get_options_string(self):
        field = sampleIndex.field("stargazer_id",
                                  time_quantum=TimeQuantum.DAY_HOUR,
                                  cache_type=CacheType.RANKED,
                                  cache_size=1000)
        target = '{"options": {"timeQuantum": "DH", "type": "time"}}'
        self.assertEquals(target, field._get_options_string())


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
        self.assertRaises(ValidationError, sampleIndex.field, "intminmax", int_min=10, int_max=9)