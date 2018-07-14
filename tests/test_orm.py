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
sampleField = sampleIndex.field("sample-field")
projectIndex = schema.index("project-db")
collabField = projectIndex.field("collaboration")


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
        b1 = sampleField.row(10)
        b2 = sampleField.row(20)
        b3 = sampleField.row(42)
        b4 = collabField.row(2)

        q1 = sampleIndex.union(b1, b2)
        self.assertEquals(
            "Union(Row(sample-field=10), Row(sample-field=20))",
            q1.serialize())

        q2 = sampleIndex.union(b1, b2, b3)
        self.assertEquals(
            "Union(Row(sample-field=10), Row(sample-field=20), Row(sample-field=42))",
            q2.serialize())

        q3 = sampleIndex.union(b1, b4)
        self.assertEquals(
            "Union(Row(sample-field=10), Row(collaboration=2))",
            q3.serialize())

    def test_intersect(self):
        b1 = sampleField.row(10)
        b2 = sampleField.row(20)
        b3 = sampleField.row(42)
        b4 = collabField.row(2)

        q1 = sampleIndex.intersect(b1, b2)
        self.assertEquals(
            "Intersect(Row(sample-field=10), Row(sample-field=20))",
            q1.serialize())

        q2 = sampleIndex.intersect(b1, b2, b3)
        self.assertEquals(
            "Intersect(Row(sample-field=10), Row(sample-field=20), Row(sample-field=42))",
            q2.serialize())

        q3 = sampleIndex.intersect(b1, b4)
        self.assertEquals(
            "Intersect(Row(sample-field=10), Row(collaboration=2))",
            q3.serialize())

    def test_difference(self):
        b1 = sampleField.row(10)
        b2 = sampleField.row(20)
        b3 = sampleField.row(42)
        b4 = collabField.row(2)

        q1 = sampleIndex.difference(b1, b2)
        self.assertEquals(
            "Difference(Row(sample-field=10), Row(sample-field=20))",
            q1.serialize())

        q2 = sampleIndex.difference(b1, b2, b3)
        self.assertEquals(
            "Difference(Row(sample-field=10), Row(sample-field=20), Row(sample-field=42))",
            q2.serialize())

        q3 = sampleIndex.difference(b1, b4)
        self.assertEquals(
            "Difference(Row(sample-field=10), Row(collaboration=2))",
            q3.serialize())

    def test_xor(self):
        b1 = sampleField.row(10)
        b2 = sampleField.row(20)
        q1 = sampleIndex.xor(b1, b2)

        self.assertEquals(
            "Xor(Row(sample-field=10), Row(sample-field=20))",
            q1.serialize())

    def test_union0(self):
        q = sampleIndex.union()
        self.assertEquals("Union()", q.serialize())

    def test_union1(self):
        q = sampleIndex.union(sampleField.row(10))
        self.assertEquals("Union(Row(sample-field=10))", q.serialize())

    def test_intersect_invalid_row_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.intersect)

    def test_difference_invalid_row_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.difference)

    def test_xor_invalid_row_count_fails(self):
        self.assertRaises(PilosaError, projectIndex.xor, sampleField.row(10))

    def test_count(self):
        b = collabField.row(42)
        q = projectIndex.count(b)
        self.assertEquals(
            "Count(Row(collaboration=42))",
            q.serialize())

    def test_set_column_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "happy": True
        }
        q = projectIndex.set_column_attrs(5, attrs_map)
        self.assertEquals(
            u"SetColumnAttrs(5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            q.serialize())

    def test_set_column_attributes_invalid_values(self):
        attrs_map = {
            "color": "blue",
            "dt": datetime.now()
        }
        self.assertRaises(PilosaError, projectIndex.set_column_attrs, 5, attrs_map)


class FieldTestCase(unittest.TestCase):

    def test_create_field(self):
        db = Index("foo")
        field = db.field("sample-field")
        self.assertEqual(db, field.index)
        self.assertEqual("sample-field", field.name)
        self.assertEqual(TimeQuantum.NONE, field.time_quantum)

    def test_same_equals(self):
        self.assertEqual(sampleField, sampleField)

    def test_other_class_not_equals(self):
        schema = Schema()
        self.assertNotEqual(sampleField, schema)

    def test_row(self):
        qry1 = collabField.row(5)
        self.assertEquals(
            "Row(collaboration=5)",
            qry1.serialize())

        qry2 = collabField.row("b7feb014-8ea7-49a8-9cd8-19709161ab63")
        self.assertEquals(
            "Row(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
            qry2.serialize())

    def test_row_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleField.row, {})

    def test_set(self):
        qry1 = collabField.set(5, 10)
        self.assertEquals(
             u"Set(10, collaboration=5)",
            qry1.serialize())

        qry2 = collabField.set("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            u"Set(some_id, collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
            qry2.serialize())

    def test_set_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleField.set, {}, 1)
        self.assertRaises(ValidationError, sampleField.set, 1, {})
        self.assertRaises(ValidationError, sampleField.set, 1, "zero")

    def test_set_with_timestamp(self):
        timestamp = datetime(2017, 4, 24, 12, 14)
        qry = collabField.set(10, 20, timestamp)
        self.assertEquals(
            u"Set(20, collaboration=10, 2017-04-24T12:14)",
            qry.serialize()
        )

    def test_clear(self):
        qry1 = collabField.clear(5, 10)
        self.assertEquals(
            "Clear(10, collaboration=5)",
            qry1.serialize())

        qry2 = collabField.clear(10, 20)
        self.assertEquals(
            "Clear(20, collaboration=10)",
            qry2.serialize())

        qry3 = collabField.clear("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id")
        self.assertEquals(
            "Clear('some_id', collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
            qry3.serialize())

    def test_clear_with_invalid_id_type(self):
        self.assertRaises(ValidationError, sampleField.clear, {}, 1)
        self.assertRaises(ValidationError, sampleField.clear, 1, {})
        self.assertRaises(ValidationError, sampleField.clear, 1, "zero")

    def test_topn(self):
        q1 = collabField.topn(27)
        self.assertEquals(
            u"TopN(collaboration, n=27)",
            q1.serialize())

        q2 = collabField.topn(10, collabField.row(3))
        self.assertEquals(
            u"TopN(collaboration, Row(collaboration=3), n=10)",
            q2.serialize())

        q3 = sampleField.topn(12, collabField.row(7), "category", 80, 81)
        self.assertEquals(
            u"TopN(sample-field, Row(collaboration=7), n=12, field='category', filters=[80,81])",
            q3.serialize())

    def test_range(self):
        start = datetime(1970, 1, 1, 0, 0)
        end = datetime(2000, 2, 2, 3, 4)

        q1 = collabField.range(10, start, end)
        self.assertEquals(
            u"Range(collaboration=10, 1970-01-01T00:00, 2000-02-02T03:04)",
            q1.serialize())

        q3 = collabField.range("b7feb014-8ea7-49a8-9cd8-19709161ab63", start, end)
        self.assertEquals(
            u"Range(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63', 1970-01-01T00:00, 2000-02-02T03:04)",
            q3.serialize())

    def test_set_row_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "active": True
        }
        q = collabField.set_row_attrs(5, attrs_map)
        self.assertEquals(
            u'SetRowAttrs(collaboration, 5, active=true, quote="\\"Don\'t worry, be happy\\"")',
            q.serialize())

    def test_field_lt(self):
        q = collabField.lt(10)
        self.assertEquals(
            "Range(collaboration < 10)",
            q.serialize())

    def test_field_lte(self):
        q = collabField.lte(10)
        self.assertEquals(
            "Range(collaboration <= 10)",
            q.serialize())

    def test_field_gt(self):
        q = collabField.gt(10)
        self.assertEquals(
            "Range(collaboration > 10)",
            q.serialize())

    def test_field_gte(self):
        q = collabField.gte(10)
        self.assertEquals(
            "Range(collaboration >= 10)",
            q.serialize())

    def test_field_equals(self):
        q = collabField.equals(10)
        self.assertEquals(
            "Range(collaboration == 10)",
            q.serialize())

    def test_field_not_equals(self):
        q = collabField.not_equals(10)
        self.assertEquals(
            "Range(collaboration != 10)",
            q.serialize())

    def test_field_not_null(self):
        q = collabField.not_null()
        self.assertEquals(
            "Range(collaboration != null)",
            q.serialize())

    def test_field_between(self):
        q = collabField.between(10, 20)
        self.assertEquals(
            "Range(collaboration >< [10,20])",
            q.serialize())

    def test_field_sum(self):
        q = collabField.sum(collabField.row(10))
        self.assertEquals(
            "Sum(Row(collaboration=10), field='collaboration')",
            q.serialize())
        q = collabField.sum()
        self.assertEquals(
            "Sum(field='collaboration')",
            q.serialize())

    def test_field_set_value(self):
        q = collabField.setvalue(10, 20)
        self.assertEquals(
            "Set(10, collaboration=20)",
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