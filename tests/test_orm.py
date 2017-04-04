import unittest
from datetime import datetime

from pilosa.exceptions import PilosaError
from pilosa.orm import Database, DatabaseOptions, FrameOptions, TimeQuantum

sampleDb = Database("sample-db")
sampleFrame = sampleDb.frame("sample-frame")
projectDb = Database("project-db",
                     DatabaseOptions(column_label="user"))
collabFrame = projectDb.frame("collaboration",
                              FrameOptions(row_label="project"))


class DatabaseTestCase(unittest.TestCase):

    def test_create_database(self):
        database = Database("sample-db")
        self.assertEqual("sample-db", database.name)
        self.assertEqual("col_id", database.options.column_label)
        self.assertEqual(TimeQuantum.NONE, database.options.time_quantum)

        options = DatabaseOptions(column_label="profile_id", time_quantum=TimeQuantum.YEAR_MONTH)
        database = Database("sample-db", options)
        self.assertEqual("sample-db", database.name)
        self.assertEqual("profile_id", database.options.column_label)
        self.assertEqual(TimeQuantum.YEAR_MONTH, database.options.time_quantum)

    def test_raw_query(self):
        q = projectDb.raw_query("No validation whatsoever for raw queries")
        self.assertEquals(
            "No validation whatsoever for raw queries",
            str(q))

    def test_union(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleDb.union(b1, b2)
        self.assertEquals(
            "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
            str(q1))

        q2 = sampleDb.union(b1, b2, b3)
        self.assertEquals(
            "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
            str(q2))

        q3 = sampleDb.union(b1, b4)
        self.assertEquals(
            "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            str(q3))

    def test_intersect(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleDb.intersect(b1, b2)
        self.assertEquals(
            "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
            str(q1))

        q2 = sampleDb.intersect(b1, b2, b3)
        self.assertEquals(
            "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
            str(q2))

        q3 = sampleDb.intersect(b1, b4)
        self.assertEquals(
            "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            str(q3))

    def test_difference(self):
        b1 = sampleFrame.bitmap(10)
        b2 = sampleFrame.bitmap(20)
        b3 = sampleFrame.bitmap(42)
        b4 = collabFrame.bitmap(2)

        q1 = sampleDb.difference(b1, b2)
        self.assertEquals(
            "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
            str(q1))

        q2 = sampleDb.difference(b1, b2, b3)
        self.assertEquals(
            "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
            str(q2))

        q3 = sampleDb.difference(b1, b4)
        self.assertEquals(
            "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
            str(q3))

    def test_union_invalid_bitmap_count_fails(self):
        self.assertRaises(PilosaError, projectDb.union)

    def test_count(self):
        b = collabFrame.bitmap(42)
        q = projectDb.count(b)
        self.assertEquals(
            "Count(Bitmap(project=42, frame='collaboration'))",
            str(q))


class FrameTestCase(unittest.TestCase):

    def test_create_frame(self):
        db = Database("foo")
        frame = db.frame("sample-frame")
        self.assertEqual(db, frame.database)
        self.assertEqual("sample-frame", frame.name)
        self.assertEqual("id", frame.options.row_label)
        self.assertEqual(TimeQuantum.NONE, frame.options.time_quantum)

    def test_bitmap(self):
        qry1 = sampleFrame.bitmap(5)
        self.assertEquals(
            "Bitmap(id=5, frame='sample-frame')",
            str(qry1))

        qry2 = collabFrame.bitmap(10)
        self.assertEquals(
            "Bitmap(project=10, frame='collaboration')",
            str(qry2))

    def test_setbit(self):
        qry1 = sampleFrame.setbit(5, 10)
        self.assertEquals(
            "SetBit(id=5, frame='sample-frame', col_id=10)",
            str(qry1))

        qry2 = collabFrame.setbit(10, 20)
        self.assertEquals(
            "SetBit(project=10, frame='collaboration', user=20)",
            str(qry2))

    def test_clearbit(self):
        qry1 = sampleFrame.clearbit(5, 10)
        self.assertEquals(
            "ClearBit(id=5, frame='sample-frame', col_id=10)",
            str(qry1))

        qry2 = collabFrame.clearbit(10, 20)
        self.assertEquals(
            "ClearBit(project=10, frame='collaboration', user=20)",
            str(qry2))

    def test_topn(self):
        q1 = sampleFrame.topn(27)
        self.assertEquals(
            "TopN(frame='sample-frame', n=27)",
            str(q1))

        q2 = sampleFrame.topn(10, collabFrame.bitmap(3))
        self.assertEquals(
            "TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10)",
            str(q2))

        q3 = sampleFrame.topn(12, collabFrame.bitmap(7), "category", 80, 81)
        self.assertEquals(
            "TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, field='category', [80,81])",
            str(q3))

    def test_range(self):
        start = datetime(1970, 1, 1, 0, 0)
        end = datetime(2000, 2, 2, 3, 4)
        q = collabFrame.range(10, start, end)
        self.assertEquals(
            "Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
            str(q))

    def test_set_bitmap_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "active": True
        }
        q = collabFrame.set_bitmap_attributes(5, attrs_map)
        self.assertEquals(
            "SetBitmapAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            str(q))

    def test_set_profile_attributes(self):
        attrs_map = {
            "quote": '''"Don't worry, be happy"''',
            "happy": True
        }
        q = projectDb.set_profile_attributes(5, attrs_map)
        self.assertEquals(
            "SetProfileAttrs(user=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
            str(q))

    def test_set_profile_attributes_invalid_values(self):
        attrs_map = {
            "color": "blue",
            "dt": datetime.now()
        }
        self.assertRaises(PilosaError, projectDb.set_profile_attributes, 5, attrs_map)


class DatabaseOptionsTestCase(unittest.TestCase):

    def test_create_database_options(self):
        options = DatabaseOptions()
        self.assertEqual("col_id", options.column_label)
        self.assertEqual(TimeQuantum.NONE, options.time_quantum)

        options = DatabaseOptions(column_label="some_label", time_quantum=TimeQuantum.DAY)
        self.assertEqual("some_label", options.column_label)
        self.assertEqual(TimeQuantum.DAY, options.time_quantum)


class FrameOptionsTestCase(unittest.TestCase):

    def test_create_frame_options(self):
        options = FrameOptions()
        self.assertEqual("id", options.row_label)
        self.assertEqual(TimeQuantum.NONE, options.time_quantum)


class TimeQuantumTestCase(unittest.TestCase):

    def test_value(self):
        tq = TimeQuantum.YEAR_MONTH_DAY_HOUR
        self.assertEqual("YMDH", str(tq))

