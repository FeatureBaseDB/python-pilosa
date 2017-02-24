import unittest
import datetime
from pilosa import SetBit, Bitmap, Union, Intersect, Difference, Count, TopN, Range, SetBitmapAttrs, ClearBit, SetProfileAttrs
from pilosa.query import InvalidQuery, _escape_string_value


class QueryTestCase(unittest.TestCase):

    def test_setbit(self):
        self.assertEqual(SetBit(1, 'foo', 2).to_pql(), 'SetBit(id=1, frame="foo", profileID=2)')

        with self.assertRaises(ValueError):
            SetBit('string', 'foo', 2).to_pql()

        with self.assertRaises(ValueError):
            SetBit(1, 'foo', 'string').to_pql()

    def test_clearbit(self):
        self.assertEqual(ClearBit(1, 'foo', 2).to_pql(), 'ClearBit(id=1, frame="foo", profileID=2)')

    def test_bitmap(self):
        self.assertEqual(Bitmap(1, 'foo').to_pql(), 'Bitmap(id=1, frame="foo")')

        with self.assertRaises(ValueError):
            Bitmap('abc', 'foo').to_pql()

    def test_union(self):
        self.assertEqual(Union(Bitmap(1, 'foo'), Bitmap(2, 'bar')).to_pql(), 'Union(Bitmap(id=1, frame="foo"), Bitmap(id=2, frame="bar"))')
        self.assertEqual(Union(Bitmap(1, 'foo'), Bitmap(2, 'bar'), Bitmap(3, 'bar')).to_pql(), 'Union(Bitmap(id=1, frame="foo"), Bitmap(id=2, frame="bar"), Bitmap(id=3, frame="bar"))')

    def test_intersect(self):
        self.assertEqual(Intersect(Bitmap(1, 'foo'), Bitmap(2, 'bar')).to_pql(), 'Intersect(Bitmap(id=1, frame="foo"), Bitmap(id=2, frame="bar"))')
        self.assertEqual(Intersect(Bitmap(1, 'foo'), Bitmap(2, 'bar'), Bitmap(3, 'bar')).to_pql(), 'Intersect(Bitmap(id=1, frame="foo"), Bitmap(id=2, frame="bar"), Bitmap(id=3, frame="bar"))')

    def test_difference(self):
        self.assertEqual(Difference(Bitmap(1, 'foo'), Bitmap(2, 'bar')).to_pql(), 'Difference(Bitmap(id=1, frame="foo"), Bitmap(id=2, frame="bar"))')

        with self.assertRaises(InvalidQuery):
            Difference(Bitmap(1, 'foo'), Bitmap(2, 'foo'), Bitmap(3, 'foo'))

    def test_count(self):
        self.assertEqual(Count(Bitmap(1, 'foo')).to_pql(), 'Count(Bitmap(id=1, frame="foo"))')

        with self.assertRaises(InvalidQuery):
            Count(Bitmap(1, 'foo'), Bitmap(2, 'foo'))

    def test_topn(self):
        self.assertEqual(TopN(None, 'foo').to_pql(), 'TopN(frame="foo", n=0)')
        self.assertEqual(TopN(Bitmap(1, 'foo'), 'bar', 20).to_pql(), 'TopN(Bitmap(id=1, frame="foo"), frame="bar", n=20)')
        self.assertEqual(TopN(None, 'bar', 20).to_pql(), 'TopN(frame="bar", n=20)')

        self.assertEqual(TopN(Bitmap(1, 'foo'), 'bar', 20, filter_field='category', filter_values=['good', 'bad', 'ugly']).to_pql(), 'TopN(Bitmap(id=1, frame="foo"), frame="bar", n=20, field="category", ["good","bad","ugly"])')

    def test_escape_string_value(self):
        self.assertEqual(_escape_string_value(1), '1')
        self.assertEqual(_escape_string_value('abc'), '"abc"')
        self.assertEqual(_escape_string_value(True), 'true')
        self.assertEqual(_escape_string_value(False), 'false')

    def test_range(self):
        start = datetime.datetime(1970, 1, 1, 0, 0)
        end = datetime.datetime(2000, 1, 2, 3, 4)
        self.assertEqual(Range(1, 'foo', start, end).to_pql(), 'Range(id=1, frame="foo", start="1970-01-01T00:00", end="2000-01-02T03:04")')

    def test_setbitmapattrs(self):
        self.assertEqual(SetBitmapAttrs(1, 'foo', cat=399).to_pql(), 'SetBitmapAttrs(id=1, frame="foo", cat=399)')
        self.assertEqual(SetBitmapAttrs(1, 'foo', foo="bar").to_pql(), 'SetBitmapAttrs(id=1, frame="foo", foo="bar")')
        self.assertEqual(SetBitmapAttrs(1, 'foo', x=True).to_pql(), 'SetBitmapAttrs(id=1, frame="foo", x=true)')
        # there's no guarantee the attrs are in this order, so checking both possibilities
        self.assertIn(SetBitmapAttrs(1, 'foo', foo="bar", xyz="abc").to_pql(),
            [
                'SetBitmapAttrs(id=1, frame="foo", foo="bar", xyz="abc")',
                'SetBitmapAttrs(id=1, frame="foo", xyz="abc", foo="bar")',
            ])
        
    def test_setprofileattrs(self):
        self.assertEqual(SetProfileAttrs(1, cat=399).to_pql(), 'SetProfileAttrs(id=1, cat=399)')
        self.assertEqual(SetProfileAttrs(1, foo="bar").to_pql(), 'SetProfileAttrs(id=1, foo="bar")')
        self.assertEqual(SetProfileAttrs(1, x=True).to_pql(), 'SetProfileAttrs(id=1, x=true)')
        # there's no guarantee the attrs are in this order, so checking both possibilities
        self.assertIn(SetProfileAttrs(1, foo="bar", xyz="abc").to_pql(),
            [
                'SetProfileAttrs(id=1, foo="bar", xyz="abc")',
                'SetProfileAttrs(id=1, xyz="abc", foo="bar")',
            ])

    def test_invalid_frame(self):
        with self.assertRaises(InvalidQuery):
            SetBit(1, '!@#$%', 1)
        with self.assertRaises(InvalidQuery):
            SetBit(1, '', 1)
        with self.assertRaises(InvalidQuery):
            SetBit(1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1)
        with self.assertRaises(InvalidQuery):
            SetBitmapAttrs(1, '!@#$%', key='value')
        with self.assertRaises(InvalidQuery):
            Bitmap(1, '!@#$%')
        start = datetime.datetime(1970, 1, 1, 0, 0)
        end = datetime.datetime(2000, 1, 2, 3, 4)
        with self.assertRaises(InvalidQuery):
            Range(1, '!@#$%', start, end)
        with self.assertRaises(InvalidQuery):
            TopN(None, '!@#$%')

if __name__ == '__main__':
    unittest.main()
