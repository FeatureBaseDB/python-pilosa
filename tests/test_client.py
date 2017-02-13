import logging
import unittest
from pilosa import Client, Bitmap, SetBit
from pilosa.query import InvalidQuery
from pilosa.client import DEFAULT_HOST
from pilosa.version import get_version
from mock import patch, Mock

logger = logging.getLogger(__name__)


class ClientTestCase(unittest.TestCase):

    @patch('pilosa.client.requests.post')
    def test_query(self, mock_post):
        db = 2
        c = Client()
        self.assertEqual(c.hosts, [DEFAULT_HOST])
        bit_map = Bitmap(10, 'foo')
        c.query(db, bit_map)
        query = bit_map.to_pql()
        mock_post.assert_called_with('http://%s/query?db=%s' % (DEFAULT_HOST, db), data=query, headers={'Content-Type': 'application/vnd.pilosa.pql.v1', 'Accept': 'application/vnd.pilosa.json.v1', 'User-Agent': 'pilosa-driver/' + get_version()})
        with self.assertRaises(InvalidQuery):
            c.query(2, [Bitmap(db, 'foo'), 'bar'])

    def test_invalid_query_input(self):
        db = 2
        c = Client()
        set_bit1 = SetBit(10, 'foo', 1).to_pql()
        set_bit2 = SetBit(20, 'foo', 2)
        with self.assertRaises(InvalidQuery):
            c.query(db, [set_bit1, set_bit2])

if __name__ == '__main__':
    unittest.main()
