import logging
import unittest
from pilosa import Cluster, Bitmap, SetBit
from pilosa.query import InvalidQuery
from pilosa.cluster import DEFAULT_HOST
from mock import patch, Mock

logger = logging.getLogger(__name__)


class ClusterTestCase(unittest.TestCase):

    @patch('pilosa.cluster.requests.post')
    def test_execute(self, mock_post):
        db = 2
        c = Cluster()
        self.assertEqual(c.hosts, [DEFAULT_HOST])
        bit_map = Bitmap(10, 'foo')
        c.execute(db, bit_map)
        query = bit_map.to_pql()
        mock_post.assert_called_with('http://%s/query?db=%s' % (DEFAULT_HOST, db), data=query)
        with self.assertRaises(InvalidQuery):
            c.execute(2, [Bitmap(db, 'foo'), 'bar'])

    def test_invalid_query_input(self):
        db = 2
        c = Cluster()
        set_bit1 = SetBit(10, 'foo', 1).to_pql()
        set_bit2 = SetBit(20, 'foo', 2)
        with self.assertRaises(InvalidQuery):
            c.execute(db, [set_bit1, set_bit2])

if __name__ == '__main__':
    unittest.main()
