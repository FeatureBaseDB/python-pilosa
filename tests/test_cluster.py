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

    def test_execute_query_string(self):
        """
        Test pilosa works with both query string and Query object
        Need pilosa host running to run this test
        """
        db = 2
        c = Cluster()
        set_bit1 = SetBit(10, 'foo', 1).to_pql()
        set_bit2 = SetBit(20, 'foo', 2)
        set_response1 = c.execute(db, set_bit1)
        set_response2 = c.execute(db, set_bit2)
        bit_map1 = Bitmap(10, 'foo').to_pql()
        bit_map2 = Bitmap(20, 'foo')
        get_response1 = c.execute(db, bit_map1)
        get_response2 = c.execute(db, bit_map2)
        self.assertEqual(set_response1.status_code, 200)
        self.assertEqual(set_response2.status_code, 200)
        self.assertEqual(get_response1.status_code, 200)
        self.assertEqual(get_response1.json()['results'][0]['bits'], [1])
        self.assertEqual(get_response2.json()['results'][0]['bits'], [2])

    def test_invalid_query_input(self):
        db = 2
        c = Cluster()
        set_bit1 = SetBit(10, 'foo', 1).to_pql()
        set_bit2 = SetBit(20, 'foo', 2)
        with self.assertRaises(InvalidQuery):
            c.execute(db, [set_bit1, set_bit2])


    @patch('pilosa.cluster.KinesisEncoder.encode')
    @patch('pilosa.cluster.boto3')
    def test_kinesis_execute(self, boto3, encode):

        query = SetBit(10, 'foo', 1)
        query.IS_WRITE = True
        settings = dict(kinesis_firehose_stream='abc')
        c = Cluster(settings=settings)
        firehorse = Mock()
        boto3.client.return_value = firehorse
        c.execute(2, query)
        self.assertEqual(firehorse.put_record.call_count, 1)

if __name__ == '__main__':
    unittest.main()
