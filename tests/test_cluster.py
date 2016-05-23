import logging
import unittest
from pilosa import Cluster, Bitmap, PilosaSettings, SetBit
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

    @patch('pilosa.cluster.KinesisEncoder.encode')
    @patch('pilosa.cluster.boto3')
    def test_kinesis_execute(self, boto3, encode):

        query = SetBit(10, 'foo', 1)
        query.IS_WRITE = True
        settings = PilosaSettings(kinesis_firehose_stream='abc')
        c = Cluster(settings=settings)
        firehorse = Mock()
        boto3.client.return_value = firehorse
        c.execute(2, query)
        self.assertEqual(firehorse.put_record.call_count, 1)

if __name__ == '__main__':
    unittest.main()
