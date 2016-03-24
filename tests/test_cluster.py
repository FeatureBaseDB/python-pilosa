import unittest
from pilosa import Cluster, Bitmap
from pilosa.query import InvalidQuery


class ClusterTestCase(unittest.TestCase):

    def test_execute(self):
        c = Cluster()
        with self.assertRaises(InvalidQuery):
            c.execute(2, [Bitmap(2, 'foo'), 'bar'])


if __name__ == '__main__':
    unittest.main()
