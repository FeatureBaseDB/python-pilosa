import unittest

from pilosa.exceptions import PilosaError
from pilosa.internal import public_pb2 as internal
from pilosa.response import QueryResponse


class QueryResultTestCase(unittest.TestCase):

    def test_invalid_attr_type(self):
        qr = internal.QueryResponse()
        result1 = qr.Results.add()
        attr = result1.Bitmap.Attrs.add()
        attr.Key = "foo"
        attr.StringValue = "bar"
        attr.Type = 0
        bin = qr.SerializeToString()
        self.assertRaises(PilosaError, QueryResponse.from_protobuf, bin)


