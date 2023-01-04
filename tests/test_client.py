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

import logging
import unittest

import pilosa.internal.public_pb2 as internal
from pilosa import TimeQuantum, CacheType
from pilosa.client import Client, URI, Cluster, _QueryRequest, \
    decode_field_meta_options, _ImportRequest, _ImportValueRequest, _Node
from pilosa.exceptions import PilosaURIError, PilosaError
from pilosa.imports import Column, FieldValue

logger = logging.getLogger(__name__)


class ClientTestCase(unittest.TestCase):

    def test_create_client(self):
        # create default client
        c = Client()
        self.assertEquals(URI(), c.cluster.hosts[0][0])
        # create with cluster
        c = Client(Cluster(URI.address(":15000")))
        self.assertEquals(URI.address(":15000"), c.cluster.hosts[0][0])
        # create with URI
        c = Client(URI.address(":20000"))
        self.assertEquals(URI.address(":20000"), c.cluster.hosts[0][0])
        # create with invalid type
        self.assertRaises(PilosaError, Client, 15000)

    def test_decode_default_field_meta_options(self):
        field_info = {}
        options = decode_field_meta_options(field_info)
        target = {
            "cache_size": 50000,
            "cache_type": CacheType.DEFAULT,
            "time_quantum": TimeQuantum.NONE,
            "int_min": 0,
            "int_max": 0,
        }
        self.assertEquals(target, options)

    def test_timeout_stay_same_after_recreate(self):
        prev_client = Client()
        client_params = {}
        for k, v in prev_client.__dict__.items():
            # don't copy protected, private params
            if k.startswith("_"):
                continue
            # don't copy these
            if k in ["cluster", "logger"]:
                continue
            client_params[k] = v

        new_client = Client(**client_params)
        self.assertEquals(prev_client.connect_timeout, new_client.connect_timeout)
        self.assertEquals(prev_client.socket_timeout, new_client.socket_timeout)

class URITestCase(unittest.TestCase):

    def test_default(self):
        uri = URI()
        self.compare(uri, "http", "localhost", 10101)

    def test_full(self):
        uri = URI.address("http+protobuf://db1.pilosa.com:3333")
        self.compare(uri, "http+protobuf", "db1.pilosa.com", 3333)

    def test_host_port_alternative(self):
        uri = URI(host="db1.pilosa.com", port=3333)
        self.compare(uri, "http", "db1.pilosa.com", 3333)

    def test_full_with_ipv4_host(self):
        uri = URI.address("http+protobuf://192.168.1.26:3333")
        self.compare(uri, "http+protobuf", "192.168.1.26", 3333)

    def test_host_only(self):
        uri = URI.address("db1.pilosa.com")
        self.compare(uri, "http", "db1.pilosa.com", 10101)

    def test_port_only(self):
        uri = URI.address(":5888")
        self.compare(uri, "http", "localhost", 5888)

    def test_host_port(self):
        uri = URI.address("db1.big-data.com:5888")
        self.compare(uri, "http", "db1.big-data.com", 5888)

    def test_scheme_host(self):
        uri = URI.address("https://db1.big-data.com")
        self.compare(uri, "https", "db1.big-data.com", 10101)

    def test_scheme_port(self):
        uri = URI.address("https://:5553")
        self.compare(uri, "https", "localhost", 5553)

    def test_normalized_address(self):
        uri = URI.address("https+pb://big-data.pilosa.com:6888")
        self.assertEquals("https://big-data.pilosa.com:6888", uri._normalize())

        uri = URI.address("https://big-data.pilosa.com:6888")
        self.assertEquals("https://big-data.pilosa.com:6888", uri._normalize())

    def test_invalid_address(self):
        for address in ["foo:bar", "http://foo:", "http://foo:", "foo:", ":bar", "fd42:4201:f86b:7e09:216:3eff:fefa:ed80"]:
            self.assertRaises(PilosaURIError, URI.address, address)

    def test_ipv6(self):
        addresses = [
            ("[::1]", "http", "[::1]", 10101),
            ("[::1]:3333", "http", "[::1]", 3333),
            ("[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "http", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333),
            ("https://[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "https",
             "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333),
        ]
        for address, scheme, host, port in addresses:
            uri = URI.address(address)
            self.assertEquals(scheme, uri.scheme)
            self.assertEquals(host, uri.host)
            self.assertEquals(port, uri.port)


    def test_to_string(self):
        uri = URI()
        self.assertEquals("http://localhost:10101", "%s" % uri)

    def test_equals(self):
        uri1 = URI(host="pilosa.com", port=1337)
        uri2 = URI.address("http://pilosa.com:1337")
        self.assertTrue(uri1 == uri2)

    def test_equals_fails_with_other_object(self):
        self.assertFalse(URI() == "http://localhost:10101")

    def test_equals_same_object(self):
        uri = URI.address("https://pilosa.com:1337")
        self.assertEquals(uri, uri)

    def test_repr(self):
        uri = URI.address("https://pilosa.com:1337")
        self.assertEquals("<URI https://pilosa.com:1337>", repr(uri))

    def compare(self, uri, scheme, host, port):
        self.assertEquals(scheme, uri.scheme)
        self.assertEquals(host, uri.host)
        self.assertEquals(port, uri.port)


class ClusterTestCase(unittest.TestCase):

    def test_create_with_host(self):
        target = [(URI.address("http://localhost:3000"), True)]
        c = Cluster(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)

    def test_add_remove_host(self):
        target = [(URI.address("http://localhost:3000"), True)]
        c = Cluster()
        c.add_host(URI.address("http://localhost:3000"))
        # add the same host, the list of hosts should be the same
        c.add_host(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)
        target = [(URI.address("http://localhost:3000"), True), (URI(), True)]
        c.add_host(URI())
        self.assertEquals(target, c.hosts)
        target = [(URI.address("http://localhost:3000"), False), (URI(), True)]
        c.remove_host(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)

    def test_get_host(self):
        target1 = URI.address("db1.pilosa.com")
        target2 = URI.address("db2.pilosa.com")

        c = Cluster()
        c.add_host(URI.address("db1.pilosa.com"))
        c.add_host(URI.address("db2.pilosa.com"))
        addr = c.get_host()
        self.assertEquals(target1, addr)
        addr = c.get_host()
        self.assertEquals(target1, addr)
        c.get_host()
        c.remove_host(URI.address("db1.pilosa.com"))
        addr = c.get_host()
        self.assertEquals(target2, addr)

    def test_get_host_when_no_hosts(self):
        c = Cluster()
        self.assertRaises(PilosaError, c.get_host)

    def test_cluster_reset(self):
        hosts = [URI.address("db1.pilosa.com"), URI.address("db2.pilosa.com")]
        c = Cluster(*hosts)
        target1 = [(host, True) for host in hosts]
        self.assertEqual(target1, c.hosts)
        c.remove_host(URI.address("db1.pilosa.com"))
        c.remove_host(URI.address("db2.pilosa.com"))
        target2 = [(host, False) for host in hosts]
        self.assertEqual(target2, c.hosts)
        c._reset()
        self.assertEqual(target1, c.hosts)


class QueryRequestTestCase(unittest.TestCase):

    def test_serialize(self):
        qr = _QueryRequest("Row(field='foo', id=1)", column_attrs=True)
        bin = qr.to_protobuf(False)  # do not return a bytearray
        self.assertIsNotNone(bin)
        qr = internal.QueryRequest()
        qr.ParseFromString(bin)
        self.assertEquals("Row(field='foo', id=1)", qr.Query)
        self.assertEquals(True, qr.ColumnAttrs)


class ImportRequestTestCase(unittest.TestCase):

    def test_serialize_row_id_column_id(self):
        field = get_schema(False, False)
        ir = _ImportRequest(field, 0, [Column(row_id=1, column_id=2, timestamp=3)])
        bin = ir.to_protobuf(False)
        self.assertIsNotNone(bin)
        ir = internal.ImportRequest()
        ir.ParseFromString(bin)
        self.assertEquals("foo", ir.Index)
        self.assertEquals("bar", ir.Field)
        self.assertEquals([1], ir.RowIDs)
        self.assertEquals([2], ir.ColumnIDs)
        self.assertEquals([], ir.RowKeys)
        self.assertEquals([], ir.ColumnKeys)
        self.assertEquals([3], ir.Timestamps)

    def test_serialize_row_id_column_key(self):
        field = get_schema(True, False)
        ir = _ImportRequest(field, 0, [Column(row_id=1, column_key="two", timestamp=3)])
        bin = ir.to_protobuf(False)
        self.assertIsNotNone(bin)
        ir = internal.ImportRequest()
        ir.ParseFromString(bin)
        self.assertEquals("foo", ir.Index)
        self.assertEquals("bar", ir.Field)
        self.assertEquals([1], ir.RowIDs)
        self.assertEquals([], ir.ColumnIDs)
        self.assertEquals([], ir.RowKeys)
        self.assertEquals(["two"], ir.ColumnKeys)
        self.assertEquals([3], ir.Timestamps)

    def test_serialize_row_key_column_id(self):
        field = get_schema(False, True)
        ir = _ImportRequest(field, 0, [Column(row_key="one", column_id=2, timestamp=3)])
        bin = ir.to_protobuf(False)
        self.assertIsNotNone(bin)
        ir = internal.ImportRequest()
        ir.ParseFromString(bin)
        self.assertEquals("foo", ir.Index)
        self.assertEquals("bar", ir.Field)
        self.assertEquals([], ir.RowIDs)
        self.assertEquals([2], ir.ColumnIDs)
        self.assertEquals(["one"], ir.RowKeys)
        self.assertEquals([], ir.ColumnKeys)
        self.assertEquals([3], ir.Timestamps)

    def test_serialize_row_key_column_key(self):
        field = get_schema(True, True)
        ir = _ImportRequest(field, 0, [Column(row_key="one", column_key="two", timestamp=3)])
        bin = ir.to_protobuf(False)
        self.assertIsNotNone(bin)
        ir = internal.ImportRequest()
        ir.ParseFromString(bin)
        self.assertEquals("foo", ir.Index)
        self.assertEquals("bar", ir.Field)
        self.assertEquals([], ir.RowIDs)
        self.assertEquals([], ir.ColumnIDs)
        self.assertEquals(["one"], ir.RowKeys)
        self.assertEquals(["two"], ir.ColumnKeys)
        self.assertEquals([3], ir.Timestamps)

    def test_import_request_invalid_format(self):
        field = get_schema(False, False)
        ir = _ImportRequest(field, 0, [Column(row_key="one", column_key="two", timestamp=3)])
        ir.format = None
        self.assertRaises(PilosaError, ir.to_protobuf, False)


class ImportValueRequestTestCase(unittest.TestCase):

    def test_invalid_format(self):
        field = get_schema(True, True)
        ir = _ImportValueRequest(field, 0, [FieldValue(column_key="foo", value=3)])
        ir.format = "invalid-format"
        self.assertRaises(PilosaError, ir.to_protobuf, False)


class NodeTestCase(unittest.TestCase):

    def test_node_url(self):
        n1 = _Node("https", "foo.com", "")
        self.assertEquals("https://foo.com", n1.url)
        n2 = _Node("https", "foo.com", 9999)
        self.assertEquals("https://foo.com:9999", n2.url)


def get_schema(index_keys, field_keys):
    from pilosa.orm import Schema
    schema = Schema()
    index = schema.index("foo", keys=index_keys)
    field = index.field("bar", keys=field_keys)
    return field



if __name__ == '__main__':
    unittest.main()
