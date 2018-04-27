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
import threading
import unittest
from wsgiref.simple_server import make_server
from wsgiref.util import setup_testing_defaults

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

from pilosa.client import Client, URI, Cluster, PilosaServerError
from pilosa.exceptions import PilosaError
from pilosa.orm import Index, TimeQuantum, Schema, IntField, CacheType
from pilosa.imports import csv_bit_reader


class ClientIT(unittest.TestCase):

    counter = 0

    def setUp(self):
        schema = Schema()
        self.index = schema.index(self.random_index_name())
        client = self.get_client()
        self.index.frame("another-frame")
        self.index.frame("test")
        self.index.frame("count-test")
        self.index.frame("topn_test")

        self.col_index = schema.index(self.index.name + "-opts")
        self.frame = self.col_index.frame("collab")
        client.sync_schema(schema)

    def tearDown(self):
        client = self.get_client()
        client.delete_index(self.index)
        client.delete_index(self.col_index)

    def test_create_frame_with_time_quantum(self):
        frame = self.index.frame("frame-with-timequantum", time_quantum=TimeQuantum.YEAR)
        client = self.get_client()
        client.ensure_frame(frame)
        schema = client.schema()
        # Check the frame time quantum
        index = schema._indexes[self.index.name]
        frame = index._frames["frame-with-timequantum"]
        self.assertEquals(TimeQuantum.YEAR.value, frame.time_quantum.value)

    def test_query(self):
        client = self.get_client()
        frame = self.index.frame("query-test")
        client.ensure_frame(frame)
        response = client.query(frame.setbit(555, 10))
        self.assertTrue(response.result is not None)

    def test_query_with_columns(self):
        client = self.get_client()
        frame = self.index.frame("query-test")
        client.ensure_frame(frame)
        client.query(frame.setbit(100, 1000))
        column_attrs = {"name": "bombo"}
        client.query(self.index.set_column_attrs(1000, column_attrs))
        response = client.query(frame.bitmap(100), columns=True)
        self.assertTrue(response is not None)
        self.assertEquals(1000, response.column.id)
        self.assertEquals({"name": "bombo"}, response.column.attributes)

        response = client.query(frame.bitmap(300))
        self.assertTrue(response.column is None)

    def test_failed_connection(self):
        client = Client("http://non-existent-sub.pilosa.com:22222")
        self.assertRaises(PilosaError, client.query, self.frame.setbit(15, 10))

    def test_parse_error(self):
        client = self.get_client()
        q = self.index.raw_query("SetBit(id=5, frame=\"test\", col_id:=10)")
        self.assertRaises(PilosaError, client.query, q)

    def test_orm_count(self):
        client = self.get_client()
        count_frame = self.index.frame("count-test")
        client.ensure_frame(count_frame)
        qry = self.index.batch_query(
            count_frame.setbit(10, 20),
            count_frame.setbit(10, 21),
            count_frame.setbit(15, 25))
        client.query(qry)
        response = client.query(self.index.count(count_frame.bitmap(10)))
        self.assertEquals(2, response.result.count)

    def test_new_orm(self):
        client = self.get_client()
        client.query(self.frame.setbit(10, 20))
        response1 = client.query(self.frame.bitmap(10))
        self.assertEquals(0, len(response1.columns))
        bitmap1 = response1.result.bitmap
        self.assertEquals(0, len(bitmap1.attributes))
        self.assertEquals(1, len(bitmap1.bits))
        self.assertEquals(20, bitmap1.bits[0])

        column_attrs = {"name": "bombo"}
        client.query(self.col_index.set_column_attrs(20, column_attrs))
        response2 = client.query(self.frame.bitmap(10), columns=True)
        column = response2.column
        self.assertTrue(column is not None)
        self.assertEquals(20, column.id)

        bitmap_attrs = {
            "active": True,
            "unsigned": 5,
            "height": 1.81,
            "name": "Mr. Pi"
        }
        client.query(self.frame.set_row_attrs(10, bitmap_attrs))
        response3 = client.query(self.frame.bitmap(10))
        bitmap = response3.result.bitmap
        self.assertEquals(1, len(bitmap.bits))
        self.assertEquals(4, len(bitmap.attributes))
        self.assertEquals(True, bitmap.attributes["active"])
        self.assertEquals(5, bitmap.attributes["unsigned"])
        self.assertEquals(1.81, bitmap.attributes["height"])
        self.assertEquals("Mr. Pi", bitmap.attributes["name"])

    def test_topn(self):
        client = self.get_client()
        frame = self.index.frame("topn_test")
        client.query(self.index.batch_query(
            frame.setbit(10, 5),
            frame.setbit(10, 10),
            frame.setbit(10, 15),
            frame.setbit(20, 5),
            frame.setbit(30, 5)))
        # XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
        client.http_request("POST", "/recalculate-caches")
        response4 = client.query(frame.topn(2))
        items = response4.result.count_items
        self.assertEquals(2, len(items))
        item = items[0]
        self.assertEquals(10, item.id)
        self.assertEquals(3, item.count)

    def test_ensure_index_exists(self):
        client = self.get_client()
        index = Index(self.index.name + "-ensure")
        client.ensure_index(index)
        client.create_frame(index.frame("frm"))
        client.ensure_index(index)
        client.delete_index(index)

    def test_delete_frame(self):
        client = self.get_client()
        frame = self.index.frame("to-delete")
        client.ensure_frame(frame)
        client.delete_frame(frame)
        # the following should succeed
        client.create_frame(frame)

    def test_frame_for_nonexisting_index(self):
        client = self.get_client()
        index = Index("non-existing-database")
        frame = index.frame("frm")
        self.assertRaises(PilosaServerError, client.create_frame, frame)

    def test_csv_import(self):
        client = self.get_client()
        text = u"""
            10, 7
            10, 5
            2, 3
            7, 1
        """
        reader = csv_bit_reader(StringIO(text))
        frame = self.index.frame("importframe")
        client.ensure_frame(frame)
        client.import_frame(frame, reader)
        bq = self.index.batch_query(
            frame.bitmap(2),
            frame.bitmap(7),
            frame.bitmap(10),
        )
        response = client.query(bq)
        target = [3, 1, 5]
        self.assertEqual(3, len(response.results))
        self.assertEqual(target, [result.bitmap.bits[0] for result in response.results])

    def test_csv_import2(self):
        # Checks against encoding errors on Python 2.x
        text = u"""
            1,10,683793200
            5,20,683793300
            3,41,683793385        
            10,10485760,683793385        
        """
        reader = csv_bit_reader(StringIO(text))
        client = self.get_client()
        schema = client.schema()
        frame = schema.index(self.index.name).frame("importframe", time_quantum=TimeQuantum.YEAR_MONTH_DAY_HOUR)
        client.sync_schema(schema)
        client.import_frame(frame, reader)

    def test_schema(self):
        client = self.get_client()
        schema = client.schema()
        self.assertGreaterEqual(len(schema._indexes), 1)
        self.assertGreaterEqual(len(list(schema._indexes.values())[0]._frames), 1)
        frame = self.index.frame("schema-test-frame",
                                 cache_type=CacheType.LRU,
                                 cache_size=9999,
                                 inverse_enabled=True,
                                 time_quantum=TimeQuantum.YEAR_MONTH_DAY)
        client.ensure_frame(frame)
        schema = client.schema()
        f = schema._indexes[self.index.name]._frames["schema-test-frame"]
        self.assertEquals(CacheType.LRU, f.cache_type)
        self.assertEquals(9999, f.cache_size)
        self.assertEquals(True, f.inverse_enabled)
        self.assertEquals(TimeQuantum.YEAR_MONTH_DAY, f.time_quantum)

    def test_sync(self):
        client = self.get_client()
        remote_index = Index("remote-index-1")
        remote_frame = remote_index.frame("remote-frame-1")
        schema1 = Schema()
        index11 = schema1.index("diff-index1")
        index11.frame("frame1-1")
        index11.frame("frame1-2")
        index12 = schema1.index("diff-index2")
        index12.frame("frame2-1")
        schema1.index(remote_index.name)
        try:
            client.ensure_index(remote_index)
            client.ensure_frame(remote_frame)
            client.sync_schema(schema1)
            # check that the schema was created
            schema2 = client.schema()
            self.assertTrue("remote-index-1" in schema2._indexes)
            self.assertTrue("remote-frame-1" in schema2.index("remote-index-1")._frames)
            self.assertTrue("diff-index1" in schema2._indexes)
            self.assertTrue("frame1-1" in schema2.index("diff-index1")._frames)
            self.assertTrue("frame1-2" in schema2.index("diff-index1")._frames)
            self.assertTrue("diff-index2" in schema2._indexes)
            self.assertTrue("frame2-1" in schema2.index("diff-index2")._frames)
        finally:
            try:
                client.delete_index(remote_index)
                client.delete_index(index11)
                client.delete_index(index12)
            except PilosaError:
                pass

    def test_failover_fail(self):
        uris = [URI.address("nonexistent%s" % i) for i in range(20)]
        client = Client(Cluster(*uris))
        self.assertRaises(PilosaError, client.query, self.frame.bitmap(5))

    def test_range_frame(self):
        client = self.get_client()
        frame = self.col_index.frame("rangeframe", fields=[IntField.int("foo", 10, 20)])
        client.ensure_frame(frame)
        foo = frame.field("foo")
        client.query(self.col_index.batch_query(
            frame.setbit(1, 10),
            frame.setbit(1, 100),
            foo.set_value(10, 11),
            foo.set_value(100, 15),
        ))
        response = client.query(foo.sum(frame.bitmap(1)))
        self.assertEquals(26, response.result.value)
        self.assertEquals(2, response.result.count)

        response = client.query(foo.min(frame.bitmap(1)))
        self.assertEquals(11, response.result.value)
        self.assertEquals(1, response.result.count)

        response = client.query(foo.max(frame.bitmap(1)))
        self.assertEquals(15, response.result.value)
        self.assertEquals(1, response.result.count)

        response = client.query(foo.lt(15))
        self.assertEquals(1, len(response.results))
        self.assertEquals(10, response.result.bitmap.bits[0])

    def test_exclude_attrs_bits(self):
        client = self.get_client()
        client.query(self.col_index.batch_query(
            self.frame.setbit(1, 100),
            self.frame.set_row_attrs(1, {"foo": "bar"})
        ))

        # test exclude bits.
        response = client.query(self.frame.bitmap(1), exclude_bits=True)
        self.assertEquals(0, len(response.result.bitmap.bits))
        self.assertEquals(1, len(response.result.bitmap.attributes))

        # test exclude attributes.
        response = client.query(self.frame.bitmap(1), exclude_attrs=True)
        self.assertEquals(1, len(response.result.bitmap.bits))
        self.assertEquals(0, len(response.result.bitmap.attributes))

    def test_http_request(self):
        self.get_client().http_request("GET", "/status")

    def test_create_index_fail(self):
        server = MockServer(404)
        with server:
            client = Client(server.uri)
            self.assertRaises(PilosaServerError, client.create_index, self.index)


    @classmethod
    def random_index_name(cls):
        cls.counter += 1
        return "testidx-%d" % cls.counter

    @classmethod
    def get_client(cls):
        import os
        server_address = os.environ.get("PILOSA_BIND", "")
        if not server_address:
            server_address = "http://:10101"
        return Client(server_address, tls_skip_verify=True)


class MockServer(threading.Thread):

    def __init__(self, status=200, headers=None, content=""):
        super(MockServer, self).__init__()
        self.stop_event = threading.Event()
        self.status = "%s STATUS" % status
        self.headers = headers or []
        self.content = content
        self.thread = None
        self.host = "localhost"
        self.port = 15000
        self.daemon = True

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()

    def _stop(self):
        self.stop_event.set()

    def _stopped(self):
        return self.stop_event.is_set()

    def _app(self):
        def app(env, start_response):
            setup_testing_defaults(env)
            start_response(self.status, self.headers)
            return self.content
        return app

    @property
    def uri(self):
        return URI(host=self.host, port=self.port)

    def run(self):
        server = make_server(self.host, self.port, self._app())
        while not self._stopped():
            server.handle_request()

