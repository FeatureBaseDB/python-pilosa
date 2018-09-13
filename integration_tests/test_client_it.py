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
from pilosa.orm import Index, TimeQuantum, Schema, CacheType
from pilosa.imports import csv_column_reader, csv_field_value_reader, \
    csv_column_id_value, csv_column_key_value, csv_row_key_column_id


class ClientIT(unittest.TestCase):

    counter = 0

    def setUp(self):
        self.schema = Schema()
        self.index = self.schema.index(self.random_index_name())
        client = self.get_client()
        self.index.field("another-field")
        self.index.field("test")
        self.index.field("count-test")
        self.index.field("topn_test")

        self.col_index = self.schema.index(self.index.name + "-opts")
        self.field = self.col_index.field("collab")

        self.key_index = self.schema.index("key-index", keys=True)

        client.sync_schema(self.schema)

    def tearDown(self):
        client = self.get_client()
        client.delete_index(self.index)
        client.delete_index(self.col_index)
        client.delete_index(self.key_index)

    def test_create_field_with_time_quantum(self):
        field = self.index.field("field-with-timequantum", time_quantum=TimeQuantum.YEAR)
        client = self.get_client()
        client.ensure_field(field)
        schema = client.schema()
        # Check the field time quantum
        index = schema._indexes[self.index.name]
        field = index._fields["field-with-timequantum"]
        self.assertEquals(TimeQuantum.YEAR.value, field.time_quantum.value)

    def test_query(self):
        client = self.get_client()
        field = self.index.field("query-test")
        client.ensure_field(field)
        response = client.query(field.set(555, 10))
        self.assertTrue(response.result is not None)

    def test_query_with_columns(self):
        client = self.get_client()
        field = self.index.field("query-test")
        client.ensure_field(field)
        client.query(field.set(100, 1000))
        column_attrs = {"name": "bombo"}
        client.query(self.index.set_column_attrs(1000, column_attrs))
        response = client.query(field.row(100), column_attrs=True)
        self.assertTrue(response is not None)
        self.assertEquals(1000, response.column.id)
        self.assertEquals({"name": "bombo"}, response.column.attributes)

        response = client.query(field.row(300))
        self.assertTrue(response.column is None)

    def test_failed_connection(self):
        client = Client("http://non-existent-sub.pilosa.com:22222")
        self.assertRaises(PilosaError, client.query, self.field.set(15, 10))

    def test_parse_error(self):
        client = self.get_client()
        q = self.index.raw_query("SetBit(id=5, field=\"test\", col_id:=10)")
        self.assertRaises(PilosaError, client.query, q)

    def test_orm_count(self):
        client = self.get_client()
        count_field = self.index.field("count-test")
        client.ensure_field(count_field)
        qry = self.index.batch_query(
            count_field.set(10, 20),
            count_field.set(10, 21),
            count_field.set(15, 25))
        client.query(qry)
        response = client.query(self.index.count(count_field.row(10)))
        self.assertEquals(2, response.result.count)

    def test_new_orm(self):
        client = self.get_client()
        response1 = client.query(self.field.set(10, 20))
        self.assertTrue(response1.result.changed)
        response2 = client.query(self.field.row(10))
        self.assertEquals(0, len(response2.columns))
        row1 = response2.result.row
        self.assertEquals(0, len(row1.attributes))
        self.assertEquals(1, len(row1.columns))
        self.assertEquals(20, row1.columns[0])

        column_attrs = {"name": "bombo"}
        client.query(self.col_index.set_column_attrs(20, column_attrs))
        response3 = client.query(self.field.row(10), column_attrs=True)
        column = response3.column
        self.assertTrue(column is not None)
        self.assertEquals(20, column.id)

        row_attrs = {
            "active": True,
            "unsigned": 5,
            "height": 1.81,
            "name": "Mr. Pi"
        }
        client.query(self.field.set_row_attrs(10, row_attrs))
        response4 = client.query(self.field.row(10))
        row = response4.result.row
        self.assertEquals(1, len(row.columns))
        self.assertEquals(4, len(row.attributes))
        self.assertEquals(True, row.attributes["active"])
        self.assertEquals(5, row.attributes["unsigned"])
        self.assertEquals(1.81, row.attributes["height"])
        self.assertEquals("Mr. Pi", row.attributes["name"])

        response5 = client.query(self.field.clear(10, 20))
        self.assertTrue(response5.result.changed)
        response6 = client.query(self.field.row(10))
        row = response6.result.row
        self.assertEquals(0, len(row.columns))

    def test_topn(self):
        client = self.get_client()
        field = self.index.field("topn_test")
        client.query(self.index.batch_query(
            field.set(10, 5),
            field.set(10, 10),
            field.set(10, 15),
            field.set(20, 5),
            field.set(30, 5)))
        # XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
        client.http_request("POST", "/recalculate-caches")
        response = client.query(field.topn(2))
        items = response.result.count_items
        self.assertEquals(2, len(items))
        item = items[0]
        self.assertEquals(10, item.id)
        self.assertEquals(3, item.count)

        response = client.query(field.topn(5, row=field.row(10)))
        items = response.result.count_items
        self.assertEquals(3, len(items))
        item = items[0]
        self.assertEquals(3, item.count)

        client.query(field.set_row_attrs(10, {"foo": "bar"}))
        response = client.query(field.topn(5, None, "foo", "bar"))
        items = response.result.count_items
        self.assertEquals(1, len(items))
        item = items[0]
        self.assertEquals(3, item.count)
        self.assertEquals(10, item.id)

    def test_keys(self):
        client = self.get_client()
        field = self.key_index.field("keys-test", keys=True)
        client.ensure_field(field)

        client.query(field.set("stringRow", "stringCol"))
        response = client.query(field.row("stringRow"))
        self.assertEqual(["stringCol"], response.result.row.keys)

    def test_not_(self):
        client = self.get_client()
        schema = client.schema()
        index = schema.index("not-test", track_existence=True)
        field = index.field("f1")
        client.sync_schema(schema)
        try:
            client.query(index.batch_query(
                field.set(1, 10),
                field.set(1, 11),
                field.set(2, 11),
                field.set(2, 12),
                field.set(2, 13),
            ))
            resp = client.query(index.not_(field.row(1)))
            self.assertEqual([12, 13], resp.result.row.columns)
        finally:
            client.delete_index(index)

    def test_ensure_index_exists(self):
        client = self.get_client()
        index = Index(self.index.name + "-ensure")
        client.ensure_index(index)
        client.create_field(index.field("frm"))
        client.ensure_index(index)
        client.delete_index(index)

    def test_delete_field(self):
        client = self.get_client()
        field = self.index.field("to-delete")
        client.ensure_field(field)
        client.delete_field(field)
        # the following should succeed
        client.create_field(field)

    def test_field_for_nonexisting_index(self):
        client = self.get_client()
        index = Index("non-existing-database")
        field = index.field("frm")
        self.assertRaises(PilosaServerError, client.create_field, field)

    def test_csv_import(self):
        client = self.get_client()
        text = u"""
            10, 7
            10, 5
            2, 3
            7, 1
        """
        reader = csv_column_reader(StringIO(text))
        field = self.index.field("importfield")
        client.ensure_field(field)
        client.import_field(field, reader)
        bq = self.index.batch_query(
            field.row(2),
            field.row(7),
            field.row(10),
        )
        response = client.query(bq)
        target = [3, 1, 5]
        self.assertEqual(3, len(response.results))
        self.assertEqual(target, [result.row.columns[0] for result in response.results])

    def test_csv_import_row_keys(self):
        client = self.get_client()
        text = u"""
            ten, 7
            ten, 5
            two, 3
            seven, 1
        """
        reader = csv_column_reader(StringIO(text), formatfunc=csv_row_key_column_id)
        field = self.index.field("importfield-keys", keys=True)
        client.ensure_field(field)
        client.import_field(field, reader)
        bq = self.index.batch_query(
            field.row("two"),
            field.row("seven"),
            field.row("ten"),
        )
        response = client.query(bq)
        target = [3, 1, 5]
        self.assertEqual(3, len(response.results))
        self.assertEqual(target, [result.row.columns[0] for result in response.results])

    def test_csv_import_time_field(self):
        text = u"""
            1,10,683793200
            5,20,683793300
            3,41,683793385
            10,10485760,683793385
        """
        reader = csv_column_reader(StringIO(text))
        client = self.get_client()
        schema = client.schema()
        field = schema.index(self.index.name).field("importfield", time_quantum=TimeQuantum.YEAR_MONTH_DAY_HOUR)
        client.sync_schema(schema)
        client.import_field(field, reader)
        bq = self.index.batch_query(
            field.row(1),
            field.row(5),
            field.row(3),
            field.row(10)
        )
        response = client.query(bq)
        target = [10, 20, 41, 10485760]
        self.assertEqual(target, [result.row.columns[0] for result in response.results])

    def test_csv_value_import(self):
        text = u"""
            10, 7
            7, 1
        """
        reader = csv_field_value_reader(StringIO(text), formatfunc=csv_column_id_value)
        client = self.get_client()
        field = self.index.field("import-value-field", int_max=100)
        field2 = self.index.field("import-value-field-set")
        client.sync_schema(self.schema)
        bq = self.index.batch_query(
            field2.set(1, 10),
            field2.set(1, 7)
        )
        client.import_field(field, reader)
        client.query(bq)
        response = client.query(field.sum(field2.row(1)))
        self.assertEqual(8, response.result.value)

    def test_csv_value_import_column_keys(self):
        text = u"""
            ten, 7
            seven, 1
        """
        reader = csv_field_value_reader(StringIO(text), formatfunc=csv_column_key_value)
        client = self.get_client()
        field = self.key_index.field("import-value-field-keys", int_max=100)
        field2 = self.key_index.field("import-value-field-keys-set")
        client.sync_schema(self.schema)
        bq = self.key_index.batch_query(
            field2.set(1, "ten"),
            field2.set(1, "seven")
        )
        client.import_field(field, reader)
        client.query(bq)
        response = client.query(field.sum(field2.row(1)))
        self.assertEqual(8, response.result.value)

    def test_schema(self):
        client = self.get_client()
        schema = client.schema()
        self.assertGreaterEqual(len(schema._indexes), 1)
        self.assertGreaterEqual(len(schema._indexes[self.col_index.name]._fields), 1)
        field = self.index.field("schema-test-field",
                                 cache_type=CacheType.LRU,
                                 cache_size=9999)
        client.ensure_field(field)
        schema = client.schema()
        f = schema._indexes[self.index.name]._fields["schema-test-field"]
        self.assertEquals(CacheType.LRU, f.cache_type)
        self.assertEquals(9999, f.cache_size)

    def test_sync(self):
        client = self.get_client()
        remote_index = Index("remote-index-1")
        remote_field = remote_index.field("remote-field-1")
        schema1 = Schema()
        index11 = schema1.index("diff-index1")
        index11.field("field1-1")
        index11.field("field1-2")
        index12 = schema1.index("diff-index2")
        index12.field("field2-1")
        schema1.index(remote_index.name)
        try:
            client.ensure_index(remote_index)
            client.ensure_field(remote_field)
            client.sync_schema(schema1)
            # check that the schema was created
            schema2 = client.schema()
            self.assertTrue("remote-index-1" in schema2._indexes)
            self.assertTrue("remote-field-1" in schema2.index("remote-index-1")._fields)
            self.assertTrue("diff-index1" in schema2._indexes)
            self.assertTrue("field1-1" in schema2.index("diff-index1")._fields)
            self.assertTrue("field1-2" in schema2.index("diff-index1")._fields)
            self.assertTrue("diff-index2" in schema2._indexes)
            self.assertTrue("field2-1" in schema2.index("diff-index2")._fields)
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
        self.assertRaises(PilosaError, client.query, self.field.row(5))

    def test_failover_coordinator_fail(self):
        content = """
            {"state":"NORMAL","nodes":[{"id":"827c7196-8875-4467-bee2-3604a4346f2b","uri":{"scheme":"%(SCHEME)s","host":"nonexistent","port":%(PORT)s},"isCoordinator":true}],"localID":"827c7196-8875-4467-bee2-3604a4346f2b"}
        """
        server = MockServer(200, content=content, interpolate=True)
        with server:
            client = Client(server.uri)
            self.assertRaises(PilosaError, client.query, self.key_index.set_column_attrs("foo", {"foo": "bar"}))


    def test_range(self):
        from datetime import datetime
        client = self.get_client()
        field = self.col_index.field("test-range-field", time_quantum=TimeQuantum.MONTH_DAY_HOUR)
        client.ensure_field(field)
        client.query(self.col_index.batch_query(
            field.set(10, 100, timestamp=datetime(2017, 1, 1, 0, 0)),
            field.set(10, 100, timestamp=datetime(2018, 1, 1, 0, 0)),
            field.set(10, 100, timestamp=datetime(2019, 1, 1, 0, 0)),
        ))
        response = client.query(field.range(10, start=datetime(2017, 5, 1, 0, 0), end=datetime(2018, 5, 1, 0, 0)))
        self.assertEqual([100], response.result.row.columns)

    def test_range_field(self):
        client = self.get_client()
        field = self.col_index.field("rangefield", int_min=10, int_max=20)
        field2 = self.col_index.field("rangefield-set")
        client.ensure_field(field)
        client.ensure_field(field2)
        client.query(self.col_index.batch_query(
            field2.set(1, 10),
            field2.set(1, 100),
            field.setvalue(10, 11),
        ))
        response = client.query(field.sum(field2.row(1)))
        self.assertEquals(11, response.result.value)
        self.assertEquals(1, response.result.count)

        response = client.query(field.min(field2.row(1)))
        self.assertEquals(11, response.result.value)
        self.assertEquals(1, response.result.count)

        response = client.query(field.max(field2.row(1)))
        self.assertEquals(11, response.result.value)
        self.assertEquals(1, response.result.count)

        response = client.query(field.lt(15))
        self.assertEquals(1, len(response.results))
        self.assertEquals(10, response.result.row.columns[0])

    def test_exclude_attrs_columns(self):
        client = self.get_client()
        client.query(self.col_index.batch_query(
            self.field.set(1, 100),
            self.field.set_row_attrs(1, {"foo": "bar"})
        ))

        # test exclude columns.
        response = client.query(self.field.row(1), exclude_columns=True)
        self.assertEquals(0, len(response.result.row.columns))
        self.assertEquals(1, len(response.result.row.attributes))

        # test exclude attributes.
        response = client.query(self.field.row(1), exclude_attrs=True)
        self.assertEquals(1, len(response.result.row.columns))
        self.assertEquals(0, len(response.result.row.attributes))

    def test_http_request(self):
        self.get_client().http_request("GET", "/status")

    def test_fetch_coordinator_node(self):
        client = self.get_client()
        node = client._fetch_coordinator_node()
        uri = URI.address(self.get_server_address())
        self.assertEquals(uri.scheme, node.scheme)
        self.assertEquals(uri.host, node.host)
        self.assertEquals(uri.port, node.port)

    def test_fetch_coordinator_node_failure(self):
        server = MockServer(content=b'{"nodes":[]}')
        with server:
            client = Client(server.uri)
            self.assertRaises(PilosaError, client._fetch_coordinator_node)


    def test_shards(self):
        shard_width = 1048576
        client = self.get_client()
        client.query(self.col_index.batch_query(
            self.field.set(1, 100),
            self.field.set(1, shard_width),
            self.field.set(1, shard_width*3),
        ))

        response = client.query(self.field.row(1), shards=[0,3])
        self.assertEquals(2, len(response.result.row.columns))
        self.assertEquals(100, response.result.row.columns[0])
        self.assertEquals(shard_width*3, response.result.row.columns[1])

    def test_create_index_fail(self):
        server = MockServer(404)
        with server:
            client = Client(server.uri)
            self.assertRaises(PilosaServerError, client.create_index, self.index)

    def test_server_warning(self):
        headers = [
            ("warning", '''299 pilosa/2.0 "Deprecated PQL version: PQL v2 will remove support for SetBit() in Pilosa 2.1. Please update your client to support Set() (See https://docs.pilosa.com/pql#versioning)." "Sat, 25 Aug 2019 23:34:45 GMT"''')
        ]
        server = MockServer(200, headers=headers)
        with server:
            client = Client(server.uri)
            client.query(self.field.row(1))

    @classmethod
    def random_index_name(cls):
        cls.counter += 1
        return "testidx-%d" % cls.counter

    @classmethod
    def get_client(cls):
        server_address = cls.get_server_address()
        return Client(server_address, tls_skip_verify=True)

    @classmethod
    def get_server_address(cls):
        import os
        server_address = os.environ.get("PILOSA_BIND", "")
        if not server_address:
            server_address = "http://:10101"
        return server_address


class MockServer(threading.Thread):

    def __init__(self, status=200, headers=None, content="", interpolate=False):
        super(MockServer, self).__init__()
        self.stop_event = threading.Event()
        self.status = "%s STATUS" % status
        self.headers = headers or []
        self.content = content
        self.thread = None
        self.host = "localhost"
        self.port = 0
        self.daemon = True
        self.interpolate = interpolate

    def __enter__(self):
        import time
        self.start()
        while self.port == 0:
            # sleep a bit until finding out the actual port
            time.sleep(1)

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
            if self.interpolate:
                return self.content % {
                    "SCHEME": "http",
                    "HOST": self.host,
                    "PORT": self.port,
                }
            return self.content
        return app

    @property
    def uri(self):
        return URI(host=self.host, port=self.port)

    def run(self):
        server = make_server(self.host, self.port, self._app())
        self.port = server.server_port
        while not self._stopped():
            server.handle_request()
