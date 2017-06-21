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

import time
import unittest

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

from pilosa.client import Client
from pilosa.exceptions import PilosaError
from pilosa.orm import Index, TimeQuantum
from pilosa.imports import csv_bit_reader

SERVER_ADDRESS = ":10101"


class ClientIT(unittest.TestCase):

    counter = 0

    def setUp(self):
        self.db = Index(self.random_index_name())
        client = self.get_client()
        client.create_index(self.db)
        client.create_frame(self.db.frame("another-frame"))
        client.create_frame(self.db.frame("test"))
        client.create_frame(self.db.frame("count-test"))
        client.create_frame(self.db.frame("topn_test"))

        self.col_db = Index(self.db.name + "-opts", column_label="user")
        client.create_index(self.col_db)

        self.frame = self.col_db.frame("collab", row_label="project")
        client.create_frame(self.frame)

    def tearDown(self):
        client = self.get_client()
        client.delete_index(self.db)
        client.delete_index(self.col_db)

    def test_create_index_with_time_quantum(self):
        db = Index("db-with-timequantum", time_quantum=TimeQuantum.YEAR)
        client = self.get_client()
        client.ensure_index(db)
        client.delete_index(db)

    def test_create_frame_with_time_quantum(self):
        frame = self.db.frame("frame-with-timequantum", time_quantum=TimeQuantum.YEAR)
        client = self.get_client()
        client.ensure_frame(frame)

    def test_query(self):
        client = self.get_client()
        frame = self.db.frame("query-test")
        client.ensure_frame(frame)
        response = client.query(frame.setbit(555, 10))
        self.assertTrue(response.result is not None)

    def test_query_with_columns(self):
        client = self.get_client()
        frame = self.db.frame("query-test")
        client.ensure_frame(frame)
        client.query(frame.setbit(100, 1000))
        column_attrs = {"name": "bombo"}
        client.query(self.db.set_column_attrs(1000, column_attrs))
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
        q = self.db.raw_query("SetBit(id=5, frame=\"test\", col_id:=10)")
        self.assertRaises(PilosaError, client.query, q)

    def test_orm_count(self):
        client = self.get_client()
        count_frame = self.db.frame("count-test")
        client.ensure_frame(count_frame)
        qry = self.db.batch_query(
            count_frame.setbit(10, 20),
            count_frame.setbit(10, 21),
            count_frame.setbit(15, 25))
        client.query(qry)
        response = client.query(self.db.count(count_frame.bitmap(10)))
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
        client.query(self.col_db.set_column_attrs(20, column_attrs))
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
        frame = self.db.frame("topn_test")
        client.query(self.db.batch_query(
            frame.setbit(10, 5),
            frame.setbit(10, 10),
            frame.setbit(10, 15),
            frame.setbit(20, 5),
            frame.setbit(30, 5)))
        # XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
        time.sleep(10)
        response4 = client.query(frame.topn(2))
        items = response4.result.count_items
        self.assertEquals(2, len(items))
        item = items[0]
        self.assertEquals(10, item.id)
        self.assertEquals(3, item.count)

    def test_ensure_index_exists(self):
        client = self.get_client()
        db = Index(self.db.name + "-ensure")
        client.ensure_index(db)
        client.create_frame(db.frame("frm"))
        client.ensure_index(db)
        client.delete_index(db)

    def test_delete_frame(self):
        client = self.get_client()
        frame = self.db.frame("to-delete")
        client.ensure_frame(frame)
        client.delete_frame(frame)
        # the following should succeed
        client.create_frame(frame)

    def test_frame_for_nonexisting_index(self):
        client = self.get_client()
        index = Index("non-existing-database")
        frame = index.frame("frm")
        self.assertRaises(PilosaError, client.create_frame, frame)

    def test_csv_import(self):
        client = self.get_client()
        text = u"""
            10, 7
            10, 5
            2, 3
            7, 1
        """
        reader = csv_bit_reader(StringIO(text))
        frame = self.db.frame("importframe")
        client.ensure_frame(frame)
        client.import_frame(frame, reader)
        bq = self.db.batch_query(
            frame.bitmap(2),
            frame.bitmap(7),
            frame.bitmap(10),
        )
        response = client.query(bq)
        target = [3, 1, 5]
        self.assertEqual(3, len(response.results))
        self.assertEqual(target, [result.bitmap.bits[0] for result in response.results])

    @classmethod
    def random_index_name(cls):
        cls.counter += 1
        return "testdb-%d" % cls.counter

    @classmethod
    def get_client(cls):
        return Client(SERVER_ADDRESS)
