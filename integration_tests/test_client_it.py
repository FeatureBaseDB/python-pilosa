import unittest

from pilosa.client import Client
from pilosa.exceptions import PilosaError
from pilosa.orm import Database, TimeQuantum

SERVER_ADDRESS = ":10101"


class ClientIT(unittest.TestCase):

    counter = 0

    def setUp(self):
        self.db = Database(self.random_database_name())
        client = self.get_client()
        client.create_database(self.db)
        client.create_frame(self.db.frame("another-frame"))
        client.create_frame(self.db.frame("test"))
        client.create_frame(self.db.frame("count-test"))
        client.create_frame(self.db.frame("topn_test"))

        self.col_db = Database(self.db.name + "-opts", column_label="user")
        client.create_database(self.col_db)

        self.frame = self.col_db.frame("collab", row_label="project")
        client.create_frame(self.frame)

    def tearDown(self):
        client = self.get_client()
        client.delete_database(self.db)
        client.delete_database(self.col_db)

    def test_create_database_with_time_quantum(self):
        db = Database("db-with-timequantum", time_quantum=TimeQuantum.YEAR)
        client = self.get_client()
        client.ensure_database(db)
        client.delete_database(db)

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

    def test_query_with_profiles(self):
        client = self.get_client()
        frame = self.db.frame("query-test")
        client.ensure_frame(frame)
        client.query(frame.setbit(100, 1000))
        profile_attrs = {"name": "bombo"}
        client.query(self.db.set_column_attrs(1000, profile_attrs))
        response = client.query(frame.bitmap(100), profiles=True)
        self.assertTrue(response is not None)
        self.assertEquals(1000, response.profile.id)
        self.assertEquals({"name": "bombo"}, response.profile.attributes)

        response = client.query(frame.bitmap(300))
        self.assertTrue(response.profile is None)

    def test_failed_connection(self):
        client = Client("http://non-existent-sub.pilosa.com:22222")
        self.assertRaises(PilosaError, client.query, self.frame.setbit(15, 10))

    def test_parse_error(self):
        client = self.get_client()
        q = self.db.raw_query("SetBit(id=5, frame=\"test\", profileID:=10)")
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
        self.assertEquals(0, len(response1.profiles))
        bitmap1 = response1.result.bitmap
        self.assertEquals(0, len(bitmap1.attributes))
        self.assertEquals(1, len(bitmap1.bits))
        self.assertEquals(20, bitmap1.bits[0])

        profile_attrs = {"name": "bombo"}
        client.query(self.col_db.set_column_attrs(20, profile_attrs))
        response2 = client.query(self.frame.bitmap(10), profiles=True)
        profile = response2.profile
        self.assertTrue(profile is not None)
        self.assertEquals(20, profile.id)

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

        topn_frame = self.db.frame("topn_test")
        client.query(topn_frame.setbit(155, 551))
        response4 = client.query(topn_frame.topn(1))
        items = response4.result.count_items
        self.assertEquals(1, len(items))
        item = items[0]
        self.assertEquals(155, item.id)
        self.assertEquals(1, item.count)

    def test_ensure_database_exists(self):
        client = self.get_client()
        db = Database(self.db.name + "-ensure")
        client.ensure_database(db)
        client.create_frame(db.frame("frm"))
        client.ensure_database(db)
        client.delete_database(db)

    def test_delete_frame(self):
        client = self.get_client()
        frame = self.db.frame("to-delete")
        client.ensure_frame(frame)
        client.delete_frame(frame)
        # the following should succeed
        client.create_frame(frame)

    def test_frame_for_nonexisting_database(self):
        client = self.get_client()
        db = Database("non-existing-database")
        frame = db.frame("frm")
        self.assertRaises(PilosaError, client.create_frame, frame)

    @classmethod
    def random_database_name(cls):
        cls.counter += 1
        return "testdb-%d" % cls.counter

    @classmethod
    def get_client(cls):
        return Client(SERVER_ADDRESS)
