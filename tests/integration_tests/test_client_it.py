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
        self.assertIsNotNone(response.result)

    def test_query_with_profiles(self):
        client = self.get_client()
        frame = self.db.frame("query-test")
        client.ensure_frame(frame)
        client.query(frame.setbit(100, 1000))
        profile_attrs = {"name": "bombo"}
        client.query(self.db.set_profile_attributes(1000, profile_attrs))
        response = client.query(frame.bitmap(100), profiles=True)
        self.assertIsNotNone(response)
        self.assertEquals(1000, response.profile.id)
        self.assertEquals({"name": "bombo"}, response.profile.attributes)

        response = client.query(frame.bitmap(300))
        self.assertIsNone(response.profile)

    def test_failed_connection(self):
        client = Client("http://non-existent-sub.pilosa.com:22222")
        self.assertRaises(PilosaError, client.query, self.frame.setbit(15, 10))

    @classmethod
    def random_database_name(cls):
        cls.counter += 1
        return "testdb-%d" % cls.counter

    @classmethod
    def get_client(cls):
        return Client(SERVER_ADDRESS)
