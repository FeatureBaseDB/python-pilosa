import unittest

from pilosa.client import Client, Cluster, URI
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

# """
#     @Test
#     public void queryWithProfilesTest() throws IOException {
#         try (PilosaClient client = getClient()) {
#             Frame frame = this.db.frame("query-test");
#             client.ensureFrame(frame);
#             client.query(frame.setBit(100, 1000));
#             Map<String, Object> profileAttrs = new HashMap<>(1);
#             profileAttrs.put("name", "bombo");
#             client.query(this.db.setProfileAttrs(1000, profileAttrs));
#             QueryOptions queryOptions = QueryOptions.builder()
#                     .setProfiles(true)
#                     .build();
#             QueryResponse response = client.query(frame.bitmap(100), queryOptions);
#             assertNotNull(response.getProfile());
#             assertEquals(1000, response.getProfile().getID());
#             assertEquals(profileAttrs, response.getProfile().getAttributes());
#
#             response = client.query(frame.bitmap(300));
#             assertNull(response.getProfile());
#         }
#     }
#
# """

    @classmethod
    def random_database_name(cls):
        cls.counter += 1
        return "testdb-%d" % cls.counter

    @classmethod
    def get_client(cls):
        return Client(Cluster(URI.address(SERVER_ADDRESS)))
