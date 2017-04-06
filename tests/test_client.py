import logging
import unittest

from mock import patch

from pilosa import Client
from pilosa.client import DEFAULT_HOST, URI, Cluster
from pilosa.exceptions import PilosaURIError, PilosaError
from pilosa.orm import Database
from pilosa.version import get_version

logger = logging.getLogger(__name__)


class ClientTestCase(unittest.TestCase):

    database = Database("testdb", column_label="user")
    frame = database.frame("collab", row_label="project")

    @patch('pilosa.client.requests.post')
    def test_query(self, mock_post):
        c = Client()
        self.assertEqual(c.hosts, [DEFAULT_HOST])
        bitmap = self.frame.bitmap(10)
        c.query(self.frame.bitmap(10))
        query = str(bitmap)
        headers = {
            'Content-Type': 'application/vnd.pilosa.pql.v1',
            'Accept': 'application/vnd.pilosa.json.v1',
            'User-Agent': 'pilosa-driver/' + get_version()
        }
        mock_post.assert_called_with('http://%s/query?db=%s' % (DEFAULT_HOST, self.database.name),
                                     data=query, headers=headers)


class URITestCase(unittest.TestCase):

    def test_default(self):
        uri = URI.default()
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
        self.assertEquals("https://big-data.pilosa.com:6888", uri.normalize())

        uri = URI.address("https://big-data.pilosa.com:6888")
        self.assertEquals("https://big-data.pilosa.com:6888", uri.normalize())

    def test_invalid_address(self):
        for address in ["foo:bar", "http://foo:", "http://foo:", "foo:", ":bar"]:
            self.assertRaises(PilosaURIError, URI.address, address)

    def test_to_string(self):
        uri = URI.default()
        self.assertEquals("http://localhost:10101", "%s" % uri)

    def test_equals(self):
        uri1 = URI(host="pilosa.com", port=1337)
        uri2 = URI.address("http://pilosa.com:1337")
        self.assertTrue(uri1 == uri2)

    def test_equals_fails_with_other_object(self):
        self.assertFalse(URI.default() == "http://localhost:10101")

    def test_equals_same_object(self):
        uri = URI.address("https://pilosa.com:1337")
        self.assertEquals(uri, uri)

    def compare(self, uri, scheme, host, port):
        self.assertEquals(scheme, uri.scheme)
        self.assertEquals(host, uri.host)
        self.assertEquals(port, uri.port)


class ClusterTestCase(unittest.TestCase):

    def test_create_with_host(self):
        target = [URI.address("http://localhost:3000")]
        c = Cluster.with_host(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)

    def test_add_remove_host(self):
        target = [URI.address("http://localhost:3000")]
        c = Cluster.default()
        c.add_host(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)
        target = [URI.address("http://localhost:3000"), URI.default()]
        c.add_host(URI.default())
        self.assertEquals(target, c.hosts)
        target = [URI.default()]
        c.remove_host(URI.address("http://localhost:3000"))
        self.assertEquals(target, c.hosts)

    def test_get_host(self):
        target1 = URI.address("db1.pilosa.com")
        target2 = URI.address("db2.pilosa.com")

        c = Cluster.default()
        c.add_host(URI.address("db1.pilosa.com"))
        c.add_host(URI.address("db2.pilosa.com"))
        addr = c.get_host()
        self.assertEquals(target1, addr)
        addr = c.get_host()
        self.assertEquals(target2, addr)
        c.get_host()
        c.remove_host(URI.address("db1.pilosa.com"))
        addr = c.get_host()
        self.assertEquals(target2, addr)

    def test_get_host_when_no_hosts(self):
        c = Cluster.default()
        self.assertRaises(PilosaError, c.get_host)


if __name__ == '__main__':
    unittest.main()
