import logging
import re

import requests
from requests import Request
from requests.exceptions import ConnectionError

from .exceptions import PilosaError, PilosaURIError, DatabaseExistsError, FrameExistsError
from .orm import TimeQuantum
from .version import get_version

logger = logging.getLogger(__name__)


class BitmapResult:

    def __init__(self, bits=None, attributes=None):
        self.bits = bits or []
        self.attributes = attributes or {}

    @classmethod
    def from_dict(cls, d):
        if d is None:
            return BitmapResult()
        return BitmapResult(bits=d.get("bits"), attributes=d.get("attrs"))


class CountResultItem:

    def __init__(self, id, count):
        self.id = id
        self.count = count

    @classmethod
    def from_dict(cls, d):
        return CountResultItem(d["id"], d["count"])


class QueryResult:

    def __init__(self, bitmap=None, count_items=None, count=0):
        self.bitmap = bitmap or BitmapResult()
        self.count_items = count_items or []
        self.count = count

    @classmethod
    def from_item(cls, item):
        result = cls()
        if isinstance(item, dict):
            result.bitmap = BitmapResult.from_dict(item)
        elif isinstance(item, list):
            result.count_items = [CountResultItem.from_dict(x) for x in item]
        elif isinstance(item, int):
            result.count = item
        return result


class ProfileItem:

    def __init__(self, id, attributes):
        self.id = id
        self.attributes = attributes

    @classmethod
    def from_dict(cls, d):
        return ProfileItem(d["id"], d["attrs"])


class QueryResponse(object):

    def __init__(self, results=None, profiles=None):
        self.results = results or []
        self.profiles = profiles or []

    @classmethod
    def from_dict(cls, d):
        response = QueryResponse()
        response.results = [QueryResult.from_item(r) for r in d.get("results", [])]
        response.profiles = [ProfileItem.from_dict(p) for p in d.get("profiles", [])]
        response.error_message = d.get("error", "")
        return response

    @property
    def result(self):
        return self.results[0] if self.results else None

    @property
    def profile(self):
        return self.profiles[0] if self.profiles else None


class Client(object):

    __NO_RESPONSE, __RAW_RESPONSE, __ERROR_CHECKED_RESPONSE = range(3)

    def __init__(self, cluster_or_uri=None, connect_timeout=30000, socket_timeout=300000):
        if cluster_or_uri is None:
            self.cluster = Cluster(URI())
        elif isinstance(cluster_or_uri, Cluster):
            self.cluster = cluster_or_uri
        elif isinstance(cluster_or_uri, URI):
            self.cluster = Cluster(cluster_or_uri)
        elif isinstance(cluster_or_uri, str):
            self.cluster = Cluster(URI.address(cluster_or_uri))
        else:
            raise PilosaError("Invalid cluster_or_uri: %s" % cluster_or_uri)

        self.connect_timeout = connect_timeout / 1000.0
        self.socket_timeout = socket_timeout / 1000.0
        self.__current_host = None
        self.__session = None

    def query(self, query, profiles=False):
        profiles_arg = "&profiles=true" if profiles else ""
        uri = "%s/query?db=%s%s" % \
              (self.__get_address(), query.database.name, profiles_arg)
        request = Request("POST", uri, data=query.serialize())
        response = self.__http_request(request, Client.__RAW_RESPONSE).json()
        if 'error' in response:
            raise PilosaError(response['error'])
        return QueryResponse.from_dict(response)

    def create_database(self, database):
        self.__create_or_delete_database("POST", database)
        if database.time_quantum != TimeQuantum.NONE:
            self.__patch_database_time_quantum(database)

    def delete_database(self, database):
        self.__create_or_delete_database("DELETE", database)

    def create_frame(self, frame):
        self.__create_or_delete_frame("POST", frame)
        if frame.time_quantum != TimeQuantum.NONE:
            self.__patch_frame_time_quantum(frame)

    def delete_frame(self, frame):
        self.__create_or_delete_frame("DELETE", frame)

    def ensure_database(self, database):
        try:
            self.create_database(database)
        except DatabaseExistsError:
            pass

    def ensure_frame(self, frame):
        try:
            self.create_frame(frame)
        except FrameExistsError:
            pass

    def __create_or_delete_database(self, method, database):
        data = '{"db": "%s", "options": {"columnLabel": "%s"}}' % \
               (database.name, database.column_label)
        uri = "%s/db" % self.__get_address()
        self.__http_request(Request(method, uri, data=data))

    def __create_or_delete_frame(self, method, frame):
        data = '{"db": "%s", "frame": "%s", "options": {"rowLabel": "%s"}}' % \
               (frame.database.name, frame.name, frame.row_label)
        uri = "%s/frame" % self.__get_address()
        self.__http_request(Request(method, uri, data=data))

    def __patch_database_time_quantum(self, database):
        uri = "%s/db/time_quantum" % self.__get_address()
        data = '{\"db\":\"%s\", \"time_quantum\":\"%s\"}"' % \
               (database.name, str(database.time_quantum))
        self.__http_request(Request("PATCH", uri, data=data))

    def __patch_frame_time_quantum(self, frame):
        uri = "%s/frame/time_quantum" % self.__get_address()
        data = '{\"db\":\"%s\", \"frame\":\"%s\", \"time_quantum\":\"%s\"}"' % \
               (frame.database.name, frame.name, str(frame.time_quantum))
        self.__http_request(Request("PATCH", uri, data=data))

    def __http_request(self, request, client_response=0):
        if not self.__session:
            self.__connect()
        request = self.__session.prepare_request(request)
        try:
            response = self.__session.send(request, stream=False,
                                           timeout=(self.connect_timeout, self.socket_timeout))
        except ConnectionError as e:
            self.cluster.remove_host(self.__current_host)
            self.__current_host = None
            raise PilosaError(str(e))

        if client_response == Client.__RAW_RESPONSE:
            return response

        if 200 <= response.status_code < 300:
            return None if client_response == Client.__NO_RESPONSE else response
        ex = self.__RECOGNIZED_ERRORS.get(response.text)
        if ex is not None:
            raise ex
        raise PilosaError("Server error (%d): %s", response.status_code, response.content)

    def __get_address(self):
        if self.__current_host is None:
            self.__current_host = self.cluster.get_host()
        return self.__current_host.normalize()

    def __connect(self):
        session = requests.Session()
        session.headers.update({
            'Accept': 'application/vnd.pilosa.json.v1',
            'Content-Type': 'application/vnd.pilosa.pql.v1',
            'User-Agent': 'python-pilosa/' + get_version(),
        })
        self.__session = session

    __HEADERS = {
        'Accept': 'application/vnd.pilosa.json.v1',
        'Content-Type': 'application/vnd.pilosa.pql.v1',
        'User-Agent': 'python-pilosa/' + get_version(),
    }

    __RECOGNIZED_ERRORS = {
        "database already exists\n": DatabaseExistsError,
        "frame already exists\n": FrameExistsError,
    }


class URI:
    """Represents a Pilosa URI

    A Pilosa URI consists of three parts:
    - Scheme: Protocol of the URI. Default: http
    - Host: Hostname or IP URI. Default: localhost
    - Port: Port of the URI. Default 10101
    
    All parts of the URI are optional. The following are equivalent:
    - http://localhost:10101
    - http://localhost
    - http://:10101
    - localhost:10101
    - localhost
    - :10101
    """
    __PATTERN = re.compile("^(([+a-z]+)://)?([0-9a-z.-]+)?(:([0-9]+))?$")

    def __init__(self, scheme="http", host="localhost", port=10101):
        self.scheme = scheme
        self.host = host
        self.port = port

    @classmethod
    def address(cls, address):
        uri = cls()
        uri._parse(address)
        return uri

    def normalize(self):
        scheme = self.scheme
        try:
            index = scheme.index("+")
            scheme = scheme[:index]
        except ValueError:
            pass
        return "%s://%s:%s" % (scheme, self.host, self.port)

    def _parse(self, address):
        m = self.__PATTERN.search(address)
        if m:
            scheme = m.group(2)
            if scheme:
                self.scheme = scheme
            host = m.group(3)
            if host:
                self.host = host
            port = m.group(5)
            if port:
                self.port = int(port)
            return
        raise PilosaURIError("Not a Pilosa URI")

    def __str__(self):
        return "%s://%s:%s" % (self.scheme, self.host, self.port)

    def __repr__(self):
        return "<URI %s>" % self

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.scheme == other.scheme and \
            self.host == other.host and \
            self.port == other.port


class Cluster:
    """Contains hosts in a Pilosa cluster"""

    def __init__(self, *hosts):
        """Returns the cluster with the given hosts"""
        self.hosts = list(hosts)
        self.__next_index = 0

    def add_host(self, uri):
        """Adds a host to the cluster"""
        self.hosts.append(uri)

    def remove_host(self, uri):
        """Removes the host with the given URI from the cluster."""
        self.hosts.remove(uri)

    def get_host(self):
        """Returns the next host in the cluster"""
        if len(self.hosts) == 0:
            raise PilosaError("There are no available hosts")
        next_host = self.hosts[self.__next_index % len(self.hosts)]
        self.__next_index = (self.__next_index + 1) % len(self.hosts)
        return next_host
