import json
import logging
import re

import urllib3

from .exceptions import PilosaError, PilosaURIError, DatabaseExistsError, FrameExistsError
from .internal import public_pb2 as internal
from .orm import TimeQuantum
from .response import QueryResponse
from .version import get_version

__all__ = ["Client", "Cluster", "URI"]

logger = logging.getLogger(__name__)


class Client(object):

    __NO_RESPONSE, __RAW_RESPONSE, __ERROR_CHECKED_RESPONSE = range(3)

    def __init__(self, cluster_or_uri=None, connect_timeout=30000, socket_timeout=300000,
                 pool_size_per_route=10, pool_size_total=100, retry_count=3):
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
        self.pool_size_per_route = pool_size_per_route
        self.pool_size_total = pool_size_total
        self.retry_count = retry_count
        self.__current_host = None
        self.__client = None

    def query(self, query, columns=False, time_quantum=TimeQuantum.NONE):
        request = QueryRequest(query.serialize(), columns=columns,
                               time_quantum=time_quantum)
        data = request.to_protobuf()
        uri = "%s/db/%s/query" % (self.__get_address(), query.database.name)
        response = self.__http_request("POST", uri, data, Client.__RAW_RESPONSE)
        query_response = QueryResponse.from_protobuf(response.data)
        if query_response.error_message:
            raise PilosaError(query_response.error_message)
        return query_response

    def create_database(self, database):
        data = json.dumps({
            "options": {"columnLabel": database.column_label}
        })
        uri = "%s/db/%s" % (self.__get_address(), database.name)
        self.__http_request("POST", uri, data=data)
        if database.time_quantum != TimeQuantum.NONE:
            self.__patch_database_time_quantum(database)

    def delete_database(self, database):
        uri = "%s/db/%s" % (self.__get_address(), database.name)
        self.__http_request("DELETE", uri)

    def create_frame(self, frame):
        data = json.dumps({
            "options": {
                "rowLabel": frame.row_label,
                "inverseEnabled": frame.inverse_enabled
            }
        })
        uri = "%s/db/%s/frame/%s" % (self.__get_address(), frame.database.name, frame.name)
        self.__http_request("POST", uri, data=data)
        if frame.time_quantum != TimeQuantum.NONE:
            self.__patch_frame_time_quantum(frame)

    def delete_frame(self, frame):
        uri = "%s/db/%s/frame/%s" % (self.__get_address(), frame.database.name, frame.name)
        self.__http_request("DELETE", uri)

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

    def __patch_database_time_quantum(self, database):
        uri = "%s/db/%s/time-quantum" % (self.__get_address(), database.name)
        data = '{\"time_quantum\":\"%s\"}"' % str(database.time_quantum)
        self.__http_request("PATCH", uri, data=data)

    def __patch_frame_time_quantum(self, frame):
        uri = "%s/db/%s/frame/%s/time-quantum" % \
              (self.__get_address(), frame.database.name, frame.name)
        data = '{\"time_quantum\":\"%s\"}"' % str(frame.time_quantum)
        self.__http_request("PATCH", uri, data=data)

    def __http_request(self, method, uri, data=None, client_response=0):
        if not self.__client:
            self.__connect()
        try:
            response = self.__client.request(method, uri, body=data)
        except urllib3.exceptions.MaxRetryError as e:
            self.cluster.remove_host(self.__current_host)
            self.__current_host = None
            raise PilosaError(str(e))

        if client_response == Client.__RAW_RESPONSE:
            return response

        if 200 <= response.status < 300:
            return None if client_response == Client.__NO_RESPONSE else response
        content = response.data.decode('utf-8')
        ex = self.__RECOGNIZED_ERRORS.get(content)
        if ex is not None:
            raise ex
        raise PilosaError("Server error (%d): %s", response.status, content)

    def __get_address(self):
        if self.__current_host is None:
            self.__current_host = self.cluster.get_host()
        return self.__current_host.normalize()

    def __connect(self):
        num_pools = float(self.pool_size_total) / self.pool_size_per_route
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
            'User-Agent': 'python-pilosa/' + get_version(),
        }

        timeout = urllib3.Timeout(connect=self.connect_timeout, read=self.socket_timeout)
        client = urllib3.PoolManager(num_pools=num_pools, maxsize=self.pool_size_per_route,
            block=True, headers=headers, timeout=timeout, retries=self.retry_count)
        self.__client = client

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


class QueryRequest:

    def __init__(self, query, columns=False, time_quantum=TimeQuantum.NONE, ):
        self.query = query
        self.columns = columns
        self.time_quantum = time_quantum

    def to_protobuf(self):
        qr = internal.QueryRequest()
        qr.Query = self.query
        qr.ColumnAttrs = self.columns
        qr.Quantum = str(self.time_quantum)
        return qr.SerializeToString()
