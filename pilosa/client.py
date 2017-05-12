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

import json
import logging
import re

import urllib3

from .exceptions import PilosaError, PilosaURIError, IndexExistsError, FrameExistsError
from .internal import public_pb2 as internal
from .orm import TimeQuantum
from .response import QueryResponse
from .version import VERSION

__all__ = ("Client", "Cluster", "URI")

logger = logging.getLogger(__name__)


class Client(object):
    """Pilosa HTTP client

    This client uses Pilosa's http+protobuf API.

    Usage: ::

        import pilosa

        # Create a Client instance
        client = pilosa.Client()

        # Create an Index instance
        index = pilosa.Index("repository")

        stargazer = index.frame("stargazer")
        response = client.query(stargazer.bitmap(5))

        # Act on the result
        print(response.result)

    * See `Pilosa API Reference <https://www.pilosa.com/docs/api-reference/>`_.
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_.
    """

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
        """Runs the given query against the server with the given options.
        
        :param pilosa.PqlQuery query: a PqlQuery object with a non-null index
        :param bool columns: Enables returning column data from bitmap queries
        :param pilosa.TimeQuantum time_quantum: Sets the time quantum for this query 
        :return: Pilosa response
        :rtype: pilosa.Response
        """
        request = _QueryRequest(query.serialize(), columns=columns,
                                time_quantum=time_quantum)
        data = request.to_protobuf()
        uri = "%s/index/%s/query" % (self.__get_address(), query.index.name)
        response = self.__http_request("POST", uri, data, Client.__RAW_RESPONSE)
        query_response = QueryResponse._from_protobuf(response.data)
        if query_response.error_message:
            raise PilosaError(query_response.error_message)
        return query_response

    def create_index(self, index):
        """Creates an index on the server using the given Index object.
        
        :param pilosa.Index index:
        :raises pilosa.IndexExistsError: if there already is a index with the given name
        """
        data = json.dumps({
            "options": {"columnLabel": index.column_label}
        })
        uri = "%s/index/%s" % (self.__get_address(), index.name)
        self.__http_request("POST", uri, data=data)
        if index.time_quantum != TimeQuantum.NONE:
            self.__patch_index_time_quantum(index)

    def delete_index(self, index):
        """Deletes the given index on the server.
        
        :param pilosa.Index index:
        :raises pilosa.PilosaError: if the index does not exist
        """
        uri = "%s/index/%s" % (self.__get_address(), index.name)
        self.__http_request("DELETE", uri)

    def create_frame(self, frame):
        """Creates a frame on the server using the given Frame object.
        
        :param pilosa.Frame frame:
        :raises pilosa.FrameExistsError: if there already is a frame with the given name
        """
        data = frame._get_options_string()
        uri = "%s/index/%s/frame/%s" % (self.__get_address(), frame.index.name, frame.name)
        self.__http_request("POST", uri, data=data)

    def delete_frame(self, frame):
        """Deletes the given frame on the server.
        
        :param pilosa.Frame frame:
        :raises pilosa.PilosaError: if the frame does not exist
        """
        uri = "%s/index/%s/frame/%s" % (self.__get_address(), frame.index.name, frame.name)
        self.__http_request("DELETE", uri)

    def ensure_index(self, index):
        """Creates an index on the server if it does not exist.
        
        :param pilosa.Index index:
        """
        try:
            self.create_index(index)
        except IndexExistsError:
            pass

    def ensure_frame(self, frame):
        """Creates a frame on the server if it does not exist.
        
        :param pilosa.Frame frame:
        """
        try:
            self.create_frame(frame)
        except FrameExistsError:
            pass

    def __patch_index_time_quantum(self, index):
        uri = "%s/index/%s/time-quantum" % (self.__get_address(), index.name)
        data = '{\"timeQuantum\":\"%s\"}"' % str(index.time_quantum)
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
        return self.__current_host._normalize()

    def __connect(self):
        num_pools = float(self.pool_size_total) / self.pool_size_per_route
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
            'User-Agent': 'python-pilosa/' + VERSION,
        }

        timeout = urllib3.Timeout(connect=self.connect_timeout, read=self.socket_timeout)
        client = urllib3.PoolManager(num_pools=num_pools, maxsize=self.pool_size_per_route,
            block=True, headers=headers, timeout=timeout, retries=self.retry_count)
        self.__client = client

    __RECOGNIZED_ERRORS = {
        "index already exists\n": IndexExistsError,
        "frame already exists\n": FrameExistsError,
    }


class URI:
    """Represents a Pilosa URI

    A Pilosa URI consists of three parts:
    
    * Scheme: Protocol of the URI. Default: ``http``
    * Host: Hostname or IP URI. Default: ``localhost``
    * Port: Port of the URI. Default ``10101``
    
    All parts of the URI are optional. The following are equivalent:
    
    * ``http://localhost:10101``
    * ``http://localhost``
    * ``http://:10101``
    * ``localhost:10101``
    * ``localhost``
    * ``:10101``

    :param str scheme: is the scheme of the Pilosa Server. Currently only ``http`` is supported     
    :param str host: is the hostname or IP address of the Pilosa server
    :param int port: is the port of the Pilosa server
    """
    __PATTERN = re.compile("^(([+a-z]+)://)?([0-9a-z.-]+)?(:([0-9]+))?$")

    def __init__(self, scheme="http", host="localhost", port=10101):
        self.scheme = scheme
        self.host = host
        self.port = port

    @classmethod
    def address(cls, address):
        """ Creates a URI from an address.
        
        :param str address: of the form ``${SCHEME}://${HOST}:{$PORT}``
        :return: a Pilosa URI
        :type: pilosa.URI
        """
        uri = cls()
        uri._parse(address)
        return uri

    def _normalize(self):
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
    """Contains hosts in a Pilosa cluster.
    
    :param hosts: URIs of hosts. Leaving out hosts creates the default cluster
    """

    def __init__(self, *hosts):
        """Returns the cluster with the given hosts"""
        self.hosts = list(hosts)
        self.__next_index = 0

    def add_host(self, uri):
        """Adds a host to the cluster.
        
        :param pilosa.URI uri:
        """
        self.hosts.append(uri)

    def remove_host(self, uri):
        """Removes the host with the given URI from the cluster.
        
        :param pilosa.URI uri:
        """
        self.hosts.remove(uri)

    def get_host(self):
        """Returns the next host in the cluster.
        
        :return: next host
        :rtype: pilosa.URI         
        """
        if len(self.hosts) == 0:
            raise PilosaError("There are no available hosts")
        next_host = self.hosts[self.__next_index % len(self.hosts)]
        self.__next_index = (self.__next_index + 1) % len(self.hosts)
        return next_host


class _QueryRequest:

    def __init__(self, query, columns=False, time_quantum=TimeQuantum.NONE):
        self.query = query
        self.columns = columns
        self.time_quantum = time_quantum

    def to_protobuf(self):
        qr = internal.QueryRequest()
        qr.Query = self.query
        qr.ColumnAttrs = self.columns
        qr.Quantum = str(self.time_quantum)
        return qr.SerializeToString()
