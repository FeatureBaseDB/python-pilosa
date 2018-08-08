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
import sys
import threading

import urllib3

from .exceptions import PilosaError, PilosaURIError, IndexExistsError, FieldExistsError
from .imports import batch_columns, \
    row_id_column_id, row_id_column_key, row_key_column_id, row_key_column_key
from .internal import public_pb2 as internal
from .orm import TimeQuantum, Schema, CacheType
from .response import QueryResponse
from .version import VERSION

__all__ = ("Client", "Cluster", "URI")

_MAX_HOSTS = 10
_IS_PY2 = sys.version_info.major == 2


class Client(object):
    """Pilosa HTTP client

    This client uses Pilosa's http+protobuf API.

    Usage: ::

        import pilosa

        # Create a Client instance
        client = pilosa.Client()

        # Create an Index instance
        index = pilosa.Index("repository")

        stargazer = index.field("stargazer")
        response = client.query(stargazer.row(5))

        # Act on the result
        print(response.result)

    * See `Pilosa API Reference <https://www.pilosa.com/docs/api-reference/>`_.
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_.
    """

    __NO_RESPONSE, __RAW_RESPONSE, __ERROR_CHECKED_RESPONSE = range(3)

    def __init__(self, cluster_or_uri=None, connect_timeout=30000, socket_timeout=300000,
                 pool_size_per_route=10, pool_size_total=100, retry_count=3,
                 tls_skip_verify=False, tls_ca_certificate_path=""):
        if cluster_or_uri is None:
            self.cluster = Cluster(URI())
        elif isinstance(cluster_or_uri, Cluster):
            self.cluster = cluster_or_uri.copy()
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
        self.tls_skip_verify = tls_skip_verify
        self.tls_ca_certificate_path = tls_ca_certificate_path
        self.__current_host = None
        self.__client = None
        self.logger = logging.getLogger("pilosa")

    def query(self, query, column_attrs=False, exclude_columns=False, exclude_attrs=False, shards=None):
        """Runs the given query against the server with the given options.
        
        :param pilosa.PqlQuery query: a PqlQuery object with a non-null index
        :param bool column_attrs: Enables returning column data from row queries
        :param bool exclude_columns: Disables returning columns from row queries
        :param bool exclude_attrs: Disables returning attributes from row queries
        :param list(int) shards: Returns data from a subset of shards
        :return: Pilosa response
        :rtype: pilosa.Response
        """
        request = _QueryRequest(query.serialize(), column_attrs=column_attrs, exclude_columns=exclude_columns, exclude_row_attrs=exclude_attrs, shards=shards)
        path = "/index/%s/query" % query.index.name
        try:
            headers = {
                'Content-Type': 'application/x-protobuf',
                'Accept': 'application/x-protobuf',
            }
            response = self.__http_request("POST", path, data=request.to_protobuf(), headers=headers)
            return QueryResponse._from_protobuf(response.data)
        except PilosaServerError as e:
            raise PilosaError(e.content)

    def create_index(self, index):
        """Creates an index on the server using the given Index object.
        
        :param pilosa.Index index:
        :raises pilosa.IndexExistsError: if there already is a index with the given name
        """
        path = "/index/%s" % index.name
        data = index._get_options_string()
        try:
            self.__http_request("POST", path, data=data)
        except PilosaServerError as e:
            if e.response.status == 409:
                raise IndexExistsError
            raise

    def delete_index(self, index):
        """Deletes the given index on the server.
        
        :param pilosa.Index index:
        :raises pilosa.PilosaError: if the index does not exist
        """
        path = "/index/%s" % index.name
        self.__http_request("DELETE", path)

    def create_field(self, field):
        """Creates a field on the server using the given Field object.
        
        :param pilosa.Field field:
        :raises pilosa.FieldExistsError: if there already is a field with the given name
        """
        data = field._get_options_string()
        path = "/index/%s/field/%s" % (field.index.name, field.name)
        try:
            self.__http_request("POST", path, data=data)
        except PilosaServerError as e:
            if e.response.status == 409:
                raise FieldExistsError
            raise


    def delete_field(self, field):
        """Deletes the given field on the server.
        
        :param pilosa.Field field:
        :raises pilosa.PilosaError: if the field does not exist
        """
        path = "/index/%s/field/%s" % (field.index.name, field.name)
        self.__http_request("DELETE", path)

    def ensure_index(self, index):
        """Creates an index on the server if it does not exist.
        
        :param pilosa.Index index:
        """
        try:
            self.create_index(index)
        except IndexExistsError:
            pass

    def ensure_field(self, field):
        """Creates a field on the server if it does not exist.
        
        :param pilosa.Field field:
        """
        try:
            self.create_field(field)
        except FieldExistsError:
            pass

    def _read_schema(self):
        response = self.__http_request("GET", "/schema")
        return json.loads(response.data.decode('utf-8')).get("indexes") or []

    def schema(self):
        schema = Schema()
        for index_info in self._read_schema():
            index = schema.index(index_info["name"])
            for field_info in index_info.get("fields") or []:
                options = decode_field_meta_options(field_info)
                index.field(field_info["name"], **options)

        return schema

    def sync_schema(self, schema):
        server_schema = self.schema()

        # find out local - remote schema
        diff_schema = schema._diff(server_schema)
        # create indexes and fields which doesn't exist on the server side
        for index_name, index in diff_schema._indexes.items():
            if index_name not in server_schema._indexes:
                self.ensure_index(index)
            for field_name, field in index._fields.items():
                self.ensure_field(field)

        # find out remote - local schema
        diff_schema = server_schema._diff(schema)
        for index_name, index in diff_schema._indexes.items():
            local_index = schema._indexes.get(index_name)
            if local_index is None:
                schema._indexes[index_name] = index
            else:
                for field_name, field in index._fields.items():
                    local_index._fields[field_name] = field

    def import_field(self, field, bit_reader, batch_size=100000):
        """Imports a field using the given bit reader

        :param field:
        :param bit_reader:
        :param batch_size:
        """
        index_name = field.index.name
        field_name = field.name
        import_columns = self._import_columns
        for shard, columns in batch_columns(bit_reader, batch_size):
            import_columns(field, shard, columns)

    def http_request(self, method, path, data=None, headers=None):
        """Sends an HTTP request to the Pilosa server

        NOTE: This function is experimental and may be removed in later revisions.

        :param method: HTTP method
        :param path: Request path
        :param data: Request body
        :param headers: Request headers
        :return HTTP response:

        """
        return self.__http_request(method, path, data=data, headers=headers)

    def _import_columns(self, field, shard, columns):
        # sort by row_id then by column_id
        columns.sort(key=lambda bit: (bit.row_id, bit.column_id))
        nodes = self._fetch_fragment_nodes(field.index.name, shard)
        # copy client params
        client_params = {}
        for k,v in self.__dict__.items():
            # don't copy protected, private params
            if k.startswith("_"):
                continue
            # don't copy these
            if k in ["cluster", "logger"]:
                continue
            client_params[k] = v
        for node in nodes:
            client = Client(URI.address(node.url), **client_params)
            client._import_node(_ImportRequest(field, shard, columns))

    def _fetch_fragment_nodes(self, index_name, shard):
        path = "/internal/fragment/nodes?shard=%d&index=%s" % (shard, index_name)
        response = self.__http_request("GET", path)
        content = response.data.decode("utf-8")
        node_dicts = json.loads(content)
        nodes = []
        for node_dict in node_dicts:
            node_dict = node_dict["uri"]
            nodes.append(_Node(node_dict["scheme"], node_dict["host"], node_dict.get("port", "")))
        return nodes

    def _import_node(self, import_request):
        data = import_request.to_protobuf()
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
        }
        path = "/index/%s/field/%s/import" % (import_request.index_name, import_request.field_name)
        self.__http_request("POST", path, data=data, headers=headers)

    def __http_request(self, method, path, data=None, headers=None):
        if not self.__client:
            self.__connect()
        # try at most 10 non-failed hosts; protect against broken cluster.remove_host
        for _ in range(_MAX_HOSTS):
            uri = "%s%s" % (self.__get_address(), path)
            try:
                self.logger.debug("Request: %s %s %s", method, uri)
                response = self.__client.request(method, uri, body=data, headers=headers)
                break
            except urllib3.exceptions.MaxRetryError as e:
                self.cluster.remove_host(self.__current_host)
                self.logger.warning("Removed %s from the cluster due to %s", self.__current_host, str(e))
                self.__current_host = None
        else:
            raise PilosaError("Tried %s hosts, still failing" % _MAX_HOSTS)

        if 200 <= response.status < 300:
            return response
        raise PilosaServerError(response)

    def __get_address(self):
        if self.__current_host is None:
            self.__current_host = self.cluster.get_host()
            self.logger.debug("Current host set: %s", self.__current_host)
        return self.__current_host._normalize()

    def __connect(self):
        num_pools = float(self.pool_size_total) / self.pool_size_per_route
        headers = {
            'User-Agent': 'python-pilosa/%s' % VERSION,
        }

        timeout = urllib3.Timeout(connect=self.connect_timeout, read=self.socket_timeout)
        client_options = {
            "num_pools": num_pools,
            "maxsize": self.pool_size_per_route,
            "block": True,
            "headers": headers,
            "timeout": timeout,
            "retries": self.retry_count,
        }
        if not self.tls_skip_verify:
            client_options["cert_reqs"] = "CERT_REQUIRED"
            client_options["ca_certs"] = self.tls_ca_certificate_path

        client = urllib3.PoolManager(**client_options)
        self.__client = client


def decode_field_meta_options(field_info):
    meta = field_info.get("options", {})
    return {
        "cache_size": meta.get("cacheSize", 50000),
        "cache_type": CacheType(meta.get("cacheType", "")),
        "time_quantum": TimeQuantum(meta.get("timeQuantum", "")),
        "int_min": meta.get("min", 0),
        "int_max": meta.get("max", 0),
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

    :param str scheme: is the scheme of the Pilosa Server, such as ``http`` or ``https``
    :param str host: is the hostname or IP address of the Pilosa server. IPv6 addresses should be enclosed in brackets, e.g., ``[fe00::0]``.
    :param int port: is the port of the Pilosa server
    """
    __PATTERN = re.compile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+|\\[[:0-9a-fA-F]+\\])?(:([0-9]+))?$")

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
        self.hosts = [(host, True) for host in hosts]
        self.__next_index = 0
        self.__lock = threading.RLock()

    def add_host(self, uri):
        """Makes a host available.
        
        :param pilosa.URI uri:
        """
        with self.__lock:
            for i, item in enumerate(self.hosts):
                host, _ = item
                if host == uri:
                    self.hosts[i] = (host, True)
                    break
            else:
                self.hosts.append((uri, True))

    def remove_host(self, uri):
        """Makes a host unavailable.
        
        :param pilosa.URI uri:
        """
        with self.__lock:
            for i, item in enumerate(self.hosts):
                if uri == item[0]:
                    self.hosts[i] = (item[0], False)

    def get_host(self):
        """Returns the next host in the cluster.
        
        :return: next host
        :rtype: pilosa.URI         
        """
        for host, ok in self.hosts:
            if not ok:
                continue
            return host
        else:
            self._reset()
            raise PilosaError("There are no available hosts")

    def copy(self):
        c = Cluster()
        c.hosts = self.hosts[:]
        return c

    def _reset(self):
        with self.__lock:
            self.hosts = [(host, True) for host, _ in self.hosts]


class _QueryRequest:

    def __init__(self, query, column_attrs=False, exclude_columns=False, exclude_row_attrs=False, shards=None):
        self.query = query
        self.column_attrs = column_attrs
        self.exclude_columns = exclude_columns
        self.exclude_row_attrs = exclude_row_attrs
        self.shards = shards or []

    def to_protobuf(self, return_bytearray=_IS_PY2):
        qr = internal.QueryRequest()
        qr.Query = self.query
        qr.ColumnAttrs = self.column_attrs
        qr.ExcludeColumns = self.exclude_columns
        qr.ExcludeRowAttrs = self.exclude_row_attrs
        qr.Shards.extend(self.shards)
        if return_bytearray:
            return bytearray(qr.SerializeToString())
        return qr.SerializeToString()


class _ImportRequest:

    def __init__(self, field, shard, columns):
        self.index_name = field.index.name
        self.field_name = field.name
        self.shard = shard
        self.columns = columns
        if field.index.keys:
            self.format = row_key_column_key if field.keys else row_id_column_key
        else:
            self.format = row_key_column_id if field.keys else row_id_column_id

    def to_protobuf(self, return_bytearray=_IS_PY2):
        import_request = internal.ImportRequest()
        import_request.Index = self.index_name
        import_request.Field = self.field_name
        import_request.Shard = self.shard
        row_ids = import_request.RowIDs
        column_ids = import_request.ColumnIDs
        row_keys = import_request.RowKeys
        column_keys = import_request.ColumnKeys
        timestamps = import_request.Timestamps

        row_format = self.format
        if row_format == row_id_column_id:
            for bit in self.columns:
                row_ids.append(bit.row_id)
                column_ids.append(bit.column_id)
                timestamps.append(bit.timestamp)
        elif row_format == row_id_column_key:
            for bit in self.columns:
                row_ids.append(bit.row_id)
                column_keys.append(bit.column_key)
                timestamps.append(bit.timestamp)
        elif row_format == row_key_column_id:
            for bit in self.columns:
                row_keys.append(bit.row_key)
                column_ids.append(bit.column_id)
                timestamps.append(bit.timestamp)
        elif row_format == row_key_column_key:
            for bit in self.columns:
                row_keys.append(bit.row_key)
                column_keys.append(bit.column_key)
                timestamps.append(bit.timestamp)
        else:
            raise PilosaError("Invalid import format")

        if return_bytearray:
            return bytearray(import_request.SerializeToString())
        return import_request.SerializeToString()


class PilosaServerError(PilosaError):

    def __init__(self, response):
        self.response = response
        self.content = response.data.decode('utf-8')
        super(Exception, PilosaServerError).__init__(self, u"Server error (%d): %s" % (response.status, self.content))


class _Node(object):

    __slots__ = "scheme", "host", "port"

    def __init__(self, scheme, host, port):
        self.scheme = scheme
        self.host = host
        self.port = port


    @property
    def url(self):
        if self.port:
            return "%s://%s:%s" % (self.scheme, self.host, self.port)
        return "%s://%s" % (self.scheme, self.host)