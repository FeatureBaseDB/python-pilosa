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

import io
import json
import logging
import re
import sys
import threading
from datetime import datetime

import urllib3
from roaring import Bitmap

from .exceptions import PilosaError, PilosaURIError, IndexExistsError, FieldExistsError
from .imports import batch_columns, \
    csv_row_id_column_id, csv_row_id_column_key, csv_row_key_column_id, csv_row_key_column_key, csv_column_id_value, \
    csv_column_key_value
from .internal import public_pb2 as internal
from .orm import TimeQuantum, Schema, CacheType
from .response import QueryResponse
from .version import VERSION

__all__ = ("Client", "Cluster", "URI")

_MAX_HOSTS = 10
PQL_VERSION = "1.0"
_IS_PY2 = sys.version_info.major == 2

RESERVED_FIELDS = ("exists",)
DEFAULT_SHARD_WIDTH = 1048576


class Client(object):
    """Pilosa HTTP client

    This client uses Pilosa's http+protobuf API.

    Usage: ::

        import pilosa

        # Create a Client instance
        client = pilosa.Client()

        # Load the schema from the Pilosa server
        schema = client.schema()

        # Create an Index instance
        index = schema.Index("repository")

        # Create a Field instance
        stargazer = index.field("stargazer")

        # Execute a query
        response = client.query(stargazer.row(5))

        # Act on the result
        print(response.result)

    * See `Pilosa API Reference <https://www.pilosa.com/docs/api-reference/>`_.
    * See `Query Language <https://www.pilosa.com/docs/query-language/>`_.
    """

    __NO_RESPONSE, __RAW_RESPONSE, __ERROR_CHECKED_RESPONSE = range(3)

    def __init__(self, cluster_or_uri=None, connect_timeout=30000, socket_timeout=300000,
                 pool_size_per_route=10, pool_size_total=100, retry_count=3,
                 tls_skip_verify=False, tls_ca_certificate_path="", use_manual_address=False):
        """Creates a Client.

        :param object cluster_or_uri: A ``pilosa.Cluster`` or ``pilosa.URI` instance
        :param int connect_timeout: The maximum amount of time in milliseconds to wait for a connection attempt to a server
        to succeed
        :param int socket_timeout: The maximum amount of time in milliseconds to wait between consecutive
        read operations for a response from the server
        :param int pool_size_per_route: Number of connections in the pool per server
        :param int pool_size_total: Total number of connections in the pool
        :param int retry_count: Number of connection trials
        :param bool tls_skip_verify: Do not verify the TLS certificate of the server (Not recommended for production)
        :param str tls_ca_certificate_path: Server's TLS certificate (Useful when using self-signed certificates)
        :param bool use_manual_address: Forces the client to use only the manual server address

        * See `Pilosa Python Client/Server Interaction <https://github.com/pilosa/python-pilosa/blob/master/docs/server-interaction.md>`_.
        """
        self.use_manual_address = use_manual_address
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
        self.__coordinator_lock = threading.RLock()
        self.__coordinator_uri = None

        if cluster_or_uri is None:
            self.cluster = Cluster(URI())
        elif isinstance(cluster_or_uri, Cluster):
            self.cluster = cluster_or_uri.copy()
        elif isinstance(cluster_or_uri, URI):
            if use_manual_address:
                self.__coordinator_uri = cluster_or_uri
                self.__current_host = cluster_or_uri
            else:
                self.cluster = Cluster(cluster_or_uri)
        elif isinstance(cluster_or_uri, str):
            uri = URI.address(cluster_or_uri)
            if use_manual_address:
                self.__coordinator_uri = uri
                self.__current_host = uri
            else:
                self.cluster = Cluster(uri)
        else:
            raise PilosaError("Invalid cluster_or_uri: %s" % cluster_or_uri)

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
        serialized_query = query.serialize()
        request = _QueryRequest(serialized_query.query,
            column_attrs=column_attrs,
            exclude_columns=exclude_columns,
            exclude_row_attrs=exclude_attrs,
            shards=shards)
        path = "/index/%s/query" % query.index.name
        try:
            headers = {
                "Content-Type": "application/x-protobuf",
                "Accept": "application/x-protobuf",
                "PQL-Version": PQL_VERSION,
            }
            response = self.__http_request("POST", path,
                                            data=request.to_protobuf(),
                                            headers=headers,
                                            use_coordinator=serialized_query.has_keys)
            warning = response.getheader("warning")
            if warning:
                self.logger.warning(warning)
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
        """Loads the schema from the server.

        :return: a Schema instance.
        :rtype: pilosa.Schema
        """
        schema = Schema()
        for index_info in self._read_schema():
            index_options = index_info.get("options", {})
            index = schema.index(index_info["name"],
                                 keys=index_options.get("keys", False),
                                 track_existence=index_options.get("trackExistence", False),
                                 shard_width=index_info.get("shardWidth", 0))
            for field_info in index_info.get("fields") or []:
                if field_info["name"] in RESERVED_FIELDS:
                    continue
                options = decode_field_meta_options(field_info)
                index.field(field_info["name"], **options)

        return schema

    def sync_schema(self, schema):
        """Syncs the given schema with the server.

        Loads new indexes/fields from the server and creates indexes/fields not existing on the server. Does not delete remote indexes/fields/

        :param pilosa.Schema schema: Local schema to be synced
        """
        server_schema = self.schema()

        # find out local - remote schema
        diff_schema = schema._diff(server_schema)
        # create indexes and fields which doesn't exist on the server side
        for index_name, index in diff_schema._indexes.items():
            if index_name not in server_schema._indexes:
                self.ensure_index(index)
            for field_name, field in index._fields.items():
                if field_name not in RESERVED_FIELDS:
                    self.ensure_field(field)

        # find out remote - local schema
        diff_schema = server_schema._diff(schema)
        for index_name, index in diff_schema._indexes.items():
            local_index = schema._indexes.get(index_name)
            if local_index is None:
                schema._indexes[index_name] = index
            else:
                for field_name, field in index._fields.items():
                    if field_name not in RESERVED_FIELDS:
                        local_index._fields[field_name] = field

    def import_field(self, field, bit_reader, batch_size=100000, fast_import=False, clear=False):
        """Imports a field using the given bit reader

        :param pilosa.Field field: The field to import into
        :param object bit_reader: An iterator that returns a bit on each call
        :param int batch_size: Number of bits to read from the bit reader before posting them to the server
        :param bool fast_import: Enables fast import for data with columnID/rowID bits
        :param clear: clear bits instead of setting them
        """
        shard_width = field.index.shard_width or DEFAULT_SHARD_WIDTH
        for shard, columns in batch_columns(bit_reader, batch_size, shard_width):
            self._import_data(field, shard, columns, fast_import, clear)

    def http_request(self, method, path, data=None, headers=None):
        """Sends an HTTP request to the Pilosa server

        NOTE: This function is experimental and may be removed in later revisions.

        :param str method: HTTP method
        :param str path: Request path
        :param bytes data: Request body
        :param headers: Request headers
        :return: HTTP response

        """
        return self.__http_request(method, path, data=data, headers=headers)

    def _import_data(self, field, shard, data, fast_import, clear):
        if field.field_type != "int":
            # sort by row_id then by column_id
            if not field.index.keys:
                data.sort(key=lambda col: (col.row_id, col.column_id))
        if self.use_manual_address:
            nodes = [_Node.from_uri(self.__current_host)]
        else:
            if field.index.keys or field.keys:
                nodes = [self._fetch_coordinator_node()]
            else:
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
            if field.field_type == "int":
                client._import_node(_ImportValueRequest(field, shard, data), clear)
            else:
                req = _ImportRequest(field, shard, data)
                if fast_import and field.field_type in ["set", "bool", "time"] and req.format == csv_row_id_column_id:
                    client._import_node_fast(req, clear)
                else:
                    client._import_node(req, clear)

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

    def _fetch_coordinator_node(self):
        response = self.__http_request("GET", "/status")
        content = response.data.decode("utf-8")
        d = json.loads(content)
        for node in d.get("nodes", []):
            if node.get("isCoordinator"):
                uri = node["uri"]
                return _Node(uri["scheme"], uri["host"], uri["port"])
        raise PilosaServerError(response)

    def _import_node(self, import_request, clear):
        data = import_request.to_protobuf()
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
        }
        clear_str = "?clear=true" if clear else ""
        path = "/index/%s/field/%s/import%s" % (import_request.index_name, import_request.field_name, clear_str)
        self.__http_request("POST", path, data=data, headers=headers)

    def _import_node_fast(self, import_request, clear):
        data = import_request.to_bitmap(clear)
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
        }
        path = "/index/%s/field/%s/import-roaring/%d" % \
               (import_request.index_name, import_request.field_name, import_request.shard)
        self.__http_request("POST", path, data=data, headers=headers)

    def __http_request(self, method, path, data=None, headers=None, use_coordinator=False):
        if not self.__client:
            self.__connect()
        # try at most 10 non-failed hosts; protect against broken cluster.remove_host
        for _ in range(_MAX_HOSTS):
            uri = ""
            if use_coordinator:
                with self.__coordinator_lock:
                    if self.__coordinator_uri is None:
                        node = self._fetch_coordinator_node()
                        uri = "%s://%s:%s%s" % (node.scheme, node.host, node.port, path)
                        self.__coordinator_uri = uri
            else:
                uri = "%s%s" % (self.__get_address(), path)
            try:
                self.logger.debug("Request: %s %s %s", method, uri)
                response = self.__client.request(method, uri, body=data, headers=headers)
                break
            except urllib3.exceptions.MaxRetryError as e:
                if not self.use_manual_address:
                    if use_coordinator:
                        self.__coordinator_uri = None
                        self.logger.warning("Removed coordinator %s due to %s", self.__coordinator_uri, str(e))
                    else:
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

    * See `Pilosa Python Client/Server Interaction <https://github.com/pilosa/python-pilosa/blob/master/docs/server-interaction.md>`_.
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
        self.field_time_quantum = ""
        if field.time_quantum and field.time_quantum != TimeQuantum.NONE:
            self.field_time_quantum = str(field.time_quantum)
        self.shard = shard
        self.columns = columns
        if field.index.keys:
            self.format = csv_row_key_column_key if field.keys else csv_row_id_column_key
        else:
            self.format = csv_row_key_column_id if field.keys else csv_row_id_column_id
        self._time_formats = {
            "Y": "%Y",
            "M": "%Y%m",
            "D": "%Y%m%d",
            "H": "%Y%m%d%H"
        }

    def to_protobuf(self, return_bytearray=_IS_PY2):
        request = internal.ImportRequest()
        request.Index = self.index_name
        request.Field = self.field_name
        request.Shard = self.shard
        row_ids = request.RowIDs
        column_ids = request.ColumnIDs
        row_keys = request.RowKeys
        column_keys = request.ColumnKeys
        timestamps = request.Timestamps

        row_format = self.format
        if row_format == csv_row_id_column_id:
            for bit in self.columns:
                row_ids.append(bit.row_id)
                column_ids.append(bit.column_id)
                timestamps.append(bit.timestamp)
        elif row_format == csv_row_id_column_key:
            for bit in self.columns:
                row_ids.append(bit.row_id)
                column_keys.append(bit.column_key)
                timestamps.append(bit.timestamp)
        elif row_format == csv_row_key_column_id:
            for bit in self.columns:
                row_keys.append(bit.row_key)
                column_ids.append(bit.column_id)
                timestamps.append(bit.timestamp)
        elif row_format == csv_row_key_column_key:
            for bit in self.columns:
                row_keys.append(bit.row_key)
                column_keys.append(bit.column_key)
                timestamps.append(bit.timestamp)
        else:
            raise PilosaError("Invalid import format")

        return bytearray(request.SerializeToString()) if return_bytearray else request.SerializeToString()

    def to_bitmap(self, clear, return_bytearray=_IS_PY2):
        shard_width = 1048576
        if self.field_time_quantum:
            data = self._field_time_to_roaring(shard_width, clear)
        else:
            data = self._field_set_to_roaring(shard_width, clear)
        return bytearray(data) if return_bytearray else data

    def _field_set_to_roaring(self, shard_width, clear):
        bitmap = Bitmap()
        for b in self.columns:
            bitmap.add(b.row_id * shard_width + b.column_id % shard_width)
        bitmaps = {"": bitmap}
        return self._make_roaring_request(bitmaps, clear)

    def _field_time_to_roaring(self, shard_width, clear):
        standard = Bitmap()
        bitmaps = {"": standard}
        time_quantum = self.field_time_quantum
        time_formats = self._time_formats
        for b in self.columns:
            bit = b.row_id * shard_width + b.column_id % shard_width
            standard.add(bit)
            for c in time_quantum:
                fmt = time_formats.get(c, "")
                view_name = datetime.utcfromtimestamp(b.timestamp).strftime(fmt)
                bmp = bitmaps.get(view_name)
                if not bmp:
                    bmp = Bitmap()
                    bitmaps[view_name] = bmp
                bmp.add(bit)
        return self._make_roaring_request(bitmaps, clear)

    def _make_roaring_request(self, bitmaps, clear):
        req = internal.ImportRoaringRequest()
        for name, bitmap in bitmaps.items():
            bio = io.BytesIO()
            bitmap.write_to(bio)
            view = req.views.add()
            view.Name = name
            view.Data = bio.getvalue()
        req.Clear = clear
        return req.SerializeToString()


class _ImportValueRequest:

    def __init__(self, field, shard, field_values):
        self.index_name = field.index.name
        self.field_name = field.name
        self.shard = shard
        self.field_values = field_values
        self.format = csv_column_key_value if field.index.keys else csv_column_id_value

    def to_protobuf(self, return_bytearray=_IS_PY2):
        request = internal.ImportValueRequest()
        request.Index = self.index_name
        request.Field = self.field_name
        request.Shard = self.shard
        column_ids = request.ColumnIDs
        column_keys = request.ColumnKeys
        values = request.Values

        if self.format == csv_column_id_value:
            for field_value in self.field_values:
                column_ids.append(field_value.column_id)
                values.append(field_value.value)
        elif self.format == csv_column_key_value:
            for field_value in self.field_values:
                column_keys.append(field_value.column_key)
                values.append(field_value.value)
        else:
            raise PilosaError("Invalid import format")

        return bytearray(request.SerializeToString()) if return_bytearray else request.SerializeToString()


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

    @classmethod
    def from_uri(cls, uri):
        return cls(uri.scheme, uri.host, uri.port)

    @property
    def url(self):
        if self.port:
            return "%s://%s:%s" % (self.scheme, self.host, self.port)
        return "%s://%s" % (self.scheme, self.host)