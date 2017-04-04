import logging
import random
import re

import requests
from requests.exceptions import ConnectionError

from .exceptions import PilosaError, PilosaNotAvailable, PilosaURIError
from .version import get_version

logger = logging.getLogger(__name__)

DEFAULT_HOST = '127.0.0.1:15000'


class QueryResult(object):
    def __init__(self, result):
        self._raw = result

    def get_key(self, key):
        try:
            return self._raw[key]
        except KeyError:
            raise PilosaError('Key {} does not exist in results dict {}'.format(
                key, self._raw
            ))

    def bits(self):
        return self.get_key('bits')

    def attrs(self):
        return self.get_key('attrs')

    def count(self):
        return self.get_key('count')

    def value(self):
        return self._raw

class PilosaResponse(object):
    def __init__(self, response):
        self._raw = response
        try:
            results = response['results']
        except KeyError:
            raise PilosaError('Response invalid: {}'.format(response))
        self.results = [QueryResult(result) for result in results]

    def _check_index(self, index):
        if len(self.results) < index + 1:
            raise PilosaError('Invalid index {}: Only {} results exist.'.format(
                index, len(self.results)
            ))

    def bits(self, index=0):
        self._check_index(index)
        return self.results[index].bits()

    def attrs(self, index=0):
        self._check_index(index)
        return self.results[index].attrs()

    def count(self, index=0):
        self._check_index(index)
        return self.results[index].count()

    def value(self, index=0):
        self._check_index(index)
        return self.results[index].value()

    def values(self):
        return [self.results[index].value() for index in range(len(self.results))]

    def __repr__(self):
        return '<PilosaResponse {}>'.format(self.values())


class QueryOptions:

    def __init__(self, profiles=False):
        self.profiles = profiles


class Client(object):

    def __init__(self, hosts=None):
        self.hosts = hosts or [DEFAULT_HOST]

    def _get_random_host(self):
        return self.hosts[random.randint(0, len(self.hosts) - 1)]

    def query(self, query, options=QueryOptions()):
        return self.send_query_string_to_pilosa(str(query), query.database.name, options.profiles)

    def send_query_string_to_pilosa(self, query_strings, db, profiles):
        url = 'http://{}/query?db={}'.format(self._get_random_host(), db)
        if profiles:
            url += '&profiles=true'

        headers = {
            'Accept': 'application/vnd.pilosa.json.v1',
            'Content-Type': 'application/vnd.pilosa.pql.v1',
            'User-Agent': 'pilosa-driver/' + get_version(),
        }

        try:
            response = requests.post(url, data=query_strings, headers=headers)
        except ConnectionError as e:
            raise PilosaNotAvailable(str(e.message))

        if response.status_code == 400:
            try:
                error = response.json()
                raise PilosaError(error['error'])
            except (ValueError, KeyError):
                raise PilosaError(response.content)

        if response.headers.get('Warning'):
            logger.warning(response.headers['Warning'])

        return PilosaResponse(response.json())


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

    def __init__(self, scheme, host, port):
        self.scheme = scheme
        self.host = host
        self.port = port

    @classmethod
    def default(cls):
        return cls("http", "localhost", 10101)

    @classmethod
    def from_address(cls, address):
        uri = cls.default()
        uri._parse(address)
        return uri

    @classmethod
    def with_host_port(cls, host, port):
        return URI("http", host, port)

    def normalized(self):
        scheme = self.scheme
        index = scheme.index("+")
        if index > 0:
            scheme = scheme[:index]
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

    def __eq__(self, other):
        if other is None or not isinstance(other, self.__class__):
            return False
        return self.scheme == other.scheme and \
            self.host == other.host and \
            self.port == other.port
