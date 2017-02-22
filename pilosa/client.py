import logging
import requests
import random
from .version import get_version
from .query import Query
from .exceptions import PilosaError, PilosaNotAvailable, InvalidQuery
from requests.exceptions import ConnectionError

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

class Client(object):
    def __init__(self, hosts=None):
        self.hosts = hosts or [DEFAULT_HOST]

    def _get_random_host(self):
        return self.hosts[random.randint(0, len(self.hosts) - 1)]

    def query(self, db, query, profiles=False):
        """
        query is either a Query object or a list of Query objects or pql string
        profiles is a binary that indicates whether to return the entire profile (inc. attrs)
        in a Bitmap() query, or just the profile ID
        """
        if not query:
            return

        # Python 3 compatibility:
        try:
            basestring
        except NameError:
            basestring = str

        if isinstance(query, basestring):
            return self.send_query_string_to_pilosa(query, db, profiles)
        elif type(query) is not list:
            query = [query]
        for q in query:
            if not isinstance(q, Query):
                raise InvalidQuery('{} is not an instance of Query'.format(q))

        query_strings = ' '.join(q.to_pql() for q in query)
        return self.send_query_string_to_pilosa(query_strings, db, profiles)

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
