import logging
import requests
import random
from .query import Query, InvalidQuery
logger = logging.getLogger(__name__)

DEFAULT_HOST = '127.0.0.1:15000'


class Cluster(object):

    def __init__(self, settings=None):
        """
        settings: if provided, cluster will be initiated with
            hosts: a list of host:port strings
        - or not provided-
        default host: 127.0.0.1:15000
        """
        if not settings:
            self.hosts = [DEFAULT_HOST]
        elif settings.get('hosts'):
            self.hosts = settings.get('hosts')

    def _get_random_host(self):
        return self.hosts[random.randint(0, len(self.hosts) - 1)]

    def execute(self, db, query, profiles=False):
        """
        query is either a Query object or a list of Query objects or pql string
        profiles is a binary that indicates whether to return the entire profile (inc. attrs)
        in a Bitmap() query, or just the profile ID
        """
        if not query:
            return

        if isinstance(query, str):
            return self.send_query_string_to_pilosa(query, db, profiles)
        elif type(query) is not list:
            query = [query]
        for q in query:
            if not isinstance(q, Query):
                raise InvalidQuery('%s is not an instance of Query' % (q))

        query_strings = ' '.join(q.to_pql() for q in query)
        return self.send_query_string_to_pilosa(query_strings, db, profiles)

    def send_query_string_to_pilosa(self, query_strings, db, profiles):
        url = 'http://%s/query?db=%s' % (self._get_random_host(), db)
        if profiles:
            url += '&profiles=true'
        return requests.post(url, data=query_strings)

class PilosaException(Exception):
    pass
