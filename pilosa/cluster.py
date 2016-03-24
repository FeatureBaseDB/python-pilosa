import requests
import random

from query import Query, InvalidQuery


class Cluster(object):

    def __init__(self, hosts=["127.0.0.1:15000"]):
        self.hosts = hosts

    def _get_random_host(self):
        return self.hosts[random.randint(0, len(self.hosts) - 1)]

    def execute(self, db, query):
        """
        query is either a Query object or a list of Query objects
        """
        if type(query) is not list:
            query = [query]
        for q in query:
            if not isinstance(q, Query):
                raise InvalidQuery('%s is not an intance of Query' % (q))

        query_strings = ' '.join(q.to_pql() for q in query)
        return requests.post('http://%s/query?db=%s'%(self._get_random_host(), db), data=query_strings)
