import requests
import random

from query import Query, InvalidQuery
from kinesis import KinesisEncoder


class Cluster(object):

    def __init__(self, hosts=["127.0.0.1:15000"], settings=None):
        """
        settings: if provided, cluster will be initiated with
            PILOSA_KINESIS_FIREHOSE_STREAM
            PILOSA_KINESIS_ENCODE_TYPE
            PILOSA_KINESIS_REGION_NAME
            AWS_ACCESS_KEY_ID
            AWS_SECRET_ACCESS_KEY
            - or -
            PILOSA_HOSTS
        - or -
        hosts: a list of host:port strings
        """
        self.USE_KINESIS = False
        if hasattr(settings, 'PILOSA_KINESIS_FIREHOSE_STREAM'):
            self.kinesis_firehose_stream = settings.PILOSA_KINESIS_FIREHOSE_STREAM
            self.kinesis_encode_type = getattr(settings, 'PILOSA_KINESIS_ENCODE_TYPE', 1)
            self.kinesis_region_name = getattr(settings, 'PILOSA_KINESIS_REGION_NAME', 'us-east-1')
            self.aws_access_key_id = getattr(settings, 'AWS_ACCESS_KEY_ID')
            self.aws_secret_access_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY')
            self.USE_KINESIS = True
        elif hasattr(settings, 'PILOSA_HOSTS'):
            self.hosts = settings.PILOSA_HOSTS
        else:
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

        #query_strings = ' '.join(q.to_pql() for q in query)

        if self.USE_KINESIS:
            import boto3
            import json
            # only send writes to kinesis
            query_strings = ' '.join(q.to_pql() for q in query if q.IS_WRITE)
            # don't bother sending data to kinesis if there are no write queries
            if not query_strings:
                return
            message = KinesisEncoder.encode(db, query_strings, encode_type=self.kinesis_encode_type)
            # TODO: we need to check to see if the message is larger than 1MB. if so, we need to
            # break it up into multiple puts to kinesis (less than 1MB each)
            firehose_client = boto3.client('firehose',
                region_name = self.kinesis_region_name,
                aws_access_key_id = self.aws_access_key_id,
                aws_secret_access_key = self.aws_secret_access_key,
                )
            return firehose_client.put_record(DeliveryStreamName=self.kinesis_firehose_stream, Record={ 'Data': message })
        else:
            query_strings = ' '.join(q.to_pql() for q in query)
            return requests.post('http://%s/query?db=%s'%(self._get_random_host(), db), data=query_strings)
