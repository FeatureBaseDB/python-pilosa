import logging
import boto3
from collections import namedtuple
import requests
import random
from query import Query, InvalidQuery
from kinesis import KinesisEncoder
logger = logging.getLogger(__name__)

name_tuple = namedtuple('PilosaSettings',
                        'kinesis_firehose_stream,'
                        'kinesis_encode_type,'
                        'kinesis_region_name, '
                        'aws_access_key_id,'
                        'aws_secret_access_key,'
                        'hosts')
DEFAULT_HOST = '127.0.0.1:15000'


class PilosaSettings(name_tuple):
    def __new__(cls,
                kinesis_firehose_stream=None,
                kinesis_encode_type=None,
                kinesis_region_name=None,
                aws_access_key_id=None,
                aws_secret_access_key=None,
                hosts=None
                ):
        return super(PilosaSettings, cls).__new__(cls, kinesis_firehose_stream,
                                                  kinesis_encode_type,
                                                  kinesis_region_name,
                                                  aws_access_key_id,
                                                  aws_secret_access_key,
                                                  hosts)


class Cluster(object):

    def __init__(self, settings=None):
        """
        settings: if provided, cluster will be initiated with
            kinesis_firehose_stream
            kinesis_encode_type
            kinesis_region_name
            aws_access_key_id
            aws_secret_access_key
            hosts: a list of host:port strings
        - or not provided-
        default host: 127.0.0.1:15000
        """
        self.USE_KINESIS = False
        if not settings:
            self.hosts = [DEFAULT_HOST]
        elif settings.kinesis_firehose_stream:
            self.kinesis_firehose_stream = settings.kinesis_firehose_stream
            self.kinesis_encode_type = settings.kinesis_encode_type or 1
            self.kinesis_region_name = settings.kinesis_region_name or 'us-east-1'
            self.aws_access_key_id = settings.aws_access_key_id
            self.aws_secret_access_key = settings.aws_secret_access_key
            self.USE_KINESIS = True
        elif settings.hosts:
            self.hosts = settings.hosts

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
            url = 'http://%s/query?db=%s' % (self._get_random_host(), db)
            return requests.post(url, data=query)
        elif type(query) is not list:
            query = [query]
        for q in query:
            if not isinstance(q, Query):
                raise InvalidQuery('%s is not an instance of Query' % (q))

        if self.USE_KINESIS:
            # only send writes to kinesis
            query_strings = ' '.join(q.to_pql() for q in query if q.IS_WRITE)
            # don't bother sending data to kinesis if there are no write queries
            if not query_strings:
                return
            message = KinesisEncoder.encode(db, query_strings, encode_type=self.kinesis_encode_type)
            # TODO: we need to check to see if the message is larger than 1MB. if so, we need to
            # break it up into multiple puts to kinesis (less than 1MB each)
            try:
                firehose_client = boto3.client('firehose',
                                               region_name=self.kinesis_region_name,
                                               aws_access_key_id=self.aws_access_key_id,
                                               aws_secret_access_key=self.aws_secret_access_key,
                                               )
            except Exception as ex:
                logger.error('boto connection error', extra={
                    'data': {
                        'kinesis_region_name': self.kinesis_region_name,
                        'error': ex.message,
                    },
                })
                raise PilosaException('Connection error: %s' % ex)
            return firehose_client.put_record(DeliveryStreamName=self.kinesis_firehose_stream, Record={'Data': message})
        else:
            query_strings = ' '.join(q.to_pql() for q in query)
            url = 'http://%s/query?db=%s' % (self._get_random_host(), db)
            if profiles:
                url += '&profiles=true'
            return requests.post(url, data=query_strings)

class PilosaException(Exception):
    pass
