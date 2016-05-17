import struct
import math


class KinesisEncoder(object):

    @staticmethod
    def encode(database, pql, encode_type=1):
        payload = "%s:%s"%(database, pql)
        size = len(payload)
        encoding =  struct.pack("<I", size + (encode_type * math.pow(2,24)))
        return "%s%s"%(encoding, payload)
