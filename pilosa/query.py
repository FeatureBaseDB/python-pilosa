from .exceptions import InvalidQuery


def escape_string_value(val):
    if type(val) is bool:
        return str(val).lower()
    if isinstance(val, str):
        return '"%s"'%(val)
    return str(val)

class Query(object):
    IS_WRITE = False
    """
    A base class that helps Cluster.execute determine that query is 
    an instance of a subclass of Query

    By default, supports any subclass that takes one or more Query objects as *inputs
    """
    def __init__(self, *inputs):
        self.inputs = inputs
        if hasattr(self, 'input_limit') and len(self.inputs) > self.input_limit:
            raise InvalidQuery("number of inputs (%s) exceeds input limit (%s) for %s query" % (len(self.inputs), self.input_limit, self.__class__.__name__))

    def to_pql(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(subq.to_pql() for subq in self.inputs))


class SetBit(Query):
    IS_WRITE = True
    def __init__(self, id, frame, profile_id):
        self.id = int(id)
        self.frame = frame
        self.profile_id = int(profile_id)
   
    def to_pql(self):
        return '%s(id=%s, frame="%s", profileID=%s)' % (self.__class__.__name__, self.id, self.frame, self.profile_id)


class ClearBit(SetBit):
    IS_WRITE = True
    pass


class SetBitmapAttrs(Query):
    IS_WRITE = True
    def __init__(self, id, frame, **attrs):
        self.id = int(id)
        self.frame = frame
        self.attrs = attrs
        if len(self.attrs) == 0:
            raise InvalidQuery("no attribute provided")
  
    def to_pql(self):
        attrs = ', '.join("%s=%s"%(k,escape_string_value(v)) for k,v in self.attrs.items())
        return 'SetBitmapAttrs(id=%s, frame="%s", %s)' % (self.id, self.frame, attrs)


class Bitmap(Query):
    def __init__(self, id, frame):
        self.id = int(id)
        self.frame = frame
   
    def to_pql(self):
        return 'Bitmap(id=%s, frame="%s")'%(self.id, self.frame)


class SetProfileAttrs(Query):
    IS_WRITE = True
    def __init__(self, id, **attrs):
        self.id = int(id)
        self.attrs = attrs
        if len(self.attrs) == 0:
            raise InvalidQuery("no attribute provided")

    def to_pql(self):
        attrs = ', '.join("%s=%s"%(k,escape_string_value(v)) for k,v in self.attrs.items())
        return 'SetProfileAttrs(id=%s, %s)' % (self.id, attrs)


class Union(Query):
    pass


class Intersect(Query):
    pass


class Difference(Query):
    input_limit = 2


class Count(Query):
    input_limit = 1


class Range(Query):
    def __init__(self, id, frame, start, end):
        self.id = id
        self.frame = frame
        self.start = start
        self.end = end

    def to_pql(self):
        return 'Range(id=%s, frame="%s", start="%s", end="%s")'%(self.id, self.frame, self.start.isoformat()[:-3], self.end.isoformat()[:-3])


# TODO: implement Profile()
# Profile(id=1)


class TopN(Query):
    """
    The query argument represents the comparison filter to use during the TopN query.
    If query is set to None, then no filter will be applied and all bitmaps within the
    frame will be considered.
    """
    def __init__(self, query, frame, n, ids=None, filter_field=None, filter_values=[]):
        self.query = query
        self.frame = frame
        self.n = int(n)
        # TODO: support the 'ids' argument
        self.filter_field = filter_field
        self.filter_values = filter_values

    def to_pql(self):
        pql = 'TopN('
        if self.query:
            pql +='%s, '%(self.query.to_pql())
        pql += 'frame="%s", n=%s'%(self.frame, self.n)
        if self.filter_field:
            pql += ', field="%s", [%s]'%(self.filter_field, ','.join(escape_string_value(v) for v in self.filter_values))
        pql += ')'
        return pql

if __name__ == '__main__':
    #print SetBitmapAttrs(1, 'foo', cat=399, foo="bar", x=True).to_pql()
    pass
    
