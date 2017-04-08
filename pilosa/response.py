class BitmapResult:

    def __init__(self, bits=None, attributes=None):
        self.bits = bits or []
        self.attributes = attributes or {}

    @classmethod
    def from_dict(cls, d):
        if d is None:
            return BitmapResult()
        return BitmapResult(bits=d.get("bits"), attributes=d.get("attrs"))


class CountResultItem:

    def __init__(self, id, count):
        self.id = id
        self.count = count

    @classmethod
    def from_dict(cls, d):
        return CountResultItem(d["id"], d["count"])


class QueryResult:

    def __init__(self, bitmap=None, count_items=None, count=0):
        self.bitmap = bitmap or BitmapResult()
        self.count_items = count_items or []
        self.count = count

    @classmethod
    def from_item(cls, item):
        result = cls()
        if isinstance(item, dict):
            result.bitmap = BitmapResult.from_dict(item)
        elif isinstance(item, list):
            result.count_items = [CountResultItem.from_dict(x) for x in item]
        elif isinstance(item, int):
            result.count = item
        return result


class ProfileItem:

    def __init__(self, id, attributes):
        self.id = id
        self.attributes = attributes

    @classmethod
    def from_dict(cls, d):
        return ProfileItem(d["id"], d["attrs"])


class QueryResponse(object):

    def __init__(self, results=None, profiles=None):
        self.results = results or []
        self.profiles = profiles or []

    @classmethod
    def from_dict(cls, d):
        response = QueryResponse()
        response.results = [QueryResult.from_item(r) for r in d.get("results", [])]
        response.profiles = [ProfileItem.from_dict(p) for p in d.get("profiles", [])]
        response.error_message = d.get("error", "")
        return response

    @property
    def result(self):
        return self.results[0] if self.results else None

    @property
    def profile(self):
        return self.profiles[0] if self.profiles else None


