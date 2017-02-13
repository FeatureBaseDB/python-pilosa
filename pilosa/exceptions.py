class PilosaException(Exception):
    pass

class PilosaError(PilosaException):
    pass

class PilosaNotAvailable(PilosaException):
    pass

class InvalidQuery(PilosaException):
    pass
