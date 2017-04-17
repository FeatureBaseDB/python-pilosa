class PilosaException(Exception):
    pass


class PilosaError(PilosaException):
    pass


class PilosaNotAvailable(PilosaException):
    pass


class InvalidQuery(PilosaException):
    pass


class ValidationError(PilosaError):
    pass


class PilosaURIError(PilosaError):
    pass


class DatabaseExistsError(PilosaError):
    pass


class FrameExistsError(PilosaError):
    pass

