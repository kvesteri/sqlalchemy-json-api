class JSONAPIException(Exception):
    pass


class InvalidField(JSONAPIException):
    pass


class UnknownField(JSONAPIException):
    pass


class UnknownModel(JSONAPIException):
    pass


class UnknownFieldKey(JSONAPIException):
    pass


class IdPropertyNotFound(JSONAPIException):
    pass
