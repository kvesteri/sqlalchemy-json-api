class QueryBuilderException(Exception):
    pass


class InvalidField(QueryBuilderException):
    pass


class UnknownField(QueryBuilderException):
    pass


class UnknownModel(QueryBuilderException):
    pass


class UnknownFieldKey(QueryBuilderException):
    pass


class IdPropertyNotFound(QueryBuilderException):
    pass
