class QueryBuilderException(Exception):
    pass


class InvalidField(QueryBuilderException):
    """
    This error is raised when trying to include a foreign key field or if the
    field is reserved keyword.
    """
    pass


class UnknownField(QueryBuilderException):
    """
    This error is raised if the selectable given to one of the select_* methods
    of QueryBuilder does not contain given field.
    """
    pass


class UnknownModel(QueryBuilderException):
    """
    If the resource registry of this query builder does not contain the
    given model.
    """
    pass


class UnknownFieldKey(QueryBuilderException):
    """
    If the given field list key is not present in the resource registry of
    a query builder.
    """
    pass


class IdPropertyNotFound(QueryBuilderException):
    """
    This error is raised when one of the referenced models in QueryBuilder
    query building process does not have an id property.
    """
    pass
