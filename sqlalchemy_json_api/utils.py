from itertools import chain

import sqlalchemy as sa
from sqlalchemy.orm.attributes import InstrumentedAttribute, QueryableAttribute


def get_attrs(obj):
    if isinstance(obj, sa.orm.Mapper):
        return obj.class_
    elif isinstance(obj, (sa.orm.util.AliasedClass, sa.orm.util.AliasedInsp)):
        return obj
    elif isinstance(obj, sa.sql.selectable.Selectable):
        return obj.c
    return obj


def get_selectable(obj):
    if isinstance(obj, sa.sql.selectable.Selectable):
        return obj
    return sa.inspect(obj).selectable


def subpaths(path):
    return [
        '.'.join(path.split('.')[0:i + 1])
        for i in range(len(path.split('.')))
    ]


def s(value):
    return sa.text("'{0}'".format(value))


def get_descriptor_columns(model, descriptor):
    if isinstance(descriptor, InstrumentedAttribute):
        return descriptor.property.columns
    elif isinstance(descriptor, sa.orm.ColumnProperty):
        return descriptor.columns
    elif isinstance(descriptor, sa.Column):
        return [descriptor]
    elif isinstance(descriptor, sa.sql.expression.ClauseElement):
        return []
    elif isinstance(descriptor, sa.ext.hybrid.hybrid_property):
        expr = descriptor.expr(model)
        try:
            return get_descriptor_columns(model, expr)
        except TypeError:
            return []
    elif (
        isinstance(descriptor, QueryableAttribute) and
        hasattr(descriptor, 'original_property')
    ):
        return get_descriptor_columns(model, descriptor.property)
    raise TypeError(
        'Given descriptor is not of type InstrumentedAttribute, '
        'ColumnProperty or Column.'
    )


def chain_if(*args):
    if args:
        return chain(*args)
    return []
