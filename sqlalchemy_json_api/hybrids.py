import sqlalchemy as sa
from sqlalchemy.ext.hybrid import Comparator


class CompositeId(Comparator):
    def __init__(self, keys, separator=':', label='id'):
        self.keys = keys
        self.separator = separator
        self.label = label

    def operate(self, op, other):
        if isinstance(other, sa.sql.selectable.Select):
            return op(sa.tuple_(*self.keys), other)
        if not isinstance(other, CompositeId):
            other = CompositeId(other)
        return sa.and_(
            op(key, other_key)
            for key, other_key in zip(self.keys, other.keys)
        )

    def __clause_element__(self):
        parts = [self.keys[0]]
        for key in self.keys[1:]:
            parts.append(sa.text("'{}'".format(self.separator)))
            parts.append(key)
        return sa.func.concat(*parts).label(self.label)

    def __str__(self):
        return self.separator.join(str(k) for k in self.keys)

    def __repr__(self):
        return '<CompositeId {}>'.format(repr(self.keys))
