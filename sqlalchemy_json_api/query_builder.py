import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import union
from sqlalchemy_utils.functions import get_mapper
from sqlalchemy_utils.functions.orm import get_all_descriptors
from sqlalchemy_utils.relationships import (
    path_to_relationships,
    select_correlated_expression
)

from .exc import (
    IdPropertyNotFound,
    InvalidField,
    UnknownField,
    UnknownFieldKey,
    UnknownModel
)
from .utils import (
    get_attrs,
    get_selectable,
    get_descriptor_columns,
    s,
    subpaths
)


json_array = sa.cast(postgresql.array([]), postgresql.ARRAY(JSON))


class QueryBuilder(object):
    def __init__(self, mapping):
        self.validate_mapping(mapping)
        self.mapping = mapping
        self.inversed = dict(
            (value, key) for key, value in self.mapping.items()
        ) if mapping else None

    def validate_mapping(self, mapping):
        for model in mapping.values():
            if 'id' not in get_all_descriptors(model).keys():
                raise IdPropertyNotFound(
                    "Couldn't find 'id' property for model {0}.".format(
                        model
                    )
                )

    def validate_column(self, field, column):
        # Check that given column is an actual Column object and not for
        # example select expression
        if isinstance(column, sa.Column):
            if column.foreign_keys:
                raise InvalidField(
                    "Field '{0}' is invalid. The underlying column "
                    "'{1}' has foreign key. You can't include foreign key "
                    "attributes. Consider including relationship "
                    "attributes.".format(
                        field, column.key
                    )
                )
            elif column.primary_key:
                raise InvalidField(
                    "Field '{0}' is invalid. The underlying column "
                    "'{1}' is primary key column.".format(
                        field, column.key
                    )
                )

    def is_relationship_field(self, model, field):
        return field in get_mapper(model).relationships.keys()

    def validate_fields(self, model, fields, from_obj):
        selectable_descriptors = get_all_descriptors(from_obj)
        for field in fields:
            if field not in selectable_descriptors.keys():
                raise UnknownField(
                    "Unknown field '{0}'. Given selectable does not have "
                    "descriptor named '{0}'.".format(field)
                )
            columns = get_descriptor_columns(
                model,
                selectable_descriptors[field]
            )
            for column in columns:
                self.validate_column(field, column)

    def is_relationship_descriptor(self, descriptor):
        return (
            isinstance(descriptor, InstrumentedAttribute) and
            isinstance(descriptor.property, sa.orm.RelationshipProperty)
        )

    def should_skip_columnar_descriptor(self, from_obj, descriptor):
        columns = get_descriptor_columns(from_obj, descriptor)
        return (
            len(columns) == 1 and
            (columns[0].foreign_keys or columns[0].primary_key)
        )

    def get_all_fields(self, from_obj):
        return [
            field
            for field, descriptor in get_all_descriptors(from_obj).items()
            if (
                field != '__mapper__' and
                not self.is_relationship_descriptor(descriptor) and
                not self.should_skip_columnar_descriptor(from_obj, descriptor)
            )
        ]

    def get_model_fields(self, model, fields, from_obj):
        model_key = self.get_model_alias(model)

        if not fields or model_key not in fields:
            model_fields = self.get_all_fields(from_obj)
        else:
            model_fields = [
                field for field in fields[model_key]
                if not self.is_relationship_field(model, field)
            ]
            self.validate_fields(model, model_fields, from_obj)
        return model_fields

    def build_attributes(self, model, fields, from_obj):
        cols = get_attrs(from_obj)
        return sum(
            (
                [s(key), getattr(cols, key)]
                for key in self.get_model_fields(model, fields, from_obj)
            ),
            []
        )

    def get_model_alias(self, model):
        if isinstance(model, sa.orm.util.AliasedClass):
            key = sa.inspect(model).mapper.class_
        else:
            key = model
        self.validate_model(key)
        return self.inversed[key]

    def build_resource_identifier(self, model, from_obj):
        model_alias = self.get_model_alias(model)
        return [
            s('id'),
            sa.cast(get_attrs(from_obj).id, sa.Text),
            s('type'),
            s(model_alias),
        ]

    def build_attrs_and_relationships(self, model, fields, from_obj):
        json_fields = []
        attrs = self.build_attributes(
            model,
            fields=fields,
            from_obj=from_obj
        )
        json_relationships = self.build_relationships(model, fields, from_obj)

        if attrs:
            json_fields.extend([
                s('attributes'),
                sa.func.json_build_object(*attrs)
            ])

        if json_relationships:
            json_fields.extend([
                s('relationships'),
                sa.func.json_build_object(
                    *json_relationships
                )
            ])
        return json_fields

    def build_relationship(self, model, fields, relationship, from_obj):
        cls = relationship.mapper.class_
        alias = sa.orm.aliased(cls)
        relationship_attrs = self.build_resource_identifier(alias, alias)
        func = sa.func.json_build_object(*relationship_attrs).label(
            'json_object'
        )
        query = select_correlated_expression(
            model,
            func,
            relationship.key,
            alias,
            get_selectable(from_obj),
            order_by=relationship.order_by
        ).alias('relationships')
        if relationship.uselist:
            query = sa.select([
                sa.func.coalesce(
                    sa.func.array_agg(query.c.json_object),
                    json_array
                )
            ]).select_from(query)

        return [
            s(relationship.key),
            sa.func.json_build_object(
                s('data'),
                query.as_scalar()
            )
        ]

    def build_relationships(self, model, fields, from_obj):
        model_alias = self.get_model_alias(model)
        if model_alias not in fields:
            relationships = list(get_mapper(model).relationships.values())
        else:
            relationships = [
                get_mapper(model).relationships[field]
                for field in fields[model_alias]
                if field in get_mapper(model).relationships.keys()
            ]
        return sum(
            (
                self.build_relationship(model, fields, relationship, from_obj)
                for relationship in relationships
            ),
            []
        )

    def build_data(self, model, fields, include, from_obj):
        json_fields = self.build_resource_identifier(model, from_obj)
        json_fields.extend(
            self.build_attrs_and_relationships(
                model,
                fields,
                from_obj
            )
        )

        subquery = sa.select(
            [
                sa.func.json_build_object(*json_fields)
                .label('json_object')
            ]
        ).correlate(from_obj).alias('main_json')
        return [
            s('data'),
            sa.select(
                [sa.func.array_agg(subquery.c.json_object)],
                from_obj=subquery
            ).correlate(from_obj).as_scalar()
        ]

    def validate_model(self, model):
        if model not in self.inversed:
            raise UnknownModel(
                'Unknown model given. Could not find model %r from given '
                'mapping.' % model
            )

    def validate_field_keys(self, fields):
        if fields:
            unknown_keys = set(fields) - set(self.mapping.keys())
            if unknown_keys:
                raise UnknownFieldKey(
                    'Unknown field keys given. Could not find {0} {1} from '
                    'given model mapping.'.format(
                        'keys' if len(unknown_keys) > 1 else 'key',
                        ','.join("'{0}'".format(key) for key in unknown_keys)
                    )
                )

    def select(self, model, fields=None, include=None, from_obj=None):
        """
        :param model:
            The root model to build the select query from.
        :param fields:
            A mapping of fields. Keys representing model keys and values as
            lists of model descriptor names.
        :param include:
            List of dot-pathed relationship paths.
        """
        self.validate_field_keys(fields)
        if fields is None:
            fields = {}
        if from_obj is None:
            from_obj = model

        args = self.build_data(
            model,
            fields,
            include,
            from_obj
        )

        empty_args = ['data', json_array]
        if include:
            empty_args.extend(['included', json_array])

        included = self.build_included(model, fields, include, from_obj)

        if included:
            args.extend(included)

        query = sa.select([
            sa.func.coalesce(
                sa.select(
                    [sa.func.json_build_object(*args)],
                    from_obj=from_obj
                ).as_scalar(),
                sa.func.json_build_object(*empty_args)
            )
        ])
        return query

    def build_single_included_fields(self, alias, fields):
        cls_key = self.get_model_alias(alias)
        json_fields = self.build_resource_identifier(alias, alias)
        if cls_key in fields:
            json_fields.extend(
                self.build_attrs_and_relationships(
                    alias,
                    fields,
                    sa.inspect(alias).selectable
                )
            )
        return json_fields

    def build_single_included(self, model, fields, path, from_obj):
        relationships = path_to_relationships(path, model)

        cls = relationships[-1].mapper.class_
        alias = sa.orm.aliased(cls)

        func = sa.cast(
            sa.func.json_build_object(
                *self.build_single_included_fields(alias, fields)
            ),
            JSONB
        ).label('json_object')

        query = select_correlated_expression(
            model,
            func,
            path,
            alias,
            get_selectable(from_obj)
        )
        if cls is model:
            query = query.where(
                alias.id.notin_(
                    sa.select(
                        [get_attrs(from_obj).id],
                        from_obj=from_obj
                    )
                )
            )
        return query

    def build_included(self, model, fields, include, from_obj):
        included = []
        if include:
            included.append(s('included'))
            selects = [
                self.build_single_included(model, fields, subpath, from_obj)
                for path in include
                for subpath in subpaths(path)
            ]

            union_select = union(*selects).alias('included_union')
            subquery = sa.select(
                [union_select.c.json_object],
                from_obj=union_select
            ).order_by(
                union_select.c.json_object[s('type')],
                union_select.c.json_object[s('id')]
            ).alias('included')
            included.append(
                sa.select(
                    [sa.func.array_agg(subquery.c.json_object, [])],
                    from_obj=subquery
                ).as_scalar()
            )
        return included
