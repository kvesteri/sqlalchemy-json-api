import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import union
from sqlalchemy.sql.util import ClauseAdapter
from sqlalchemy_utils import get_hybrid_properties
from sqlalchemy_utils.functions import cast_if, get_mapper
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
    get_descriptor_columns,
    get_selectable,
    s,
    subpaths
)

json_array = sa.cast(
    postgresql.array([], type_=JSON), postgresql.ARRAY(JSON)
)
jsonb_array = sa.cast(
    postgresql.array([], type_=JSONB), postgresql.ARRAY(JSONB)
)


RESERVED_KEYWORDS = (
    'id',
    'type',
    'links',
    'included',
    'attributes',
    'relationships'
)


class QueryBuilder(object):
    """
    ::

        query_builder = QueryBuilder({
            'articles': Article,
            'users': User,
            'comments': Comment
        })


    :param model_mapping:
        A mapping with keys representing JSON API resource identifier type
        names and values as SQLAlchemy models.

        It is recommended to use lowercased pluralized and hyphenized names for
        resource identifier types. So for example model such as
        LeagueInvitiation should have an equivalent key of
        'league-invitations'.
    """
    def __init__(self, model_mapping):
        self.validate_model_mapping(model_mapping)
        self.model_mapping = model_mapping
        self.inversed_model_mapping = dict(
            (value, key) for key, value in self.model_mapping.items()
        ) if model_mapping else None

    def validate_model_mapping(self, model_mapping):
        for model in model_mapping.values():
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

    def is_relationship_field(self, model, field):
        return field in get_mapper(model).relationships.keys()

    def validate_fields(self, model, fields, from_obj):
        selectable_descriptors = get_all_descriptors(from_obj)
        for field in fields:
            if field in get_hybrid_properties(model):
                continue
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
        return (len(columns) == 1 and columns[0].foreign_keys)

    def get_adapted_descriptors(self, model, from_obj):
        return (
            get_all_descriptors(from_obj).items() +
            [
                (key, ClauseAdapter(from_obj).traverse(getattr(model, key)))
                for key in get_hybrid_properties(model).keys()
            ]
        )

    def get_all_fields(self, model, from_obj):
        return [
            field
            for field, descriptor
            in self.get_adapted_descriptors(model, from_obj)
            if (
                field != '__mapper__' and
                field not in RESERVED_KEYWORDS and
                not self.is_relationship_descriptor(descriptor) and
                not self.should_skip_columnar_descriptor(from_obj, descriptor)
            )
        ]

    def get_model_fields(self, model, fields, from_obj):
        model_key = self.get_model_alias(model)

        if not fields or model_key not in fields:
            model_fields = self.get_all_fields(model, from_obj)
        else:
            model_fields = [
                field for field in fields[model_key]
                if not self.is_relationship_field(model, field)
            ]
            self.validate_fields(model, model_fields, from_obj)
        return model_fields

    def adapt_attribute(self, attr_name, model, from_obj):
        cols = get_attrs(from_obj)
        hybrids = get_hybrid_properties(model).keys()
        if attr_name in hybrids:
            return ClauseAdapter(from_obj).traverse(getattr(model, attr_name))
        else:
            return getattr(cols, attr_name)

    def build_attributes(self, model, fields, from_obj):
        return sum(
            (
                [s(key), self.adapt_attribute(key, model, from_obj)]
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
        return self.inversed_model_mapping[key]

    def build_resource_identifier(self, model, from_obj):
        model_alias = self.get_model_alias(model)
        return [
            s('id'),
            cast_if(get_attrs(from_obj).id, sa.String),
            s('type'),
            s(model_alias),
        ]

    def build_attrs_and_relationships(self, model, fields, from_obj):
        parts = {
            'attributes': self.build_attributes(model, fields, from_obj),
            'relationships': self.build_relationships(model, fields, from_obj)
        }
        return sum(
            (
                [s(key), sa.func.json_build_object(*values)]
                for key, values in parts.items()
                if values
            ),
            []
        )

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

    def get_relationship_properties(self, model, fields):
        model_alias = self.get_model_alias(model)
        if model_alias not in fields:
            return list(get_mapper(model).relationships.values())
        else:
            return [
                get_mapper(model).relationships[field]
                for field in fields[model_alias]
                if field in get_mapper(model).relationships.keys()
            ]

    def build_relationships(self, model, fields, from_obj):
        return sum(
            (
                self.build_relationship(model, fields, relationship, from_obj)
                for relationship
                in self.get_relationship_properties(model, fields)
            ),
            []
        )

    def build_data_expr(self, model, fields, include, from_obj):
        json_fields = self.build_resource_identifier(model, from_obj)
        json_fields.extend(
            self.build_attrs_and_relationships(
                model,
                fields,
                from_obj
            )
        )
        return sa.func.json_build_object(*json_fields).label('data')

    def build_data(self, model, fields, include, from_obj):
        expr = self.build_data_expr(
            model,
            fields,
            include,
            from_obj
        )
        return sa.select([expr], from_obj=from_obj)

    def build_data_array(self, model, fields, include, from_obj):
        data_query = self.build_data(model, fields, include, from_obj).alias()
        return sa.select(
            [sa.func.coalesce(
                sa.func.array_agg(data_query.c.data),
                json_array
            )],
            from_obj=data_query
        ).correlate(from_obj).as_scalar().label('data')

    def validate_model(self, model):
        if model not in self.inversed_model_mapping:
            raise UnknownModel(
                'Unknown model given. Could not find model %r from given '
                'model mapping.' % model
            )

    def validate_field_keys(self, fields):
        if fields:
            unknown_keys = set(fields) - set(self.model_mapping.keys())
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
        Builds a query for selecting multiple resource instances.

        ::

            query = query_builder.select(
                Article,
                fields={'articles': ['name', 'author', 'comments']},
                include=['author', 'comments.author'],
                from_obj=session.query(Article).filter(
                    Article.id.in_([1, 2, 3, 4])
                )
            )


        :param model:
            The root model to build the select query from.
        :param fields:
            A mapping of fields. Keys representing model keys and values as
            lists of model descriptor names.
        :param include:
            List of dot-separated relationship paths.
        :param from_obj:
            A SQLAlchemy selectable (for example a Query object) to select the
            query results from.


        :raises sqlalchemy_json_api.IdPropertyNotFound:
            If one of the referenced models does not have an id property

        :raises sqlalchemy_json_api.InvalidField:
            If trying to include foreign key field.

        :raises sqlalchemy_json_api.UnknownModel:
            If the model mapping of this QueryBuilder does not contain the
            given root model.

        :raises sqlalchemy_json_api.UnknownField:
            If the given selectable does not contain given field.

        :raises sqlalchemy_json_api.UnknownFieldKey:
            If the given field list key is not present in the model mapping of
            this query builder.

        """
        if from_obj is None:
            from_obj = sa.orm.query.Query(model)

        from_obj = from_obj.subquery()

        return self._select(model, from_obj, fields, include)

    def select_one(self, model, id, fields=None, include=None, from_obj=None):
        """
        Builds a query for selecting single resource instance.

        ::

            query = query_builder.select_one(
                Article,
                1,
                fields={'articles': ['name', 'author', 'comments']},
                include=['author', 'comments.author'],
            )


        :param model:
            The root model to build the select query from.
        :param id:
            The id of the resource to select.
        :param fields:
            A mapping of fields. Keys representing model keys and values as
            lists of model descriptor names.
        :param include:
            List of dot-separated relationship paths.
        :param from_obj:
            A SQLAlchemy selectable (for example a Query object) to select the
            query results from.


        :raises sqlalchemy_json_api.IdPropertyNotFound:
            If one of the referenced models does not have an id property

        :raises sqlalchemy_json_api.InvalidField:
            If trying to include foreign key field.

        :raises sqlalchemy_json_api.UnknownModel:
            If the model mapping of this QueryBuilder does not contain the
            given root model.

        :raises sqlalchemy_json_api.UnknownField:
            If the given selectable does not contain given field.

        :raises sqlalchemy_json_api.UnknownFieldKey:
            If the given field list key is not present in the model mapping of
            this query builder.

        """
        if from_obj is None:
            from_obj = sa.orm.query.Query(model)

        from_obj = from_obj.filter(model.id == id).subquery()

        return self._select(model, from_obj, fields, include, multiple=False)

    def _select(
        self,
        model,
        from_obj,
        fields=None,
        include=None,
        multiple=True
    ):
        self.validate_field_keys(fields)
        if fields is None:
            fields = {}

        from_args = [
            self.build_data_array(model, fields, include, from_obj)
            if multiple else
            self.build_data(
                model,
                fields,
                include,
                from_obj
            ).as_scalar().label('data')
        ]

        if include is not None:
            included_query = self.build_included(
                model,
                fields,
                include,
                from_obj
            )
            from_args.append(included_query.as_scalar().label('included'))

        main_json_query = sa.select(from_args).alias('main_json_query')

        query = sa.select(
            [sa.func.row_to_json(sa.text('main_json_query.*'))],
            from_obj=main_json_query
        )
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
        ).label('included')

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

    def build_included_union(self, model, fields, include, from_obj):
        selects = [
            self.build_single_included(model, fields, subpath, from_obj)
            for path in include
            for subpath in subpaths(path)
        ]

        union_select = union(*selects).alias()
        return sa.select(
            [union_select.c.included],
            from_obj=union_select
        ).order_by(
            union_select.c.included[s('type')],
            union_select.c.included[s('id')]
        ).correlate(from_obj)

    def build_included(self, model, fields, include, from_obj):
        if include:
            from_obj = get_selectable(from_obj).alias()
            included_union = self.build_included_union(
                model,
                fields,
                include,
                from_obj
            ).alias()
            array_query = sa.select(
                [sa.func.array_agg(included_union.c.included)],
                from_obj=included_union
            )
            query = sa.select(
                [array_query.as_scalar()],
                from_obj=from_obj
            )
            return sa.select(
                [
                    sa.func.coalesce(
                        query.as_scalar(),
                        jsonb_array
                    ).label('included')
                ]
            )
