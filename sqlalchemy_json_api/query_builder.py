from collections import namedtuple

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

Parameters = namedtuple('Parameters', ['fields', 'include', 'sort'])

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


class ResourceRegistry(object):
    def __init__(self, model_mapping):
        self.by_type = model_mapping
        self.by_model_class = dict(
            (value, key) for key, value in model_mapping.items()
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
    :param base_url:
        Base url to be used for building JSON API compatible links objects. By
        default this is `None` indicating that no link objects will be built.
    """
    def __init__(self, model_mapping, base_url=None):
        self.validate_model_mapping(model_mapping)
        self.resource_registry = ResourceRegistry(model_mapping)
        self.base_url = base_url

    def validate_model_mapping(self, model_mapping):
        for model in model_mapping.values():
            if 'id' not in get_all_descriptors(model).keys():
                raise IdPropertyNotFound(
                    "Couldn't find 'id' property for model {0}.".format(
                        model
                    )
                )

    def get_model_alias(self, model):
        if isinstance(model, sa.orm.util.AliasedClass):
            model = sa.inspect(model).mapper.class_
        try:
            return self.resource_registry.by_model_class[model]
        except KeyError:
            raise UnknownModel(
                'Unknown model given. Could not find model %r from given '
                'model mapping.' % model
            )

    def get_id(self, from_obj):
        return cast_if(get_attrs(from_obj).id, sa.String)

    def build_resource_identifier(self, model, from_obj):
        model_alias = self.get_model_alias(model)
        return [
            s('id'),
            self.get_id(from_obj),
            s('type'),
            s(model_alias),
        ]

    def validate_field_keys(self, fields):
        if fields:
            unknown_keys = (
                set(fields) - set(self.resource_registry.by_type.keys())
            )
            if unknown_keys:
                raise UnknownFieldKey(
                    'Unknown field keys given. Could not find {0} {1} from '
                    'given model mapping.'.format(
                        'keys' if len(unknown_keys) > 1 else 'key',
                        ','.join("'{0}'".format(key) for key in unknown_keys)
                    )
                )

    def select(
        self,
        model,
        fields=None,
        include=None,
        sort=None,
        from_obj=None
    ):
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

        Results can be sorted.

        ::

            # Sort by id in descending order
            query = query_builder.select(
                Article,
                sort=['-id']
            )

            # Sort by name and id in ascending order
            query = query_builder.select(
                Article,
                sort=['name', 'id']
            )

        :param model:
            The root model to build the select query from.
        :param fields:
            A mapping of fields. Keys representing model keys and values as
            lists of model descriptor names.
        :param include:
            List of dot-separated relationship paths.
        :param sort:
            List of attributes to apply as an order by for the root model.
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

        return self._select(
            model,
            from_obj,
            fields=fields,
            include=include,
            sort=sort
        )

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
        sort=None,
        multiple=True
    ):
        self.validate_field_keys(fields)
        if fields is None:
            fields = {}

        params = Parameters(fields=fields, include=include, sort=sort)
        from_args = self._get_from_args(model, from_obj, params, multiple)

        main_json_query = sa.select(from_args).alias('main_json_query')

        query = sa.select(
            [sa.func.row_to_json(sa.text('main_json_query.*'))],
            from_obj=main_json_query
        )
        return query

    def _get_from_args(self, model, from_obj, params, multiple):
        data_expr = DataExpression(self, model, from_obj)
        data_query = (
            data_expr.build_data_array(params)
            if multiple else
            data_expr.build_data(params)
        )
        from_args = [data_query.as_scalar().label('data')]

        if params.include is not None:
            include_expr = IncludeExpression(
                self,
                model,
                get_selectable(from_obj).alias()
            )
            included_query = include_expr.build_included(params)
            from_args.append(included_query.as_scalar().label('included'))
        return from_args


class Expression(object):
    def __init__(self, query_builder, model, from_obj):
        self.query_builder = query_builder
        self.model = model
        self.from_obj = from_obj

    @property
    def args(self):
        return [self.query_builder, self.model, self.from_obj]


class AttributesExpression(Expression):
    @property
    def all_fields(self):
        return [
            field
            for field, descriptor
            in self.adapted_descriptors
            if (
                field != '__mapper__' and
                field not in RESERVED_KEYWORDS and
                not self.is_relationship_descriptor(descriptor) and
                not self.should_skip_columnar_descriptor(descriptor)
            )
        ]

    def should_skip_columnar_descriptor(self, descriptor):
        columns = get_descriptor_columns(self.from_obj, descriptor)
        return (len(columns) == 1 and columns[0].foreign_keys)

    @property
    def adapted_descriptors(self):
        return (
            get_all_descriptors(self.from_obj).items() +
            [
                (
                    key,
                    ClauseAdapter(self.from_obj).traverse(
                        getattr(self.model, key)
                    )
                )
                for key in get_hybrid_properties(self.model).keys()
            ]
        )

    def adapt_attribute(self, attr_name):
        cols = get_attrs(self.from_obj)
        hybrids = get_hybrid_properties(self.model).keys()
        if attr_name in hybrids:
            return ClauseAdapter(self.from_obj).traverse(
                getattr(self.model, attr_name)
            )
        else:
            return getattr(cols, attr_name)

    def is_relationship_field(self, field):
        return field in get_mapper(self.model).relationships.keys()

    def is_relationship_descriptor(self, descriptor):
        return (
            isinstance(descriptor, InstrumentedAttribute) and
            isinstance(descriptor.property, sa.orm.RelationshipProperty)
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

    def validate_field(self, field, descriptors):
        if field not in descriptors.keys():
            raise UnknownField(
                "Unknown field '{0}'. Given selectable does not have "
                "descriptor named '{0}'.".format(field)
            )
        columns = get_descriptor_columns(self.model, descriptors[field])
        for column in columns:
            self.validate_column(field, column)

    def validate_fields(self, fields):
        descriptors = get_all_descriptors(self.from_obj)
        for field in fields:
            if field in get_hybrid_properties(self.model):
                continue
            self.validate_field(field, descriptors)

    def get_model_fields(self, fields):
        model_key = self.query_builder.get_model_alias(self.model)

        if not fields or model_key not in fields:
            model_fields = self.all_fields
        else:
            model_fields = [
                field for field in fields[model_key]
                if not self.is_relationship_field(field)
            ]
            self.validate_fields(model_fields)
        return model_fields

    def build_attributes(self, fields):
        return sum(
            (
                [s(key), self.adapt_attribute(key)]
                for key in self.get_model_fields(fields)
            ),
            []
        )


class RelationshipsExpression(Expression):
    def build_relationships(self, fields):
        return sum(
            (
                self.build_relationship(relationship)
                for relationship
                in self.get_relationship_properties(fields)
            ),
            []
        )

    def build_relationship_data(self, relationship, alias):
        identifier = self.query_builder.build_resource_identifier(
            alias,
            alias
        )
        expr = sa.func.json_build_object(*identifier).label('json_object')
        query = select_correlated_expression(
            self.model,
            expr,
            relationship.key,
            alias,
            get_selectable(self.from_obj),
            order_by=relationship.order_by
        ).alias('relationships')
        return query

    def build_relationship_data_array(self, relationship, alias):
        query = self.build_relationship_data(relationship, alias)
        return sa.select([
            sa.func.coalesce(
                sa.func.array_agg(query.c.json_object),
                json_array
            )
        ]).select_from(query)

    def build_relationship(self, relationship):
        cls = relationship.mapper.class_
        alias = sa.orm.aliased(cls)
        query = (
            self.build_relationship_data_array(relationship, alias)
            if relationship.uselist else
            self.build_relationship_data(relationship, alias)
        )
        args = [s('data'), query.as_scalar()]
        if self.query_builder.base_url:
            links = LinksExpression(*self.args).build_relationship_links(
                relationship.key
            )
            args.extend([
                s('links'),
                sa.func.json_build_object(*links)
            ])
        return [
            s(relationship.key),
            sa.func.json_build_object(*args)
        ]

    def get_relationship_properties(self, fields):
        model_alias = self.query_builder.get_model_alias(self.model)
        mapper = get_mapper(self.model)
        if model_alias not in fields:
            return list(mapper.relationships.values())
        else:
            return [
                mapper.relationships[field]
                for field in fields[model_alias]
                if field in mapper.relationships.keys()
            ]


class LinksExpression(Expression):
    def build_link(self, postfix=None):
        args = [
            s(self.query_builder.base_url),
            s(self.query_builder.get_model_alias(self.model)),
            s('/'),
            self.query_builder.get_id(self.from_obj),
        ]
        if postfix is not None:
            args.append(postfix)
        return sa.func.concat(*args)

    def build_links(self):
        if self.query_builder.base_url:
            return [s('self'), self.build_link()]

    def build_relationship_links(self, key):
        if self.query_builder.base_url:
            return [
                s('self'),
                self.build_link(s('/relationships/{0}'.format(key))),
                s('related'),
                self.build_link(s('/{0}'.format(key)))
            ]


class DataExpression(Expression):
    def build_attrs_relationships_and_links(self, fields):
        args = (self.query_builder, self.model, self.from_obj)
        parts = {
            'attributes': AttributesExpression(*args).build_attributes(fields),
            'relationships': RelationshipsExpression(
                *args
            ).build_relationships(fields),
            'links': LinksExpression(*args).build_links()
        }
        return sum(
            (
                [s(key), sa.func.json_build_object(*values)]
                for key, values in parts.items()
                if values
            ),
            []
        )

    def build_data_expr(self, params):
        json_fields = self.query_builder.build_resource_identifier(
            self.model,
            self.from_obj
        )
        json_fields.extend(
            self.build_attrs_relationships_and_links(params.fields)
        )
        return sa.func.json_build_object(*json_fields).label('data')

    def build_data(self, params):
        expr = self.build_data_expr(params)
        query = sa.select([expr], from_obj=self.from_obj)
        if params.sort is not None:
            query = self.apply_sort(query, params.sort)
        return query

    def apply_sort(self, query, sort):
        for param in sort:
            query = query.order_by(
                sa.desc(getattr(self.from_obj.c, param[1:]))
                if param[0] == '-' else
                getattr(self.from_obj.c, param)
            )
        return query

    def build_data_array(self, params):
        data_query = self.build_data(params).alias()
        return sa.select(
            [sa.func.coalesce(
                sa.func.array_agg(data_query.c.data),
                json_array
            )],
            from_obj=data_query
        ).correlate(self.from_obj)


class IncludeExpression(Expression):
    def build_included_union(self, params):
        selects = [
            self.build_single_included(params.fields, subpath)
            for path in params.include
            for subpath in subpaths(path)
        ]

        union_select = union(*selects).alias()
        return sa.select(
            [union_select.c.included],
            from_obj=union_select
        ).order_by(
            union_select.c.included[s('type')],
            union_select.c.included[s('id')]
        ).correlate(self.from_obj)

    def build_included(self, params):
        included_union = self.build_included_union(params).alias()
        array_query = sa.select(
            [sa.func.array_agg(included_union.c.included)],
            from_obj=included_union
        )
        query = sa.select(
            [array_query.as_scalar()],
            from_obj=self.from_obj
        )
        return sa.select(
            [
                sa.func.coalesce(
                    query.as_scalar(),
                    jsonb_array
                ).label('included')
            ]
        )

    def build_single_included_fields(self, alias, fields):
        #cls_key = self.query_builder.get_model_alias(alias)
        json_fields = self.query_builder.build_resource_identifier(
            alias,
            alias
        )
        data_expr = DataExpression(
            self.query_builder,
            alias,
            sa.inspect(alias).selectable
        )
        json_fields.extend(
            data_expr.build_attrs_relationships_and_links(fields)
        )
        return json_fields

    def build_included_json_object(self, alias, fields):
        return sa.cast(
            sa.func.json_build_object(
                *self.build_single_included_fields(alias, fields)
            ),
            JSONB
        ).label('included')

    def build_single_included(self, fields, path):
        relationships = path_to_relationships(path, self.model)

        cls = relationships[-1].mapper.class_
        alias = sa.orm.aliased(cls)
        expr = self.build_included_json_object(alias, fields)
        query = select_correlated_expression(
            self.model,
            expr,
            path,
            alias,
            get_selectable(self.from_obj)
        )
        if cls is self.model:
            query = query.where(
                alias.id.notin_(
                    sa.select(
                        [get_attrs(self.from_obj).id],
                        from_obj=self.from_obj
                    )
                )
            )
        return query
