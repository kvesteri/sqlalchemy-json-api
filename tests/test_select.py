import json

import pytest
import sqlalchemy as sa

from sqlalchemy_json_api import (
    IdPropertyNotFound,
    InvalidField,
    QueryBuilder,
    RESERVED_KEYWORDS,
    UnknownField,
    UnknownFieldKey,
    UnknownModel
)


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestQueryBuilderSelect(object):
    def test_throws_exception_for_unknown_fields_key(self, composite_pk_cls):
        with pytest.raises(IdPropertyNotFound) as e:
            QueryBuilder({'something': composite_pk_cls})
        assert str(e.value) == (
            "Couldn't find 'id' property for model {0}.".format(
                composite_pk_cls
            )
        )

    def test_throws_exception_for_model_without_id_property(
        self,
        query_builder,
        article_cls
    ):
        with pytest.raises(UnknownFieldKey) as e:
            query_builder.select(article_cls, fields={'bogus': []})
        assert str(e.value) == (
            "Unknown field keys given. Could not find key 'bogus' from "
            "given model mapping."
        )

    def test_throws_exception_for_unknown_model(self, user_cls, article_cls):
        with pytest.raises(UnknownModel) as e:
            QueryBuilder({'users': user_cls}).select(article_cls)
        assert str(e.value) == (
            "Unknown model given. Could not find model {0} from given "
            "model mapping.".format(
                article_cls
            )
        )

    def test_throws_exception_for_unknown_field(
        self,
        query_builder,
        article_cls
    ):
        with pytest.raises(UnknownField) as e:
            query_builder.select(article_cls, fields={'articles': ['bogus']})
        assert str(e.value) == (
            "Unknown field 'bogus'. Given selectable does not have "
            "descriptor named 'bogus'."
        )

    @pytest.mark.parametrize(
        'field',
        RESERVED_KEYWORDS
    )
    def test_throws_exception_for_reserved_keyword(
        self,
        query_builder,
        article_cls,
        field
    ):
        with pytest.raises(InvalidField) as e:
            query_builder.select(article_cls, fields={'articles': [field]})
        assert str(e.value) == (
            "Given field '{0}' is reserved keyword.".format(field)
        )

    def test_throws_exception_for_foreign_key_field(
        self,
        query_builder,
        article_cls
    ):
        with pytest.raises(InvalidField) as e:
            query_builder.select(
                article_cls,
                fields={'articles': ['author_id']}
            )
        assert str(e.value) == (
            "Field 'author_id' is invalid. The underlying column "
            "'author_id' has foreign key. You can't include foreign key "
            "attributes. Consider including relationship attributes."
        )

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                None,
                {
                    'data': [{
                        'id': '1',
                        'type': 'articles',
                        'attributes': {
                            'comment_count': 4,
                            'content': None,
                            'name': 'Some article',
                            'name_upper': 'SOME ARTICLE'
                        },
                        'relationships': {
                            'author': {
                                'data': {'id': '1', 'type': 'users'}
                            },
                            'category': {
                                'data': {'id': '1', 'type': 'categories'}
                            },
                            'comments': {
                                'data': [
                                    {'id': '1', 'type': 'comments'},
                                    {'id': '2', 'type': 'comments'},
                                    {'id': '3', 'type': 'comments'},
                                    {'id': '4', 'type': 'comments'},
                                ]
                            },
                            'owner': {
                                'data': {'id': '2', 'type': 'users'}
                            }
                        },
                    }]
                }
            ),
            (
                {'articles': ['name', 'content']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        }
                    }]
                }
            ),
            (
                {'articles': ['name']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name': 'Some article'
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'category']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': '1'}
                            }
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'comments']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'comments': {
                                'data': [
                                    {'type': 'comments', 'id': '1'},
                                    {'type': 'comments', 'id': '2'},
                                    {'type': 'comments', 'id': '3'},
                                    {'type': 'comments', 'id': '4'}
                                ]
                            }
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'comments', 'category']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'comments': {
                                'data': [
                                    {'type': 'comments', 'id': '1'},
                                    {'type': 'comments', 'id': '2'},
                                    {'type': 'comments', 'id': '3'},
                                    {'type': 'comments', 'id': '4'}
                                ]
                            },
                            'category': {
                                'data': {'type': 'categories', 'id': '1'}
                            }
                        }
                    }]
                }
            ),
        )
    )
    def test_fields_parameter(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        result
    ):
        query = query_builder.select(article_cls, fields=fields)
        assert session.execute(query).scalar() == result

    def test_custom_order_by_for_relationship(
        self,
        query_builder,
        session,
        article_cls,
        comment_cls
    ):
        article_cls.comments.property.order_by = [sa.desc(comment_cls.id)]
        query = query_builder.select(
            article_cls,
            fields={'articles': ['comments']}
        )
        assert session.execute(query).scalar() == {
            'data': [{
                'type': 'articles',
                'id': '1',
                'relationships': {
                    'comments': {
                        'data': [
                            {'type': 'comments', 'id': '4'},
                            {'type': 'comments', 'id': '3'},
                            {'type': 'comments', 'id': '2'},
                            {'type': 'comments', 'id': '1'}
                        ]
                    }
                }
            }]
        }

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                {'articles': ['comment_count']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'comment_count': 4
                        }
                    }]
                }
            ),
        )
    )
    def test_fields_parameter_with_column_property(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        result
    ):
        query = query_builder.select(article_cls, fields=fields)
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                {'articles': ['comment_count']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'comment_count': 4
                        }
                    }]
                }
            ),
        )
    )
    def test_column_property_with_custom_from_obj(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        result
    ):
        from_obj = session.query(article_cls).with_entities(
            article_cls.id,
            article_cls.comment_count
        )
        query = query_builder.select(
            article_cls,
            from_obj=from_obj,
            fields=fields
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                {'articles': ['name_upper']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name_upper': 'SOME ARTICLE'
                        }
                    }]
                }
            ),
        )
    )
    def test_fields_parameter_with_hybrid_property(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        result
    ):
        query = query_builder.select(article_cls, fields=fields)
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                {'articles': ['name_upper']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name_upper': 'SOME ARTICLE'
                        }
                    }]
                }
            ),
        )
    )
    def test_hybrid_property_inclusion_uses_clause_adaptation(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        result
    ):
        query = query_builder.select(
            article_cls,
            fields=fields,
            from_obj=session.query(article_cls)
        )
        compiled = query.compile(dialect=sa.dialects.postgresql.dialect())
        assert 'upper(anon_2.name)' in str(compiled)

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {'users': []},
                ['groups'],
                {'data': [], 'included': []}
            ),
            (
                {'users': []},
                None,
                {'data': []}
            ),
        )
    )
    def test_empty_data(
        self,
        query_builder,
        session,
        user_cls,
        fields,
        include,
        result
    ):
        query = query_builder.select(
            user_cls,
            fields=fields,
            include=include,
            from_obj=session.query(user_cls).filter(user_cls.id == 99)
        )
        assert session.execute(query).scalar() == result

    def test_fetch_multiple_results(self, query_builder, session, user_cls):
        query = query_builder.select(
            user_cls,
            fields={'users': ['all_friends']},
            from_obj=(
                session.query(user_cls)
                .filter(user_cls.id.in_([1, 2]))
                .order_by(user_cls.id)
            )
        )
        assert session.execute(query).scalar() == {
            'data': [
                {
                    'relationships': {
                        'all_friends': {'data': [{'id': '2', 'type': 'users'}]}
                    },
                    'id': '1',
                    'type': 'users'
                },
                {
                    'relationships': {
                        'all_friends': {
                            'data': [
                                {'id': '1', 'type': 'users'},
                                {'id': '3', 'type': 'users'},
                                {'id': '4', 'type': 'users'}
                            ]
                        }
                    },
                    'id': '2',
                    'type': 'users'
                }
            ]
        }

    def test_as_text_parameter(self, query_builder, session, article_cls):
        query = query_builder.select(
            article_cls,
            fields={'articles': ['name']},
            as_text=True
        )

        assert json.loads(session.execute(query).scalar()) == {
            'data': [{
                'type': 'articles',
                'id': '1',
                'attributes': {
                    'name': 'Some article'
                }
            }]
        }

    @pytest.mark.parametrize(
        ('limit', 'offset', 'result'),
        (
            (
                3,
                0,
                [
                    {
                        'id': '1',
                        'type': 'users'
                    },
                    {
                        'id': '2',
                        'type': 'users'
                    },
                    {
                        'id': '3',
                        'type': 'users'
                    }
                ]
            ),
            (
                3,
                2,
                [
                    {
                        'id': '3',
                        'type': 'users'
                    },
                    {
                        'id': '4',
                        'type': 'users'
                    },
                    {
                        'id': '5',
                        'type': 'users'
                    }
                ]
            ),
            (
                1,
                5,
                []
            ),
        )
    )
    def test_limit_and_offset(
        self,
        query_builder,
        session,
        user_cls,
        limit,
        offset,
        result
    ):
        query = query_builder.select(
            user_cls,
            sort=['id'],
            fields={'users': []},
            limit=limit,
            offset=offset
        )
        assert session.execute(query).scalar() == {
            'data': result
        }

    @pytest.mark.parametrize(
        ('limit', 'offset', 'result'),
        (
            (
                1,
                0,
                {
                    'data': [
                        {
                            'id': '1',
                            'type': 'users'
                        }
                    ],
                    'included': [
                        {
                            'id': '1',
                            'type': 'groups'
                        },
                        {
                            'id': '2',
                            'type': 'groups'
                        }
                    ]
                }
            ),
            (
                1,
                1,
                {
                    'data': [
                        {
                            'id': '2',
                            'type': 'users'
                        }
                    ],
                    'included': []
                }
            ),
            (
                1,
                5,
                {'data': [], 'included': []}
            ),
        )
    )
    def test_limit_and_offset_with_included(
        self,
        query_builder,
        session,
        user_cls,
        limit,
        offset,
        result
    ):
        query = query_builder.select(
            user_cls,
            sort=['id'],
            fields={'users': [], 'groups': []},
            include={'groups'},
            limit=limit,
            offset=offset
        )
        assert session.execute(query).scalar() == result
