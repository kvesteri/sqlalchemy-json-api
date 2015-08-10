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
                            'comment_count': 1,
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
                                'data': [{'id': '1', 'type': 'comments'}]
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
                                'data': [{'type': 'comments', 'id': '1'}]
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
                                'data': [{'type': 'comments', 'id': '1'}]
                            },
                            'category': {
                                'data': {'type': 'categories', 'id': '1'}
                            }
                        }
                    }]
                }
            )
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
                            'comment_count': 1
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
                {'articles': ['name', 'content', 'category']},
                ['category'],
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
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'attributes': {
                            'created_at': None,
                            'name': 'Some category'
                        },
                        'relationships': {
                            'articles': {
                                'data': [{'type': 'articles', 'id': '1'}]
                            },
                            'subcategories': {
                                'data': [
                                    {'type': 'categories', 'id': '2'},
                                    {'type': 'categories', 'id': '4'}
                                ]
                            },
                            'parent': {'data': None}
                        }
                    }]
                }
            ),
            (
                {'articles': [], 'categories': ['name']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {'articles': ['category'], 'categories': ['name']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': '1'}
                            }
                        }
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': ['name']
                },
                ['category'],
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
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {
                    'articles': ['name', 'content', 'category', 'comments'],
                    'categories': ['name'],
                    'comments': ['content']
                },
                ['category', 'comments'],
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
                            },
                            'comments': {
                                'data': [{'type': 'comments', 'id': '1'}]
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'categories',
                            'id': '1',
                            'attributes': {'name': 'Some category'}
                        },
                        {
                            'type': 'comments',
                            'id': '1',
                            'attributes': {'content': 'Some comment'}
                        },
                    ]
                }
            ),
        )
    )
    def test_include_parameter(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        include,
        result
    ):
        query = query_builder.select(
            article_cls,
            fields=fields,
            include=include
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': ['name', 'subcategories'],
                },
                ['category.subcategories'],
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
                    }],
                    'included': [
                        {
                            'type': 'categories',
                            'id': '1',
                            'attributes': {'name': 'Some category'},
                            'relationships': {
                                'subcategories': {
                                    'data': [
                                        {'type': 'categories', 'id': '2'},
                                        {'type': 'categories', 'id': '4'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'categories',
                            'id': '2',
                            'attributes': {'name': 'Subcategory 1'},
                            'relationships': {
                                'subcategories': {
                                    'data': [{'type': 'categories', 'id': '3'}]
                                }
                            }
                        },
                        {
                            'type': 'categories',
                            'id': '4',
                            'attributes': {'name': 'Subcategory 2'},
                            'relationships': {
                                'subcategories': {
                                    'data': []
                                }
                            }
                        },
                    ]
                }
            ),
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': ['name', 'subcategories'],
                },
                ['category.subcategories.subcategories'],
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
                    }],
                    'included': [
                        {
                            'type': 'categories',
                            'id': '1',
                            'attributes': {'name': 'Some category'},
                            'relationships': {
                                'subcategories': {
                                    'data': [
                                        {'type': 'categories', 'id': '2'},
                                        {'type': 'categories', 'id': '4'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'categories',
                            'id': '2',
                            'attributes': {'name': 'Subcategory 1'},
                            'relationships': {
                                'subcategories': {
                                    'data': [{'type': 'categories', 'id': '3'}]
                                }
                            }
                        },
                        {
                            'type': 'categories',
                            'id': '3',
                            'attributes': {'name': 'Subsubcategory 1'},
                            'relationships': {
                                'subcategories': {
                                    'data': [
                                        {'type': 'categories', 'id': '5'},
                                        {'type': 'categories', 'id': '6'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'categories',
                            'id': '4',
                            'attributes': {'name': 'Subcategory 2'},
                            'relationships': {
                                'subcategories': {
                                    'data': []
                                }
                            }
                        },
                    ]
                }
            ),
        )
    )
    def test_deep_relationships(
        self,
        query_builder,
        session,
        article_cls,
        fields,
        include,
        result
    ):
        query = query_builder.select(
            article_cls,
            fields=fields,
            include=include
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'users': ['name', 'all_friends'],
                },
                ['all_friends'],
                {
                    'data': [{
                        'type': 'users',
                        'id': '1',
                        'attributes': {
                            'name': 'User 1',
                        },
                        'relationships': {
                            'all_friends': {
                                'data': [
                                    {'type': 'users', 'id': '2'}
                                ]
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'users',
                            'id': '2',
                            'attributes': {'name': 'User 2'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '1'},
                                        {'type': 'users', 'id': '3'},
                                        {'type': 'users', 'id': '4'}
                                    ]
                                }
                            }
                        },
                    ]
                }
            ),
            (
                {
                    'users': ['name', 'all_friends'],
                },
                ['all_friends.all_friends'],
                {
                    'data': [{
                        'type': 'users',
                        'id': '1',
                        'attributes': {
                            'name': 'User 1',
                        },
                        'relationships': {
                            'all_friends': {
                                'data': [
                                    {'type': 'users', 'id': '2'}
                                ]
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'users',
                            'id': '2',
                            'attributes': {'name': 'User 2'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '1'},
                                        {'type': 'users', 'id': '3'},
                                        {'type': 'users', 'id': '4'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'users',
                            'id': '3',
                            'attributes': {'name': 'User 3'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '2'},
                                        {'type': 'users', 'id': '5'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'users',
                            'id': '4',
                            'attributes': {'name': 'User 4'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '2'}
                                    ]
                                }
                            }
                        },
                    ]
                }
            ),
            (
                {
                    'users': ['name', 'all_friends'],
                },
                ['all_friends.all_friends.all_friends'],
                {
                    'data': [{
                        'type': 'users',
                        'id': '1',
                        'attributes': {
                            'name': 'User 1',
                        },
                        'relationships': {
                            'all_friends': {
                                'data': [
                                    {'type': 'users', 'id': '2'}
                                ]
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'users',
                            'id': '2',
                            'attributes': {'name': 'User 2'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '1'},
                                        {'type': 'users', 'id': '3'},
                                        {'type': 'users', 'id': '4'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'users',
                            'id': '3',
                            'attributes': {'name': 'User 3'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '2'},
                                        {'type': 'users', 'id': '5'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'users',
                            'id': '4',
                            'attributes': {'name': 'User 4'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '2'}
                                    ]
                                }
                            }
                        },
                        {
                            'type': 'users',
                            'id': '5',
                            'attributes': {'name': 'User 5'},
                            'relationships': {
                                'all_friends': {
                                    'data': [
                                        {'type': 'users', 'id': '3'}
                                    ]
                                }
                            }
                        },
                    ]
                }
            ),
        )
    )
    def test_self_referencing_m2m(
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
            from_obj=session.query(user_cls).filter(
                user_cls.id == 1
            )
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'users': ['groups'],
                },
                ['groups'],
                {
                    'data': [{
                        'type': 'users',
                        'id': '5',
                        'relationships': {
                            'groups': {
                                'data': []
                            }
                        }
                    }],
                    'included': []
                }
            ),
        )
    )
    def test_included_as_empty(
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
            from_obj=session.query(user_cls).filter(
                user_cls.id == 5
            )
        )
        assert session.execute(query).scalar() == result

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
