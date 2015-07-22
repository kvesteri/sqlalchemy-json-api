import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import sessionmaker

from sqlalchemy_json_api import (
    IdPropertyNotFound,
    InvalidField,
    QueryBuilder,
    UnknownField,
    UnknownFieldKey,
    UnknownModel
)


@pytest.fixture(scope='class')
def base():
    return declarative_base()


@pytest.fixture(scope='class')
def group_user_cls(base):
    return sa.Table(
        'group_user',
        base.metadata,
        sa.Column('user_id', sa.Integer, sa.ForeignKey('user.id')),
        sa.Column('group_id', sa.Integer, sa.ForeignKey('group.id'))
    )


@pytest.fixture(scope='class')
def group_cls(base):
    class Group(base):
        __tablename__ = 'group'
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)

    return Group


@pytest.fixture(scope='class')
def friendship_cls(base):
    return sa.Table(
        'friendships',
        base.metadata,
        sa.Column(
            'friend_a_id',
            sa.Integer,
            sa.ForeignKey('user.id'),
            primary_key=True
        ),
        sa.Column(
            'friend_b_id',
            sa.Integer,
            sa.ForeignKey('user.id'),
            primary_key=True
        )
    )


@pytest.fixture(scope='class')
def user_cls(base, group_user_cls, friendship_cls):
    class User(base):
        __tablename__ = 'user'
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)
        groups = sa.orm.relationship(
            'Group',
            secondary=group_user_cls,
            backref='users'
        )

        # this relationship is used for persistence
        friends = sa.orm.relationship(
            'User',
            secondary=friendship_cls,
            primaryjoin=id == friendship_cls.c.friend_a_id,
            secondaryjoin=id == friendship_cls.c.friend_b_id,
        )

    friendship_union = sa.select([
        friendship_cls.c.friend_a_id,
        friendship_cls.c.friend_b_id
        ]).union(
            sa.select([
                friendship_cls.c.friend_b_id,
                friendship_cls.c.friend_a_id]
            )
    ).alias()

    User.all_friends = sa.orm.relationship(
        'User',
        secondary=friendship_union,
        primaryjoin=User.id == friendship_union.c.friend_a_id,
        secondaryjoin=User.id == friendship_union.c.friend_b_id,
        viewonly=True,
        order_by=User.id
    )
    return User


@pytest.fixture(scope='class')
def category_cls(base, group_user_cls, friendship_cls):
    class Category(base):
        __tablename__ = 'category'
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String)
        created_at = sa.Column(sa.DateTime)
        parent_id = sa.Column(sa.Integer, sa.ForeignKey('category.id'))
        parent = sa.orm.relationship(
            'Category',
            backref='subcategories',
            remote_side=[id],
            order_by=id
        )
    return Category


@pytest.fixture(scope='class')
def article_cls(base, category_cls, user_cls):
    class Article(base):
        __tablename__ = 'article'
        id = sa.Column('_id', sa.Integer, primary_key=True)
        name = sa.Column(sa.String)
        name_synonym = sa.orm.synonym('name')

        @hybrid_property
        def name_upper(self):
            return self.name.upper() if self.name else None

        @name_upper.expression
        def name_upper(cls):
            return sa.func.upper(cls.name)

        content = sa.Column(sa.String)

        category_id = sa.Column(sa.Integer, sa.ForeignKey(category_cls.id))
        category = sa.orm.relationship(category_cls, backref='articles')

        author_id = sa.Column(sa.Integer, sa.ForeignKey(user_cls.id))
        author = sa.orm.relationship(
            user_cls,
            primaryjoin=author_id == user_cls.id,
            backref='authored_articles'
        )

        owner_id = sa.Column(sa.Integer, sa.ForeignKey(user_cls.id))
        owner = sa.orm.relationship(
            user_cls,
            primaryjoin=owner_id == user_cls.id,
            backref='owned_articles'
        )
    return Article


@pytest.fixture(scope='class')
def comment_cls(base, article_cls, user_cls):
    class Comment(base):
        __tablename__ = 'comment'
        id = sa.Column(sa.Integer, primary_key=True)
        content = sa.Column(sa.String)
        article_id = sa.Column(sa.Integer, sa.ForeignKey(article_cls.id))
        article = sa.orm.relationship(article_cls, backref='comments')

        author_id = sa.Column(sa.Integer, sa.ForeignKey(user_cls.id))
        author = sa.orm.relationship(user_cls, backref='comments')

    article_cls.comment_count = sa.orm.column_property(
        sa.select([sa.func.count(Comment.id)])
        .where(Comment.article_id == article_cls.id)
        .correlate_except(article_cls)
    )

    return Comment


@pytest.fixture(scope='class')
def composite_pk_cls(base):
    class CompositePKModel(base):
        __tablename__ = 'composite_pk_model'
        a = sa.Column(sa.Integer, primary_key=True)
        b = sa.Column(sa.Integer, primary_key=True)
    return CompositePKModel


@pytest.fixture(scope='class')
def dns():
    return 'postgres://postgres@localhost/sqlalchemy_json_api_test'


@pytest.yield_fixture(scope='class')
def engine(dns):
    engine = create_engine(dns)
    engine.echo = True
    yield engine
    engine.dispose()


@pytest.yield_fixture(scope='class')
def connection(engine):
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='class')
def model_mapping(article_cls, category_cls, comment_cls, group_cls, user_cls):
    return {
        'articles': article_cls,
        'categories': category_cls,
        'comments': comment_cls,
        'groups': group_cls,
        'users': user_cls
    }


@pytest.yield_fixture(scope='class')
def table_creator(base, connection, model_mapping):
    sa.orm.configure_mappers()
    base.metadata.create_all(connection)
    yield
    base.metadata.drop_all(connection)


@pytest.yield_fixture(scope='class')
def session(connection):
    Session = sessionmaker(bind=connection)
    session = Session()
    yield session
    session.close_all()


@pytest.fixture(scope='class')
def dataset(
    session,
    user_cls,
    group_cls,
    article_cls,
    category_cls,
    comment_cls
):
    group = group_cls(name='Group 1')
    group2 = group_cls(name='Group 2')
    user = user_cls(id=1, name='User 1', groups=[group, group2])
    user2 = user_cls(id=2, name='User 2')
    user3 = user_cls(id=3, name='User 3', groups=[group])
    user4 = user_cls(id=4, name='User 4', groups=[group2])
    user5 = user_cls(id=5, name='User 5')

    user.friends = [user2]
    user2.friends = [user3, user4]
    user3.friends = [user5]

    article = article_cls(
        name='Some article',
        author=user,
        owner=user2,
        category=category_cls(
            id=1,
            name='Some category',
            subcategories=[
                category_cls(
                    id=2,
                    name='Subcategory 1',
                    subcategories=[
                        category_cls(
                            id=3,
                            name='Subsubcategory 1',
                            subcategories=[
                                category_cls(
                                    id=5,
                                    name='Subsubsubcategory 1',
                                ),
                                category_cls(
                                    id=6,
                                    name='Subsubsubcategory 2',
                                )
                            ]
                        )
                    ]
                ),
                category_cls(id=4, name='Subcategory 2'),
            ]
        ),
        comments=[
            comment_cls(
                content='Some comment',
                author=user
            )
        ]
    )
    session.add(user3)
    session.add(user4)
    session.add(article)
    session.commit()


@pytest.fixture
def json_api(model_mapping):
    return QueryBuilder(model_mapping)


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestQueryBuilder(object):
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
        json_api,
        article_cls
    ):
        with pytest.raises(UnknownFieldKey) as e:
            json_api.select(article_cls, fields={'bogus': []})
        assert str(e.value) == (
            "Unknown field keys given. Could not find key 'bogus' from "
            "given model mapping."
        )

    def test_throws_exception_for_unknown_model(self, user_cls, article_cls):
        with pytest.raises(UnknownModel) as e:
            QueryBuilder({'users': user_cls}).select(article_cls)
        assert str(e.value) == (
            "Unknown model given. Could not find model {0} from given "
            "mapping.".format(
                article_cls
            )
        )

    def test_throws_exception_for_unknown_field(self, json_api, article_cls):
        with pytest.raises(UnknownField) as e:
            json_api.select(article_cls, fields={'articles': ['bogus']})
        assert str(e.value) == (
            "Unknown field 'bogus'. Given selectable does not have "
            "descriptor named 'bogus'."
        )

    def test_throws_exception_for_foreign_key_field(
        self,
        json_api,
        article_cls
    ):
        with pytest.raises(InvalidField) as e:
            json_api.select(article_cls, fields={'articles': ['author_id']})
        assert str(e.value) == (
            "Field 'author_id' is invalid. The underlying column "
            "'author_id' has foreign key. You can't include foreign key "
            "attributes. Consider including relationship attributes."
        )

    def test_throws_exception_for_primary_key_field(
        self,
        json_api,
        article_cls
    ):
        with pytest.raises(InvalidField) as e:
            json_api.select(article_cls, fields={'articles': ['id']})
        assert str(e.value) == (
            "Field 'id' is invalid. The underlying column "
            "'_id' is primary key column."
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
                            'name_upper': 'SOME ARTICLE',
                            'name_synonym': 'Some article'
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
        json_api,
        session,
        article_cls,
        fields,
        result
    ):
        query = json_api.select(article_cls, fields=fields)
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
        json_api,
        session,
        article_cls,
        fields,
        result
    ):
        query = json_api.select(article_cls, fields=fields)
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                {'articles': ['name_synonym']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'attributes': {
                            'name_synonym': 'Some article'
                        }
                    }]
                }
            ),
        )
    )
    def test_fields_parameter_with_synonym_property(
        self,
        json_api,
        session,
        article_cls,
        fields,
        result
    ):
        query = json_api.select(article_cls, fields=fields)
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
        json_api,
        session,
        article_cls,
        fields,
        result
    ):
        query = json_api.select(article_cls, fields=fields)
        assert session.execute(query).scalar() == result

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
                        'id': '1'
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
        json_api,
        session,
        article_cls,
        fields,
        include,
        result
    ):
        query = json_api.select(
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
        json_api,
        session,
        article_cls,
        fields,
        include,
        result
    ):
        query = json_api.select(
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
        json_api,
        session,
        user_cls,
        fields,
        include,
        result
    ):
        query = json_api.select(
            user_cls,
            fields=fields,
            include=include,
            from_obj=session.query(user_cls).filter(
                user_cls.id == 1
            ).subquery('main_query')
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
        json_api,
        session,
        user_cls,
        fields,
        include,
        result
    ):
        query = json_api.select(
            user_cls,
            fields=fields,
            include=include,
            from_obj=session.query(user_cls).filter(
                user_cls.id == 5
            ).subquery('main_query')
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'users': [],
                },
                ['groups'],
                {
                    'data': [],
                    'included': []
                }
            ),
        )
    )
    def test_empty_data(
        self,
        json_api,
        session,
        user_cls,
        fields,
        include,
        result
    ):
        query = json_api.select(
            user_cls,
            fields=fields,
            include=include,
            from_obj=session.query(user_cls).filter(
                user_cls.id == 99
            ).subquery('main_query')
        )
        assert session.execute(query).scalar() == result
