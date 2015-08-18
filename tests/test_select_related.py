import json

import pytest

from sqlalchemy_json_api import QueryBuilder


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestSelectRelated(object):
    @pytest.mark.parametrize(
        ('id', 'result'),
        (
            (
                1,
                {'data': [{'type': 'users', 'id': '2'}]}
            ),
            (
                2,
                {'data': [
                    {'type': 'users', 'id': '1'},
                    {'type': 'users', 'id': '3'},
                    {'type': 'users', 'id': '4'}
                ]}
            )
        )
    )
    def test_to_many_relationship_with_ids_only(
        self,
        query_builder,
        session,
        user_cls,
        id,
        result
    ):
        query = query_builder.select_related(
            session.query(user_cls).get(id),
            'all_friends',
            fields={'users': []}
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'fields', 'result'),
        (
            (
                2,
                {'categories': ['name']},
                {'data': {
                    'type':
                    'categories',
                    'id': '1',
                    'attributes': {
                        'name': 'Some category'
                    }
                }}
            ),
            (
                5,
                {'categories': ['parent']},
                {'data': {
                    'type': 'categories',
                    'id': '3',
                    'relationships': {
                        'parent': {
                            'data': {
                                'id': '2',
                                'type': 'categories'
                            }
                        }
                    }
                }}
            )
        )
    )
    def test_to_one_relationship(
        self,
        query_builder,
        session,
        category_cls,
        id,
        fields,
        result
    ):
        query = query_builder.select_related(
            session.query(category_cls).get(id),
            'parent',
            fields=fields
        )
        assert session.execute(query).scalar() == result


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestSelectRelationshipWithLinks(object):
    @pytest.fixture
    def query_builder(self, model_mapping):
        return QueryBuilder(model_mapping, base_url='/')

    @pytest.mark.parametrize(
        ('id', 'links', 'result'),
        (
            (
                1,
                {'self': '/users/1/all_friends'},
                {
                    'data': [
                        {
                            'type': 'users',
                            'id': '2',
                            'links': {'self': '/users/2'}
                        }
                    ],
                    'links': {'self': '/users/1/all_friends'}
                }
            ),
            (
                2,
                {'self': '/users/2/all_friends'},
                {
                    'data': [
                        {
                            'type': 'users',
                            'id': '1',
                            'links': {'self': '/users/1'}
                        },
                        {
                            'type': 'users',
                            'id': '3',
                            'links': {'self': '/users/3'}
                        },
                        {
                            'type': 'users',
                            'id': '4',
                            'links': {'self': '/users/4'}
                        }
                    ],
                    'links': {
                        'self': '/users/2/all_friends',
                    }
                }
            )
        )
    )
    def test_to_many_relationship(
        self,
        query_builder,
        session,
        user_cls,
        id,
        links,
        result
    ):
        query = query_builder.select_related(
            session.query(user_cls).get(id),
            'all_friends',
            fields={'users': []},
            links=links
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'links', 'result'),
        (
            (
                2,
                {'self': '/categories/2/parent'},
                {
                    'data': {
                        'type': 'categories',
                        'id': '1',
                        'links': {'self': '/categories/1'}
                    },
                    'links': {'self': '/categories/2/parent'}
                }
            ),
            (
                5,
                {'self': '/categories/5/parent'},
                {
                    'data': {
                        'type': 'categories',
                        'id': '3',
                        'links': {'self': '/categories/3'}
                    },
                    'links': {'self': '/categories/5/parent'}
                }
            )
        )
    )
    def test_to_one_parent_child_relationship(
        self,
        query_builder,
        session,
        category_cls,
        id,
        result,
        links
    ):
        query = query_builder.select_related(
            session.query(category_cls).get(id),
            'parent',
            fields={'categories': []},
            links=links
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'links', 'result'),
        (
            (
                1,
                {
                    'self': '/articles/1/category',
                },
                {
                    'data': {
                        'type': 'categories',
                        'id': '1',
                        'links': {
                            'self': '/categories/1',
                        }
                    },
                    'links': {
                        'self': '/articles/1/category',
                    }
                }
            ),
        )
    )
    def test_to_one_relationship(
        self,
        query_builder,
        session,
        article_cls,
        id,
        links,
        result
    ):
        query = query_builder.select_related(
            session.query(article_cls).get(id),
            'category',
            fields={'categories': []},
            links=links
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'result'),
        (
            (
                1,
                {
                    'data': {
                        'type': 'categories',
                        'id': '1',
                        'links': {
                            'self': '/categories/1',
                        }
                    }
                }
            ),
        )
    )
    def test_as_text_parameter(
        self,
        query_builder,
        session,
        article_cls,
        id,
        result
    ):
        query = query_builder.select_related(
            session.query(article_cls).get(id),
            'category',
            fields={'categories': []},
            as_text=True
        )
        assert json.loads(session.execute(query).scalar()) == result

    @pytest.mark.parametrize(
        ('id', 'result'),
        (
            (
                1,
                {'data': None}
            ),
        )
    )
    def test_empty_result(
        self,
        query_builder,
        session,
        category_cls,
        id,
        result
    ):
        query = query_builder.select_related(
            session.query(category_cls).get(id),
            'parent',
            fields={'categories': []}
        )
        assert session.execute(query).scalar() == result
