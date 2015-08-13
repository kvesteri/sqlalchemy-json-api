import json

import pytest

from sqlalchemy_json_api import QueryBuilder


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestSelectRelationship(object):
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
    def test_to_many_relationship(
        self,
        query_builder,
        session,
        user_cls,
        id,
        result
    ):
        query = query_builder.select_relationship(
            session.query(user_cls).get(id),
            'all_friends'
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'result'),
        (
            (
                2,
                {'data': {'type': 'categories', 'id': '1'}}
            ),
            (
                5,
                {'data': {'type': 'categories', 'id': '3'}}
            )
        )
    )
    def test_to_one_relationship(
        self,
        query_builder,
        session,
        category_cls,
        id,
        result
    ):
        query = query_builder.select_relationship(
            session.query(category_cls).get(id),
            'parent'
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
                {
                    'self': '/users/1/relationships/all_friends',
                    'related': '/users/1/all_friends'
                },
                {
                    'data': [
                        {
                            'type': 'users',
                            'id': '2',
                        }
                    ],
                    'links': {
                        'self': '/users/1/relationships/all_friends',
                        'related': '/users/1/all_friends'
                    }
                }
            ),
            (
                2,
                {
                    'self': '/users/2/relationships/all_friends',
                    'related': '/users/2/all_friends'
                },
                {
                    'data': [
                        {
                            'type': 'users',
                            'id': '1',
                        },
                        {
                            'type': 'users',
                            'id': '3',
                        },
                        {
                            'type': 'users',
                            'id': '4',
                        }
                    ],
                    'links': {
                        'self': '/users/2/relationships/all_friends',
                        'related': '/users/2/all_friends'
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
        query = query_builder.select_relationship(
            session.query(user_cls).get(id),
            'all_friends',
            links=links
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'links', 'result'),
        (
            (
                2,
                {
                    'self': '/categories/2/relationships/parent',
                    'related': '/categories/2/parent'
                },
                {
                    'data': {
                        'type': 'categories',
                        'id': '1',
                    },
                    'links': {
                        'self': '/categories/2/relationships/parent',
                        'related': '/categories/2/parent'
                    }
                }
            ),
            (
                5,
                {
                    'self': '/categories/5/relationships/parent',
                    'related': '/categories/5/parent'
                },
                {
                    'data': {
                        'type': 'categories',
                        'id': '3',
                    },
                    'links': {
                        'self': '/categories/5/relationships/parent',
                        'related': '/categories/5/parent'
                    }
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
        query = query_builder.select_relationship(
            session.query(category_cls).get(id),
            'parent',
            links=links
        )
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('id', 'links', 'result'),
        (
            (
                1,
                {
                    'self': '/articles/1/relationships/category',
                    'related': '/articles/1/category'
                },
                {
                    'data': {
                        'type': 'categories',
                        'id': '1',
                    },
                    'links': {
                        'self': '/articles/1/relationships/category',
                        'related': '/articles/1/category'
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
        query = query_builder.select_relationship(
            session.query(article_cls).get(id),
            'category',
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
        query = query_builder.select_relationship(
            session.query(article_cls).get(id),
            'category',
            as_text=True
        )
        assert json.loads(session.execute(query).scalar()) == result
