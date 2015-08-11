import pytest

from sqlalchemy_json_api import QueryBuilder


@pytest.fixture
def query_builder(model_mapping):
    return QueryBuilder(model_mapping, base_url='/')


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestQueryBuilderSelectWithLinks(object):
    def test_root_data_links(self, session, article_cls, query_builder):
        query = query_builder.select(article_cls, fields={'articles': []})
        result = {
            'data': [
                {
                    'id': '1',
                    'type': 'articles',
                    'links': {
                        'self': '/articles/1'
                    }
                }
            ]
        }
        assert session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': []
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
                        'links': {'self': '/articles/1'},
                        'relationships': {
                            'category': {
                                'data': {
                                    'type': 'categories',
                                    'id': '1',
                                },
                                'links': {
                                    'self': (
                                        '/articles/1/relationships/category'
                                    ),
                                    'related': '/articles/1/category'
                                }
                            }
                        }
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'links': {'self': '/categories/1'}
                    }]
                }
            ),
            (
                {'articles': [], 'categories': ['name', 'subcategories']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': '1',
                        'links': {'self': '/articles/1'},
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': '1',
                        'attributes': {
                            'name': 'Some category'
                        },
                        'links': {'self': '/categories/1'},
                        'relationships': {
                            'subcategories': {
                                'links': {
                                    'self': (
                                        '/categories/1/relationships'
                                        '/subcategories'
                                    ),
                                    'related': '/categories/1/subcategories'
                                },
                                'data': [
                                    {'id': '2', 'type': 'categories'},
                                    {'id': '4', 'type': 'categories'}
                                ]
                            }
                        },
                    }]
                }
            ),
        )
    )
    def test_links_with_relationships_and_include(
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
