import json

import pytest


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestSelectOne(object):
    def test_with_from_obj(self, query_builder, session, user_cls):
        query = query_builder.select_one(
            user_cls,
            1,
            fields={'users': ['all_friends']},
            from_obj=session.query(user_cls)
        )
        assert session.execute(query).scalar() == {
            'data': {
                'relationships': {
                    'all_friends': {'data': [{'id': '2', 'type': 'users'}]}
                },
                'id': '1',
                'type': 'users'
            }
        }

    def test_without_from_obj(self, query_builder, session, user_cls):
        query = query_builder.select_one(
            user_cls,
            1,
            fields={'users': ['all_friends']},
        )
        assert session.execute(query).scalar() == {
            'data': {
                'relationships': {
                    'all_friends': {'data': [{'id': '2', 'type': 'users'}]}
                },
                'id': '1',
                'type': 'users'
            }
        }

    def test_empty_result(self, query_builder, session, user_cls):
        query = query_builder.select_one(
            user_cls,
            99,
        )
        assert session.execute(query).scalar() is None

    def test_as_text_parameter(self, query_builder, session, article_cls):
        query = query_builder.select_one(
            article_cls,
            1,
            fields={'articles': ['name']},
            as_text=True
        )

        assert json.loads(session.execute(query).scalar()) == {
            'data': {
                'type': 'articles',
                'id': '1',
                'attributes': {
                    'name': 'Some article'
                }
            }
        }
