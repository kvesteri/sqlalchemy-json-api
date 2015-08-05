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
        assert session.execute(query).scalar() == {'data': None}
