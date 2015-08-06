import pytest


@pytest.fixture(scope='class')
def dataset(session, category_cls):
    session.add_all([
        category_cls(name='Category A', id=1),
        category_cls(name='Category A', id=2),
        category_cls(name='Category A', id=3),
        category_cls(name='Category B', id=4),
        category_cls(name='Category B', id=5),
        category_cls(name='Category B', id=6)
    ])
    session.commit()


@pytest.mark.usefixtures('table_creator', 'dataset')
class TestQueryBuilderSelectWithSorting(object):
    @pytest.mark.parametrize(
        ('sort', 'result'),
        (
            (
                ['id'],
                {'data': [
                    {'type': 'categories', 'id': '1'},
                    {'type': 'categories', 'id': '2'},
                    {'type': 'categories', 'id': '3'},
                    {'type': 'categories', 'id': '4'},
                    {'type': 'categories', 'id': '5'},
                    {'type': 'categories', 'id': '6'}
                ]}
            ),
            (
                ['-id'],
                {'data': [
                    {'type': 'categories', 'id': '6'},
                    {'type': 'categories', 'id': '5'},
                    {'type': 'categories', 'id': '4'},
                    {'type': 'categories', 'id': '3'},
                    {'type': 'categories', 'id': '2'},
                    {'type': 'categories', 'id': '1'}
                ]}
            ),
            (
                ['name', 'id'],
                {'data': [
                    {'type': 'categories', 'id': '1'},
                    {'type': 'categories', 'id': '2'},
                    {'type': 'categories', 'id': '3'},
                    {'type': 'categories', 'id': '4'},
                    {'type': 'categories', 'id': '5'},
                    {'type': 'categories', 'id': '6'}
                ]}
            ),
            (
                ['name', '-id'],
                {'data': [
                    {'type': 'categories', 'id': '3'},
                    {'type': 'categories', 'id': '2'},
                    {'type': 'categories', 'id': '1'},
                    {'type': 'categories', 'id': '6'},
                    {'type': 'categories', 'id': '5'},
                    {'type': 'categories', 'id': '4'}
                ]}
            ),
            (
                ['-name', 'id'],
                {'data': [
                    {'type': 'categories', 'id': '4'},
                    {'type': 'categories', 'id': '5'},
                    {'type': 'categories', 'id': '6'},
                    {'type': 'categories', 'id': '1'},
                    {'type': 'categories', 'id': '2'},
                    {'type': 'categories', 'id': '3'}
                ]}
            ),
        )
    )
    def test_sort_root_resource(
        self,
        session,
        query_builder,
        category_cls,
        sort,
        result
    ):
        query = query_builder.select(
            category_cls,
            fields={'categories': []},
            sort=sort
        )
        assert session.execute(query).scalar() == result
