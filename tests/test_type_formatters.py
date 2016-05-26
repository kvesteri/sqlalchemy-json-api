from datetime import datetime

import pytest
import sqlalchemy as sa


def isoformat(date):
    return sa.func.to_char(
        date,
        sa.text('\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'')
    ).label(date.name)


@pytest.mark.usefixtures('table_creator')
class TestTypeFormatters(object):
    @pytest.fixture
    def category(self, session, category_cls):
        category = category_cls(
            name='Category', created_at=datetime(2011, 1, 1)
        )
        session.add(category)
        session.commit()
        return category

    def test_formats_columns_with_matching_types(
        self,
        query_builder,
        category,
        session,
        category_cls
    ):
        query_builder.type_formatters = {
            sa.DateTime: isoformat
        }
        query = query_builder.select_one(
            category_cls,
            1,
            fields={'categories': ['created_at', 'name']},
            from_obj=session.query(category_cls)
        )
        assert session.execute(query).scalar() == {
            'data': {
                'attributes': {
                    'created_at': '2011-01-01T00:00:00.000000Z',
                    'name': 'Category'
                },
                'id': '1',
                'type': 'categories'
            }
        }
