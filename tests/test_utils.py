import pytest

from sqlalchemy_json_api import assert_json_document


@pytest.mark.parametrize(
    ('value', 'expected'),
    (
        (
            {
                'data': [{'type': 'articles', 'id': '1'}],
                'included': [
                    {
                        'type': 'categories',
                        'id': '2',
                    },
                    {
                        'type': 'categories',
                        'id': '1',
                    },
                ]
            },
            {
                'data': [{
                    'type': 'articles',
                    'id': '1',
                }],
                'included': [
                    {
                        'type': 'categories',
                        'id': '1',
                    },
                    {
                        'type': 'categories',
                        'id': '2',
                    },
                ]
            }
        ),
    )
)
def test_assert_json_document_for_matching_documents(value, expected):
    assert_json_document(value, expected)
