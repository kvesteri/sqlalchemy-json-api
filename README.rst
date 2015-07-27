SQLAlchemy-JSON-API
===================

Fast `SQLAlchemy`_ query builder for returning `JSON API`_ compatible results. Currently supports only `PostgreSQL`_.

Why?
----

Speed is essential for JSON APIs. Fetching objects in `SQLAlchemy`_ and serializing them
on Python server is an order of magnitude slower than returning JSON directly from database. This is because

1. Complex object structures are hard or impossible to fetch with single query when the serialization happens on Python side. Any kind of JSON API compatible object structure can be returned with a single query from database.
2. SQLAlchemy objects are memory hungry.
3. Rather than returning the data directly as JSON from the database it has to be first converted to Python data types and then serialized back to JSON.

By following this logic it would seem like a no-brainer to return the JSON directly from the database. However the queries are very hard to write. Luckily this is where SQLAlchemy-JSON-API comes to rescue the day. So instead of writing something like this:

.. code-block:: sql

    SELECT row_to_json(main_json_query.*)
    FROM (
        SELECT (
            SELECT coalesce(
                array_agg(data_query.data),
                CAST(ARRAY[] AS JSON[])
            ) AS data
            FROM (
                SELECT
                    json_build_object(
                        'id',
                        CAST(article.id AS VARCHAR),
                        'type',
                        'articles',
                        'attributes',
                        json_build_object(
                            'name',
                            article.name
                        ),
                        'relationships',
                        json_build_object(
                            'comments',
                            json_build_object(
                                'data',
                                (
                                    SELECT
                                    coalesce(
                                        array_agg(relationships.json_object),
                                        CAST(ARRAY[] AS JSON[])
                                    ) AS coalesce_2
                                    FROM (
                                        SELECT json_build_object(
                                            'id',
                                            CAST(comment.id AS VARCHAR),
                                            'type',
                                            'comments'
                                        ) AS json_object
                                        FROM comment
                                        WHERE article.id = comment.article_id
                                    ) AS relationships
                                )
                            )
                        )
                    ) AS data
                FROM article
            ) AS data_query
        ) AS data
    ) AS main_json_query


You can simply write:

.. code-block:: python


    from sqlalchemy_json_api import QueryBuilder


    query_builder = QueryBuilder({'articles': Article, 'comments': Comment})
    query_builder.select(Article, {'articles': ['name', 'comments']})
    result = session.execute(query).scalar()


To get results such as:

.. code-block:: python


    {
        'data': [{
            'id': '1',
            'type': 'articles',
            'attributes': {
                'content': 'Some content',
                'name': 'Some article',
            },
            'relationships': {
                'comments': {
                    'data': [
                        {'id': '1', 'type': 'comments'},
                        {'id': '2', 'type': 'comments'}
                    ]
                },
            },
        }],
    }


.. image:: https://c1.staticflickr.com/1/56/188370562_8fe0f3cba9.jpg


.. _SQLAlchemy: http://www.sqlalchemy.org
.. _PostgreSQL: http://www.postgresql.org
.. _`JSON API`: http://jsonapi.org
