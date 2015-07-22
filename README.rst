SQLAlchemy-JSON-API
===================

Fast SQLAlchemy query builder for returning JSON API compatible results. Currently supports only PostgreSQL.

Why?
----

Speed is essential for JSON APIs. Fetching objects in SQLAlchemy and serializing them
on Python server is an order of magnitude slower than returning JSON directly from database. This is because

1. Complex object structures are hard or impossible to fetch with single query when the serialization happens on Python side. Any kind of JSON API compatible object structure can be returned with a single query from database.
2. SQLAlchemy objects are memory hungry.
3. Rather than returning the data directly as JSON from the database it has to be first converted to Python data types and then serialized back to JSON.

By following this logic it would seem like a no-brainer to return the JSON directly from the database. However the queries are very hard to write. Luckily this is where SQLAlchemy-JSON-API comes to rescue the day. So instead of writing something like this:

.. code-block:: sql

    SELECT coalesce(
        (SELECT json_build_object(
            'data',
            (SELECT array_agg(main_json.json_object) AS array_agg_1
            FROM (
                SELECT json_build_object(
                    'id',
                    CAST(article.id AS TEXT),
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
                            (SELECT
                                coalesce(
                                    array_agg(relationships.json_object),
                                    CAST(ARRAY[] AS JSON[])
                                ) AS coalesce_2
                            FROM (
                                SELECT json_build_object(
                                    'id',
                                    CAST(comment_1.id AS TEXT),
                                    'type',
                                    'comments'
                                ) AS json_object
                                FROM comment AS comment_1
                                WHERE article._id = comment_1.article_id
                            ) AS relationships)
                        )
                    )
                ) AS json_object
            ) AS main_json
        )) AS json_build_object_1
        FROM article
    ),
    json_build_object('data', CAST(ARRAY[] AS JSON[]))) AS coalesce_1


You can simply write:

::


    from sqlalchemy_json_api import QueryBuilder


    query_builder = QueryBuilder({'articles': Article, 'comments': Comment})
    query_builder.select(Article, {'articles': ['name', 'comments']})
    result = session.execute(query).scalar()


.. image: https://c1.staticflickr.com/1/56/188370562_8fe0f3cba9.jpg
