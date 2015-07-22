Selecting specific fields
-------------------------

By default SQLAlchemy-JSON-API selects all orm descriptors for given model. This includes:

* Column properties
* Synonyms
* Hybrid properties
* Relationship properties

You can customize this behaviour by providing the ``fields`` parameter to :meth:`.QueryBuilder.select`.

::


    query_builder.select(Article, {'articles': ['name']})
    result = session.execute(query).scalar()
    # {
    #     'data': [{
    #         'id': '1',
    #         'type': 'articles',
    #         'attributes': {
    #             'name': 'Some article',
    #         },
    #     }]
    # }

If you only want to select id for given model you need to provide empty list for given model key.


::


    query = query_builder.select(Article, {'articles': []})
    result = session.execute(query).scalar()
    # {
    #     'data': [{
    #         'id': '1',
    #         'type': 'articles',
    #     }]
    # }
