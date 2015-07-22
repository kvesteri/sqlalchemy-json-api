Filtering queries
-----------------


You can filter query results by providing the ``from_obj`` parameter for :meth:`.QueryBuilder.select`.
This parameter can be any SQLAlchemy selectable construct.


::


    base_query = session.query(Article).filter(Article.name == 'Some article')

    query = query_builder.select(
        Article,
        fields={'articles': ['name']},
        from_obj=base_query
    )
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
