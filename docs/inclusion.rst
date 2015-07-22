Inclusion of related models
---------------------------

You can include related models by providing the `include` parameter to :func:`.QueryBuilder.select`.

::


    query = query_builder.select(
        Article,
        fields={'articles': ['name', 'comments']},
        include=['comments']
    )
    result = session.execute(query).scalar()
    # {
    #     'data': [{
    #         'id': '1',
    #         'type': 'articles',
    #         'attributes': {
    #             'content': 'Some content',
    #             'name': 'Some article',
    #         },
    #         'relationships': {
    #             'comments': {
    #                 'data': [
    #                     {'id': '1', 'type': 'comments'},
    #                     {'id': '2', 'type': 'comments'}
    #                 ]
    #             },
    #         },
    #     }],
    #     'included': [
    #         {
    #             'id': '1',
    #             'type': 'comments',
    #             'attributes': {
    #                 'content': 'Some comment'
    #             }
    #         }
    #     ]
    # }
