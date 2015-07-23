Compound documents
------------------

You can create queries returning `compound document responses`_ by providing the ``include`` parameter to :meth:`.QueryBuilder.select`.


.. _`compound document responses`: http://jsonapi.org/format/#document-compound-documents

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
    #         },
    #         {
    #             'id': '2',
    #             'type': 'comments',
    #             'attributes': {
    #                 'content': 'Some other comment'
    #             }
    #         }
    #     ]
    # }


.. note::

    SQLAlchemy-JSON-API always returns all included resources ordered by first
    type and then by id in ascending order. The consistent order of resources
    helps testing APIs.
