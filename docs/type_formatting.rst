Type based formatting
---------------------


Sometimes you may want type based formatting, eg. forcing all datetimes in ISO standard format.
You can easily achieve this by using ``type_formatters`` parameter for :meth:`.QueryBuilder`.


::


    def isoformat(date):
        return sa.func.to_char(
            date,
            sa.text('\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'')
        ).label(date.name)

    query_builder.type_formatters = {
        sa.DateTime: isoformat
    }

    query = query_builder.select(
        Article,
        fields={'articles': ['name', 'created_at']},
        from_obj=base_query
    )
    result = session.execute(query).scalar()
    # {
    #     'data': [{
    #         'id': '1',
    #         'type': 'articles',
    #         'attributes': {
    #             'name': 'Some article',
    #             'created_at': '2011-01-01T00:00:00.000000Z'
    #         },
    #     }]
    # }
