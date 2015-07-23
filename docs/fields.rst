Selecting specific fields
-------------------------

By default SQLAlchemy-JSON-API selects all orm descriptors for given model. This includes:

* Column properties
* Synonyms
* Hybrid properties
* Relationship properties

Each included model MUST have an ``id`` property. Usually this should be the primary key of your model. If your model doesn't have an ``id`` property you can add one by using for example SQLAlchemy hybrids.


::

    from sqlalchemy.ext.hybrid import hybrid_property


    class GroupInvitation(Base):
        group_id = sa.Column(
            sa.Integer,
            sa.ForeignKey(Group.id),
            primary_key=True
        )
        user_id = sa.Column(
            sa.Integer,
            sa.ForeignKey(User.id),
            primary_key=True
        )
        issued_at = sa.Column(sa.DateTime)

        @hybrid_property
        def id(self):
            return self.group_id + ':' + self.user_id



Please notice that you can't include regular descriptors, only orm descriptors.

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


