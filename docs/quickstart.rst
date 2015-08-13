Quickstart
----------

Consider the following model definition.

::

    import sqlalchemy as sa
    from sqlalchemy.ext.declarative import declarative_base


    Base = declarative_base()


    class Article(Base):
        __tablename__ = 'article'
        id = sa.Column('_id', sa.Integer, primary_key=True)
        name = sa.Column(sa.String)
        content = sa.Column(sa.String)


    class Comment(Base):
        __tablename__ = 'comment'
        id = sa.Column(sa.Integer, primary_key=True)
        content = sa.Column(sa.String)
        article_id = sa.Column(sa.Integer, sa.ForeignKey(Article.id))
        article = sa.orm.relationship(article_cls, backref='comments')


In order to use SQLAlchemy-JSON-API you need to first initialize a :class:`.QueryBuilder` by providing it
a class mapping.

::


    from sqlalchemy_json_api import QueryBuilder


    query_builder = QueryBuilder({
        'articles': Article,
        'comments': Comment
    })


Now we can start using it by selecting from the existing resources.

::

    query = query_builder.select(Article)
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
    #     }]
    # }

You can also make the query builder build queries that return the results as
raw json by using the ``as_text`` parameter.

::

    query = query_builder.select(Article, as_text=True)
    result = session.execute(query).scalar()
    # '{
    #     "data": [{
    #         "id": "1",
    #         "type": "articles",
    #         "attributes": {
    #             "content": "Some content",
    #             "name": "Some article",
    #         },
    #         "relationships": {
    #             "comments": {
    #                 "data": [
    #                     {"id": "1", "type": "comments"},
    #                     {"id": "2", "type": "comments"}
    #                 ]
    #             },
    #         },
    #     }]
    # }'
