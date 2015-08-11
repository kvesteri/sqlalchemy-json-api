Sorting queries
---------------

You can apply an order by to builded query by providing it a ``sort`` parameter.
This parameter should be a list of root resource attribute names.

Sort by name ascending

::


    query = query_builder.select(
        Article,
        sort=['name']
    )


Sort by name descending first and id ascending second


::

    query = query_builder.select(
        Article,
        sort=['-name', 'id']
    )


.. note::

    SQLAlchemy-JSON-API does NOT support sorting by related resource attribute
    at the moment.
