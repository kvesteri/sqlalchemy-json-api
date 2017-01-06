Changelog
---------

Here you can see the full list of changes between each SQLAlchemy-JSON-API release.


0.4.1 (2016-06-01)
^^^^^^^^^^^^^^^^^^

- Fixed unambiguous column reference passed for relationship order by (#10)
- Dropped python 2.6 support
- Added python 3.5 to test matrix
- Added SQLAlchemy 1.1 to test matrix


0.4.0 (2016-06-06)
^^^^^^^^^^^^^^^^^^

- Added type formatting (#5)
- Added limit and offset parameters (#6)
- Fixed passing empty array as `included`` parameter (#9)
- Default order by for relationships in order to force deterministic results (#10)
- Added column property adaptation in similar manner as hybrid properties are adapted (#11)


0.3.0 (2015-08-18)
^^^^^^^^^^^^^^^^^^

- Made select_one return ``None`` when main data equals null


0.2.2 (2015-08-16)
^^^^^^^^^^^^^^^^^^

- Fixed included to use distinct for included resources queries


0.2.1 (2015-08-16)
^^^^^^^^^^^^^^^^^^

- Fixed included parameter when fetching multiple included objects for multiple root resources
- Update SA-Utils dependency to 0.30.17


0.2 (2015-08-11)
^^^^^^^^^^^^^^^^

- Added sort parameter to select method
- Added support for links objects
- Added reserved keyword checking
- Added select_related and select_relationship methods
- Added as_text parameter to all select_* methods


0.1 (2015-07-29)
^^^^^^^^^^^^^^^^

- Initial release
