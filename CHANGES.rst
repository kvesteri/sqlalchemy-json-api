Changelog
---------

Here you can see the full list of changes between each SQLAlchemy-JSON-API release.


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
