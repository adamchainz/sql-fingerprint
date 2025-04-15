=========
Changelog
=========

* Clear expressions in ``JOIN`` clauses.

1.3.0 (2025-04-04)
------------------

* Clear ``GROUP BY`` clauses in ``SELECT`` statements.

1.2.0 (2025-04-03)
------------------

* Clear ``ON CONFLICT``, ``UPDATE``, and ``WHERE`` clauses in ``INSERT`` statements.

1.1.0 (2025-04-03)
------------------

* Clear ``WHERE``, ``LIMIT``, and ``OFFSET`` clauses in ``SELECT`` statements.

* Unquote table and column names in more cases.

* Handle unparseable SQL statements.

1.0.0 (2025-04-03)
------------------

* Rename ``fingerprint()`` to ``fingerprint_many()``, add ``fingerprint_one()``.

* Support combined queries (``UNION`` etc.),

* Simplify ``INSERT`` and ``UPDATE`` statements.

0.1.0 (2025-03-02)
------------------

* Initial release.
