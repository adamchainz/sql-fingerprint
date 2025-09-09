=========
Changelog
=========

1.10.0 (2025-09-09)
-------------------

* Upgrade sqlparser to Git install of commit `b8539a52af7419b86d9c2fc51ba247d022f7a6f8 <https://github.com/apache/datafusion-sqlparser-rs/commit/b8539a52af7419b86d9c2fc51ba247d022f7a6f8>`__, including a bug fix known to affect this package: `PR #2001 <https://github.com/apache/datafusion-sqlparser-rs/pull/2001>`__.

1.9.0 (2025-07-31)
------------------

* Upgrade sqlparser to version 0.58.0, including a bug fix known to affect this package: `#1899 <https://github.com/apache/datafusion-sqlparser-rs/pull/1899>`__.

1.8.0 (2025-07-05)
------------------

* Elide contents of ``UNNEST(...)`` function calls in ``FROM`` clauses.

  Thanks to Nick Pope for the suggestion in `Issue #25 <https://github.com/adamchainz/sql-fingerprint/issues/25>`__.

1.7.0 (2025-06-26)
------------------

* Simplify set expressions (``UNION``, ``INTERSECT``, ``EXCEPT``, and so on) even when non-parenthesized, and up to unlimited chaining.

1.6.0 (2025-06-26)
------------------

* Upgrade sqlparser, including some bug fixes known to affect this package: `#1792 <https://github.com/apache/datafusion-sqlparser-rs/issues/1792>`__ and `#1815 <https://github.com/apache/datafusion-sqlparser-rs/pull/1815>`__.

* Simplify expressions used in special ``LIMIT`` syntax used in Clickhouse and MySQL.

1.5.0 (2025-04-28)
------------------

* Clear expressions in ``DISTINCT ON`` clauses.

1.4.0 (2025-04-15)
------------------

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

* Handle unparsable SQL statements.

1.0.0 (2025-04-03)
------------------

* Rename ``fingerprint()`` to ``fingerprint_many()``, add ``fingerprint_one()``.

* Support combined queries (``UNION`` etc.),

* Simplify ``INSERT`` and ``UPDATE`` statements.

0.1.0 (2025-03-02)
------------------

* Initial release.
