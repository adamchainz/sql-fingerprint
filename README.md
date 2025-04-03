# sql-fingerprint

[![Documentation](https://img.shields.io/docsrs/sql-fingerprint?style=for-the-badge)](https://docs.rs/sql-fingerprint/latest/sql_fingerprint/)
[![Changelog](https://img.shields.io/badge/changelog-blue?style=for-the-badge)](https://github.com/adamchainz/sql-fingerprint/blob/main/CHANGELOG.rst)
[![CI](https://img.shields.io/github/actions/workflow/status/adamchainz/sql-fingerprint/main.yml.svg?branch=main&style=for-the-badge)](https://github.com/adamchainz/sql-fingerprint/actions?workflow=CI)
[![Crates.io](https://img.shields.io/crates/v/sql-fingerprint.svg?style=for-the-badge)](https://crates.io/crates/sql-fingerprint)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=for-the-badge)](https://github.com/pre-commit/pre-commit)

A SQL fingerprinter.

sql-fingerprint reduces SQL queries to recognizable fingerprints for easier comparison.
The goal is to provide readable traces from queries captured during tests, so that changes can be tracked over time.

For example, given a query like:

```sql
SELECT name, age /* computed */ FROM cheeses WHERE origin = 'France'
```

…it will output a fingerprint like:

```sql
SELECT ... FROM cheeses WHERE ...
```

The fingerprinting process applies these changes:

* Comments are dropped.
* Whitespace is normalized to a single space.
* Identifier and value lists are reduced to '...'.
* Table names consisting of letters, numbers, and underscores have any quoting removed.
* Savepoint IDs are replaced with 's1', 's2', etc.
