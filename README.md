# sql-fingerprint

[![CI](https://img.shields.io/github/actions/workflow/status/adamchainz/sql-fingerprint/main.yml.svg?branch=main&style=for-the-badge)](https://github.com/adamchainz/sql-fingerprint/actions?workflow=CI)
[![Crates.io](https://img.shields.io/crates/v/sql-fingerprint.svg?style=for-the-badge)](https://crates.io/crates/sql-fingerprint)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=for-the-badge)](https://github.com/pre-commit/pre-commit)

A SQL fingerprinter.
Given a query like:

```sql
SELECT name, age /* computed */ FROM cheeses WHERE origin = 'France'
```

…it will output a fingerprint like:

```sql
SELECT ... FROM cheeses WHERE ...
```

---

**Improve your Django and Git skills** with [my books](https://adamj.eu/books/).

---

TODO
