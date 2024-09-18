# Changelog

### Unreleased

### Added

- `GROUP BY` support (Jussi Saurio)

- Overflow page support (Pere Diaz Bou)

- Improvements to Python bindings (Jean Arhancet and Lauri Virtanen)

### Fixed

- Fix `LIKE` to be case insensitive (RJ Barman)

## 0.0.4 - 2024-08-22

- Query planner rewrite (Jussi Saurio)

- Initial pass on Python bindings (Jean Arhancet)

- Improve scalar function support (Kim Seon Woo and Jean Arhancet)

### Added

- Partial support for `json()` function (Jean Arhancet)

## 0.0.3 - 2024-08-01

### Added

- Initial pass on the write path. Note that the write path is not transactional yet. (Pere Diaz Bou)

- More scalar functions: `unicode()` (Ethan Niser)

- Optimize point queries with integer keys (Jussi Saurio)

### Fixed

- `ORDER BY` support for nullable sorting columns and qualified identifiers (Jussi Saurio)

- Fix `.schema` command crash in the CLI ([#212](https://github.com/penberg/limbo/issues/212) (Jussi Saurio)

## 0.0.2 - 2024-07-24

### Added

- Partial `LEFT JOIN` support.

- Partial `ORDER BY` support.

- Partial scalar function support.

### Fixed

- Lock database file with POSIX filesystem advisory lock when database
  is opened to prevent concurrent processes from corrupting a file.
  Please note that the locking scheme differs from SQLite, which uses
  POSIX advisory locks for every transaction. We're defaulting to
  locking on open because it's faster. (Issue #94)

### Changed

- Install to `~/.limbo/` instead of `CARGO_HOME`.

## 0.0.1 - 2024-07-17

### Added

- Partial `SELECT` statement support, including `WHERE`, `LIKE`,
  `LIMIT`, `CROSS JOIN`, and `INNER JOIN`.

- Aggregate function support.

- `EXPLAIN` statement support.

- Partial `PRAGMA` statement support, including `cache_size`.

- Asynchronous I/O support with Linux io_uring using direct I/O and
  Darwin kqueue.

- Initial pass on command line shell with following commands:
    - `.schema` command that describes the database schema.
    - `.opcodes <opcode>` that describes what a VDBE opcode does.
