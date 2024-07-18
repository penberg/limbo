# Changelog

## Unreleased

### Added

- Partial `LEFT JOIN` support.

### Fixed

- Lock database file with POSIX filesystem advisory lock when database
  is opened to prevent concurrent processes from corrupting a file.
  Please note that the locking scheme differs from SQLite, which uses
  POSIX advisory locks for every transaction. We're defaulting to
  locking on open because it's faster.

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
