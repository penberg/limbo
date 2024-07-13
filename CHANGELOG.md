# Changelog

## [Unreleased]

### Added

- Partial `SELECT` statement support, including `WHERE`, `LIMIT`, and `CROSS JOIN`.

- Partial `PRAGMA` statement support, including `cache_size`.

- Partial aggregate function support, including `avg()`, `count()`, `max()`, `min()`, `sum()`, and `total()`.

- `EXPLAIN` statement support.

- Asynchronous I/O support with Linux io_uring using direct I/O and Darwin kqueue.

- Initial pass on command line shell with following commands:
    - `.schema` command that describes the database schema.
    - `.opcodes <opcode>` that describes what a VDBE opcode does.
