# Changelog

## [Unreleased]

### Added

- Partial `SELECT` statement support, including `LIMIT`.

- Partial `PRAGMA` statement support, including `cache_size`.

- Partial aggregate function support, including `avg()`, `count()`, `max()`, `min()`, and `sum()`.

- `EXPLAIN` statement support.

- Linux io_uring support using direct I/O.

- Initial pass on command line shell with following commands:
    - `.schema` command that describes the database schema.
    - `.opcodes <opcode>` that describes what a VDBE opcode does.
