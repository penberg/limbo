# Changelog

## 0.0.14 - 2025-02-04

### Added

**Core:**

* Improve changes() and total_changes() functions and add tests (Ben Li)
* Add support for `json_object` function (Jorge Hermo)
* Implemented json_valid function (Harin)
* Implement Not (Vrishabh)
* Initial support for wal_checkpoint pragma (Sonny)
* Implement Or and And bytecodes (Diego Reis)
* Implement strftime function (Pedro Muniz)
* implement sqlite_source_id function (Glauber Costa)
* json_patch() function implementation (Ihor Andrianov)
* json_remove() function implementation (Ihor Andrianov)
* Implement isnull / not null for filter expressions (Glauber Costa)
* Add support for offset in select queries (Ben Li)
* Support returning column names from prepared statement (Preston Thorpe)
* Implement Concat opcode (Harin)
* Table info (Glauber Costa)
* Pragma list (Glauber Costa)
* Implement Noop bytecode (Pedro Muniz)
* implement is and is not where constraints (Glauber Costa)
* Pagecount (Glauber Costa)
* Support column aliases in GROUP BY, ORDER BY and HAVING (Jussi Saurio)
* Implement json_pretty (Pedro Muniz)

**Extensions:**

* Initial pass on vector extension (Pekka Enberg)
* Enable static linking for 'built-in' extensions (Preston Thorpe)

**Go Bindings:**

* Initial support for Go database/sql driver (Preston Thorpe)
* Avoid potentially expensive operations on prepare' (Glauber Costa)

**Java Bindings:**

* Implement JDBC `ResultSet` (Kim Seon Woo)
* Implement LimboConnection `close()` (Kim Seon Woo)
* Implement close() for `LimboStatement` and `LimboResultSet` (Kim Seon Woo)
* Implement methods in `JDBC4ResultSet` (Kim Seon Woo)
* Load native library from Jar (Kim Seon Woo)
* Change logger dependency (Kim Seon Woo)
* Log driver loading error (Pekka Enberg)

**Simulator:**

* Implement `--load` and `--watch` flags (Alperen Keleş)

**Build system and CI:**

* Add Nyrkiö change point detection to 'cargo bench' workflow (Henrik Ingo)

### Fixed

* Fix `select X'1';` causes limbo to go in infinite loop (Krishna Vishal)
* Fix rowid search codegen (Nikita Sivukhin)
* Fix logical codegen (Nikita Sivukhin)
* Fix parser panic when duplicate column names are given to `CREATE TABLE` (Krishna Vishal)
* Fix panic when double quoted strings are used for column names. (Krishna Vishal)
* Fix `SELECT -9223372036854775808` result differs from SQLite (Krishna Vishal)
* Fix `SELECT ABS(-9223372036854775808)` causes limbo to panic.  (Krishna Vishal)
* Fix memory leaks, make extension types more efficient (Preston Thorpe)
* Fix table with single column PRIMARY KEY to not create extra btree (Krishna Vishal)
* Fix null cmp codegen (Nikita Sivukhin)
* Fix null expr codegen (Nikita Sivukhin)
* Fix rowid generation (Nikita Sivukhin)
* Fix shr instruction (Nikita Sivukhin)
* Fix strftime function compatibility problems (Pedro Muniz)
* Dont fsync the WAL on read queries (Jussi Saurio)

## 0.0.13 - 2025-01-19

### Added

* Initial support for native Limbo extensions (Preston Thorpe)
      
* npm packaging for node and web (Elijah Morgan)

* Add support for `rowid` keyword' (Kould)

* Add support for shift left, shift right, is and is not operators (Vrishabh)

* Add regexp extension (Vrishabh)
      
* Add counterexample minimization to simulator (Alperen Keleş)

* Initial support for binding values to prepared statements (Levy A.)

### Updated

* Java binding improvements (Kim Seon Woo)

* Reduce `liblimbo_sqlite3.a` size' (Pekka Enberg)

### Fixed

* Fix panics on invalid aggregate function arguments (Krishna Vishal)
 
* Fix null compare operations not giving null (Vrishabh)

* Run all statements from SQL argument in CLI (Vrishabh)

* Fix MustBeInt opcode semantics (Vrishabh)

* Fix recursive binary operation logic (Jussi Saurio)

* Fix SQL comment parsing in Limbo shell (Diego Reis and Clyde)

## 0.0.12 - 2025-01-14

### Added

**Core:**

* Improve JSON function support (Kacper Madej, Peter Sooley)

* Support nested parenthesized conditional expressions (Preston Thorpe)

* Add support for changes() and total_changes() functions (Lemon-Peppermint)

* Auto-create index in CREATE TABLE when necessary (Jussi Saurio)

* Add partial support for datetime() function (Preston Thorpe)

* SQL parser performance improvements (Jussi Saurio)

**Shell:**

* Show pretty parse errors in the shell (Samyak Sarnayak)

* Add CSV import support to shell (Vrishabh)

* Selectable IO backend with --io={syscall,io-uring} argument (Jorge López Tello)

**Bindings:**

* Initial version of Java bindings (Kim Seon Woo)

* Initial version of Rust bindings (Pekka Enberg)

* Add OPFS support to Wasm bindings (Elijah Morgan)

* Support uncorrelated FROM clause subqueries (Jussi Saurio)

* In-memory support to `sqlite3_open()` (Pekka Enberg)

### Fixed

* Make iterate() lazy in JavaScript bindings (Diego Reis)

* Fix integer overflow output to be same as sqlite3 (Vrishabh)

* Fix 8-bit serial type to encoding (Preston Thorpe)

* Query plan optimizer bug fixes (Jussi Saurio)

* B-Tree balancing fixes (Pere Diaz Bou)

* Fix index seek wrong on `SeekOp::LT`\`SeekOp::GT` (Kould)

* Fix arithmetic operations for text values' from Vrishabh

* Fix quote escape in SQL literals (Vrishabh)

## 0.0.11 - 2024-12-31

### Added

* Add in-memory mode to Python bindings (Jean Arhancet)

* Add json_array_length function (Peter Sooley)

* Add support for the UUID extension (Preston Thorpe)

### Changed

* Enable sqpoll by default in io_uring (Preston Thorpe)

* Simulator improvements (Alperen Keleş)

### Fixed

* Fix escaping issues with like and glob functions (Vrishabh)

* Fix `sqlite_version()` out of bound panics' (Diego Reis)

* Fix on-disk file format bugs (Jussi Saurio)

## 0.0.10 - 2024-12-18

### Added

* In-memory mode (Preston Thorpe)

* More CLI improvements (Preston Thorpe)

* Add support for replace() function (Alperen Keleş)

* Unary operator improvements (Jean Arhancet)

* Add support for unex(x, y) function (Kacper Kołodziej)

### Fixed

* Fix primary key handling when there's rowid and PK is not alias (Jussi Saurio)

## 0.0.9 - 2024-12-12

### Added

* Improve CLI (Preston Thorpe)

* Add support for iif() function (Alex Miller)

* Add suport for last_insert_rowid() function (Krishna Vishal)

* Add support JOIN USING and NATURAL JOIN (Jussi Saurio)

* Add support for more scalar functions (Kacper Kołodziej)

* Add support for `HAVING` clause (Jussi Saurio)

* Add `get()` and `iterate()` to JavaScript/Wasm API (Jean Arhancet)

## 0.0.8 - 2024-11-20

### Added

* Python package build and example usage (Pekka Enberg)

## 0.0.7 - 2024-11-20

### Added

* Minor improvements to JavaScript API (Pekka Enberg)
* `CAST` support (Jussi Saurio)

### Fixed

* Fix issues found in-btree code with the DST (Pere Diaz Bou)

## 0.0.6 - 2024-11-18

### Fixed

- Fix database truncation caused by `limbo-wasm` opening file in wrong mode (Pere Diaz Bou)

## 0.0.5 - 2024-11-18

### Added

- `CREATE TABLE` support (Pere Diaz Bou)

- Add Add Database.prepare() and Statement.all() to Wasm bindings (Pekka Enberg)

- WAL improvements (Pere Diaz Bou)

- Primary key index scans and single-column secondary index scans (Jussi Saurio)

- `GROUP BY` support (Jussi Saurio)

- Overflow page support (Pere Diaz Bou)

- Improvements to Python bindings (Jean Arhancet and Lauri Virtanen)

- Improve scalar function support (Lauri Virtanen)

### Fixed

- Panic in codegen with `COUNT(*)` (Jussi Saurio)

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
