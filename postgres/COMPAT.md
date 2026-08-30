# Turso Postgres Compatibility Reference

This document tracks PostgreSQL feature compatibility for the Turso Postgres
frontend. The feature list is based on the official
[PostgreSQL feature matrix](https://www.postgresql.org/about/featurematrix/),
with additional rows for baseline features the matrix does not enumerate.

Status legend:

| Status | Meaning |
|--------|---------|
| ✅ Supported | Feature works and is covered by tests |
| 🟡 Partial | Feature works with limitations (see notes) |
| ❌ Not supported | Feature is not implemented |

## Architecture

The frontend parses SQL with `pg_query` (libpg_query, the real PostgreSQL
grammar), so virtually all PostgreSQL *syntax* is accepted at parse time.
Support is decided by the translator (`postgres/parser/translator.rs`), which
converts the PostgreSQL AST into Turso's native AST, and by what the Turso
engine can execute. Components:

- `postgres/parser` — parse + translate PostgreSQL SQL to Turso AST
- `postgres/frontend` — pg_catalog emulation, COPY, schemas, session handling
- `postgres/server` — PostgreSQL wire protocol (v3) server, built on `pgwire`
- `postgres/cli` — `tursopg`, a psql-like REPL that can also host the server

Because the parser accepts the full PostgreSQL grammar, some clauses parse
and run but their semantics are silently dropped, producing wrong results or
lost information without any error. These are called out as "silently
ignored" in the notes of the tables below.

## Core SQL baseline

Basics not enumerated by the official feature matrix.

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT (projections, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET) | ✅ Supported | |
| JOINs (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, USING) | ✅ Supported | |
| UNION / UNION ALL / INTERSECT / EXCEPT | ✅ Supported | Including ORDER BY/LIMIT on compounds |
| Subqueries (FROM, IN, EXISTS, scalar) | ✅ Supported | `= ANY(array)` and `<> ALL(array)` work, including bound text-array parameters; other ANY/ALL array operators are rejected; `ALL`/row comparison subqueries error |
| INSERT (column lists, multi-row VALUES, DEFAULT, INSERT ... SELECT) | ✅ Supported | |
| UPDATE (incl. FROM clause) | ✅ Supported | Multi-column `SET (a,b) = (...)` not supported |
| DELETE | 🟡 Partial | `USING` clause silently dropped |
| CREATE TABLE | ✅ Supported | PK, NOT NULL, UNIQUE, DEFAULT, CHECK, FK (with ON DELETE/UPDATE actions); IF NOT EXISTS; tables are created STRICT |
| CREATE TABLE AS / SELECT INTO | ✅ Supported | Schema derived from the SELECT; WITH NO DATA supported (lowered to LIMIT 0, so errors in an overridden LIMIT go unreported); explicit column list rejected; INTO on the first leaf of a compound SELECT (legal in PG) rejected; TEMP silently ignored; completes with `SELECT n` like PostgreSQL, though an IF NOT EXISTS skip tags `SELECT 0` instead of `CREATE TABLE AS` |
| ALTER TABLE | 🟡 Partial | ADD/DROP COLUMN, RENAME TABLE/COLUMN work; ALTER COLUMN TYPE translates but fails at execution; SET/DROP DEFAULT, SET/DROP NOT NULL, ADD CONSTRAINT rejected |
| CREATE INDEX | ✅ Supported | UNIQUE, multi-column, partial (WHERE), expression indexes, IF NOT EXISTS |
| CREATE VIEW | ✅ Supported | Column aliases supported; TEMP silently ignored |
| COMMENT ON | 🟡 Partial | Accepted but discarded; comments are not persisted in `pg_description` |
| CREATE SCHEMA / DROP SCHEMA | ✅ Supported | Schemas are ATTACHed databases; DROP ... CASCADE; `public` is special-cased |
| CREATE SEQUENCE / nextval / currval / setval | ✅ Supported | START, INCREMENT, MIN/MAXVALUE, CYCLE; CACHE accepted (no-op); pg_sequences view |
| CREATE DOMAIN | ✅ Supported | Base type + DEFAULT, NOT NULL, CHECK constraints enforced |
| CREATE TYPE ... AS ENUM | ✅ Supported | Values validated on write; other CREATE TYPE forms unsupported |
| TRUNCATE | 🟡 Partial | Lowered to DELETE of the first table only; CASCADE / RESTART IDENTITY dropped |
| BEGIN / COMMIT / ROLLBACK | ✅ Supported | Isolation-level and READ ONLY/WRITE options accepted but ignored |
| Casts (`expr::type`, CAST) | ✅ Supported | Also `int4(x)`-style cast functions |
| Parameters (`$1`, `$2`, ...) | 🟡 Partial | Work through the extended wire protocol; text-format values only |
| Operators: `\|\|`, `%`, bitwise, ILIKE, SIMILAR TO, `~`/`~*`/`!~`/`!~*`, IS [NOT] DISTINCT FROM, BETWEEN | ✅ Supported | Regex operators lower to REGEXP; case-insensitive variants treated as sensitive |
| Dollar-quoted strings, escape strings (`E'...'`), bit/hex string literals | ✅ Supported | |
| generate_series | ✅ Supported | In FROM and with joins; column aliases on the function (`AS g(x)`) do not resolve |
| pg_catalog emulation | 🟡 Partial | See Backend section |
| SET / SHOW | 🟡 Partial | Passed through as PRAGMAs; no PostgreSQL GUCs (e.g. `SHOW search_path` returns nothing) |

## Backend

The pg_catalog tables emulated (live, reflecting real schema): `pg_class`,
`pg_namespace`, `pg_attribute`, `pg_type` (builtin + array + enum types),
`pg_index`, `pg_constraint`, `pg_attrdef`, `pg_tables`, `pg_sequences`,
`pg_database`, `pg_roles` (single hardcoded `turso` role), `pg_proc`, `pg_am`,
plus `pg_input_error_info`. Present but always empty: `pg_policy`,
`pg_trigger`, `pg_statistic_ext`, `pg_inherits`, `pg_rewrite`,
`pg_foreign_table`, `pg_partitioned_table`, `pg_collation`, `pg_description`,
`pg_publication*`. Catalog introspection functions: `format_type`,
`pg_get_constraintdef`, `pg_get_indexdef`, `pg_get_expr` (returns TursoPG's
stored SQL; relation oid and pretty printing do not change the output),
`pg_get_userbyid`, `pg_*_is_visible`, `pg_encoding_to_char`,
`pg_input_is_valid`, `to_char` (numeric), `now`/`clock_timestamp`/
`transaction_timestamp`/`statement_timestamp`.
Session information functions: `version()`, `current_database()` /
`current_catalog`, `current_schema` (call, bare-keyword, and FROM-position
forms), `pg_backend_pid()`, `quote_ident()`, `quote_literal()`;
`obj_description()`/`col_description()` always return NULL because COMMENT ON
is not persisted. DML against pg_catalog is rejected. `pg_typeof` is not
implemented.

| Feature | Status | Notes |
|---------|--------|-------|
| 64-bit large objects | ❌ Not supported | |
| Advisory locks | ❌ Not supported | |
| Custom background workers | ❌ Not supported | |
| Disk based FSM | ❌ Not supported | |
| Dynamic Background Workers | ❌ Not supported | |
| EXPLAIN (BUFFERS) support | ❌ Not supported | EXPLAIN is not supported at all |
| EXPLAIN (MEMORY) | ❌ Not supported | |
| EXPLAIN (SERIALIZE) support | ❌ Not supported | |
| EXPLAIN (WAL) support | ❌ Not supported | |
| "jsonlog" logging format | ❌ Not supported | |
| Loadable plugin infrastructure for monitoring the planner | ❌ Not supported | |
| Payload support for LISTEN/NOTIFY | ❌ Not supported | LISTEN/NOTIFY not supported |
| pg_stat_checkpointer system view | ❌ Not supported | |
| pg_stat_io - I/O metrics view | ❌ Not supported | |
| pg_wait_events system view | ❌ Not supported | |
| Server statistics in shared memory | ❌ Not supported | |
| SQL-standard information schema | ❌ Not supported | Only an `information_schema` row in pg_namespace; no views |
| Support for anonymous shared memory | ❌ Not supported | |
| XML, JSON and YAML output for EXPLAIN | ❌ Not supported | |

## Data Types, Functions, & Operators

Type mapping: serial/smallserial/bigserial (and serial2/4/8) become
`INTEGER NOT NULL DEFAULT nextval(...)` with an implicit sequence. boolean,
smallint, bigint, uuid, date, time, timestamp[tz], bytea, json, jsonb, inet,
cidr, macaddr, macaddr8 map to Turso custom types. varchar(n)/char(n) and
numeric(p,s) keep their type modifiers. interval, xml, tsvector/tsquery,
bit/varbit, geometric types degrade to TEXT; money to REAL; OID/reg* types to
INTEGER. Unknown type names pass through as custom types.

| Feature | Status | Notes |
|---------|--------|-------|
| Arrays of compound types | ❌ Not supported | |
| Array support | ✅ Supported | `type[]` columns, ARRAY[...] literals, subscripts `a[i]`, slices `a[i:j]`, `@>`, `<@`, `&&`, array_length, array_append, array_agg; backed by native Turso arrays |
| ENUM data type | ✅ Supported | CREATE TYPE ... AS ENUM; values validated on write; DROP TYPE [IF EXISTS] |
| GUID/UUID data type | ✅ Supported | uuid columns, gen_random_uuid(), input validation, text casts |
| macaddr8 data type | ✅ Supported | Along with inet, cidr, macaddr (round-trip tested) |
| Multiranges | ❌ Not supported | |
| NULLs in Array | ✅ Supported | `ARRAY[1, NULL, 3]` round-trips |
| Phrase search | ❌ Not supported | tsvector/tsquery degrade to TEXT; no full-text search |
| Range types | ❌ Not supported | |
| smallserial type | ✅ Supported | serial2/serial4/serial8 aliases too; serial implies NOT NULL + implicit sequence, not PRIMARY KEY |
| Type modifier support | ✅ Supported | varchar(n) length enforced ("value too long for varchar"); numeric(p,s) scale applied |
| UUIDv7 | ❌ Not supported | |
| XML data type | ❌ Not supported | xml columns degrade to TEXT; no XML functions |

## Indexing & Constraints

| Feature | Status | Notes |
|---------|--------|-------|
| Block-range (BRIN) indexes | ❌ Not supported | |
| B-tree bottom-up index deletion | ❌ Not supported | |
| B-tree deduplication | ❌ Not supported | |
| Concurrent GiST indexes | ❌ Not supported | |
| Covering Indexes for B-trees (INCLUDE) | ❌ Not supported | INCLUDE clause silently ignored |
| Covering indexes for GiST (INCLUDE) | ❌ Not supported | |
| Deferrable unique constraints | ❌ Not supported | |
| Exclusion constraints | ❌ Not supported | |
| GIN (Generalized Inverted Index) indexes | ❌ Not supported | `USING <method>` silently ignored; every index is a B-tree |
| GIN indexes partial match | ❌ Not supported | |
| GIN index performance and size improvements | ❌ Not supported | |
| GiST (Generalized Search Tree) indexes | ❌ Not supported | |
| Indexes on expressions | ✅ Supported | Partial (WHERE) indexes also supported |
| Index-only scans | ❌ Not supported | |
| Index-only scans on GiST | ❌ Not supported | |
| Index support for IS NULL | ❌ Not supported | |
| In-memory Bitmap Indexes | ❌ Not supported | |
| K-nearest neighbor GiST support | ❌ Not supported | |
| K-nearest neighbor SP-GiST support | ❌ Not supported | |
| Non-blocking CREATE INDEX | ❌ Not supported | CONCURRENTLY silently ignored |
| Parallel B-tree index scans | ❌ Not supported | |
| Parallelized CREATE INDEX for BRIN indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for B-tree indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for GIN indexes | ❌ Not supported | |
| Skip scan on multicolumn B-tree indexes | ❌ Not supported | |
| Space-Partitioned GiST (SP-GiST) indexes | ❌ Not supported | |
| SP-GiST indexes for range types | ❌ Not supported | |
| UNIQUE NULLS NOT DISTINCT | ❌ Not supported | Silently ignored |
| WAL support for hash indexes | ❌ Not supported | |

## SQL

| Feature | Status | Notes |
|---------|--------|-------|
| ANY_VALUE aggregate | ❌ Not supported | |
| DISTINCT ON | ❌ Not supported | Accepted but silently degrades to plain DISTINCT (wrong results) |
| FETCH FIRST .. WITH TIES | ❌ Not supported | `FETCH FIRST n ROWS ONLY` works (lowered to LIMIT); WITH TIES silently ignored |
| GROUPING SETS, CUBE and ROLLUP support | ❌ Not supported | Translation error |
| INSERT/UPDATE/DELETE RETURNING | ✅ Supported | Including `RETURNING *` and UPDATE ... FROM ... RETURNING |
| LATERAL clause | ❌ Not supported | Keyword accepted but silently ignored |
| MERGE | ❌ Not supported | |
| MERGE ... RETURNING | ❌ Not supported | |
| Multirow VALUES | ✅ Supported | In INSERT and as standalone VALUES lists |
| ORDER BY / LIMIT on a standalone VALUES list | ❌ Not supported | Rejected: "ORDER BY clause is not allowed with VALUES clause" |
| Non-decimal integer literals | ✅ Supported | `0x`, `0o`, `0b` |
| ORDER BY NULLS FIRST/LAST | ✅ Supported | Honored in SELECT, window, and compound SELECT ORDER BY; rejected (matching SQLite) in CREATE INDEX |
| range_agg range type aggregation function | ❌ Not supported | |
| Recursive queries | 🟡 Partial | WITH RECURSIVE works with SQLite semantics (row-at-a-time recursive term, so e.g. DISTINCT in the recursive term over a multi-row anchor can differ from PG); SEARCH/CYCLE clauses are rejected |
| regexp_count, regexp_instr, regexp_like | ❌ Not supported | Regex *operators* (`~`, `~*`, SIMILAR TO) work |
| Return OLD and NEW values from modified rows | ❌ Not supported | |
| Row-wise comparison | ❌ Not supported | Row constructors `(a,b) < (c,d)` fail to translate |
| SELECT ... FOR UPDATE/SHARE | ❌ Not supported | Accepted but silently ignored — no locking happens |
| SELECT FOR NO KEY UPDATE/SELECT FOR KEY SHARE lock modes | ❌ Not supported | Accepted but silently ignored — no locking happens |
| SQL standard interval handling | ❌ Not supported | interval degrades to TEXT; no interval arithmetic |
| SYSTEM_USER | ❌ Not supported | current_user/current_role return stub values |
| TABLE statement | ✅ Supported | |
| Underscores (_) for thousands separators | ✅ Supported | |
| unnest/array_agg | 🟡 Partial | array_agg works; unnest is not implemented |
| Upsert (INSERT ... ON CONFLICT DO ...) | ✅ Supported | DO NOTHING and DO UPDATE SET ... (with EXCLUDED and conflict targets) |
| Window functions | 🟡 Partial | Aggregate window functions (COUNT/SUM/AVG/MIN/MAX OVER), row_number, PARTITION BY/ORDER BY, frame clauses, and named WINDOW clauses work; rank, dense_rank, lag, lead, etc. are not implemented |
| WITHIN GROUP clause | 🟡 Partial | Supports mode(), percentile_cont(), and percentile_disc() with one ORDER BY expression; DESC and explicit NULLS ordering are not supported |
| WITH ORDINALITY clause | ❌ Not supported | |
| WITH queries (Common Table Expressions) | ✅ Supported | Including WITH RECURSIVE; MATERIALIZED hints accepted |
| Writable WITH queries (Common Table Expressions) | ❌ Not supported | "CTE query is not a SELECT statement" |

## Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER object IF EXISTS | 🟡 Partial | ALTER TABLE IF EXISTS works when the table exists, but a missing table still errors instead of being skipped |
| ALTER TABLE ... ADD UNIQUE/PRIMARY KEY USING INDEX | ❌ Not supported | ADD CONSTRAINT is rejected |
| ALTER TABLE ... SET ACCESS METHOD | ❌ Not supported | |
| ALTER TABLE ... SET LOGGED / UNLOGGED | ❌ Not supported | |
| Changing column types (ALTER TABLE .. ALTER COLUMN TYPE) | ❌ Not supported | Translates but breaks at execution ("no such column" on subsequent use) |
| CREATE ACCESS METHOD | ❌ Not supported | |
| CREATE TABLE ... (LIKE) with foreign tables, views and composite types | ❌ Not supported | LIKE is silently ignored |
| DROP object IF EXISTS | ✅ Supported | TABLE, INDEX, VIEW, MATERIALIZED VIEW, TYPE, DOMAIN, SEQUENCE, SCHEMA |
| ON COMMIT clause for CREATE TEMPORARY TABLE | ❌ Not supported | TEMP itself is silently ignored |
| REINDEX CONCURRENTLY | ❌ Not supported | REINDEX not supported at all |
| Stored generated columns | ❌ Not supported | GENERATED ... AS silently ignored |
| Temporal constraints | ❌ Not supported | |
| Temporary tables (CREATE TEMP TABLE) | ❌ Not supported | TEMP silently ignored — the table is persistent |
| Typed tables | ❌ Not supported | |
| Virtual generated columns | ❌ Not supported | GENERATED ... AS silently ignored |

## Performance

| Feature | Status | Notes |
|---------|--------|-------|
| Abbreviated keys | ❌ Not supported | |
| Asynchronous commit | ❌ Not supported | |
| Asynchronous I/O (AIO) | ❌ Not supported | |
| Automatic plan invalidation | ❌ Not supported | |
| Background checkpointer | ❌ Not supported | |
| Background writer | ❌ Not supported | |
| Base backup throttling | ❌ Not supported | |
| CREATE STATISTICS - most-common values (MCV) statistics | ❌ Not supported | |
| CREATE STATISTICS - multicolumn | ❌ Not supported | |
| CREATE STATISTICS - "OR" and "IN/ANY" statistics | ❌ Not supported | |
| Cross datatype hashing support | ❌ Not supported | |
| Distributed checkpointing | ❌ Not supported | |
| Foreign keys marked as NOT VALID | ❌ Not supported | |
| Frozen page map | ❌ Not supported | |
| Full text search | ❌ Not supported | tsvector/tsquery degrade to TEXT |
| Hash aggregation can use disk | ❌ Not supported | |
| Hashing support for DISTINCT/UNION/INTERSECT/EXCEPT | ❌ Not supported | |
| Hashing support for FULL OUTER JOIN, LEFT OUTER JOIN and RIGHT OUTER JOIN | ❌ Not supported | |
| Heap Only Tuples (HOT) | ❌ Not supported | |
| Improved performance for sorts exceeding working memory | ❌ Not supported | |
| Improved window function performance | ❌ Not supported | |
| Incremental sort | ❌ Not supported | |
| Incremental sort for SELECT DISTINCT | ❌ Not supported | |
| Incremental sort for window functions | ❌ Not supported | |
| Inlined WITH queries (Common Table Expressions) | ❌ Not supported | |
| Inlining of SQL functions | ❌ Not supported | |
| Just-in-Time (JIT) compilation for expression evaluation and tuple deforming | ❌ Not supported | |
| Load balancing for libpq / psql | ❌ Not supported | |
| LZ4 compression for TOAST tables | ❌ Not supported | |
| Multi-core scalability for read-only workloads | ❌ Not supported | |
| Multiple temporary tablespaces | ❌ Not supported | |
| Outer join reordering | ❌ Not supported | |
| Parallel bitmap heap scans | ❌ Not supported | |
| Parallel FULL and RIGHT joins | ❌ Not supported | |
| Parallel full table scans (sequential scans) | ❌ Not supported | |
| Parallel hash joins | ❌ Not supported | |
| Parallel JOIN, aggregate | ❌ Not supported | |
| Parallel merge joins | ❌ Not supported | |
| Parallel query | ❌ Not supported | |
| Parallel "SELECT DISTINCT" | ❌ Not supported | |
| Partial sort capability (top-n sorting) | ❌ Not supported | |
| Query pipelining | ❌ Not supported | |
| Reduced lock levels for ALTER TABLE commands | ❌ Not supported | |
| SELECT ... FOR UPDATE/SHARE NOWAIT | ❌ Not supported | Silently ignored |
| Set costs specific to TABLESPACEs | ❌ Not supported | |
| Shared row level locking | ❌ Not supported | |
| SIMD support for ARM | ❌ Not supported | |
| SIMD support for x86 | ❌ Not supported | |
| SKIP LOCKED clause | ❌ Not supported | Silently ignored |
| Synchronized sequential scanning | ❌ Not supported | |
| TABLESAMPLE clause | ❌ Not supported | Translation error |
| Tablespaces | ❌ Not supported | |
| Unlogged tables | ❌ Not supported | |
| WAL buffer auto-tuning | ❌ Not supported | |

## JSON

| Feature | Status | Notes |
|---------|--------|-------|
| Improved set of JSON functions and operators | ❌ Not supported | |
| JSONB data type | 🟡 Partial | jsonb columns supported (Turso custom type); `->` and `->>` operators work; `#>`, `#>>`, `?`, `?&`, `?\|`, `@@` fail to translate |
| JSONB-modifying operators and functions | ❌ Not supported | |
| JSONB subscripting | ❌ Not supported | |
| JSON data type | 🟡 Partial | Same as jsonb (see above) |
| SQL/JSON constructors | ❌ Not supported | |
| SQL/JSON: datetime() | ❌ Not supported | |
| SQL/JSON IS JSON | ❌ Not supported | |
| SQL/JSON JSON_TABLE | ❌ Not supported | |
| SQL/JSON path expressions | ❌ Not supported | |
| SQL/JSON query functions | ❌ Not supported | |

## Partitioning & Inheritance

| Feature | Status | Notes |
|---------|--------|-------|
| Accelerated partition pruning | ❌ Not supported | |
| Declarative table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF silently dropped — the table is created unpartitioned |
| Default partition | ❌ Not supported | |
| Foreign key references for partitioned tables | ❌ Not supported | |
| Foreign table inheritance | ❌ Not supported | |
| Partitioning by a hash key | ❌ Not supported | |
| Partition pruning during query execution | ❌ Not supported | |
| Support for PRIMARY KEY, FOREIGN KEY, indexes, and triggers on partitioned tables | ❌ Not supported | |
| Table inheritance (INHERITS) | ❌ Not supported | INHERITS silently dropped |
| Table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF silently dropped |
| UPDATE on a partition key | ❌ Not supported | |

## Views & Materialized Views

| Feature | Status | Notes |
|---------|--------|-------|
| Materialized views | ✅ Supported | Incrementally maintained (live, DBSP-based) — always fresh, unlike PostgreSQL snapshots; REFRESH MATERIALIZED VIEW is accepted as a no-op |
| Materialized views with concurrent refresh | 🟡 Partial | Moot: views are always fresh; REFRESH (CONCURRENTLY) is a no-op |
| SECURITY INVOKER views | ❌ Not supported | |
| Temporary VIEWs | ❌ Not supported | TEMP silently ignored; the view is persistent |
| Updatable views | ❌ Not supported | |
| WITH CHECK clause | ❌ Not supported | |

## Replication

Replication is not supported.

## Backup, Restore, & Data Integrity

Backup and restore is not supported.

## Upgrade

Upgrade is not supported.

## Data Import & Export

| Feature | Status | Notes |
|---------|--------|-------|
| COPY table FROM 'file' (text format) | ✅ Supported | DELIMITER, NULL string, HEADER, column lists, backslash escapes, `\.` end-of-data marker; atomic (rolls back on malformed rows) |
| COPY from/to STDIN/STDOUT | ❌ Not supported | Rejected with an error; no wire-level COPY sub-protocol |
| COPY FROM ... WHERE | ❌ Not supported | |
| COPY ... ON_ERROR | ❌ Not supported | |
| COPY with arbitrary SELECT | ❌ Not supported | COPY TO is not supported at all |
| CSV support for COPY | ❌ Not supported | `FORMAT csv` and `FORMAT binary` rejected with an error |

## Configuration Management

| Feature | Status | Notes |
|---------|--------|-------|
| SET / SHOW configuration parameters | 🟡 Partial | Passed through as PRAGMAs; PostgreSQL GUCs (search_path, work_mem, ...) are not recognized |
| ALTER SYSTEM | ❌ Not supported | |
| Fractional input for "integer" values | ❌ Not supported | |
| Include directives for pg_hba.conf and pg_ident.conf | ❌ Not supported | |
| Per user/database server configuration settings | ❌ Not supported | |
| pg_config system view | ❌ Not supported | |
| Regular expression matching in pg_hba.conf and pg_ident.conf | ❌ Not supported | |

## Security

| Feature | Status | Notes |
|---------|--------|-------|
| Authentication (any method) | ❌ Not supported | The wire server trusts every connection; no password, MD5, SCRAM, or certificate checks |
| Channel binding for SCRAM authentication | ❌ Not supported | |
| Client can require SCRAM channel binding | ❌ Not supported | |
| Client-specified requirements for authentication | ❌ Not supported | |
| Column level permissions | ❌ Not supported | |
| Default permissions | ❌ Not supported | |
| Direct TLS negotiation ("sslnegotiation") | ❌ Not supported | |
| FIPS mode validation | ❌ Not supported | |
| GRANT/REVOKE ON ALL TABLES/SEQUENCES/FUNCTIONS | ❌ Not supported | GRANT/REVOKE not supported at all |
| GSSAPI client and server-side encryption | ❌ Not supported | |
| GSSAPI support | ❌ Not supported | |
| Kerberos credential delegation | ❌ Not supported | |
| krb5 authentication (without gssapi) | ❌ Not supported | |
| Large object access controls | ❌ Not supported | |
| LDAP server discovery | ❌ Not supported | |
| Multifactor authentication via valid client SSL/TLS certificate | ❌ Not supported | |
| Native LDAP authentication | ❌ Not supported | |
| Native RADIUS authentication | ❌ Not supported | |
| OAuth authentication / authorization | ❌ Not supported | |
| Per user/database connection limits | ❌ Not supported | |
| Predefined roles | ❌ Not supported | |
| Privileges for setting configuration parameters | ❌ Not supported | |
| ROLES | ❌ Not supported | pg_roles exposes a single hardcoded `turso` role |
| Row-level security | ❌ Not supported | |
| SCRAM-SHA-256 authentication | ❌ Not supported | |
| Search+bind mode operation for LDAP authentication | ❌ Not supported | |
| security_barrier option on views | ❌ Not supported | |
| Security Service Provider Interface (SSPI) | ❌ Not supported | |
| SHA-2 encryption for password hashing | ❌ Not supported | |
| SSL certificate validation in libpq | ❌ Not supported | |
| SSL client certificate authentication | ❌ Not supported | |
| SSPI authentication via GSSAPI | ❌ Not supported | |
| Support using the client's OS trusted CA | ❌ Not supported | |
| TLS v1.3 cipher suite allowlisting | ❌ Not supported | |

## Transactions and Visibility

| Feature | Status | Notes |
|---------|--------|-------|
| Cursors | ❌ Not supported | DECLARE/FETCH/MOVE not translated |
| Savepoints | ✅ Supported | SAVEPOINT, RELEASE, ROLLBACK TO |
| Serializable Snapshot Isolation | ❌ Not supported | BEGIN ISOLATION LEVEL ... accepted but the level is ignored |
| Two-phase commit | ❌ Not supported | PREPARE TRANSACTION rejected |
| Updatable cursors | ❌ Not supported | |

## VACUUM and Maintenance

| Feature | Status | Notes |
|---------|--------|-------|
| VACUUM / ANALYZE statements | ❌ Not supported | Rejected with an error |
| Inserted data can trigger autovacuum | ❌ Not supported | |
| Integrated autovacuum daemon | ❌ Not supported | |
| Page freezing optimizations | ❌ Not supported | |
| Parallelized VACUUM for indexes | ❌ Not supported | |
| Parallel vacuumdb jobs | ❌ Not supported | |
| Radix tree memory structure for vacuum | ❌ Not supported | |
| Vacuum "emergency mode" | ❌ Not supported | |
| Visibility map for vacuuming | ❌ Not supported | |

## Foreign Data Wrappers

| Feature | Status | Notes |
|---------|--------|-------|
| Certificate authentication with postgres_fdw | ❌ Not supported | |
| CREATE FOREIGN TABLE ... LIKE | ❌ Not supported | |
| Foreign data wrapper query parallelism | ❌ Not supported | |
| Foreign data wrappers | ❌ Not supported | |
| Foreign tables | ❌ Not supported | |
| IMPORT FOREIGN SCHEMA | ❌ Not supported | |
| Import foreign table partitions | ❌ Not supported | |
| Parallel query execution on remote databases | ❌ Not supported | |
| postgres_fdw parallel commit | ❌ Not supported | |
| postgres_fdw pushdown | ❌ Not supported | |
| postgres_fdw SCRAM authentication passthrough | ❌ Not supported | |
| PostgreSQL Foreign Data Wrapper | ❌ Not supported | |
| Writable Foreign Data Wrappers | ❌ Not supported | |

## Custom Functions, Stored Procedures, & Triggers

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER TABLE ENABLE/DISABLE TRIGGER | ❌ Not supported | |
| ALTER TABLE / ENABLE REPLICA TRIGGER/RULE | ❌ Not supported | |
| BEGIN ATOMIC function bodies | ❌ Not supported | |
| CALL syntax for executing procedures | ❌ Not supported | |
| CREATE FUNCTION | ❌ Not supported | Rejected with an error |
| Column level triggers | ❌ Not supported | |
| CREATE PROCEDURE syntax for SQL stored procedures | ❌ Not supported | |
| Event triggers | ❌ Not supported | |
| FILTER clause for aggregate functions | ✅ Supported | |
| ORDER BY support within aggregates | ❌ Not supported | Silently dropped |
| Per function GUC settings | ❌ Not supported | |
| Per function statistics | ❌ Not supported | |
| RETURN QUERY EXECUTE | ❌ Not supported | |
| RETURNS TABLE | ❌ Not supported | |
| Statement level triggers | ❌ Not supported | CREATE TRIGGER not translated |
| Statement level TRUNCATE triggers | ❌ Not supported | |
| Triggers on views | ❌ Not supported | |
| Variadic functions | ❌ Not supported | |
| WHEN clause for CREATE TRIGGER | ❌ Not supported | |

## Procedural Languages

| Feature | Status | Notes |
|---------|--------|-------|
| Procedural languages (PL/pgSQL, PL/Python, ...) | ❌ Not supported | CREATE FUNCTION and DO are rejected with an error |
| CASE in pl/pgsql | ❌ Not supported | |
| CONTINUE statement for PL/pgSQL | ❌ Not supported | |
| CREATE TRANSFORM | ❌ Not supported | |
| DO statement for pl/perl | ❌ Not supported | |
| DO statement for pl/pgsql | ❌ Not supported | |
| EXCEPTION support in PL/pgSQL | ❌ Not supported | |
| EXECUTE USING in PL/pgSQL | ❌ Not supported | |
| FOREACH IN ARRAY in pl/pgsql | ❌ Not supported | |
| IN/OUT/INOUT parameters for pl/pgsql and PL/SQL | ❌ Not supported | |
| Named parameters | ❌ Not supported | |
| Non-superuser language creation | ❌ Not supported | |
| pl/pgsql installed by default | ❌ Not supported | |
| Polymorphic functions | ❌ Not supported | |
| Python 3 support for pl/python | ❌ Not supported | |
| Qualified function parameters | ❌ Not supported | |
| Query parallelism for RETURN QUERY | ❌ Not supported | |
| RETURN QUERY in pl/pgsql | ❌ Not supported | |
| ROWS and COST specification for functions | ❌ Not supported | |
| Scrollable and updatable cursor support for pl/pgsql | ❌ Not supported | |
| SQLERRM/SQLSTATE for pl/pgsql | ❌ Not supported | |
| Unicode object support in PL/python | ❌ Not supported | |
| User defined exceptions | ❌ Not supported | |
| Validator function for pl/perl | ❌ Not supported | |

## Extensions

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE EXTENSION .. CASCADE | ❌ Not supported | |
| Extension installation | ❌ Not supported | CREATE EXTENSION not translated |
| Trusted extensions | ❌ Not supported | |

## Internationalisation

| Feature | Status | Notes |
|---------|--------|-------|
| Built-in, platform independent immutable collation | ❌ Not supported | |
| casefold | ❌ Not supported | |
| Column-level collation support | ❌ Not supported | COLLATE clauses silently stripped |
| Database level collation | ❌ Not supported | |
| Default ICU collations for clusters/databases | ❌ Not supported | |
| EUC_JIS_2004 / SHIFT_JIS_2004 support | ❌ Not supported | |
| ICU collations | ❌ Not supported | |
| LIKE comparisons for nondeterministic collations | ❌ Not supported | |
| Multibyte encoding support, incl. UTF8 | 🟡 Partial | UTF-8 only (Turso native encoding); no other server encodings |
| Multiple language support | ❌ Not supported | |
| Nondeterministic collations | ❌ Not supported | |
| pg_unicode_fast collation | ❌ Not supported | |
| Unicode string literals and identifiers | ❌ Not supported | |
| UTF8 support on Windows | ❌ Not supported | |

## Client Applications

| Feature | Status | Notes |
|---------|--------|-------|
| psql-style REPL | 🟡 Partial | `tursopg` (not psql itself); supports `\d[+]`, `\dt[+]`, `\di`, `\dv`, `\dn`, `\dT`, `\du`/`\dg`, `\df`, `\l`, `\x`, `\timing`, `\echo`, `\conninfo`, `\?`, `\q`, and interactive `quit`/`exit`; no `\copy`, `\i`, `\e`, `\set`, `\pset`, `\g`, `\watch` |
| pgbench | ❌ Not supported | |
| pg_combinebackup | ❌ Not supported | |
| pg_createsubscriber | ❌ Not supported | |
| pg_prewarm | ❌ Not supported | |
| pg_rewind | ❌ Not supported | |
| pg_standby | ❌ Not supported | |
| pg_upgrade | ❌ Not supported | |
| pg_waldump | ❌ Not supported | |
| pg_walsummary | ❌ Not supported | |
| pg_xlogdump | ❌ Not supported | |
| psql \bind | ❌ Not supported | |
| psql \dconfig | ❌ Not supported | |
| psql pipeline queries | ❌ Not supported | |
| psql named prepared statements | ❌ Not supported | SQL-level PREPARE/EXECUTE/DEALLOCATE are not translated |
| Version aware psql | ❌ Not supported | |

## Additional Modules (contrib)

| Feature | Status | Notes |
|---------|--------|-------|
| adminpack | ❌ Not supported | |
| auth_delay | ❌ Not supported | |
| auto_explain | ❌ Not supported | |
| btree_gin | ❌ Not supported | |
| btree_gist | ❌ Not supported | |
| citext | ❌ Not supported | |
| dblink | ❌ Not supported | |
| dblink asynchronous notification support | ❌ Not supported | |
| file_fdw | ❌ Not supported | |
| fuzzystrmatch | ❌ Not supported | |
| hstore | ❌ Not supported | |
| intarray | ❌ Not supported | |
| isn (ISBN) | ❌ Not supported | |
| KNN support for CUBE | ❌ Not supported | |
| ltree | ❌ Not supported | |
| pageinspect | ❌ Not supported | |
| passwordcheck | ❌ Not supported | |
| pg_buffercache | ❌ Not supported | |
| pg_freespacemap | ❌ Not supported | |
| pg_logicalinspect | ❌ Not supported | |
| pg_overexplain | ❌ Not supported | |
| pg_stat_statements | ❌ Not supported | |
| pgstattuple | ❌ Not supported | |
| pg_trgm | ❌ Not supported | |
| pg_trgm regular expressions indexing | ❌ Not supported | |
| pg_walinspect | ❌ Not supported | |
| seg | ❌ Not supported | |
| sepgsql | ❌ Not supported | |
| sslinfo | ❌ Not supported | |
| tablefunc | ❌ Not supported | |
| tcn | ❌ Not supported | |
| tsearch2 compatibility wrapper | ❌ Not supported | |
| unaccent | ❌ Not supported | |
| uuid-ossp | ❌ Not supported | gen_random_uuid() (core) is available |
| xml2 | ❌ Not supported | |

## Network

| Feature | Status | Notes |
|---------|--------|-------|
| Full SSL support | ❌ Not supported | SSLRequest answered with "SSL not available"; plaintext only |
| IPv6 support | 🟡 Partial | Server binds whatever address it is given, including IPv6 literals; no dual-stack handling |
| V2 client protocol | ❌ Not supported | |
| V3 client protocol | 🟡 Partial | Via pgwire: simple query and extended query (Parse/Bind/Execute/Describe/Sync) protocols; trust auth only; parameter values must be text-format; Execute row limits ignored (no portal suspension); no COPY sub-protocol, CancelRequest, or NotificationResponse |

## Platforms

Not applicable: the Turso Postgres frontend is Rust and builds on every
platform Turso supports; PostgreSQL's platform/compiler feature entries do
not carry over.
