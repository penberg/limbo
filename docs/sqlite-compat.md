# SQLite Compatibility

This document describes the SQLite compatibility status of Limbo:

* [SQL statements](#sql-statements)
* [SQL functions](#sql-functions)
* [SQLite API](#sqlite-api)

## SQL statements

| Statement                    | Status  | Comment |
|------------------------------|---------|---------|
| ALTER TABLE                  | No      |         |
| ANALYZE                      | No      |         |
| ATTACH DATABASE              | No      |         |
| BEGIN TRANSACTION            | No      |         |
| COMMIT TRANSACTION           | No      |         |
| CREATE INDEX                 | No      |         |
| CREATE TABLE                 | No      |         |
| CREATE TRIGGER               | No      |         |
| CREATE VIEW                  | No      |         |
| CREATE VIRTUAL TABLE         | No      |         |
| DELETE                       | No      |         |
| DETACH DATABASE              | No      |         |
| DROP INDEX                   | No      |         |
| DROP TABLE                   | No      |         |
| DROP TRIGGER                 | No      |         |
| DROP VIEW                    | No      |         |
| END TRANSACTION              | No      |         |
| EXPLAIN                      | No      |         |
| INDEXED BY                   | No      |         |
| INSERT                       | No      |         |
| ON CONFLICT clause           | No      |         |
| PRAGMA                       | Partial |         |
| PRAGMA cache_size            | Yes     |         |
| REINDEX                      | No      |         |
| RELEASE SAVEPOINT            | No      |         |
| REPLACE                      | No      |         |
| RETURNING clause             | No      |         |
| ROLLBACK TRANSACTION         | No      |         |
| SAVEPOINT                    | No      |         |
| SELECT                       | Partial |         |
| SELECT ... WHERE             | No      |         |
| SELECT ... LIMIT             | Yes     |         |
| SELECT ... ORDER BY          | No      |         |
| SELECT ... GROUP BY          | No      |         |
| SELECT ... JOIN              | No      |         |
| UPDATE                       | No      |         |
| UPSERT                       | No      |         |
| VACUUM                       | No      |         |
| WITH clause                  | No      |         |

## SQL functions

### Scalar functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| abs(X)                       | No      |         |
| changes()                    | No      |         |
| char(X1,X2,...,XN)           | No      |         |
| coalesce(X,Y,...)            | No      |         |
| concat(X,...)                | No      |         |
| concat_ws(SEP,X,...)         | No      |         |
| format(FORMAT,...)           | No      |         |
| glob(X,Y)                    | No      |         |
| hex(X)                       | No      |         |
| ifnull(X,Y)                  | No      |         |
| iif(X,Y,Z)                   | No      |         |
| instr(X,Y)                   | No      |         |
| last_insert_rowid()          | No      |         |
| length(X)                    | No      |         |
| like(X,Y)                    | No      |         |
| like(X,Y,Z)                  | No      |         |
| likelihood(X,Y)              | No      |         |
| likely(X)                    | No      |         |
| load_extension(X)            | No      |         |
| load_extension(X,Y)          | No      |         |
| lower(X)                     | No      |         |
| ltrim(X)                     | No      |         |
| ltrim(X,Y)                   | No      |         |
| max(X,Y,...)                 | No      |         |
| min(X,Y,...)                 | No      |         |
| nullif(X,Y)                  | No      |         |
| octet_length(X)              | No      |         |
| printf(FORMAT,...)           | No      |         |
| quote(X)                     | No      |         |
| random()                     | No      |         |
| randomblob(N)                | No      |         |
| replace(X,Y,Z)               | No      |         |
| round(X)                     | No      |         |
| round(X,Y)                   | No      |         |
| rtrim(X)                     | No      |         |
| rtrim(X,Y)                   | No      |         |
| sign(X)                      | No      |         |
| soundex(X)                   | No      |         |
| sqlite_compileoption_get(N)  | No      |         |
| sqlite_compileoption_used(X) | No      |         |
| sqlite_offset(X)             | No      |         |
| sqlite_source_id()           | No      |         |
| sqlite_version()             | No      |         |
| substr(X,Y,Z)                | No      |         |
| substr(X,Y)                  | No      |         |
| substring(X,Y,Z)             | No      |         |
| substring(X,Y)               | No      |         |
| total_changes()              | No      |         |
| trim(X)                      | No      |         |
| trim(X,Y)                    | No      |         |
| typeof(X)                    | No      |         |
| unhex(X)                     | No      |         |
| unhex(X,Y)                   | No      |         |
| unicode(X)                   | No      |         |
| unlikely(X)                  | No      |         |
| upper(X)                     | No      |         |
| zeroblob(N)                  | No      |         |

### Aggregate functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| avg(X)                       | Yes     |         |
| count()                      | No      |         |
| count(*)                     | No      |         |
| group_concat(X)              | No      |         |
| group_concat(X,Y)            | No      |         |
| string_agg(X,Y)              | No      |         |
| max(X)                       | No      |         |
| min(X)                       | No      |         |
| sum(X)                       | Yes     |         |
| total(X)                     | No      |         |


### Date and time functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| date()                       | No      |         |
| time()                       | No      |         |
| datetime()                   | No      |         |
| julianday()                  | No      |         |
| unixepoch()                  | No      |         |
| strftime()                   | No      |         |
| timediff()                   | No      |         |


## SQLite API

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_open                 | Partial |         |
| sqlite3_close                | Yes     |         |
| sqlite3_prepare              | Partial |         |
| sqlite3_finalize             | Yes     |         |
| sqlite3_step                 | Yes     |         |
| sqlite3_column_text          | Yes     |         |
