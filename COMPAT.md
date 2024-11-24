# SQLite Compatibility

This document describes the SQLite compatibility status of Limbo:

- [SQLite Compatibility](#sqlite-compatibility)
  - [Limitations](#limitations)
  - [SQL statements](#sql-statements)
    - [SELECT Expressions](#select-expressions)
  - [SQL functions](#sql-functions)
    - [Scalar functions](#scalar-functions)
    - [Aggregate functions](#aggregate-functions)
    - [Date and time functions](#date-and-time-functions)
    - [JSON functions](#json-functions)
  - [SQLite API](#sqlite-api)
  - [SQLite VDBE opcodes](#sqlite-vdbe-opcodes)

## Limitations

* Limbo does not support database access from multiple processes.

## SQL statements

| Statement                 | Status  | Comment |
| ------------------------- | ------- | ------- |
| ALTER TABLE               | No      |         |
| ANALYZE                   | No      |         |
| ATTACH DATABASE           | No      |         |
| BEGIN TRANSACTION         | No      |         |
| COMMIT TRANSACTION        | No      |         |
| CREATE INDEX              | No      |         |
| CREATE TABLE              | Partial |         |
| CREATE TRIGGER            | No      |         |
| CREATE VIEW               | No      |         |
| CREATE VIRTUAL TABLE      | No      |         |
| DELETE                    | No      |         |
| DETACH DATABASE           | No      |         |
| DROP INDEX                | No      |         |
| DROP TABLE                | No      |         |
| DROP TRIGGER              | No      |         |
| DROP VIEW                 | No      |         |
| END TRANSACTION           | No      |         |
| EXPLAIN                   | Yes     |         |
| INDEXED BY                | No      |         |
| INSERT                    | Partial |         |
| ON CONFLICT clause        | No      |         |
| PRAGMA                    | Partial |         |
| PRAGMA cache_size         | Yes     |         |
| REINDEX                   | No      |         |
| RELEASE SAVEPOINT         | No      |         |
| REPLACE                   | No      |         |
| RETURNING clause          | No      |         |
| ROLLBACK TRANSACTION      | No      |         |
| SAVEPOINT                 | No      |         |
| SELECT                    | Yes     |         |
| SELECT ... WHERE          | Yes     |         |
| SELECT ... WHERE ... LIKE | Yes     |         |
| SELECT ... LIMIT          | Yes     |         |
| SELECT ... ORDER BY       | Yes     |         |
| SELECT ... GROUP BY       | Yes     |         |
| SELECT ... HAVING         | Yes     |         |
| SELECT ... JOIN           | Yes     |         |
| SELECT ... CROSS JOIN     | Yes     | SQLite CROSS JOIN means "do not reorder joins". We don't support that yet anyway. |
| SELECT ... INNER JOIN     | Yes     |         |
| SELECT ... OUTER JOIN     | Partial | no RIGHT JOIN |
| SELECT ... JOIN USING     | Yes     |         |
| SELECT ... NATURAL JOIN   | Yes     |         |
| UPDATE                    | No      |         |
| UPSERT                    | No      |         |
| VACUUM                    | No      |         |
| WITH clause               | No      |         |

### SELECT Expressions

Feature support of [sqlite expr syntax](https://www.sqlite.org/lang_expr.html).

| Syntax                       | Status  | Comment |
|------------------------------|---------|---------|
| literals                     | Yes     |         |
| schema.table.column          | Partial | Schemas aren't supported |
| unary operator               | Yes     | |
| binary operator              | Partial | Only `%`, `!<`, and `!>` are unsupported |
| agg() FILTER (WHERE ...)     | No      | Is incorrectly ignored |
| ... OVER (...)               | No      | Is incorrectly ignored |
| (expr)                       | Yes     |         |
| CAST (expr AS type)          | Yes     |         |
| COLLATE                      | No      |         |
| (NOT) LIKE                   | No      |         |
| (NOT) GLOB                   | No      |         |
| (NOT) REGEXP                 | No      |         |
| (NOT) MATCH                  | No      |         |
| IS (NOT)                     | No      |         |
| IS (NOT) DISTINCT FROM       | No      |         |
| (NOT) BETWEEN ... AND ...    | No      |         |
| (NOT) IN (subquery)          | No      |         |
| (NOT) EXISTS (subquery)      | No      |         |
| CASE WHEN THEN ELSE END      | Yes     |         |
| RAISE                        | No      |         |

## SQL functions

### Scalar functions

| Function                     | Status | Comment |
|------------------------------|--------|---------|
| abs(X)                       | Yes    |         |
| changes()                    | No     |         |
| char(X1,X2,...,XN)           | Yes    |         |
| coalesce(X,Y,...)            | Yes    |         |
| concat(X,...)                | Yes    |         |
| concat_ws(SEP,X,...)         | Yes    |         |
| format(FORMAT,...)           | No     |         |
| glob(X,Y)                    | Yes    |         |
| hex(X)                       | Yes    |         |
| ifnull(X,Y)                  | Yes    |         |
| iif(X,Y,Z)                   | Yes    |         |
| instr(X,Y)                   | Yes    |         |
| last_insert_rowid()          | Yes    |         |
| length(X)                    | Yes    |         |
| like(X,Y)                    | No     |         |
| like(X,Y,Z)                  | No     |         |
| likelihood(X,Y)              | No     |         |
| likely(X)                    | No     |         |
| load_extension(X)            | No     |         |
| load_extension(X,Y)          | No     |         |
| lower(X)                     | Yes    |         |
| ltrim(X)                     | Yes    |         |
| ltrim(X,Y)                   | Yes    |         |
| max(X,Y,...)                 | Yes    |         |
| min(X,Y,...)                 | Yes    |         |
| nullif(X,Y)                  | Yes    |         |
| octet_length(X)              | Yes    |         |
| printf(FORMAT,...)           | No     |         |
| quote(X)                     | Yes    |         |
| random()                     | Yes    |         |
| randomblob(N)                | Yes    |         |
| replace(X,Y,Z)               | Yes    |         |
| round(X)                     | Yes    |         |
| round(X,Y)                   | Yes    |         |
| rtrim(X)                     | Yes    |         |
| rtrim(X,Y)                   | Yes    |         |
| sign(X)                      | Yes    |         |
| soundex(X)                   | Yes    |         |
| sqlite_compileoption_get(N)  | No     |         |
| sqlite_compileoption_used(X) | No     |         |
| sqlite_offset(X)             | No     |         |
| sqlite_source_id()           | No     |         |
| sqlite_version()             | Yes    |         |
| substr(X,Y,Z)                | Yes    |         |
| substr(X,Y)                  | Yes    |         |
| substring(X,Y,Z)             | Yes    |         |
| substring(X,Y)               | Yes    |         |
| total_changes()              | No     |         |
| trim(X)                      | Yes    |         |
| trim(X,Y)                    | Yes    |         |
| typeof(X)                    | Yes    |         |
| unhex(X)                     | Yes    |         |
| unhex(X,Y)                   | Yes    |         |
| unicode(X)                   | Yes    |         |
| unlikely(X)                  | No     |         |
| upper(X)                     | Yes    |         |
| zeroblob(N)                  | Yes    |         |

### Mathematical functions

| Function   | Status | Comment |
| ---------- | ------ | ------- |
| acos(X)    | No     |         |
| acosh(X)   | No     |         |
| asin(X)    | No     |         |
| asinh(X)   | No     |         |
| atan(X)    | No     |         |
| atan2(Y,X) | No     |         |
| atanh(X)   | No     |         |
| ceil(X)    | No     |         |
| ceiling(X) | No     |         |
| cos(X)     | No     |         |
| cosh(X)    | No     |         |
| degrees(X) | No     |         |
| exp(X)     | No     |         |
| floor(X)   | No     |         |
| ln(X)      | No     |         |
| log(B,X)   | No     |         |
| log(X)     | No     |         |
| log10(X)   | No     |         |
| log2(X)    | No     |         |
| mod(X,Y)   | No     |         |
| pi()       | No     |         |
| pow(X,Y)   | No     |         |
| power(X,Y) | No     |         |
| radians(X) | No     |         |
| sin(X)     | No     |         |
| sinh(X)    | No     |         |
| sqrt(X)    | No     |         |
| tan(X)     | No     |         |
| tanh(X)    | No     |         |
| trunc(X)   | No     |         |

### Aggregate functions

| Function                     | Status  | Comment |
|------------------------------|---------|---------|
| avg(X)                       | Yes     |         |
| count()                      | Yes     |         |
| count(*)                     | Yes     |         |
| group_concat(X)              | Yes     |         |
| group_concat(X,Y)            | Yes     |         |
| string_agg(X,Y)              | Yes     |         |
| max(X)                       | Yes     |         |
| min(X)                       | Yes     |         |
| sum(X)                       | Yes     |         |
| total(X)                     | Yes     |         |

### Date and time functions

| Function    | Status  | Comment                      |
|-------------|---------|------------------------------|
| date()      | Yes     | partially supports modifiers |
| time()      | Yes     | partially supports modifiers |
| datetime()  | No      |                              |
| julianday() | No      |                              |
| unixepoch() | Partial | does not support modifiers   |
| strftime()  | No      |                              |
| timediff()  | No      |                              |

### JSON functions

| Function                           | Status  | Comment |
|------------------------------------|---------|---------|
| json(json)                         | Partial |         |
| jsonb(json)                        |         |         |
| json_array(value1,value2,...)      |         |         |
| jsonb_array(value1,value2,...)     |         |         |
| json_array_length(json)            |         |         |
| json_array_length(json,path)       |         |         |
| json_error_position(json)          |         |         |
| json_extract(json,path,...)        |         |         |
| jsonb_extract(json,path,...)       |         |         |
| json -> path                       |         |         |
| json ->> path                      |         |         |
| json_insert(json,path,value,...)   |         |         |
| jsonb_insert(json,path,value,...)  |         |         |
| json_object(label1,value1,...)     |         |         |
| jsonb_object(label1,value1,...)    |         |         |
| json_patch(json1,json2)            |         |         |
| jsonb_patch(json1,json2)           |         |         |
| json_pretty(json)                  |         |         |
| json_remove(json,path,...)         |         |         |
| jsonb_remove(json,path,...)        |         |         |
| json_replace(json,path,value,...)  |         |         |
| jsonb_replace(json,path,value,...) |         |         |
| json_set(json,path,value,...)      |         |         |
| jsonb_set(json,path,value,...)     |         |         |
| json_type(json)                    |         |         |
| json_type(json,path)               |         |         |
| json_valid(json)                   |         |         |
| json_valid(json,flags)             |         |         |
| json_quote(value)                  |         |         |
| json_group_array(value)            |         |         |
| jsonb_group_array(value)           |         |         |
| json_group_object(label,value)     |         |         |
| jsonb_group_object(name,value)     |         |         |
| json_each(json)                    |         |         |
| json_each(json,path)               |         |         |
| json_tree(json)                    |         |         |
| json_tree(json,path)               |         |         |

## SQLite API

| Interface                    | Status  | Comment |
|------------------------------|---------|---------|
| sqlite3_open                 | Partial |         |
| sqlite3_close                | Yes     |         |
| sqlite3_prepare              | Partial |         |
| sqlite3_finalize             | Yes     |         |
| sqlite3_step                 | Yes     |         |
| sqlite3_column_text          | Yes     |         |


## SQLite VDBE opcodes

| Opcode          | Status |
|-----------------|--------|
| Add             | Yes    |
| AddImm          | No     |
| Affinity        | No     |
| AggFinal        | Yes    |
| AggStep         | Yes    |
| AggStep         | Yes    |
| And             | No     |
| AutoCommit      | No     |
| BitAnd          | Yes    |
| BitNot          | Yes    |
| BitOr           | Yes    |
| Blob            | Yes    |
| Checkpoint      | No     |
| Clear           | No     |
| Close           | No     |
| CollSeq         | No     |
| Column          | Yes    |
| Compare         | Yes    |
| Concat          | No     |
| Copy            | Yes    |
| Count           | No     |
| CreateIndex     | No     |
| CreateTable     | No     |
| DecrJumpZero    | Yes    |
| Delete          | No     |
| Destroy         | No     |
| Divide          | Yes    |
| DropIndex       | No     |
| DropTable       | No     |
| DropTrigger     | No     |
| EndCoroutine    | Yes    |
| Eq              | Yes    |
| Expire          | No     |
| Explain         | No     |
| FkCounter       | No     |
| FkIfZero        | No     |
| Found           | No     |
| Function        | Yes    |
| Ge              | Yes    |
| Gosub           | Yes    |
| Goto            | Yes    |
| Gt              | Yes    |
| Halt            | Yes    |
| HaltIfNull      | No     |
| IdxDelete       | No     |
| IdxGE           | Yes    |
| IdxInsert       | No     |
| IdxLT           | No     |
| IdxRowid        | No     |
| If              | Yes    |
| IfNeg           | No     |
| IfNot           | Yes    |
| IfPos           | Yes    |
| IfZero          | No     |
| IncrVacuum      | No     |
| Init            | Yes    |
| InitCoroutine   | Yes    |
| Insert          | No     |
| InsertAsync     | Yes    |
| InsertAwait     | Yes    |
| InsertInt       | No     |
| Int64           | No     |
| Integer         | Yes    |
| IntegrityCk     | No     |
| IsNull          | Yes    |
| IsUnique        | No     |
| JournalMode     | No     |
| Jump            | Yes    |
| Last            | No     |
| Le              | Yes    |
| LoadAnalysis    | No     |
| Lt              | Yes    |
| MakeRecord      | Yes    |
| MaxPgcnt        | No     |
| MemMax          | No     |
| Move            | No     |
| Multiply        | Yes    |
| MustBeInt       | Yes    |
| Ne              | Yes    |
| NewRowid        | Yes    |
| Next            | No     |
| NextAsync       | Yes    |
| NextAwait       | Yes    |
| Noop            | No     |
| Not             | No     |
| NotExists       | Yes    |
| NotFound        | No     |
| NotNull         | Yes    |
| Null            | Yes    |
| NullRow         | Yes    |
| Once            | No     |
| OpenAutoindex   | No     |
| OpenEphemeral   | No     |
| OpenPseudo      | Yes    |
| OpenRead        | Yes    |
| OpenReadAsync   | Yes    |
| OpenWrite       | No     |
| OpenWriteAsync  | Yes    |
| OpenWriteAwait  | Yes    |
| Or              | No     |
| Pagecount       | No     |
| Param           | No     |
| ParseSchema     | No     |
| Permutation     | No     |
| Prev            | No     |
| PrevAsync       | Yes    |
| PrevAwait       | Yes    |
| Program         | No     |
| ReadCookie      | No     |
| Real            | Yes    |
| RealAffinity    | Yes    |
| Remainder       | No     |
| ResetCount      | No     |
| ResultRow       | Yes    |
| Return          | Yes    |
| Rewind          | Yes    |
| RewindAsync     | Yes    |
| RewindAwait     | Yes    |
| RowData         | No     |
| RowId           | Yes    |
| RowKey          | No     |
| RowSetAdd       | No     |
| RowSetRead      | No     |
| RowSetTest      | No     |
| Rowid           | Yes    |
| SCopy           | No     |
| Savepoint       | No     |
| Seek            | No     |
| SeekGe          | Yes    |
| SeekGt          | Yes    |
| SeekLe          | No     |
| SeekLt          | No     |
| SeekRowid       | Yes    |
| Sequence        | No     |
| SetCookie       | No     |
| ShiftLeft       | No     |
| ShiftRight      | No     |
| SoftNull        | Yes    |
| Sort            | No     |
| SorterCompare   | No     |
| SorterData      | Yes    |
| SorterInsert    | Yes    |
| SorterNext      | Yes    |
| SorterOpen      | Yes    |
| SorterSort      | Yes    |
| String          | No     |
| String8         | Yes    |
| Subtract        | Yes    |
| TableLock       | No     |
| ToBlob          | No     |
| ToInt           | No     |
| ToNumeric       | No     |
| ToReal          | No     |
| ToText          | No     |
| Trace           | No     |
| Transaction     | Yes    |
| VBegin          | No     |
| VColumn         | No     |
| VCreate         | No     |
| VDestroy        | No     |
| VFilter         | No     |
| VNext           | No     |
| VOpen           | No     |
| VRename         | No     |
| VUpdate         | No     |
| Vacuum          | No     |
| Variable        | No     |
| VerifyCookie    | No     |
| Yield           | Yes    |
