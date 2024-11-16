# Extra consistency checks

- `ALTER TABLE ... RENAME TO ...` when old and new table names are the same => `Stmt::check`
- `ALTER TABLE ... ADD COLUMN ...` with new primary key / unique constraint => `Stmt::check`
- `CREATE TABLE ...`
  - with duplicated column name => `ColumnDefinition::add_column`
  - with STRICT option and invalid or missing column type(s) => `CreateTableBody::check`
  - WITHOUT ROWID and without primary key => `CreateTableBody::check`
- `CREATE VIEW ... (...) ...`
  - when view columns count does not match select columns count => `Stmt::check`
  - with duplicated columns (same name) => `Stmt::check`
- `DELETE FROM ... ORDER BY ...` with ORDER BY but without LIMIT => `Stmt::check`
- `INSERT INTO ... (...) ...` when columns count does not match select columns / values count => `Stmt::check`
- `INSERT INTO ... (...) DEFAULT VALUES` with columns and DEFAULT VALUES => `Stmt::check`
- `SELECT ... EXCEPT|INTERSECT|UNION SELECT ...` when all SELECT does not have the same number of result columns => `SelectBody::push`
- `NATURAL JOIN ...` with ON or USING clause => `FromClause::push`
- `UPDATE ... ORDER BY ...` with ORDER BY but without LIMIT => `Stmt::check`
- `VALUES (...), (...), ...` when all VALUES does not have the same number of terms => `OneSelect::push`
- `WITH ...` with duplicated table name => `CommonTableExpr::add_cte`

## TODO

### `CREATE TABLE`

- [x] qualified (different of `temp`) temporary table

```sql
sqlite> ATTACH DATABASE ':memory:' AS mem;
sqlite> CREATE TEMPORARY TABLE mem.x AS SELECT 1;
Parse error: temporary table name must be unqualified
```

```sql
sqlite> CREATE TEMPORARY TABLE temp.x AS SELECT 1;
-- OK
```

- [x] must have at least one non-generated column

```sql
sqlite> CREATE TABLE test(data AS (1));
Parse error: must have at least one non-generated column
```

- [ ] column constraint(s) checks

```sql
sqlite> CREATE TABLE t(a REFERENCES o(a,b));
Parse error: foreign key on a should reference only one column of table o
  CREATE TABLE t(a REFERENCES o(a,b));
                error here ---^
sqlite> CREATE TABLE t(a PRIMARY KEY AUTOINCREMENT) WITHOUT ROWID;
Parse error: AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY
sqlite> CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT) WITHOUT ROWID;
Parse error: AUTOINCREMENT not allowed on WITHOUT ROWID tables
```

- [ ] table constraint(s) checks

```sql
sqlite> CREATE TABLE test (a, b, FOREIGN KEY (b) REFERENCES test(a,b));
Parse error: number of columns in foreign key does not match the number of columns in the referenced table
```

```sql
sqlite> create table test (a,b, primary key(a), primary key(b));
Parse error: table "test" has more than one primary key
sqlite> create table test (a primary key, b primary key);
Parse error: table "test" has more than one primary key
sqlite> create table test (a primary key, b, primary key(a));
Parse error: table "test" has more than one primary key
```

### `HAVING`

- [x] HAVING clause on a non-aggregate query (`GroupBy::having`): grammar already prevents this case (grammar differs from SQLite official grammar).

```sql
sqlite> SELECT 1 as i HAVING i > 1;
Parse error: HAVING clause on a non-aggregate query
```

vs

```
[ERROR sqlite3Parser] near HAVING, "Token(None)": syntax error
Err: near HAVING, "None": syntax error at (1, 21) in SELECT 1 as i HAVING i > 1
```

### `SELECT ...`

- [ ] no duplicated column name in `selcollist`/`Select::columns`

```sql
sqlite> SELECT 1 as i, 2 as i;
-- no error (idem for postgres)
```

### `SELECT ... ORDER BY ...`

- [ ] ORDER BY term does not match any column in the result set (`Select::order_by`)

```sql
sqlite> SELECT 1 as i ORDER BY j;
Parse error: no such column: j
  SELECT 1 as i ORDER BY j;
                         ^--- error here
```

### `WITH`

- [ ] no duplicated column name in `CommonTableExpr::IndexedColumn`

### DML

```sql
sqlite> CREATE TABLE test (n, m);
sqlite> INSERT INTO test (n, n, m) VALUES (1, 0, 1); -- pgsql KO
sqlite> SELECT * FROM test;
1|1
sqlite> UPDATE test SET n = 1, n = 0; -- pgsql KO
sqlite> SELECT * FROM test;
0|1
```
