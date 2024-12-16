# class Database

The `Database` class represents a connection that can prepare and execute SQL statements.

## Methods

### new Database(path) ⇒ Database

Creates a new database connection.

| Param   | Type                | Description               |
| ------- | ------------------- | ------------------------- |
| path    | <code>string</code> | Path to the database file |

### prepare(sql) ⇒ Statement

Prepares a SQL statement for execution.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to prepare. |

The function returns a `Statement` object.

### transaction(function) ⇒ function

This function is currently not supported.

### pragma(string, [options]) ⇒ results

This function is currently not supported.

### backup(destination, [options]) ⇒ promise

This function is currently not supported.

### serialize([options]) ⇒ Buffer

This function is currently not supported.

### function(name, [options], function) ⇒ this

This function is currently not supported.

### aggregate(name, options) ⇒ this

This function is currently not supported.

### table(name, definition) ⇒ this

This function is currently not supported.

### loadExtension(path, [entryPoint]) ⇒ this

This function is currently not supported.

### exec(sql) ⇒ this

This function is currently not supported.

### close() ⇒ this

This function is currently not supported.

# class Statement

## Methods

### run([...bindParameters]) ⇒ object

This function is currently not supported.

### get([...bindParameters]) ⇒ row

Executes the SQL statement and returns the first row.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

### all([...bindParameters]) ⇒ array of rows

Executes the SQL statement and returns an array of the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

### iterate([...bindParameters]) ⇒ iterator

Executes the SQL statement and returns an iterator to the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

### pluck([toggleState]) ⇒ this

This function is currently not supported.

### expand([toggleState]) ⇒ this

This function is currently not supported.

### raw([rawMode]) ⇒ this

Toggle raw mode.

| Param   | Type                 | Description                                                                       |
| ------- | -------------------- | --------------------------------------------------------------------------------- |
| rawMode | <code>boolean</code> | Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled. |

This function enables or disables raw mode. Prepared statements return objects by default, but if raw mode is enabled, the functions return arrays instead.

### columns() ⇒ array of objects

This function is currently not supported.

### bind([...bindParameters]) ⇒ this

This function is currently not supported.
