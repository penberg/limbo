# SQL Parser Supported Features

This document outlines the features currently supported by our work-in-progress SQL parser.

## SELECT Statement

- [x] Basic SELECT ... FROM ... structure
- [x] Column selection
  - [x] Star (*) selection
  - [x] Individual column selection
  - [x] Qualified column names (e.g., table.column)
- [x] Table selection in FROM clause
- [x] Table aliasing
- [x] WHERE clause with conditions
- [x] GROUP BY clause
- [x] ORDER BY clause
  - [x] ASC and DESC directives
  - [x] Multiple column ordering
- [x] LIMIT clause
- [ ] OFFSET clause
- [ ] HAVING clause
- [ ] Subqueries

## Operators and Expressions

- [x] Basic arithmetic operators (+, -, *, /)
- [x] Comparison operators (=, !=, <>, >, <, >=, <=)
- [x] Logical operators (AND, OR, NOT)
- [x] IN operator
- [x] NOT IN operator
- [x] LIKE operator
- [x] BETWEEN operator
- [x] Parenthesized expressions
- [x] Function calls
- [x] CASE expressions (both simple and searched)

## JOINs

- [x] INNER JOIN
- [x] LEFT OUTER JOIN
- [x] Multiple joins in a single query
- [x] Join conditions (ON clause)
- [ ] RIGHT OUTER JOIN
- [ ] FULL OUTER JOIN
- [ ] CROSS JOIN

## Data Types and Literals

- [x] String literals
- [x] Numeric literals (integers and floats)
- [ ] Date and time literals
- [ ] Boolean literals
- [ ] NULL

## Functions

- [x] Basic function calls with arguments
- [x] Aggregate functions (SUM, AVG, etc.)
- [ ] Window functions

## Additional Features

- [x] Case insensitivity for keywords
- [x] Column aliasing
- [ ] Common Table Expressions (CTEs)
- [ ] Set operations (UNION, INTERSECT, EXCEPT)

## Other SQL Statement Types

- [ ] INSERT
- [ ] UPDATE
- [ ] DELETE
- [ ] CREATE TABLE
- [ ] ALTER TABLE
- [ ] DROP TABLE
- [ ] CREATE INDEX
- [ ] CREATE VIEW