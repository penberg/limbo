#!/usr/bin/env tclsh

set sqlite_exec [expr {[info exists env(SQLITE_EXEC)] ? $env(SQLITE_EXEC) : "sqlite3"}]

proc start_sqlite_repl {sqlite_exec init_commands} {
    set command [list $sqlite_exec ":memory:"]
    set pipe [open "|[join $command]" RDWR]
    puts $pipe $init_commands
    flush $pipe
    return $pipe
}

proc execute_sql {pipe sql} {
    puts $pipe $sql
    flush $pipe
    puts $pipe "SELECT 'END_OF_RESULT';"
    flush $pipe
    set output ""
    while {[gets $pipe line] >= 0} {
        if {$line eq "END_OF_RESULT"} {
            break
        }
        append output "$line\n"
    }
    return [string trim $output]
}

proc run_test {pipe sql expected_output} {
    set actual_output [execute_sql $pipe $sql]
    if {$actual_output ne $expected_output} {
        puts "Test FAILED: '$sql'"
        puts "returned '$actual_output'"
        puts "expected '$expected_output'"
        exit 1
    }
}

proc do_execsql_test {pipe test_name sql expected_output} {
    puts "Running test: $test_name"
    run_test $pipe $sql $expected_output
}

set init_commands {
CREATE TABLE users (
        id INTEGER PRIMARY KEY,
		first_name TEXT,
		last_name TEXT,
		age INTEGER
	);
    CREATE TABLE products (
	id INTEGER PRIMARY KEY,
	name TEXT,
	price INTEGER
	);
    INSERT INTO users (id, first_name, last_name, age) VALUES
	(1, 'Alice', 'Smith', 30), (2, 'Bob', 'Johnson', 25), (3, 'Charlie', 'Brown', 66), (4, 'David', 'Nichols', 70); INSERT INTO products (id, name, price) VALUES (1, 'Hat', 19.99), (2, 'Shirt', 29.99), (3, 'Shorts', 39.99), (4, 'Dress', 49.99);
    CREATE TABLE t(x1, x2, x3, x4);
    INSERT INTO t VALUES (zeroblob(1024 - 1), zeroblob(1024 - 2), zeroblob(1024 - 3), zeroblob(1024 - 4));
}

set pipe [start_sqlite_repl $sqlite_exec $init_commands]

do_execsql_test $pipe schema {
  .schema 
} {CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
CREATE TABLE t (x1, x2, x3, x4);}

do_execsql_test $pipe "select-avg" {
  SELECT avg(age) FROM users;
} {47.75}

do_execsql_test $pipe select-avg-text {
  SELECT avg(first_name) FROM users;
} {0.0}

do_execsql_test $pipe select-sum {
  SELECT sum(age) FROM users;
} {191}

do_execsql_test $pipe select-sum-text {
  SELECT sum(first_name) FROM users;
} {0.0}

do_execsql_test $pipe select-total {
  SELECT total(age) FROM users;
} {191.0}

do_execsql_test $pipe select-total-text {
  SELECT total(first_name) FROM users WHERE id < 3;
} {0.0}

do_execsql_test $pipe select-limit {
  SELECT typeof(id) FROM users LIMIT 1;
} {integer}

do_execsql_test $pipe select-count {
  SELECT count(id) FROM users;
} {4}

do_execsql_test $pipe select-count {
  SELECT count(*) FROM users;
} {4}

do_execsql_test $pipe select-count-constant-true {
  SELECT count(*) FROM users WHERE true;
} {4}

do_execsql_test $pipe select-count-constant-false {
  SELECT count(*) FROM users WHERE false;
} {0}

# **atrocious hack to test that we can open new connection
do_execsql_test $pipe test-open-new-db {
  .open testing/testing.db 
} {}

# now grab random tests from other areas and make sure we are querying that database
do_execsql_test $pipe schema-1 {
  .schema users
} {CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    );
CREATE INDEX age_idx on users (age);}

do_execsql_test $pipe cross-join {
    select * from users, products limit 1;
} {1|Jamie|Foster|dylan00@example.com|496-522-9493|62375 Johnson Rest Suite 322|West Lauriestad|IL|35865|94|1|hat|79.0}

do_execsql_test $pipe left-join-self {
    select u1.first_name as user_name, u2.first_name as neighbor_name from users u1 left join users as u2 on u1.id = u2.id + 1 limit 2;
} {Jamie|
Cindy|Jamie}

do_execsql_test $pipe where-clause-eq-string {
    select count(1) from users where last_name = 'Rodriguez';
} {61}

puts "All tests passed successfully."
close $pipe
exit 0
