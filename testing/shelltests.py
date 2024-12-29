#!/usr/bin/env python3
import os
import subprocess

# Configuration
sqlite_exec = os.getenv("SQLITE_EXEC", "./target/debug/limbo")
cwd = os.getcwd()

# Initial setup commands
init_commands = """
CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO users (id, first_name, last_name, age) VALUES
(1, 'Alice', 'Smith', 30), (2, 'Bob', 'Johnson', 25), (3, 'Charlie', 'Brown', 66), (4, 'David', 'Nichols', 70);
INSERT INTO products (id, name, price) VALUES
(1, 'Hat', 19.99), (2, 'Shirt', 29.99), (3, 'Shorts', 39.99), (4, 'Dress', 49.99);
CREATE TABLE t (x1, x2, x3, x4);
INSERT INTO t VALUES (zeroblob(1024 - 1), zeroblob(1024 - 2), zeroblob(1024 - 3), zeroblob(1024 - 4));
"""


def start_sqlite_repl(sqlite_exec, init_commands):
    # start limbo shell in quiet mode and pipe in init_commands
    pipe = subprocess.Popen(
        [sqlite_exec, "-q"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=0,
    )
    if init_commands and pipe.stdin is not None:
        pipe.stdin.write(init_commands + "\n")
        pipe.stdin.flush()
    return pipe


# get new pipe to limbo shell
pipe = start_sqlite_repl(sqlite_exec, init_commands)


def execute_sql(pipe, sql):
    write_to_pipe(sql + "\n")
    write_to_pipe("SELECT 'END_OF_RESULT';\n")

    output = []
    while True:
        line = pipe.stdout.readline().strip()
        if line == "END_OF_RESULT":
            break
        output.append(line)
    return "\n".join(output).strip()


def run_test(pipe, sql, expected_output):
    actual_output = execute_sql(pipe, sql)
    if actual_output != expected_output:
        print(f"Test FAILED: '{sql}'")
        print(f"Expected: {expected_output}")
        print(f"Returned: {actual_output}")
        exit(1)


def do_execshell_test(pipe, test_name, sql, expected_output):
    print(f"Running test: {test_name}")
    run_test(pipe, sql, expected_output)


def write_to_pipe(line):
    if pipe.stdin is None:
        print("Failed to start SQLite REPL")
        exit(1)
    pipe.stdin.write(line + "\n")
    pipe.stdin.flush()


# Run tests
do_execshell_test(pipe, "select-1", "SELECT 1;", "1")
do_execshell_test(
    pipe,
    "schema-memory",
    ".schema",
    """CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
CREATE TABLE t (x1, x2, x3, x4);""",
)
do_execshell_test(pipe, "select-avg", "SELECT avg(age) FROM users;", "47.75")
do_execshell_test(pipe, "select-sum", "SELECT sum(age) FROM users;", "191")

do_execshell_test(pipe, "mem-sum-zero", "SELECT sum(first_name) FROM users;", "0.0")
do_execshell_test(pipe, "mem-total-age", "SELECT total(age) FROM users;", "191.0")
do_execshell_test(
    pipe, "mem-typeof", "SELECT typeof(id) FROM users LIMIT 1;", "integer"
)

# test we can open a different db file and can attach to it
do_execshell_test(pipe, "file-schema-1", ".open testing/testing.db", "")

# test some random queries to ensure the proper schema
do_execshell_test(
    pipe,
    "file-schema-1",
    ".schema users",
    """CREATE TABLE users (
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
CREATE INDEX age_idx on users (age);""",
)

do_execshell_test(pipe, "file-users-count", "select count(*) from users;", "10000")

do_execshell_test(
    pipe,
    "file-cross-join",
    "select * from users, products limit 1;",
    "1|Jamie|Foster|dylan00@example.com|496-522-9493|62375 Johnson Rest Suite 322|West Lauriestad|IL|35865|94|1|hat|79.0",
)

do_execshell_test(
    pipe,
    "file-left-join-self",
    "select u1.first_name as user_name, u2.first_name as neighbor_name from users u1 left join users as u2 on u1.id = u2.id + 1 limit 2;",
    "Jamie|\nCindy|Jamie",
)

do_execshell_test(
    pipe,
    "where-clause-eq-string",
    "select count(1) from users where last_name = 'Rodriguez';",
    "61",
)

# test we can cd into a directory
dir = "testing"
outfile = "limbo_output.txt"

write_to_pipe(f".cd {dir}")

# test we can enable echo
write_to_pipe(".echo on")

# Redirect output to a file in the new directory
write_to_pipe(f".output {outfile}")

# make sure we cannot use pretty mode while outfile isnt a tty
write_to_pipe(".mode pretty")

# this should print an error to the new outfile

write_to_pipe("SELECT 'TEST_ECHO';")
write_to_pipe("")

write_to_pipe(".echo off")

# test we can set the null value
write_to_pipe(".nullvalue LIMBO")

# print settings to evaluate in file
write_to_pipe(".show")

# set output back to stdout
write_to_pipe(".output stdout")

do_execshell_test(
    pipe,
    "test-switch-output-stdout",
    ".show",
    f"""Settings:
Output mode: raw
DB: testing/testing.db
Output: STDOUT
Null value: LIMBO
CWD: {cwd}/testing
Echo: off""",
)

do_execshell_test(pipe, "test-show-tables", ".tables", "products users")

do_execshell_test(pipe, "test-show-tables-with-pattern", ".tables us%", "users")

# test we can set the null value

write_to_pipe(".open :memory:")

do_execshell_test(
    pipe,
    "test-can-switch-back-to-in-memory",
    ".schema users",
    "Error: Table 'users' not found.",
)

do_execshell_test(pipe, "test-verify-null-value", "select NULL;", "LIMBO")


# Verify the output file exists and contains expected content
filepath = os.path.join(cwd, dir, outfile)

if not os.path.exists(filepath):
    print("Test FAILED: Output file not created")
    exit(1)

with open(filepath, "r") as f:
    file_contents = f.read()

# verify command was echo'd as well as mode was unchanged
expected_lines = {
    f"Output: {outfile}": "Can direct output to a file",
    "Output mode: raw": "Output mode doesn't change when redirected from stdout",
    "Error: pretty output can only be written to a tty": "No ansi characters printed to non-tty",
    "SELECT 'TEST_ECHO'": "Echo properly echoes the command",
    "TEST_ECHO": "Echo properly prints the result",
    "Null value: LIMBO": "Null value is set properly",
    f"CWD: {cwd}/testing": "Shell can change directory",
    "DB: testing/testing.db": "Shell can open a different db file",
    "Echo: off": "Echo can be toggled on and off",
}

all_lines_found = True
for line, value in expected_lines.items():
    if line not in file_contents:
        print(f"Test FAILED: Expected line not found in file: {line}")
        all_lines_found = False
    else:
        print(f"Testing that: {value}")

if all_lines_found:
    print("Test PASSED: File contains all expected lines")
else:
    print(f"File contents:\n{file_contents}")
    exit(1)

# Cleanup
os.remove(filepath)
pipe.terminate()
print("All shell tests passed successfully.")
