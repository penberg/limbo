#!/usr/bin/env python3
import os
import subprocess
import select
import time

sqlite_exec = "./target/debug/limbo"
sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")

test_data = """CREATE TABLE numbers ( id INTEGER PRIMARY KEY, value FLOAT NOT NULL);
INSERT INTO numbers (value) VALUES (1.0);
INSERT INTO numbers (value) VALUES (2.0);
INSERT INTO numbers (value) VALUES (3.0);
INSERT INTO numbers (value) VALUES (4.0);
INSERT INTO numbers (value) VALUES (5.0);
INSERT INTO numbers (value) VALUES (6.0);
INSERT INTO numbers (value) VALUES (7.0);
CREATE TABLE test (value REAL, percent REAL);
INSERT INTO test values (10, 25);
INSERT INTO test values (20, 25);
INSERT INTO test values (30, 25);
INSERT INTO test values (40, 25);
INSERT INTO test values (50, 25);
INSERT INTO test values (60, 25);
INSERT INTO test values (70, 25);
"""


def init_limbo():
    pipe = subprocess.Popen(
        [sqlite_exec, *sqlite_flags],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    write_to_pipe(pipe, test_data)
    return pipe


def execute_sql(pipe, sql):
    end_suffix = "END_OF_RESULT"
    write_to_pipe(pipe, sql)
    write_to_pipe(pipe, f"SELECT '{end_suffix}';\n")
    stdout = pipe.stdout
    stderr = pipe.stderr
    output = ""
    while True:
        ready_to_read, _, error_in_pipe = select.select(
            [stdout, stderr], [], [stdout, stderr]
        )
        ready_to_read_or_err = set(ready_to_read + error_in_pipe)
        if stderr in ready_to_read_or_err:
            exit_on_error(stderr)

        if stdout in ready_to_read_or_err:
            fragment = stdout.read(select.PIPE_BUF)
            output += fragment.decode()
            if output.rstrip().endswith(end_suffix):
                output = output.rstrip().removesuffix(end_suffix)
                break
    output = strip_each_line(output)
    return output


def strip_each_line(lines: str) -> str:
    split = lines.split("\n")
    res = [line.strip() for line in split if line != ""]
    return "\n".join(res)


def write_to_pipe(pipe, command):
    if pipe.stdin is None:
        raise RuntimeError("Failed to write to shell")
    pipe.stdin.write((command + "\n").encode())
    pipe.stdin.flush()


def exit_on_error(stderr):
    while True:
        ready_to_read, _, _ = select.select([stderr], [], [])
        if not ready_to_read:
            break
        print(stderr.read().decode(), end="")
    exit(1)


def run_test(pipe, sql, validator=None, name=None):
    print(f"Running test {name}: {sql}")
    result = execute_sql(pipe, sql)
    if validator is not None:
        if not validator(result):
            print(f"Test FAILED: {sql}")
            print(f"Returned: {result}")
            raise Exception("Validation failed")
    print("Test PASSED")


def validate_true(result):
    return result == "1"


def validate_false(result):
    return result == "0"


def validate_blob(result):
    # HACK: blobs are difficult to test because the shell
    # tries to return them as utf8 strings, so we call hex
    # and assert they are valid hex digits
    return int(result, 16) is not None


def validate_string_uuid(result):
    return len(result) == 36 and result.count("-") == 4


def returns_error(result):
    return "error: no such function: " in result


def returns_null(result):
    return result == "" or result == "\n"


def assert_now_unixtime(result):
    return result == str(int(time.time()))


def assert_specific_time(result):
    return result == "1736720789"


def test_uuid(pipe):
    specific_time = "01945ca0-3189-76c0-9a8f-caf310fc8b8e"
    # these are built into the binary, so we just test they work
    run_test(
        pipe,
        "SELECT hex(uuid4());",
        validate_blob,
        "uuid functions are registered properly with ext loaded",
    )
    run_test(pipe, "SELECT uuid4_str();", validate_string_uuid)
    run_test(pipe, "SELECT hex(uuid7());", validate_blob)
    run_test(
        pipe,
        "SELECT uuid7_timestamp_ms(uuid7()) / 1000;",
    )
    run_test(pipe, "SELECT uuid7_str();", validate_string_uuid)
    run_test(pipe, "SELECT uuid_str(uuid7());", validate_string_uuid)
    run_test(pipe, "SELECT hex(uuid_blob(uuid7_str()));", validate_blob)
    run_test(pipe, "SELECT uuid_str(uuid_blob(uuid7_str()));", validate_string_uuid)
    run_test(
        pipe,
        f"SELECT uuid7_timestamp_ms('{specific_time}') / 1000;",
        assert_specific_time,
    )
    run_test(
        pipe,
        "SELECT gen_random_uuid();",
        validate_string_uuid,
        "scalar alias's are registered properly",
    )


def test_regexp(pipe):
    extension_path = "./target/debug/liblimbo_regexp.so"

    # before extension loads, assert no function
    run_test(pipe, "SELECT regexp('a.c', 'abc');", returns_error)
    run_test(pipe, f".load {extension_path}", returns_null)
    print(f"Extension {extension_path} loaded successfully.")
    run_test(pipe, "SELECT regexp('a.c', 'abc');", validate_true)
    run_test(pipe, "SELECT regexp('a.c', 'ac');", validate_false)
    run_test(pipe, "SELECT regexp('[0-9]+', 'the year is 2021');", validate_true)
    run_test(pipe, "SELECT regexp('[0-9]+', 'the year is unknow');", validate_false)
    run_test(pipe, "SELECT regexp_like('the year is 2021', '[0-9]+');", validate_true)
    run_test(
        pipe, "SELECT regexp_like('the year is unknow', '[0-9]+');", validate_false
    )
    run_test(
        pipe,
        "SELECT regexp_substr('the year is 2021', '[0-9]+') = '2021';",
        validate_true,
    )
    run_test(
        pipe, "SELECT regexp_substr('the year is unknow', '[0-9]+');", returns_null
    )


def validate_median(res):
    return res == "4.0"


def validate_median_odd(res):
    return res == "4.5"


def validate_percentile1(res):
    return res == "25.0"


def validate_percentile2(res):
    return res == "43.0"


def validate_percentile_disc(res):
    return res == "40.0"


def test_aggregates(pipe):
    extension_path = "./target/debug/liblimbo_percentile.so"
    # assert no function before extension loads
    run_test(
        pipe,
        "SELECT median(1);",
        returns_error,
        "median agg function returns null when ext not loaded",
    )
    run_test(
        pipe,
        f".load {extension_path}",
        returns_null,
        "load extension command works properly",
    )
    run_test(
        pipe,
        "select median(value) from numbers;",
        validate_median,
        "median agg function works",
    )
    write_to_pipe(pipe, "INSERT INTO numbers (value) VALUES (8.0);\n")
    run_test(
        pipe,
        "select median(value) from numbers;",
        validate_median_odd,
        "median agg function works with odd number of elements",
    )
    run_test(
        pipe,
        "SELECT percentile(value, percent) from test;",
        validate_percentile1,
        "test aggregate percentile function with 2 arguments works",
    )
    run_test(
        pipe,
        "SELECT percentile(value, 55) from test;",
        validate_percentile2,
        "test aggregate percentile function with 1 argument works",
    )
    run_test(
        pipe, "SELECT percentile_cont(value, 0.25) from test;", validate_percentile1
    )
    run_test(
        pipe, "SELECT percentile_disc(value, 0.55) from test;", validate_percentile_disc
    )


def main():
    pipe = init_limbo()
    try:
        test_regexp(pipe)
        test_uuid(pipe)
        test_aggregates(pipe)
    except Exception as e:
        print(f"Test FAILED: {e}")
        pipe.terminate()
        exit(1)
    pipe.terminate()
    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
