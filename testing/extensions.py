#!/usr/bin/env python3
import os
import subprocess
import select
import time
import uuid

sqlite_exec = "./target/debug/limbo"
sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


def init_limbo():
    pipe = subprocess.Popen(
        [sqlite_exec, *sqlite_flags],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
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
    lines = lines.split("\n")
    lines = [line.strip() for line in lines if line != ""]
    return "\n".join(lines)


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


def run_test(pipe, sql, validator=None):
    print(f"Running test: {sql}")
    result = execute_sql(pipe, sql)
    if validator is not None:
        if not validator(result):
            print(f"Test FAILED: {sql}")
            print(f"Returned: {result}")
            raise Exception("Validation failed")
    print("Test PASSED")


def validate_blob(result):
    # HACK: blobs are difficult to test because the shell
    # tries to return them as utf8 strings, so we call hex
    # and assert they are valid hex digits
    return int(result, 16) is not None


def validate_string_uuid(result):
    return len(result) == 36 and result.count("-") == 4


def returns_null(result):
    return result == "" or result == b"\n" or result == b""


def assert_now_unixtime(result):
    return result == str(int(time.time()))


def assert_specific_time(result):
    return result == "1736720789"


def main():
    specific_time = "01945ca0-3189-76c0-9a8f-caf310fc8b8e"
    extension_path = "./target/debug/liblimbo_uuid.so"
    pipe = init_limbo()
    try:
        # before extension loads, assert no function
        run_test(pipe, "SELECT uuid4();", returns_null)
        run_test(pipe, "SELECT uuid4_str();", returns_null)
        run_test(pipe, f".load {extension_path}", returns_null)
        print("Extension loaded successfully.")
        run_test(pipe, "SELECT hex(uuid4());", validate_blob)
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
    except Exception as e:
        print(f"Test FAILED: {e}")
        pipe.terminate()
        exit(1)
    pipe.terminate()
    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
