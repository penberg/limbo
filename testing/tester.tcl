set sqlite_exec [expr {[info exists env(SQLITE_EXEC)] ? $env(SQLITE_EXEC) : "sqlite3"}]

proc evaluate_sql {sqlite_exec sql} {
    set command [list $sqlite_exec testing/testing.db $sql]
    set output [exec {*}$command]
    return $output
}

proc run_test {sqlite_exec sql expected_output} {
    set actual_output [evaluate_sql $sqlite_exec $sql]
    if {$actual_output ne $expected_output} {
        puts "Test FAILED: '$sql'"
        puts "returned '$actual_output'"
        puts "expected '$expected_output'"
        exit 1
    }
}

proc run_failing_test {sqlite_exec sql expected_err_msg} {
    set actual_output [catch {set actual_output [evaluate_sql $sqlite_exec $sql]} err_msg]
    if {$err_msg ne $expected_err_msg} {
        puts "Test FAILED: '$sql'"
        puts "returned '$err_msg'"
        puts "expected '$expected_err_msg'"
        exit 1
    }
}

proc run_test_with_cleanup {sqlite_exec sql expected_output cleanup_sql} {
    set actual_output [evaluate_sql $sqlite_exec $sql]
    if {$actual_output ne $expected_output} {
        puts "Test FAILED: '$sql'"
        puts "returned '$actual_output'"
        puts "expected '$expected_output'"
        evaluate_sql $sqlite_exec $cleanup_sql
        exit 1
    }
    evaluate_sql $sqlite_exec $cleanup_sql
}

proc do_execsql_test {test_name sql_statements expected_outputs} {
    puts "Running test: $test_name"
    set combined_sql [string trim $sql_statements]
    set combined_expected_output [join $expected_outputs "\n"]
    run_test $::sqlite_exec $combined_sql $combined_expected_output
}

proc do_execsql_check_err_msg {test_name sql_statements expected_err_msg} {
    puts "Running test: $test_name"
    set combined_sql [string trim $sql_statements]
    run_failing_test $::sqlite_exec $combined_sql $expected_err_msg
}

proc do_execsql_with_cleanup_test {test_name sql_statements expected_outputs cleanup_statements} {
    puts "Running test: $test_name"
    set combined_sql [string trim $sql_statements]
    set combined_expected_output [join $expected_outputs "\n"]
    set combined_cleanup [string trim $cleanup_statements]
    run_test_with_cleanup $::sqlite_exec $combined_sql $combined_expected_output $combined_cleanup
}