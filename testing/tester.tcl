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

proc do_execsql_test {test_name sql_statements expected_outputs} {
    puts "Running test: $test_name"
    set combined_sql [string trim $sql_statements]
    set combined_expected_output [join $expected_outputs "\n"]
    run_test $::sqlite_exec $combined_sql $combined_expected_output
}
