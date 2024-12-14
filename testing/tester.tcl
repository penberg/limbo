set sqlite_exec [expr {[info exists env(SQLITE_EXEC)] ? $env(SQLITE_EXEC) : "sqlite3"}]
set test_dbs [list "testing/testing.db" "testing/testing_norowidalias.db"]

proc evaluate_sql {sqlite_exec db_name sql} {
    set command [list $sqlite_exec $db_name $sql]
    set output [exec {*}$command]
    return $output
}

proc run_test {sqlite_exec db_name sql expected_output} {
    set actual_output [evaluate_sql $sqlite_exec $db_name $sql]
    if {$actual_output ne $expected_output} {
        puts "Test FAILED: '$sql'"
        puts "returned '$actual_output'"
        puts "expected '$expected_output'"
        exit 1
    }
}

proc do_execsql_test {test_name sql_statements expected_outputs} {
    foreach db $::test_dbs {
        puts [format "(%s) %s Running test: %s" $db [string repeat " " [expr {40 - [string length $db]}]] $test_name]
        set combined_sql [string trim $sql_statements]
        set combined_expected_output [join $expected_outputs "\n"]
        run_test $::sqlite_exec $db $combined_sql $combined_expected_output
    }
}

proc do_execsql_test_on_specific_db {db_name test_name sql_statements expected_outputs} {
    puts [format "(%s) %s Running test: %s" $db_name [string repeat " " [expr {40 - [string length $db_name]}]] $test_name]
    set combined_sql [string trim $sql_statements]
    set combined_expected_output [join $expected_outputs "\n"]
    run_test $::sqlite_exec $db_name $combined_sql $combined_expected_output
}
