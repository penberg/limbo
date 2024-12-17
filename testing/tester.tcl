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

proc within_tolerance {actual expected tolerance} {
    expr {abs($actual - $expected) <= $tolerance}
}

# This function is used to test floating point values within a tolerance
# FIXME: When Limbo's floating point presentation matches to SQLite, this could/should be removed
proc do_execsql_test_tolerance {test_name sql_statements expected_outputs tolerance} {
    foreach db $::test_dbs {
        puts [format "(%s) %s Running test: %s" $db [string repeat " " [expr {40 - [string length $db]}]] $test_name]
        set combined_sql [string trim $sql_statements]
        set actual_output [evaluate_sql $::sqlite_exec $db $combined_sql]
        set actual_values [split $actual_output "\n"]
        set expected_values [split $expected_outputs "\n"]

        if {[llength $actual_values] != [llength $expected_values]} {
            puts "Test FAILED: '$sql_statements'"
            puts "returned '$actual_output'"
            puts "expected '$expected_outputs'"
            exit 1
        }

        for {set i 0} {$i < [llength $actual_values]} {incr i} {
            set actual [lindex $actual_values $i]
            set expected [lindex $expected_values $i]

            if {![within_tolerance $actual $expected $tolerance]} {
                set lower_bound [expr {$expected - $tolerance}]
                set upper_bound [expr {$expected + $tolerance}]
                puts "Test FAILED: '$sql_statements'"
                puts "returned '$actual'"
                puts "expected a value within the range \[$lower_bound, $upper_bound\]"
                exit 1
            }
        }
    }
}
