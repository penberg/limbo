# Runs one upstream test file in this process and always prints its
# summary, even when the file stops on an error outside of a do_test.
#
# Usage: tclsh run_file.tcl FILE.test
#
# A TCL error at the top level of a test file (a missing harness command,
# a setup statement the engine rejects, a helper file that is not there)
# ends the script before finish_test runs, so all.test would see no
# summary line and count the file as running zero tests. This wrapper
# catches that error, reports it as one extra failure named
# FILE-ABORTED, and then prints the summary for the tests that did run.

# The variables here have a __run_file prefix so a test file's own globals
# (several use "file" and "name") cannot clobber them.
set __run_file_path [lindex $argv 0]
set argv0 $__run_file_path
set argv [lrange $argv 1 end]
set testdir [file dirname $__run_file_path]

set __run_file_rc [catch {uplevel #0 [list source $__run_file_path]} \
    __run_file_err __run_file_opts]
if {$__run_file_rc != 0 && $__run_file_rc != 2} {
  set __run_file_name [file rootname [file tail $__run_file_path]]
  set __run_file_where ""
  regexp {\(file "[^"]*" line (\d+)\)} \
      [dict get $__run_file_opts -errorinfo] -> __run_file_where
  puts ""
  puts "ABORTED: $__run_file_name.test line $__run_file_where: [string range $__run_file_err 0 200]"
  if {[llength [info procs finish_test]] > 0} {
    incr ::TC(count)
    incr ::TC(errors)
    lappend ::TC(fail_list) "$__run_file_name-ABORTED"
    finish_test
  }
}
