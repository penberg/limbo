# SQLite Test Framework - Simplified Version
# Based on the official SQLite tester.tcl
#
# Requires the native TCL extension (libturso_tcl) to be built.
# Build with: make -C bindings/tcl

# Global variables for test execution (safe to re-initialize)
if {![info exists TC(errors)]} {
  set TC(errors) 0
}
if {![info exists TC(count)]} {
  set TC(count) 0
}
if {![info exists TC(fail_list)]} {
  set TC(fail_list) [list]
}
if {![info exists testprefix]} {
  set testprefix ""
}

set script_dir [file dirname [file dirname [file dirname [file dirname [file normalize [info script]]]]]]
set test_db "test.db"

# Load the native TCL extension (libturso_tcl).
# This provides a real in-process sqlite3 command backed by the Turso engine.
set _native_loaded 0
foreach _native_candidate [list \
    [file join $script_dir "bindings" "tcl" "libturso_tcl.so"] \
    [file join $script_dir "bindings" "tcl" "libturso_tcl.dylib"]] {
  if {[file exists $_native_candidate]} {
    if {![catch {load $_native_candidate Tursotcl} _native_load_err]} {
      set _native_loaded 1
      break
    } else {
      puts stderr "Failed to load $_native_candidate: $_native_load_err"
    }
  }
}
if {!$_native_loaded} {
  puts stderr "FATAL: Could not load native TCL extension (libturso_tcl)."
  puts stderr "Build it with: make -C bindings/tcl"
  puts stderr "Searched:"
  puts stderr "  [file join $script_dir bindings tcl libturso_tcl.so]"
  puts stderr "  [file join $script_dir bindings tcl libturso_tcl.dylib]"
  exit 1
}
catch {unset _native_candidate}
catch {unset _native_load_err}
catch {unset _native_loaded}

# Create or reset test database
proc reset_db {} {
  global test_db
  file delete -force $test_db
  file delete -force "${test_db}-journal"
  file delete -force "${test_db}-wal"

  if {[llength [info commands db]] > 0} {
    catch {db close}
  }
  sqlite3 db $test_db
}

# Execute SQL and return results
# Tcl 9 removed the tcl_precision magic variable; its default float
# formatting (shortest exact representation) matches the old
# tcl_precision=0 behavior. Define a plain global so upstream tests that
# save/set/restore tcl_precision keep working.
if {![info exists ::tcl_precision]} {
  set ::tcl_precision 0
}

# Upstream tester.tcl no-op hooks: breakpoint is a debugger anchor and
# do_not_use_codec only matters for codec-enabled builds.
proc breakpoint {} {}
proc do_not_use_codec {} {}

# Name of the current test permutation, as in upstream tester.tcl. We only
# run the default configuration, so this is "" unless a permutation script
# sets G(perm:name).
proc permutation {} {
  set perm ""
  catch {set perm $::G(perm:name)}
  set perm
}

# Modern SQLite builds default to schema file format 4; upstream tests
# read this to decide format-dependent expectations.
if {![info exists ::SQLITE_DEFAULT_FILE_FORMAT]} {
  set ::SQLITE_DEFAULT_FILE_FORMAT 4
}

proc execsql {sql {db db}} {
  # Evaluate in the caller's scope so that TCL variables referenced inside
  # the SQL (e.g. {SELECT round($x1)}) bind to the caller's values instead
  # of silently binding NULL.
  return [uplevel [list $db eval $sql]]
}

# Execute SQL and return first value only (similar to db one)
proc db_one {sql {db db}} {
  set result [execsql $sql $db]
  if {[llength $result] > 0} {
    return [lindex $result 0]
  } else {
    return ""
  }
}

# Execute SQL and return results with column names
# Format: column1 value1 column2 value2 ... (alternating for each row)
proc execsql2 {sql {db db}} {
  set result {}
  $db eval $sql row {
    foreach col $row(*) {
      lappend result $col $row($col)
    }
  }
  return $result
}

# Normalize Turso error messages to match SQLite's format.
# Turso prefixes some messages (e.g. "Parse error: no such table: t1")
# where SQLite would just say "no such table: t1".
proc normalize_errmsg {msg} {
  regsub {^Parse error: } $msg {} msg
  return $msg
}

# Normalize a test result. If the result is a two-element list whose first
# element is "1" (i.e. an error result from catchsql or catch+execsql),
# strip known Turso prefixes from the error message so it matches SQLite.
# Plain results are returned unchanged.
proc normalize_result {result} {
  # A result that is not a valid TCL list (e.g. an error message with an
  # unmatched brace or a quoted word followed by ':') cannot be an
  # {1 msg} error pair, so leave it alone instead of letting llength
  # abort the test file.
  if {[catch {llength $result} n]} {
    return $result
  }
  if {$n == 2 && [lindex $result 0] eq "1"} {
    set msg [normalize_errmsg [lindex $result 1]]
    return [list 1 $msg]
  }
  return $result
}

# Execute SQL and catch errors
proc catchsql {sql {db db}} {
  # Do not route through execsql: its uplevel would land in this frame, one
  # level short of the caller whose variables the SQL may reference.
  if {[catch {uplevel [list $db eval $sql]} result]} {
    return [list 1 [normalize_errmsg $result]]
  } else {
    return [list 0 $result]
  }
}

# Compare two doubles, tolerating formatting noise. The tolerance is
# relative, not absolute: TCL 8.6 and 9 print the same double differently
# (15 significant digits vs shortest round-trip), so two renderings of one
# value can differ in their last digits at any magnitude.
proc floats_equal {a b} {
  # NaN passes [string is double] but throws in expr arithmetic; treat any
  # non-computable comparison as unequal instead of aborting the test file.
  if {[catch {expr {abs($a - $b) <= 1e-12 * (abs($a) + abs($b) + 1.0)}} eq]} {
    return 0
  }
  return $eq
}

# Main test execution function
proc do_test {name cmd expected} {
  global TC testprefix

  fix_testname name

  incr TC(count)
  puts -nonewline "$name... "
  flush stdout

  if {[catch {uplevel #0 $cmd} result]} {
    puts "ERROR: [truncate_for_output $result]"
    lappend TC(fail_list) $name
    incr TC(errors)
    return
  }

  # Normalize Turso error prefixes so results match SQLite's format.
  set result [normalize_result $result]

  # Compare result with expected. The pattern forms are upstream's:
  #   /RE/     the regular expression RE must match the result
  #   ~/RE/    RE must not match
  #   /*GLOB/  a leading * means the body is a glob, not a regexp
  #   #/RE/    a # in RE stands for a number
  set ok 0
  if {[regexp {^[~#]?/.*/$} $expected]} {
    set re $expected
    set negate 0
    if {[string index $re 0] eq "~"} {
      set negate 1
      set re [string range $re 1 end]
    }
    set numbers 0
    if {[string index $re 0] eq "#"} {
      set numbers 1
      set re [string range $re 1 end]
    }
    set re [string range $re 1 end-1]
    if {[string index $re 0] eq "*"} {
      set ok [string match $re $result]
    } else {
      if {$numbers} {
        set re [string map {# {[-0-9.]+}} $re]
      }
      set ok [regexp $re $result]
    }
    if {$negate} {
      set ok [expr {!$ok}]
    }
  } elseif {[string first "*" $expected] != -1} {
    # Glob pattern match (only if expected string contains a literal '*')
    set ok [string match $expected $result]
  } else {
    # Exact match - handle both list and string formats with true mathematical fallback.
    # A value that is not a valid TCL list (e.g. an error message with an
    # unmatched brace) can only be compared as a string.
    set is_lists [expr {![catch {llength $expected} nexpected] &&
                        ![catch {llength $result} nresult]}]
    if {$is_lists && ($nexpected > 1 || $nresult > 1)} {
      # List comparison
      set ok [expr {$nresult == $nexpected}]
      if {$ok} {
        for {set i 0} {$i < $nresult} {incr i} {
          set r [lindex $result $i]
          set e [lindex $expected $i]
          if {$r ne $e} {
            if {[string is double -strict $r] && [string is double -strict $e]} {
              # True mathematical comparison for floating point noise
              if {![floats_equal $r $e]} {
                set ok 0; break
              }
            } else {
              set ok 0; break
            }
          }
        }
      }
    } else {
      # String comparison
      set r [string trim $result]
      set e [string trim $expected]
      if {$r ne $e} {
        if {[string is double -strict $r] && [string is double -strict $e]} {
          # True mathematical comparison for floating point noise
          set ok [floats_equal $r $e]
        } else {
          set ok 0
        }
      } else {
        set ok 1
      }
    }
  }

  if {$ok} {
    puts "Ok"
  } else {
    puts "FAILED"
    puts "  Expected: [truncate_for_output $expected]"
    puts "  Got:      [truncate_for_output $result]"
    lappend TC(fail_list) $name
    incr TC(errors)
  }
}

# Some tests compare values of many megabytes; printing them in full
# turns the run log into gigabytes. Show the start and the total size.
proc truncate_for_output {value} {
  set limit 2000
  if {[string length $value] <= $limit} {
    return $value
  }
  return "[string range $value 0 [expr {$limit - 1}]]... ([string length $value] bytes)"
}

# Upstream prefixes a test name with $testprefix only when the name
# starts with a digit; names that already carry a prefix are left alone.
proc fix_testname {varname} {
  upvar $varname testname
  if {[info exists ::testprefix] && $::testprefix ne ""
   && [string is digit [string range $testname 0 0]]
  } {
    set testname "${::testprefix}-$testname"
  }
}

# Execute SQL test with expected results. Accepts upstream's optional
# leading "-db HANDLE" to run against a connection other than "db".
proc do_execsql_test {args} {
  set db db
  if {[lindex $args 0] eq "-db"} {
    set db [lindex $args 1]
    set args [lrange $args 2 end]
  }
  if {[llength $args] == 2} {
    lassign $args name sql
    set expected {}
  } elseif {[llength $args] == 3} {
    lassign $args name sql expected
  } else {
    error "wrong # args: should be \"do_execsql_test ?-db DB? name sql ?expected?\""
  }
  uplevel [list do_test $name [list execsql $sql $db] [list {*}$expected]]
}

# Execute SQL test expecting an error
proc do_catchsql_test {args} {
  set db db
  if {[lindex $args 0] eq "-db"} {
    set db [lindex $args 1]
    set args [lrange $args 2 end]
  }
  if {[llength $args] != 3} {
    error "wrong # args: should be \"do_catchsql_test ?-db DB? name sql expected\""
  }
  lassign $args name sql expected
  uplevel [list do_test $name [list catchsql $sql $db] $expected]
}

# Placeholder for virtual table conditional tests
proc do_execsql_test_if_vtab {name sql expected} {
  # For now, just run the test (assume vtab support)
  do_execsql_test $name $sql $expected
}

# Database integrity check
proc integrity_check {name} {
  do_execsql_test $name {PRAGMA integrity_check} {ok}
}

# Query execution plan test (simplified)
proc do_eqp_test {args} {
  set db db
  if {[lindex $args 0] eq "-db"} {
    set db [lindex $args 1]
    set args [lrange $args 2 end]
  }
  lassign $args name sql expected
  uplevel [list do_execsql_test -db $db $name "EXPLAIN QUERY PLAN $sql" $expected]
}

# Run the plan check and then the query itself, as upstream.
proc do_eqp_execsql_test {name sql eqp res} {
  uplevel [list do_eqp_test $name.eqp $sql $eqp]
  uplevel [list do_execsql_test $name.res $sql $res]
}

# Capability checking (simplified - assume all features available)
proc ifcapable {expr code {else_keyword ""} {elsecode ""}} {
  # Check capabilities and execute appropriate code
  set capable 1

  # Simple capability checking for common features
  foreach capability [split $expr {&|}] {
    set capability [string trim $capability]
    set negate 0
    if {[string index $capability 0] eq "!"} {
      set negate 1
      set capability [string range $capability 1 end]
    }

    # Check specific capabilities
    set has_capability 1
    switch -- $capability {
      "autovacuum" { set has_capability [expr {$::AUTOVACUUM != 0}] }
      "vacuum" { set has_capability [expr {$::OMIT_VACUUM == 0}] }
      "tempdb" { set has_capability 1 }
      "attach" { set has_capability 1 }
      "compound" { set has_capability 1 }
      "subquery" { set has_capability 1 }
      "view" { set has_capability 1 }
      "trigger" { set has_capability 1 }
      "foreignkey" { set has_capability 1 }
      "check" { set has_capability 1 }
      "vtab" { set has_capability 1 }
      "rtree" { set has_capability 0 }
      "fts3" { set has_capability 0 }
      "fts4" { set has_capability 0 }
      "fts5" { set has_capability 0 }
      "json1" { set has_capability 1 }
      "windowfunc" { set has_capability 1 }
      "altertable" { set has_capability 1 }
      "analyze" { set has_capability 1 }
      "cte" { set has_capability 1 }
      "with" { set has_capability 1 }
      "upsert" { set has_capability 1 }
      "gencol" { set has_capability 1 }
      "generated_always" { set has_capability 1 }
      "update_delete_limit" { set has_capability 0 }
      "utf16" { set has_capability 0 }
      default { set has_capability 1 }
    }

    if {$negate} {
      set has_capability [expr {!$has_capability}]
    }

    # Handle AND/OR logic (simplified - just use AND for now)
    if {!$has_capability} {
      set capable 0
      break
    }
  }

  # Propagate return codes (like `return` inside the block) to the caller, so
  # tests that early-exit with `ifcapable !foo { finish_test; return }` work.
  if {$capable} {
    set c [catch {uplevel 1 $code} r]
    return -code $c $r
  } elseif {$else_keyword eq "else" && $elsecode ne ""} {
    set c [catch {uplevel 1 $elsecode} r]
    return -code $c $r
  }
}

# Capability test (simplified)
proc capable {expr} {
  # For simplicity, assume all capabilities are available
  return 1
}

# Sanitizer detection (simplified - assume no sanitizers)
proc clang_sanitize_address {} {
  return 0
}

# SQLite configuration constants (set to reasonable defaults)
# These are typically set based on compile-time options
set SQLITE_MAX_COMPOUND_SELECT 500
set SQLITE_MAX_VDBE_OP 25000
set SQLITE_MAX_FUNCTION_ARG 127
set SQLITE_MAX_ATTACHED 10
set SQLITE_MAX_VARIABLE_NUMBER 999
set SQLITE_MAX_COLUMN 2000
set SQLITE_MAX_SQL_LENGTH 1000000
# Turso does not enforce SQLite's default 1e9 string-length limit, so
# report the practical 32-bit cap; tests guarded on a smaller limit
# (e.g. printf.test's 2e9-width allocation probe) skip themselves.
set SQLITE_MAX_LENGTH 2147483647
set SQLITE_MAX_EXPR_DEPTH 1000
set SQLITE_MAX_LIKE_PATTERN_LENGTH 50000
set SQLITE_MAX_TRIGGER_DEPTH 1000

# SQLite compile-time option variables
set AUTOVACUUM 1      ;# Whether AUTOVACUUM is enabled
set OMIT_VACUUM 0     ;# Whether VACUUM is omitted
set TEMP_STORE 1      ;# Where temp tables are stored (0=disk, 1=file, 2=memory)
set DEFAULT_AUTOVACUUM 0  ;# Default autovacuum setting

# Support for sqlite3_limit command at the global level
# This is called as sqlite3_limit db LIMIT_TYPE ?VALUE?
proc sqlite3_limit {db limit_type {value {}}} {
  # If a value is provided, we're setting the limit
  if {$value ne ""} {
    return $value
  } else {
    switch -- $limit_type {
      SQLITE_LIMIT_COMPOUND_SELECT { return 500 }
      SQLITE_LIMIT_VDBE_OP { return 25000 }
      SQLITE_LIMIT_FUNCTION_ARG { return 127 }
      SQLITE_LIMIT_ATTACHED { return 10 }
      SQLITE_LIMIT_VARIABLE_NUMBER { return 999 }
      SQLITE_LIMIT_COLUMN { return 2000 }
      SQLITE_LIMIT_SQL_LENGTH { return 1000000 }
      SQLITE_LIMIT_EXPR_DEPTH { return 1000 }
      SQLITE_LIMIT_LIKE_PATTERN_LENGTH { return 50000 }
      SQLITE_LIMIT_TRIGGER_DEPTH { return 1000 }
      default { return 1000000 }
    }
  }
}

# Support for sqlite3_db_config command
proc sqlite3_db_config {db option {value {}}} {
  if {$value ne ""} {
    return 0
  } else {
    switch -- $option {
      SQLITE_DBCONFIG_DQS_DML { return 0 }
      SQLITE_DBCONFIG_DQS_DDL { return 0 }
      SQLITE_DBCONFIG_LOOKASIDE { return {1 1200 100} }
      SQLITE_DBCONFIG_ENABLE_FKEY { return 0 }
      SQLITE_DBCONFIG_ENABLE_TRIGGER { return 1 }
      SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER { return 0 }
      SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION { return 0 }
      SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE { return 0 }
      SQLITE_DBCONFIG_ENABLE_QPSG { return 0 }
      SQLITE_DBCONFIG_TRIGGER_EQP { return 0 }
      SQLITE_DBCONFIG_RESET_DATABASE { return 0 }
      SQLITE_DBCONFIG_DEFENSIVE { return 0 }
      SQLITE_DBCONFIG_WRITABLE_SCHEMA { return 0 }
      SQLITE_DBCONFIG_LEGACY_ALTER_TABLE { return 0 }
      SQLITE_DBCONFIG_ENABLE_VIEW { return 1 }
      SQLITE_DBCONFIG_LEGACY_FILE_FORMAT { return 0 }
      SQLITE_DBCONFIG_TRUSTED_SCHEMA { return 1 }
      default { return 0 }
    }
  }
}

# Support for optimization_control command
proc optimization_control {db optimization setting} {
  return ""
}

# Run every statement in $sql through the C-API commands
# (sqlite3_prepare/step/finalize), collecting result values. Returns
# {1 errmsg} on error, or 0 followed by the collected values. Ported
# from upstream tester.tcl.
proc stepsql {dbptr sql} {
  set sql [string trim $sql]
  set r 0
  while {[string length $sql]>0} {
    if {[catch {sqlite3_prepare $dbptr $sql -1 sqltail} vm]} {
      return [list 1 $vm]
    }
    set sql [string trim $sqltail]
    while {[sqlite3_step $vm]=="SQLITE_ROW"} {
      for {set i 0} {$i<[sqlite3_data_count $vm]} {incr i} {
        lappend r [sqlite3_column_text $vm $i]
      }
    }
    if {[catch {sqlite3_finalize $vm} errmsg]} {
      return [list 1 $errmsg]
    }
  }
  return $r
}

# The hexio_* commands from upstream test_hexio.c, reimplemented in
# plain TCL: they only do file I/O and hex conversion, no engine access.

# Read AMT bytes at OFFSET from FILENAME, returned as uppercase hex.
# Reading past the end of the file returns the bytes that exist.
proc hexio_read {filename offset amt} {
  set fd [open $filename rb]
  seek $fd $offset
  set data [read $fd $amt]
  close $fd
  binary scan $data H* hex
  return [string toupper $hex]
}

# Write the hex-encoded DATA into FILENAME at OFFSET, creating the file
# if needed. Returns the number of bytes written.
proc hexio_write {filename offset hexdata} {
  set data [binary format H* $hexdata]
  if {[file exists $filename]} {
    set fd [open $filename r+b]
  } else {
    set fd [open $filename wb]
  }
  seek $fd $offset
  puts -nonewline $fd $data
  close $fd
  return [string length $data]
}

# Interpret a hex string as a 32-bit integer: big-endian by default,
# little-endian with -l. Shorter input is zero-padded on the high end;
# longer input uses only the first four bytes, as upstream.
proc hexio_get_int {args} {
  set little 0
  if {[llength $args]==2} {
    if {[lindex $args 0] eq "-l"} { set little 1 }
    set hex [lindex $args 1]
  } else {
    set hex [lindex $args 0]
  }
  set data [binary format H* $hex]
  set n [string length $data]
  binary scan $data c* bytes
  set bytes [lmap b $bytes {expr {$b & 0xff}}]
  if {$n >= 4} {
    set bytes [lrange $bytes 0 3]
  } else {
    while {[llength $bytes] < 4} { set bytes [linsert $bytes 0 0] }
  }
  if {$little} { set bytes [lreverse $bytes] }
  lassign $bytes b0 b1 b2 b3
  set val [expr {($b0<<24) | ($b1<<16) | ($b2<<8) | $b3}]
  # The upstream command returns a signed 32-bit C int.
  if {$val > 0x7fffffff} { set val [expr {$val - (1<<32)}] }
  return $val
}

# Render an integer as 4 or 8 uppercase hex digits, big-endian.
proc hexio_render_int16 {value} {
  return [string toupper [binary encode hex [binary format S $value]]]
}
proc hexio_render_int32 {value} {
  return [string toupper [binary encode hex [binary format I $value]]]
}

# Upstream harness directives telling the test framework whether extra
# corruption checks may fire; we run no such checks, so they are no-ops.
proc database_may_be_corrupt {} {}
proc database_never_corrupt {} {}

# Turso never reserves bytes at the end of each page (that is a codec
# feature), so tests guarded on a reserved-bytes build always run.
proc nonzero_reserved_bytes {} {
  return 0
}

# Whether the file-system supports atomic batch writes (F2FS); plain
# filesystems do not, which is also upstream's common answer.
proc atomic_batch_write {file} {
  return 0
}

# Drop every table, view and explicitly created index, ported verbatim
# from upstream tester.tcl. Tests use these to reset the schema without
# recreating the database file.
proc drop_all_tables {{db db}} {
  ifcapable trigger&&foreignkey {
    set pk [$db one "PRAGMA foreign_keys"]
    $db eval "PRAGMA foreign_keys = OFF"
  }
  foreach {idx name file} [db eval {PRAGMA database_list}] {
    if {$idx==1} {
      set master sqlite_temp_master
    } else {
      set master $name.sqlite_master
    }
    foreach {t type} [$db eval "
      SELECT name, type FROM $master
      WHERE type IN('table', 'view') AND name NOT LIKE 'sqliteX_%' ESCAPE 'X'
    "] {
      $db eval "DROP $type \"$t\""
    }
  }
  ifcapable trigger&&foreignkey {
    $db eval "PRAGMA foreign_keys = $pk"
  }
}

proc drop_all_indexes {{db db}} {
  set L [$db eval {
    SELECT name FROM sqlite_master WHERE type='index' AND sql LIKE 'create%'
  }]
  foreach idx $L { $db eval "DROP INDEX $idx" }
}

# Run a batch of {name sql result} SELECT tests, ported verbatim from
# upstream tester.tcl.
proc do_select_tests {prefix args} {

  set testlist [lindex $args end]
  set switches [lrange $args 0 end-1]

  set errfmt ""
  set countonly 0
  set tclquery ""
  set repair ""

  for {set i 0} {$i < [llength $switches]} {incr i} {
    set s [lindex $switches $i]
    set n [string length $s]
    if {$n>=2 && [string equal -length $n $s "-query"]} {
      set tclquery [list execsql [lindex $switches [incr i]]]
    } elseif {$n>=2 && [string equal -length $n $s "-tclquery"]} {
      set tclquery [lindex $switches [incr i]]
    } elseif {$n>=2 && [string equal -length $n $s "-errorformat"]} {
      set errfmt [lindex $switches [incr i]]
    } elseif {$n>=2 && [string equal -length $n $s "-repair"]} {
      set repair [lindex $switches [incr i]]
    } elseif {$n>=2 && [string equal -length $n $s "-count"]} {
      set countonly 1
    } else {
      error "unknown switch: $s"
    }
  }

  if {$countonly && $errfmt!=""} {
    error "Cannot use -count and -errorformat together"
  }
  set nTestlist [llength $testlist]
  if {$nTestlist%3 || $nTestlist==0 } {
    error "SELECT test list contains [llength $testlist] elements"
  }

  eval $repair
  foreach {tn sql res} $testlist {
    if {$tclquery != ""} {
      execsql $sql
      uplevel do_test ${prefix}.$tn [list $tclquery] [list [list {*}$res]]
    } elseif {$countonly} {
      set nRow 0
      db eval $sql {incr nRow}
      uplevel do_test ${prefix}.$tn [list [list set {} $nRow]] [list $res]
    } elseif {$errfmt==""} {
      uplevel do_execsql_test ${prefix}.${tn} [list $sql] [list [list {*}$res]]
    } else {
      set res [list 1 [string trim [format $errfmt {*}$res]]]
      uplevel do_catchsql_test ${prefix}.${tn} [list $sql] [list $res]
    }
    eval $repair
  }

}

# Real-number comparison helpers, ported verbatim from upstream
# tester.tcl: different TCL versions display floating point values
# differently, so both sides are normalized before comparing.
proc realnum_normalize {r} {
  string map {1.#INF inf Inf inf .0e e} [regsub -all {(e[+-])0+} $r {\1}]
}

proc do_realnum_test {name cmd expected} {
  uplevel [list do_test $name [
    subst -nocommands { realnum_normalize [ $cmd ] }
  ] [realnum_normalize $expected]]
}

# Assert the extended error code of a connection, ported from upstream
# tester.tcl.
proc verify_ex_errcode {name expected {db db}} {
  do_test $name [list sqlite3_extended_errcode $db] $expected
}

# Run a query and assert a bound on its VM step count, ported from
# upstream tester.tcl over our [db status vmstep].
proc do_vmstep_test {tn sql nstep {res {}}} {
  uplevel [list do_execsql_test $tn.0 $sql $res]

  set vmstep [db status vmstep]
  if {[string range $nstep 0 0]=="+"} {
    set body "if {$vmstep<$nstep} {
      error \"got $vmstep, expected more than [string range $nstep 1 end]\"
    }"
  } else {
    set body "if {$vmstep>$nstep} {
      error \"got $vmstep, expected less than $nstep\"
    }"
  }

  set name "$tn.1"
  uplevel [list do_test $name $body {}]
}

# TCL 8.5+ integers are arbitrary precision, so 64-bit arithmetic
# always works; upstream probes the platform here.
proc working_64bit_int {} {
  return 1
}

# Turso has no soft heap limit; tests only save and restore the value,
# so report it as unset.
proc sqlite3_soft_heap_limit {args} {
  return 0
}
proc sqlite3_soft_heap_limit64 {args} {
  return 0
}

# File operation utilities
proc forcedelete {args} {
  foreach filename $args {
    catch {file delete -force $filename}
  }
}

proc delete_file {args} {
  foreach filename $args {
    file delete $filename
  }
}

proc forcecopy {from to} {
  catch {file delete -force $to}
  file copy -force $from $to
}

# Save and restore snapshots of the test database and its sidecar files
# (-wal, -journal), as in upstream tester.tcl. Tests use these to rewind
# the database to a known state, e.g. before injected corruption.
proc db_save {} {
  foreach f [glob -nocomplain sv_test.db*] { forcedelete $f }
  foreach f [glob -nocomplain test.db*] {
    set f2 "sv_$f"
    forcecopy $f $f2
  }
}
proc db_save_and_close {} {
  db_save
  catch { db close }
  return ""
}
proc db_restore {} {
  foreach f [glob -nocomplain test.db*] { forcedelete $f }
  foreach f2 [glob -nocomplain sv_test.db*] {
    set f [string range $f2 3 end]
    forcecopy $f2 $f
  }
}
proc db_restore_and_reopen {{dbfile test.db}} {
  catch { db close }
  db_restore
  sqlite3 db $dbfile
}
proc db_delete_and_reopen {{file test.db}} {
  catch { db close }
  foreach f [glob -nocomplain test.db*] { forcedelete $f }
  sqlite3 db $file
}

proc copy_file {from to} {
  file copy $from $to
}

# Finish test execution and report results
proc finish_test {} {
  global TC

  # Check if we're running as part of all.test - if so, don't exit
  if {[info exists ::ALL_TESTS]} {
    return
  }

  puts ""
  puts "=========================================="
  if {$TC(errors) == 0} {
    puts "All $TC(count) tests passed!"
  } else {
    puts "$TC(errors) errors out of $TC(count) tests"
    puts "Failed tests: $TC(fail_list)"
  }
  puts "=========================================="
}

reset_db
