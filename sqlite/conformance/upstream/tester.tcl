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
  set ::DB [sqlite3_connection_pointer db]
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

    # Check specific capabilities: the sqlite_options table first (that is
    # what upstream consults), then the legacy switch below.
    set has_capability 1
    if {[info exists ::sqlite_options($capability)]} {
      set has_capability $::sqlite_options($capability)
    } else {
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

# The sqlite_options table that upstream's testfixture exports from its
# compile-time configuration. 1 means the feature exists. Almost every
# entry is 1 even where Turso lacks the feature, so the file runs and its
# tests fail on their own assertions instead of the file skipping itself.
# The 0 entries are modules (fts, rtree, utf16 encoding, sessions) where
# every test in the file would stop the file at its first statement.
array set sqlite_options {
  altertable 1 analyze 1 api_armor 1 atomicwrite 1 attach 1 auth 1
  autoinc 1 autoindex 1 autoreset 1 autovacuum 1 between_opt 1 bloblit 1
  builtin_test 1 cast 1 check 1 columnmetadata 1 compileoption_diags 1
  complete 1 compound 1 conflict 1 crashtest 1 cte 1 datetime 1
  dbpage_vtab 1 dbstat_vtab 1 decltype 1 deprecated 1 deserialize 1
  direct_read 1 explain 1 floatingpoint 1 foreignkey 1 fts1 0 fts2 0
  fts3 0 fts4 0 fts5 0 gencol 1 generated_always 1 geopoly 0 getmutex 1
  icu 1 icu_collations 1 incrblob 1 incrvacuum 1 integrityck 1 json1 1
  like_opt 1 load_ext 1 lock_proxy_pragmas 1 long_double 1 lookaside 1
  malloc_usable_size 1 math 1 mem3 1 mem5 1 memdebug 0 memorymanage 1
  memsys3 1 memsys5 1 mergesort 1 mmap 1 mutex 1 mutex_noop 1
  normalize 1 offset_sql_func 1 oversize_cell_check 1 pager_pragmas 1
  pragma 1 prefer_proxy_locking 0 preupdate_hook 1 progress 1 reindex 1
  rtree 0 rtree_int_only 0 schema_pragmas 1 schema_version 1
  secure_delete 1 session 0 shared_cache 1 snapshot 1 stat4 1
  stmt_scanstatus 1 subquery 1 tclvar 1 tempdb 1 threadsafe 1
  threadsafe1 1 threadsafe2 1 trace 1 trigger 1 truncate_opt 1
  unlock_notify 1 update_delete_limit 0 upsert 1 uri 1 utf16 0 vacuum 1
  view 1 vtab 1 wal 1 windowfunc 1 with 1 worker_threads 1 wsd 1
  casesensitivelike 0 default_autovacuum 0 default_ckptfullfsync 0
  default_memstatus 0 secure_delete 1 windowfunc 1
}
if {![info exists ::SQLITE_DEFAULT_SYNCHRONOUS]} { set ::SQLITE_DEFAULT_SYNCHRONOUS 2 }
if {![info exists ::SQLITE_DEFAULT_WAL_SYNCHRONOUS]} { set ::SQLITE_DEFAULT_WAL_SYNCHRONOUS 2 }
if {![info exists ::SQLITE_MAX_WORKER_THREADS]} { set ::SQLITE_MAX_WORKER_THREADS 0 }
if {![info exists ::sqlite_pending_byte]} { set ::sqlite_pending_byte 0x40000000 }
if {![info exists ::cmdlinearg(soft-heap-limit)]} { set ::cmdlinearg(soft-heap-limit) 0 }
if {![info exists ::cmdlinearg(TESTFIXTURE_HOME)]} { set ::cmdlinearg(TESTFIXTURE_HOME) [pwd] }
if {![info exists ::cmdlinearg(binarylog)]} { set ::cmdlinearg(binarylog) 0 }
if {![info exists ::cmdlinearg(maxerror)]} { set ::cmdlinearg(maxerror) 1000 }
if {![info exists ::cmdlinearg(malloctrace)]} { set ::cmdlinearg(malloctrace) 0 }
if {![info exists ::cmdlinearg(verbose)]} { set ::cmdlinearg(verbose) 0 }
if {![info exists ::G(isquick)]} { set ::G(isquick) 0 }
if {![info exists ::sqlite_open_file_count]} { set ::sqlite_open_file_count 0 }
if {![info exists ::SQLITE_MAX_PAGE_SIZE]} { set ::SQLITE_MAX_PAGE_SIZE 65536 }
if {![info exists ::bitmask_size]} { set ::bitmask_size 64 }
proc isquick {} { return 0 }
proc autoinstall_test_functions {args} { return "" }
proc sqlite_register_test_function {args} { return "" }
proc dbconfig_maindbname_icecube {args} { return "" }
proc test_create_sumint {args} { return "" }
proc sqlite3_setlk_timeout {args} { return SQLITE_OK }
proc sqlite3_config_sqllog {args} { return "" }
proc sqlite3_config_alt_pcache {args} { return "" }
proc register_devsim {args} { return "" }
proc unregister_devsim {args} { return "" }
proc sqlite3_open_v2 {filename flags {vfs ""}} { sqlite3_open $filename }
proc test_syscall {args} { return "" }
proc clear_mutex_counters {args} { return "" }
proc install_mutex_counters {args} { return "" }
proc read_mutex_counters {args} { return "" }
proc thread_spawn {args} { return "" }
proc thread_result {args} { return "" }
proc thread_wait {args} { return "" }
proc do_filepath_test {name cmd expected} {
  uplevel [list do_test $name [
    subst -nocommands { filepath_normalize [ $cmd ] }
  ] [filepath_normalize $expected]]
}

# Commands that upstream's testfixture binary provides from C and that
# have no Turso equivalent. They are no-ops so a file that calls them at
# top level keeps running; tests that depend on their effect fail on
# their own assertions instead of taking the whole file down with them.
proc sqlite3_test_control {args} { return "" }
proc sqlite3_test_control_pending_byte {args} { return $::sqlite_pending_byte }
proc sqlite3_shutdown {args} { return "" }
proc sqlite3_initialize {args} { return "" }
proc sqlite3_reset_auto_extension {args} { return "" }
proc sqlite3_enable_shared_cache {args} { return 0 }
proc sqlite3_config {args} { return SQLITE_OK }
proc sqlite3_config_uri {args} { return SQLITE_OK }
proc sqlite3_config_lookaside {args} { return SQLITE_OK }
proc sqlite3_config_pagecache {args} { return SQLITE_OK }
proc sqlite3_config_memstatus {args} { return SQLITE_OK }
proc sqlite3_config_cis {args} { return SQLITE_OK }
proc sqlite3_config_pmasz {args} { return SQLITE_OK }
proc sqlite3_config_sorterref {args} { return SQLITE_OK }
proc sqlite3_config_heap {args} { return SQLITE_OK }
proc sqlite3_config_error {args} { return SQLITE_OK }
proc sqlite3_db_config_lookaside {args} { return 0 }
proc sqlite3_db_status {args} { return {0 0 0} }
proc sqlite3_status {args} { return {0 0 0} }
proc sqlite3_memory_used {args} { return 0 }
proc sqlite3_memory_highwater {args} { return 0 }
proc sqlite3_memdebug_fail {args} { return 0 }
proc sqlite3_memdebug_settitle {args} { return "" }
proc sqlite3_memdebug_log {args} { return "" }
proc sqlite3_memdebug_malloc_count {args} { return 0 }
proc sqlite3_memdebug_pending {args} { return -1 }
proc sqlite3_memdebug_backtrace {args} { return "" }
proc sqlite3_memdebug_dump {args} { return "" }
proc sqlite3_memdebug_vfs_oom_test {args} { return 0 }
proc sqlite3_release_memory {args} { return 0 }
proc sqlite3_db_release_memory {args} { return 0 }
proc sqlite3_db_cacheflush {args} { return 0 }
proc sqlite3_stmt_status {args} { return 0 }
proc sqlite3_stmt_scanstatus {args} { return "" }
proc sqlite3_stmt_scanstatus_reset {args} { return "" }
proc sqlite3_system_errno {args} { return 0 }
proc sqlite3_mmap_warm {args} { return SQLITE_OK }
proc sqlite3_autovacuum_pages {args} { return "" }
proc sqlite3_rekey {args} { return "" }
proc sqlite3_key {args} { return "" }
proc sqlite3_normalize {sql} { return $sql }
proc sqlite3_expanded_sql {args} { return "" }
proc sqlite3_column_database_name {args} { return "" }
proc sqlite3_column_origin_name {args} { return "" }
proc sqlite3_create_function {args} { return "" }
proc sqlite3_create_function_v2 {args} { return "" }
proc sqlite3_create_aggregate {args} { return "" }
proc sqlite3_create_window_function {args} { return "" }
proc sqlite3_create_collation_v2 {args} { return "" }
proc sqlite3_multiplex_initialize {args} { return "" }
proc sqlite3_multiplex_shutdown {args} { return "" }
proc sqlite3_register_cksumvfs {args} { return "" }
proc sqlite3_unregister_cksumvfs {args} { return "" }
proc sqlite3_simulate_device {args} { return "" }
proc sqlite3_sleep {ms} { after $ms; return $ms }
proc sqlite3_snapshot_get {args} { error "snapshots are not supported" }
proc sqlite3_snapshot_get_blob {args} { error "snapshots are not supported" }
proc sqlite3_snapshot_recover {args} { error "snapshots are not supported" }
proc sqlite3_snapshot_open {args} { error "snapshots are not supported" }
proc sqlite3_snapshot_free {args} { return "" }
proc sqlite3_txn_state {args} { return 0 }
proc sqlite3_stmt_explain {args} { return 0 }
proc sqlite3_bind_pointer {args} { return "" }
proc sqlite3_bind_value_from_preupdate {args} { return "" }
proc sqlite3_bind_value_from_select {args} { return "" }
proc sqlite3_carray_bind {args} { return "" }
proc sqlite3_preupdate_count {args} { return 0 }
proc sqlite3_preupdate_depth {args} { return 0 }
proc sqlite3_preupdate_new {args} { return "" }
proc sqlite3_preupdate_old {args} { return "" }
proc save_prng_state {args} { return "" }
proc restore_prng_state {args} { return "" }
proc reset_prng_state {args} { return "" }
proc extra_schema_checks {args} { return "" }
proc test_set_config_pagecache {args} { return "" }
proc test_restore_config_pagecache {args} { return "" }
proc sqlite3_soft_heap_limit_set {args} { return 0 }
proc uses_stmt_journal {args} { return 0 }
proc sql_uses_stmt {db sql} { return 0 }
proc pcache_stats {args} { return {current 0 max 0 min 0 recyclable 0} }
proc btree_from_db {args} { return "" }
proc btree_pager_stats {args} { return "" }
proc vfs_shmlock {args} { return "" }
proc vfs_set_readmark {args} { return "" }
proc vfs_unlink_test {args} { return "" }
proc vfs_initfail_test {args} { return "" }
proc vfs_reregister_all {args} { return "" }
proc file_control_test {args} { return "" }
proc file_control_lasterrno_test {args} { return "" }
proc file_control_lockproxy_test {args} { return "" }
proc file_control_chunksize_test {args} { return "" }
proc file_control_sizehint_test {args} { return "" }
proc file_control_win32_av_retry {args} { return "" }
proc file_control_persist_wal {args} { return "" }
proc file_control_powersafe_overwrite {args} { return "" }
proc file_control_vfsname {args} { return "" }
proc file_control_reservebytes {args} { return "" }
proc file_control_tempfilename {args} { return "" }
proc file_control_external_reader {args} { return "" }
proc file_control_data_version {args} { return 0 }
proc load_static_extension {args} { return "" }
proc register_echo_module {args} { return "" }
proc register_tcl_module {args} { return "" }
proc register_fs_module {args} { return "" }
proc register_dbstat_vtab {args} { return "" }
proc register_wholenumber_module {args} { return "" }
proc register_schema_module {args} { return "" }
proc register_tclvar_module {args} { return "" }
proc register_intarray_module {args} { return "" }
proc register_demovfs {args} { return "" }
proc unregister_demovfs {args} { return "" }
proc run_thread_tests {args} { return "" }
proc sqlite3_thread_cleanup {args} { return "" }
proc tcl_objproc {args} { return "" }
proc tcl_variable_type {varname} { return "" }
proc getsubtype {args} { return 0 }
# strftime FORMAT SECONDS from upstream test1.c: the C library's strftime
# on a UTC broken-down time. TCL's clock format takes the same % codes.
proc strftime {format seconds} {
  set format [string map {%F %Y-%m-%d} $format]
  clock format [expr {int($seconds)}] -format $format -gmt 1
}
proc sqlite3_libversion {args} { return 3.46.0 }
proc sqlite3_libversion_number {args} { return 3046000 }
proc sqlite3_sourceid {args} { return "" }
proc sqlite3_compileoption_used {args} { return 0 }
proc sqlite3_compileoption_get {args} { return "" }
proc test_find_cli {args} { return "" }
proc test_find_sqldiff {args} { return "" }
proc test_find_binary {args} { return "" }
proc test_binary_name {args} { return "" }

# testvfs NAME ?options? creates a command NAME in upstream; here it
# creates one that accepts every subcommand and does nothing.
proc testvfs {name args} {
  proc ::$name {args} { return "" }
  return $name
}
proc sqlite3_backup {name db1 dbname1 db2 dbname2} {
  proc ::$name {args} {
    switch -- [lindex $args 0] {
      step { return SQLITE_DONE }
      finish { return SQLITE_OK }
      remaining { return 0 }
      pagecount { return 0 }
      default { return "" }
    }
  }
  return $name
}

# Fault-injection drivers from upstream malloc_common.tcl need the memdebug
# allocator; without it they run nothing. The faultsim_* file helpers
# only copy files around, so those are ported.
proc do_faultsim_test {args} { return "" }
proc do_malloc_test {args} { return "" }
proc do_ioerr_test {args} { return "" }
proc do_one_faultsim_test {args} { return "" }
proc run_ioerr_prep {args} { return "" }
proc faultsim_save {args} {
  db_save
  foreach f [glob -nocomplain *] {
    if {[string match "sv_*" $f] || $f eq "test.db"} continue
    if {[string match "test.db-*" $f]} {
      forcecopy $f sv_$f
    }
  }
}
proc faultsim_save_and_close {} {
  faultsim_save
  catch { db close }
  return ""
}
proc faultsim_restore {} {
  db_restore
}
proc faultsim_restore_and_reopen {{dbfile test.db}} {
  catch { db close }
  faultsim_restore
  sqlite3 db $dbfile
  sqlite3_extended_result_codes db 1
  sqlite3_db_config_lookaside db 0 0 0
}
proc faultsim_delete_and_reopen {{file test.db}} {
  catch { db close }
  foreach f [glob -nocomplain test.db*] { forcedelete $f }
  sqlite3 db $file
}
proc faultsim_integrity_check {{db db}} {
  set ic [$db eval { PRAGMA integrity_check }]
  if {$ic != "ok"} { error "Integrity check: $ic" }
}
proc db_enter {db} { return "" }
proc db_leave {db} { return "" }
proc presql {args} { return "" }
proc catchcmd {db {cmd ""}} { return {1 {command-line shell not available}} }
proc catchcmdex {db {cmd ""}} { return {1 {command-line shell not available}} }
proc catchsafecmd {db {cmd ""}} { return {1 {command-line shell not available}} }
proc test_sqlite3_log {args} { return "" }
proc sqlite3_multiplex_control {args} { return SQLITE_OK }
proc sqlite3_multiplex_shutdown {args} { return "" }
proc sorter_test_fakeheap {args} { return "" }
proc sorter_test_sort4_helper {args} { return "" }
proc sqlite3_stmt_busy_v2 {args} { return 0 }

# Pure-TCL helpers ported from upstream tester.tcl.
proc delete_all_data {} {
  db eval {SELECT tbl_name AS t FROM sqlite_master WHERE type = 'table'} {
    db eval "DELETE FROM '[string map {' ''} $t]'"
  }
}
proc omit_test {name reason {append 1}} {
  set omitList [set ::omitList]
  if {$append} {
    lappend omitList [list $name $reason]
  }
  set ::omitList $omitList
}
if {![info exists ::omitList]} { set ::omitList [list] }
proc wal_is_capable {} {
  ifcapable !wal { return 0 }
  if {[permutation]=="journaltest"} { return 0 }
  return 1
}
proc wal_set_journal_mode {{db db}} {
  if { [wal_is_capable] } {
    $db eval "PRAGMA journal_mode = WAL"
  }
}
proc wal_check_journal_mode {testname {db db}} {
  if { [wal_is_capable] } {
    $db eval { SELECT * FROM sqlite_master }
    do_test $testname [list $db eval "PRAGMA main.journal_mode"] {wal}
  }
}
proc wal_is_wal_mode {} {
  expr {[permutation] eq "wal"}
}
proc explain {sql {db db}} {
  puts ""
  puts "addr  opcode        p1      p2      p3      p4               p5  #"
  puts "----  ------------  ------  ------  ------  ---------------  --  -"
  $db eval "explain $sql" {} {
    puts [format {%-4d  %-12.12s  %-6d  %-6d  %-6d  % -17s %s  %s} \
      $addr $opcode $p1 $p2 $p3 $p4 $p5 $comment
    ]
  }
}
proc explain_i {sql {db db}} {
  puts ""
  puts "addr  opcode        p1      p2      p3      p4               p5  #"
  puts "----  ------------  ------  ------  ------  ---------------  --  -"
  $db eval "explain $sql" {} {
    puts [format {%-4d  %-12.12s  %-6d  %-6d  %-6d  % -17s %s  %s} \
      $addr $opcode $p1 $p2 $p3 $p4 $p5 $comment
    ]
  }
  puts "----  ------------  ------  ------  ------  ---------------  --  -"
}
proc explain_no_trace {sql} {
  set tr [db eval "EXPLAIN $sql"]
  return [lrange $tr 7 end]
}
proc md5 {str} {
  if {![catch {package require md5}]} {
    return [string tolower [::md5::md5 -hex $str]]
  }
  set f [file tempfile]
  set fd [open $f wb]
  puts -nonewline $fd $str
  close $fd
  set sum [lindex [exec md5sum $f] 0]
  file delete $f
  return $sum
}
proc md5file {filename {offset 0} {amt -1}} {
  set fd [open $filename rb]
  seek $fd $offset
  if {$amt < 0} {
    set data [read $fd]
  } else {
    set data [read $fd $amt]
  }
  close $fd
  return [md5 $data]
}
proc cksum {{db db}} {
  set txt [$db eval {
    SELECT name, type, sql FROM sqlite_master order by name, type, sql
  }]\n
  foreach tbl [$db eval {
    SELECT name FROM sqlite_master WHERE type='table' order by name
  }] {
    append txt [$db eval "SELECT * FROM $tbl"]\n
  }
  foreach prag {default_synchronous default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  set cksum [string length $txt]-[md5 $txt]
  return $cksum
}
proc allcksum {{db db}} {
  set ret [list]
  ifcapable tempdb {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT name FROM sqlite_temp_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' UNION
      SELECT 'sqlite_temp_master' ORDER BY 1
    }
  } else {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' ORDER BY 1
    }
  }
  set tbllist [$db eval $sql]
  set txt {}
  foreach tbl $tbllist {
    append txt [$db eval "SELECT * FROM $tbl"]
  }
  foreach prag {default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  return [md5 $txt]
}
proc dbcksum {db dbname} {
  if {$dbname=="temp"} {
    set master sqlite_temp_master
  } else {
    set master $dbname.sqlite_master
  }
  set alltab [$db eval "SELECT name FROM $master WHERE type='table'"]
  set txt [$db eval "SELECT * FROM $master"]\n
  foreach tab $alltab {
    append txt [$db eval "SELECT * FROM $dbname.$tab"]\n
  }
  return [md5 $txt]
}
proc do_timed_execsql_test {testname sql {result {}}} {
  uplevel [list do_execsql_test $testname $sql $result]
}
proc dumpbytes {s} {
  set r ""
  for {set i 0} {$i < [string length $s]} {incr i} {
    if {$i > 0} {append r " "}
    append r [format %02X [scan [string index $s $i] %c]]
  }
  return $r
}
proc speed_trial {name numstmt units sql} {
  uplevel [list do_execsql_test $name $sql {}]
}
proc speed_trial_tcl {name numstmt units script} {
  uplevel [list do_test $name $script {}]
}
proc speed_trial_init {name} { return "" }
proc speed_trial_summary {name} { return "" }
proc fail_test {name} {
  incr ::TC(errors)
  lappend ::TC(fail_list) $name
}
proc incr_ntest {} { incr ::TC(count) }
proc filepath_normalize {p} {
  regsub -all {[^/]+/\.\./} $p {} p
  set p
}
proc is_relative_file {file} {
  return [expr {[file pathtype $file] != "absolute"}]
}
proc test_pwd {args} {
  if {[llength $args]==1} {
    set trail [lindex $args 0]
  } else {
    set trail ""
  }
  set pwd [pwd]
  if {[string index $pwd end] eq "/"} {
    set pwd [string range $pwd 0 end-1]
  }
  return "$pwd$trail"
}
proc get_pwd {} {
  if {$::tcl_platform(platform) eq "windows"} {
    return [string map {\\ /} [pwd]]
  }
  return [pwd]
}

# randstr(MIN,MAX) from upstream test_func.c: a random string of letters
# and digits, between MIN and MAX bytes long. Registered on every
# connection the sqlite3 command opens.
set ::randstr_chars "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-!,:*^+=_|?/<> "
proc randstr_impl {min max} {
  set n [expr {int($min)}]
  if {$max > $min} {
    set n [expr {int($min) + int(rand() * ($max - $min + 1))}]
  }
  set s ""
  set nc [string length $::randstr_chars]
  for {set i 0} {$i < $n} {incr i} {
    append s [string index $::randstr_chars [expr {int(rand() * $nc)}]]
  }
  return $s
}
# md5sum is an aggregate in upstream test_md5.c; the binding can only
# register scalar functions, so this is the per-row scalar form. A query
# whose result is compared with an earlier run of the same query still
# works; a query that expects one row for the whole table does not.
proc md5sum_scalar {args} {
  md5 [join $args ""]
}
proc register_test_functions {db} {
  catch {$db func randstr {min max} {randstr_impl $min $max}}
  catch {$db func md5sum {} {md5sum_scalar}}
}

# Wrap the native sqlite3 command so that every new connection also
# carries the test-only SQL functions.
if {[llength [info commands sqlite3_native]] == 0} {
  rename sqlite3 sqlite3_native
}
proc sqlite3 {args} {
  if {[llength $args] == 1 && [string index [lindex $args 0] 0] eq "-"} {
    switch -- [lindex $args 0] {
      -has-codec { return 0 }
      -version { return [sqlite3_libversion] }
      -sourceid { return [sqlite3_sourceid] }
      -tcl-uses-utf { return 1 }
      default { error "unknown option [lindex $args 0]" }
    }
  }
  set r [uplevel 1 [list sqlite3_native {*}$args]]
  if {[llength $args] >= 2} {
    register_test_functions [lindex $args 0]
  }
  return $r
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
set AUTOVACUUM 0      ;# Whether databases are auto-vacuum by default
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

# A child process started by lock_common.tcl's launch_testfixture sources
# this file to get the engine and the helpers, but must not touch the
# database the parent is testing.
if {![info exists ::TURSO_CHILD_PROCESS]} {
  reset_db
}
