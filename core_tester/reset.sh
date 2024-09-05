#!/bin/bash

set -ex
export RUST_BACKTRACE=1
rm test.db -f
rm query-log.log -f 
# for now only integer primary key supported
echo "create table test (x INTEGER PRIMARY KEY);" | tee -a query-log.log | sqlite3 test.db
