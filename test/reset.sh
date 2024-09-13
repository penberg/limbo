#!/bin/bash

set -ex
export RUST_BACKTRACE=1
rm -f test.db
rm -f query-log.log
# for now only integer primary key supported
touch test.db
echo "create table test (x INTEGER PRIMARY KEY);" | tee -a query-log.log | sqlite3 test.db
