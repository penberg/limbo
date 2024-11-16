Adapted from https://github.com/ballista-compute/sqlparser-rs/tree/main/sqlparser_bench

## sqlparser-rs

```
sqlparser-rs parsing benchmark/sqlparser::select
                        time:   [9.9697 µs 10.068 µs 10.184 µs]
Found 14 outliers among 100 measurements (14.00%)
  5 (5.00%) high mild
  9 (9.00%) high severe
sqlparser-rs parsing benchmark/sqlparser::with_select
                        time:   [59.569 µs 60.088 µs 60.743 µs]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe
```

## sqlite3-parser

```
sqlparser-rs parsing benchmark/sqlparser::select
                        time:   [6.5488 µs 6.5773 µs 6.6108 µs]
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe
sqlparser-rs parsing benchmark/sqlparser::with_select
                        time:   [22.182 µs 22.321 µs 22.473 µs]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  4 (4.00%) high severe
```