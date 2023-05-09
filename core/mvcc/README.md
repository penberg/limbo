# MVCC for Rust

This is a _work-in-progress_ the Hekaton optimistic multiversion concurrency control library in Rust.
The aim of the project is to provide a building block for implementing database management systems.

## Features

* Main memory architecture, rows are accessed via an index
* Optimistic multi-version concurrency control
* Rust and C APIs

## Experimental Evaluation

**Single-threaded micro-benchmarks**

Operations                         | Throughput
-----------------------------------|------------
`begin_tx`, `read`, and `commit`   | 2.2M ops/second
`begin_tx`, `update`, and `commit` | 2.2M ops/second
`read`                             | 12.9M ops/second
`update`                           | 6.2M ops/second

(The `cargo bench` was run on a AMD Ryzen 9 3900XT 2.2 GHz CPU.)

## Development

Run tests:

```console
cargo test
```

Test coverage report:

```console
cargo tarpaulin -o html
```

Run benchmarks:

```console
cargo bench
```

Run benchmarks and generate flamegraphs:

```console
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
cargo bench --bench my_benchmark -- --profile-time=5
```

## References

Larson et al. [High-Performance Concurrency Control Mechanisms for Main-Memory Databases](https://vldb.org/pvldb/vol5/p298_per-akelarson_vldb2012.pdf). VLDB '11

Paper errata: The visibility check in Table 2 is wrong and causes uncommitted delete to become visible to transactions (fixed in [commit 6ca3773]( https://github.com/penberg/mvcc-rs/commit/6ca377320bb59b52ecc0430b9e5e422e8d61658d)).
