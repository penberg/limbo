# MVCC for Rust

This is a _work-in-progress_ the Hekaton optimistic multiversion concurrency control library in Rust.
The aim of the project is to provide a building block for implementing database management systems.

## Features

* Main memory architecture, rows are accessed via an index
* Optimistic multi-version concurrency control

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
