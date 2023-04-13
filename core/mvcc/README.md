# MVCC for Rust

This is a _work-in-progress_ Rust implementation of the Hekaton optimistic multiversion concurrency control algorithm.

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
