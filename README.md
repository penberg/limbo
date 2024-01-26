<img src="limbo.png" width="200" align="right" />

# Limbo

A modern SQLite-compatible database library built with Rust.

## Features

* SQLite SQL dialect support ([_wip_](docs/sqlite-compat.md))
* SQLite file format support (_read-only_)
* Asynchronous I/O support with `io_uring` 
* WebAssembly bindings (_wip_)

## Getting Started

Limbo is currently read-only. You can either use the `sqlite3` program to create a database:

```console
$ sqlite3 database.db
SQLite version 3.42.0 2023-05-16 12:36:15
Enter ".help" for usage hints.
sqlite> CREATE TABLE users (id INT PRIMARY KEY, username TEXT);
sqlite> INSERT INTO users VALUES (1, 'alice');
sqlite> INSERT INTO users VALUES (2, 'bob');
```

or use the testing script to generate one for you:

```console
./testing/gen-database.py
```

You can then start the Limbo shell with:

```console
$ cargo run database.db
Welcome to Limbo SQL shell!
> SELECT * FROM users LIMIT 1;
|1|Cody|Miller|mhurst@example.org|525.595.7319x21268|33667 Shaw Extension Suite 104|West Robert|VA|45161|`
```

## Developing

Run tests:

```console
cargo test
```

Test coverage report:

```
cargo tarpaulin -o html
```

Run benchmarks:

```console
cargo bench
```

Run benchmarks and generate flamegraphs:

```console
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
cargo bench --bench benchmark -- --profile-time=5
```

## Publications

* Pekka Enberg, Sasu Tarkoma, and Ashwin Rao (2023). Towards Database and Serverless Runtime Co-Design. In _CoNEXT-SW ’23_. [[PDF](https://penberg.org/papers/penberg-conext-sw-23.pdf)] [[Slides](https://penberg.org/papers/penberg-conext-sw-23-slides.pdf)]

## License

This project is licensed under the [MIT license].

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Limbo by you, shall be licensed as MIT, without any additional
terms or conditions.

[MIT license]: https://github.com/penberg/limbo/blob/main/LICENSE.md
