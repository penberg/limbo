<p align="center">
  <img src="limbo.png" alt="Limbo" width="200"/>
  <h1 align="center">Limbo</h1>
</p>

<p align="center">
  Limbo is a work-in-progress, in-process OLTP database management system, compatible with SQLite.
</p>

<p align="center">
  <a href="https://github.com/penberg/limbo/actions">
    <img src="https://github.com/penberg/limbo/actions/workflows/rust.yml/badge.svg" alt="Build badge">
  </a>
  <a href="https://github.com/penberg/limbo/blob/main/LICENSE.md">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT" title="MIT License" />
  </a>
  <a href="https://discord.gg/jgjmyYgHwB">
    <img src="https://img.shields.io/discord/1258658826257961020" alt="Discord" title="Discord" />
  </a>
  

</p>

---

## Features

* In-process OLTP database engine library
* Asynchronous I/O support on Linux with `io_uring`
* SQLite compatibility ([status](COMPAT.md))
  * SQL dialect support
  * File format support
  * SQLite C API
* JavaScript/WebAssembly bindings (_wip_)
* Support for Linux, macOS, and Windows

## Getting Started

### CLI

Install `limbo` with:

```shell 
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/penberg/limbo/releases/latest/download/limbo-installer.sh | sh
```

Then use the SQL shell to create and query a database:

```console
$ limbo database.db
Limbo v0.0.6
Enter ".help" for usage hints.
limbo> CREATE TABLE users (id INT PRIMARY KEY, username TEXT);
limbo> INSERT INTO users VALUES (1, 'alice');
limbo> INSERT INTO users VALUES (2, 'bob');
limbo> SELECT * FROM users;
1|alice
2|bob
```

### JavaScript (wip)

Installation:

```console
npm i limbo-wasm
```

Example usage:

```js
import { Database } from 'limbo-wasm';

const db = new Database('sqlite.db');
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();
console.log(users);
```

### Python (wip)

```console
pip install pylimbo
```

Example usage:

```python
import limbo

con = limbo.connect("sqlite.db")
cur = con.cursor()
res = cur.execute("SELECT * FROM users")
print(res.fetchone())
```

## Developing

Build and run `limbo` cli: 

```shell 
cargo run --package limbo --bin limbo database.db
```

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

## FAQ

### How is Limbo different from libSQL?

Limbo is a research project to build a SQLite compatible in-process database in Rust with native async support. The libSQL project, on the other hand, is an open source, open contribution fork of SQLite, with focus on production features such as replication, backups, encryption, and so on. There is no hard dependency between the two projects. Of course, if Limbo becomes widely successful, we might consider merging with libSQL, but that is something that will be decided in the future.

## Publications

* Pekka Enberg, Sasu Tarkoma, Jon Crowcroft Ashwin Rao (2024). Serverless Runtime / Database Co-Design With Asynchronous I/O. In _EdgeSys ‘24_. [[PDF]](https://penberg.org/papers/penberg-edgesys24.pdf)
* Pekka Enberg, Sasu Tarkoma, and Ashwin Rao (2023). Towards Database and Serverless Runtime Co-Design. In _CoNEXT-SW ’23_. [[PDF](https://penberg.org/papers/penberg-conext-sw-23.pdf)] [[Slides](https://penberg.org/papers/penberg-conext-sw-23-slides.pdf)]

## Contributing

We'd love to have you contribute to Limbo! Check out the [contribution guide] to get started.

## License

This project is licensed under the [MIT license].

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Limbo by you, shall be licensed as MIT, without any additional
terms or conditions.

[contribution guide]: https://github.com/penberg/limbo/blob/main/CONTRIBUTING.md
[MIT license]: https://github.com/penberg/limbo/blob/main/LICENSE.md
