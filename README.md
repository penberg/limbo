<p align="center">
  <img src="limbo.png" alt="Limbo" width="200"/>
  <h1 align="center">Limbo</h1>
</p>

<p align="center">
  Limbo is a <i>work-in-progress</i>, in-process OLTP database management system, compatible with SQLite.
</p>

<p align="center">
  <a title="Build Status" target="_blank" href="https://github.com/tursodatabase/limbo/actions/workflows/rust.yml"><img src="https://img.shields.io/github/actions/workflow/status/tursodatabase/limbo/rust.yml?style=flat-square"></a>
  <a title="Releases" target="_blank" href="https://github.com/tursodatabase/limbo/releases"><img src="https://img.shields.io/github/release/tursodatabase/limbo?style=flat-square&color=9CF"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/limbo/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
  <br>
  <a title="GitHub Pull Requests" target="_blank" href="https://github.com/tursodatabase/limbo/pulls"><img src="https://img.shields.io/github/issues-pr-closed/tursodatabase/limbo.svg?style=flat-square&color=FF9966"></a>
  <a title="GitHub Commits" target="_blank" href="https://github.com/tursodatabase/limbo/commits/main"><img src="https://img.shields.io/github/commit-activity/m/tursodatabase/limbo.svg?style=flat-square"></a>
  <a title="Last Commit" target="_blank" href="https://github.com/tursodatabase/limbo/commits/main"><img src="https://img.shields.io/github/last-commit/tursodatabase/limbo.svg?style=flat-square&color=FF9900"></a>
</p>
<p align="center">
  <a title="Discord" target="_blank" href="[https://discord.gg/jgjmyYgHwB](https://discord.gg/jgjmyYgHwB)"><img alt="Chat on Discord" src="https://img.shields.io/discord/1258658826257961020?label=Discord&logo=Discord&style=social"></a>
</p>

---

## Features

Limbo is an in-process OLTP database engine library that has:

* **Asynchronous I/O** support on Linux with `io_uring`
* **SQLite compatibility** [[doc](COMPAT.md)] for SQL dialect, file formats, and the C API
* **Language bindings** for JavaScript/WebAssembly, Rust, Go, Python, and [Java](bindings/java)
* **OS support** for Linux, macOS, and Windows

## Getting Started

### 💻 Command Line

You can install the latest `limbo` release with:

```shell 
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/tursodatabase/limbo/releases/latest/download/limbo-installer.sh | sh
```

Then launch the shell to execute SQL statements:

```console
Limbo
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database
limbo> CREATE TABLE users (id INT PRIMARY KEY, username TEXT);
limbo> INSERT INTO users VALUES (1, 'alice');
limbo> INSERT INTO users VALUES (2, 'bob');
limbo> SELECT * FROM users;
1|alice
2|bob
```

You can also build and run the latest development version with:

```shell
cargo run
```

### ✨ JavaScript (wip)

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

### 🐍 Python (wip)

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

### 🐹 Go (wip)

1. Clone the repository
2. Build the library and set your LD_LIBRARY_PATH to include limbo's target directory
```console
cargo build --package limbo-go
export LD_LIBRARY_PATH=/path/to/limbo/target/debug:$LD_LIBRARY_PATH
```
3. Use the driver

```console
go get github.com/tursodatabase/limbo
go install github.com/tursodatabase/limbo
```

Example usage:
```go
import (
    "database/sql"
    _"github.com/tursodatabase/limbo"
)

conn, _ = sql.Open("sqlite3", "sqlite.db")
defer conn.Close()

stmt, _ := conn.Prepare("select * from users")
defer stmt.Close()

rows, _ = stmt.Query()
for rows.Next() {
    var id int 
    var username string
    _ := rows.Scan(&id, &username)
    fmt.Printf("User: ID: %d, Username: %s\n", id, username)
}
```

## Contributing

We'd love to have you contribute to Limbo! Please check out the [contribution guide] to get started.

## FAQ

### How is Limbo different from libSQL?

Limbo is a research project to build a SQLite compatible in-process database in Rust with native async support. The libSQL project, on the other hand, is an open source, open contribution fork of SQLite, with focus on production features such as replication, backups, encryption, and so on. There is no hard dependency between the two projects. Of course, if Limbo becomes widely successful, we might consider merging with libSQL, but that is something that will be decided in the future.

## Publications

* Pekka Enberg, Sasu Tarkoma, Jon Crowcroft Ashwin Rao (2024). Serverless Runtime / Database Co-Design With Asynchronous I/O. In _EdgeSys ‘24_. [[PDF]](https://penberg.org/papers/penberg-edgesys24.pdf)
* Pekka Enberg, Sasu Tarkoma, and Ashwin Rao (2023). Towards Database and Serverless Runtime Co-Design. In _CoNEXT-SW ’23_. [[PDF](https://penberg.org/papers/penberg-conext-sw-23.pdf)] [[Slides](https://penberg.org/papers/penberg-conext-sw-23-slides.pdf)]

## License

This project is licensed under the [MIT license].

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Limbo by you, shall be licensed as MIT, without any additional
terms or conditions.

[contribution guide]: https://github.com/tursodatabase/limbo/blob/main/CONTRIBUTING.md
[MIT license]: https://github.com/tursodatabase/limbo/blob/main/LICENSE.md
