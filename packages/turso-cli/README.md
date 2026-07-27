# Turso

Turso is an embedded database engine that runs anywhere — on servers, in browsers, or on-device. It's a drop-in replacement for SQLite, rewritten in Rust for concurrent access and async I/O.

This package provides the `turso` CLI — an interactive SQL shell, a local sync server, and an MCP server for AI assistants.

## Install

```bash
npm install -g turso
```

Or run directly without installing:

```bash
npx turso
```

## Quick Start

```bash
# Start an interactive shell with an in-memory database
npx turso

# Open or create a database file
npx turso myapp.db

# Execute a SQL statement directly
npx turso myapp.db "SELECT * FROM users;"
```

## Features Beyond SQLite

Turso is a drop-in replacement for SQLite, but adds features that SQLite doesn't have:

- **Concurrent Writers** — `BEGIN CONCURRENT` allows multiple writers without blocking, powered by MVCC
- **Native Vector Search** — `vector32`/`vector64` types with distance functions (`vector_distance_cos`, `vector_distance_l2`)
- **Change Data Capture** — track row-level changes per connection with `PRAGMA capture_data_changes_conn`
- **MCP Server** — run as a [Model Context Protocol](https://modelcontextprotocol.io/) server for AI assistants (`--mcp`)
- **Local Sync Server** — serve a database over HTTP for client SDKs to sync against (`--sync-server`), or serve a whole directory of databases at once (`--sync-dir`)
- **Array Types** — array columns in STRICT tables with operators like `@>`, `<@`, `||`
- **Built-in Extensions** — crypto, regexp, fuzzy matching, IP address functions, CSV, percentile

### Experimental Features

These features are available behind `--experimental-*` flags:

- **Materialized Views** — incrementally maintained views with automatic change tracking
- **Custom Types** — user-defined types with `CREATE TYPE`, custom encode/decode and operators
- **At-Rest Encryption** — transparent database encryption (AES-GCM, AEGIS ciphers)
- **Full-Text Search** — Tantivy-powered FTS with custom index methods
- **Generated Columns** — virtual and stored computed columns
- **Triggers** — `CREATE TRIGGER` / `DROP TRIGGER`
- **Attach** — `ATTACH DATABASE` / `DETACH DATABASE`
- **Autovacuum** — automatic database compaction

Run `npx turso --help` for the full list of flags.

## Examples

### Interactive Shell

```bash
npx turso myapp.db
```

```
turso> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
turso> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
turso> SELECT * FROM users;
┌────┬───────┬───────────────────┐
│ id │ name  │ email             │
├────┼───────┼───────────────────┤
│  1 │ Alice │ alice@example.com │
└────┴───────┴───────────────────┘
```

### One-Shot Queries

```bash
# Run a query and exit
npx turso myapp.db "SELECT count(*) FROM users;"

# Pipe-friendly list output
npx turso -q -m list myapp.db "SELECT * FROM users;"
```

### Embedded Database

Use Turso directly as an embedded database in your Node.js application with [`@tursodatabase/database`](https://www.npmjs.com/package/@tursodatabase/database):

```javascript
import { connect } from "@tursodatabase/database";

const db = await connect("local.db");

await db.exec(`
  CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
`);

const insert = db.prepare("INSERT INTO users (name, email) VALUES (?, ?)");
await insert.run(["Alice", "alice@example.com"]);

const select = db.prepare("SELECT * FROM users");
console.log(await select.all());
```

### Local Sync Server

Start a local HTTP server that implements the Turso sync protocol. The `@tursodatabase/sync` SDK can sync against it:

```bash
npx turso myapp.db --sync-server "0.0.0.0:8080"
```

Serve a whole directory of databases instead of a single file with `--sync-dir`. `PATH` must already exist, and each database is addressed by name beneath `/db/{name}`:

```bash
npx turso --sync-server "127.0.0.1:8080" --sync-dir ./dbs
```

A client syncing against `http://localhost:8080/db/db1` reads and writes `./dbs/db1/data` (with its WAL alongside as `data-wal`, matching `sqld`'s layout), created on first request — except under `--readonly`, where a database that doesn't already exist can't be created and the request fails with `404 Not Found`. Names must match `^[A-Za-z0-9_-]+$` and are capped at 128 characters; anything else gets `400 Bad Request`, as do the Windows reserved device names (`con`, `prn`, `aux`, `nul`, `com1`–`com9`, `lpt1`–`lpt9`, in any case), which Windows resolves as devices from any directory whatever the extension. `--sync-dir` requires `--sync-server` and can't be combined with a positional database argument. `--readonly`, `--vfs`, and `--experimental-*` flags apply to every database served this way; with `--experimental-attach`, a client can `ATTACH DATABASE` an arbitrary filesystem path and reach files outside the served directory, so the name restrictions above don't confine it.

Requests are handled one at a time across all databases in a directory, so distinct databases don't progress independently, and opened database handles are cached for the life of the process without eviction, up to `--sync-max-databases` at once (default 256) — past that, a request for a database that isn't already open gets `503 Service Unavailable`. Any client that can reach the port can create a database by requesting a new valid name, so disk space and inodes still accumulate without a cap. This mode has no authentication or multi-tenancy and is intended for local and development use.

### MCP Server

Start an [MCP](https://modelcontextprotocol.io/) server so AI assistants can query your databases:

```bash
npx turso --mcp
```

### Shell Commands

Inside the interactive shell, use `.commands` for database operations:

| Command                    | Description                                  |
|----------------------------|----------------------------------------------|
| `.open <FILE>`             | Open a different database                    |
| `.tables`                  | List all tables                              |
| `.schema [TABLE]`          | Show table schema                            |
| `.mode <MODE>`             | Switch output mode (pretty, list, line)       |
| `.import <FILE> <TABLE>`   | Import data from a file into a table         |
| `.dump`                    | Dump the database as SQL                     |
| `.quit`                    | Exit the shell                               |

## Supported Platforms

| Platform              | Architecture |
|-----------------------|-------------|
| macOS                 | ARM64, x64  |
| Linux (glibc)         | ARM64, x64  |
| Windows               | ARM64, x64  |

## Links

- [GitHub](https://github.com/tursodatabase/turso)
- [Documentation](https://docs.turso.tech)
- [Discord](https://tur.so/discord)

## License

MIT
