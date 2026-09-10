# Turso .NET

ADO.NET bindings for Turso local and remote databases.

The `Turso.Data.Sqlite.Provider` package includes a SQLite-compatible `Turso.Data.Sqlite` facade. It depends on the implementation packages `Turso.Data.Common` and `Turso.Data.Native`, which provide the shared managed ADO.NET types and native runtime.

## Install

```bash
dotnet add package Turso.Data.Sqlite.Provider
```

Application code only needs to install `Turso.Data.Sqlite.Provider` and reference the `Turso.Data.Sqlite` namespace.

The package targets `net8.0`, `net9.0`, and `net10.0`. Its `Turso.Data.Native` dependency supplies native runtime assets for Windows, Linux, macOS, Android (`android-arm64`, `android-arm`, `android-x64`, and `android-x86`), and iOS as an XCFramework with device and simulator slices.

## NativeAOT static linking

NativeAOT apps can opt into statically linking the Turso native library so publish output does not include a sidecar `turso_sdk_kit` DLL, `.so`, or `.dylib`. Reference the neutral RID-specific static package alongside `Turso.Data.Sqlite.Provider`:

```xml
<ItemGroup>
  <PackageReference Include="Turso.Data.Sqlite.Provider" Version="0.8.0-pre.2" />
  <PackageReference Include="Turso.Data.NativeAot.win-x64" Version="0.8.0-pre.2" PrivateAssets="all" />
</ItemGroup>
```

Then enable static linking:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
  <SelfContained>true</SelfContained>
  <TursoUseStaticNativeLibrary>true</TursoUseStaticNativeLibrary>
</PropertyGroup>
```

Publish with a supported runtime identifier, for example:

```bash
dotnet publish -c Release -r win-x64
```

Static native packages are published for `win-x64`, `win-arm64`, `linux-x64`, `linux-arm64`, `osx-x64`, and `osx-arm64`. The dynamic native assets remain the default for non-AOT apps and for mobile targets. See `samples/NativeAot` for a complete executable sample.

## Getting started

```C#
using Turso;

using var connection = new TursoConnection("Data Source=:memory:");
connection.Open();

connection.ExecuteNonQuery("CREATE TABLE t(a, b)");
var rowsAffected = connection.ExecuteNonQuery("INSERT INTO t(a, b) VALUES (1, 2), (3, 4)");
Console.WriteLine($"RowsAffected: {rowsAffected}");

using var command = connection.CreateCommand();
command.CommandText = "SELECT * FROM t";
using var reader = command.ExecuteReader();
while (reader.Read())
{
    var a = reader.GetInt32(0);
    var b = reader.GetInt32(1);
    Console.WriteLine($"Value1: {a}, Value2: {b}");
}
```

## ADO.NET usage

Code written against `DbConnection` can use `TursoConnection` directly:

```C#
using System.Data.Common;
using Turso;

await using DbConnection connection = new TursoConnection("Data Source=app.db");
connection.Open();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT $value";
var parameter = command.CreateParameter();
parameter.ParameterName = "$value";
parameter.Value = 42;
command.Parameters.Add(parameter);

var value = command.ExecuteScalar();
```

Remote Turso/libSQL databases can use the same `TursoConnection` surface with a remote URL and auth token:

```C#
await using var connection = new TursoConnection(
    "Data Source=libsql://example-org.turso.io;Auth Token=eyJ...");
await connection.OpenAsync();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT name FROM customers WHERE id = $id";
command.Parameters.Add(new TursoParameter("$id", 42));

var name = await command.ExecuteScalarAsync();
```

Remote mode uses the Hrana HTTP `/v2/pipeline` protocol. `libsql://` URLs default to HTTPS; `Tls=False` maps them to HTTP for local development. `ws://` and `wss://` URLs are accepted and mapped to the equivalent HTTP pipeline endpoint. `Auth Token` requires HTTPS unless the host is `localhost` or loopback.

Add `Replica Path` to use the same provider surface with a local embedded replica:

```C#
await using var replica = new TursoConnection(
    "Data Source=turso://example-org.turso.io;"
    + "Auth Token=eyJ...;"
    + "Replica Path=./replica.db;"
    + "Pooling=True;"
    + "Sync Interval=30");
await replica.OpenAsync();

// Pull immediately in addition to the shared 30-second automatic schedule.
await replica.SyncAsync();
```

`Pooling=True` shares a file replica and one automatic-sync schedule among connections that use the same path and options. Pooling remains opt-in; `Pooling=False` keeps an exclusive path lease, and `:memory:` replicas are always private. Connections for one pooled path must use identical sync settings and credentials.

`Sync Interval` is the automatic pull period in seconds. `AutomaticSyncStatus` and `AutomaticSyncStatusChanged` expose waiting, running, retrying, faulted, and stopped states along with attempt times, the last pull result, the next attempt, and terminal failures. Automatic sync retries transient transport, I/O, and timeout failures twice; other failures are terminal and are also surfaced by `Close`.

Connection-string replicas also map `Sync Client Name`, `Sync Long Poll Timeout`, `Bootstrap If Empty`, `Partial Bootstrap Prefix`/`Query`, `Partial Sync Segment Size`/`Prefetch`, `Remote Encryption Cipher`/`Key`, `Push Operations Threshold`, `Pull Bytes Threshold`, `Force Logical MVCC Pull`, and `Sync Experimental Features` to `TursoSyncDatabaseOptions`. Local `Encryption Cipher` and `Encryption Key` do not configure remote replica encryption.

Direct remote and opened embedded replica connections support ADO.NET `DbBatch`:

```C#
await using var batch = connection.CreateBatch();

var insert = batch.CreateBatchCommand();
insert.CommandText = "INSERT INTO customers(name) VALUES ($name)";
var name = insert.CreateParameter();
name.ParameterName = "$name";
name.Value = "Alice";
insert.Parameters.Add(name);
batch.BatchCommands.Add(insert);

var select = batch.CreateBatchCommand();
select.CommandText = "SELECT COUNT(*) FROM customers";
batch.BatchCommands.Add(select);

await using var reader = await batch.ExecuteReaderAsync();
```

Direct remote batches are sent in one Hrana request. Replica batches execute each
command sequentially against the local replica connection and expose each result
through `NextResult`. They are not implicitly atomic. Use an explicit transaction
when every command must commit or roll back together:

```C#
await using var transaction = await replica.BeginTransactionAsync();
await using var batch = replica.CreateBatch();
batch.Transaction = transaction;

var first = batch.CreateBatchCommand();
first.CommandText = "INSERT INTO customers(name) VALUES ('Alice')";
batch.BatchCommands.Add(first);

var second = batch.CreateBatchCommand();
second.CommandText = "INSERT INTO customers(name) VALUES ('Bob')";
batch.BatchCommands.Add(second);

await batch.ExecuteNonQueryAsync();
await transaction.CommitAsync();
```

Use `TursoSyncDatabase` when an application needs an embedded replica with explicit sync control and advanced sync configuration:

```C#
var options = new TursoSyncDatabaseOptions(
    "./replica.db",
    new Uri("turso://example-org.turso.io"))
{
    AuthToken = authToken,
    PartialSync = new TursoPartialSyncOptions
    {
        PrefixLength = 4 * 1024 * 1024,
        SegmentSize = 256 * 1024,
        Prefetch = true,
    },
    PushOperationsThreshold = 1000,
    PullBytesThreshold = 1024 * 1024,
    ForceLogicalMvccPull = true,
    ExperimentalFeatures = "views",
};

await using var database = await TursoSyncDatabase.CreateAsync(options);
await using var local = await database.ConnectAsync();
var changed = await database.PullAsync();
var stats = await database.GetStatsAsync();

// Push is explicit. Do not call it for pull-only replicas.
await database.PushAsync();
await database.CheckpointAsync();
```

`PullAsync` never pushes local writes. `PushAsync` currently follows the sync engine's last-write-wins conflict behavior, so use it only when that policy is acceptable. `CheckpointAsync` runs the sync engine's local checkpoint operation, while `GetStatsAsync` reports WAL sizes, CDC operations, transfer totals, revision, and the most recent pull and push times. Sync failures are `TursoSyncException` values carrying the operation, native status, sanitized endpoint, HTTP method/status, and original exception.

Remote encryption is configured with `RemoteEncryption = new TursoRemoteEncryptionOptions { Key = key, Cipher = TursoRemoteEncryptionCipher.Aes256Gcm }`. It cannot be combined with partial sync. Query-based partial sync cannot set `PullBytesThreshold`, all partial-sync strategies require `BootstrapIfEmpty`, and partial sync is unavailable on Windows until native sparse-file hole detection is implemented.

Provider factories are available through `TursoFactory.Instance`:

```C#
DbProviderFactory factory = TursoFactory.Instance;
using var connection = factory.CreateConnection();
connection!.ConnectionString = "Data Source=:memory:";
connection.Open();
```

## Migrating from Microsoft.Data.Sqlite

For common embedded SQLite usage, the `Turso.Data.Sqlite.Provider` package exposes a SQLite-compatible `Turso.Data.Sqlite` facade over the Turso engine:

```diff
- using Microsoft.Data.Sqlite;
+ using Turso.Data.Sqlite;

- using var connection = new SqliteConnection("Data Source=app.db");
+ using var connection = new SqliteConnection("Data Source=app.db");
```

The same facade can connect directly to a remote database:

```C#
using Turso.Data.Sqlite;

await using var connection = new SqliteConnection(
    "Data Source=libsql://example-org.turso.io;Auth Token=eyJ...");
await connection.OpenAsync();
```

Add `Replica Path` to keep queries local while syncing an embedded replica:

```C#
await using var connection = new SqliteConnection(
    "Data Source=libsql://example-org.turso.io;"
    + "Auth Token=eyJ...;"
    + "Replica Path=./replica.db;"
    + "Sync Interval=30");
await connection.OpenAsync();
await connection.SyncAsync();
```

Local paths continue to use the native SQLite-compatible backend. Remote URLs without
`Replica Path` use direct remote execution; remote URLs with `Replica Path` use the
local replica backend. Client-side functions, aggregates, collations, backup, blob,
and extension helpers are unavailable on direct remote connections because they
require a local database handle.

Supported common connection string keywords include:

| Keyword | Notes |
| --- | --- |
| `Data Source` | Database path or `:memory:`. Aliases include `DataSource` and `Filename`. |
| `Mode` | Parsed and preserved for compatibility. |
| `Cache` | Parsed and preserved for compatibility. |
| `Foreign Keys` | Parsed and preserved for compatibility. |
| `Recursive Triggers` | Parsed and preserved for compatibility. |
| `Default Timeout` | Used as the default command timeout. Aliases include `Command Timeout`. |
| `Pooling` | Parsed and preserved for compatibility. |
| `Vfs` | Parsed and preserved for compatibility. |
| `Encryption Cipher` | Turso local encryption cipher. |
| `Encryption Key` | Hex-encoded encryption key used with `Encryption Cipher`. |
| `Auth Token` | Bearer token for remote Turso/libSQL URLs. Aliases include `AuthToken` and `Authentication Token`. |
| `Replica Path` | Local path for an embedded replica of the remote `Data Source`. |
| `Read Your Writes` | Keeps the remote Hrana session baton across commands. Defaults to `True`. Set `False` for stateless one-shot remote requests. |
| `Sync Interval` | Embedded replica automatic pull interval in seconds. `0` disables automatic sync. |
| `Tls` | Optional override for `libsql://` development URLs. Conflicting values with explicit `http://` or `https://` schemes fail early. |

## SQLite-compatible facade coverage

- `Turso.Data.Sqlite` is the migration-oriented facade. It includes SQLite-style connection strings, commands, readers, schema metadata, transactions and savepoints, backup, SQL-backed blob streams, scalar and aggregate UDFs, custom collations, and disabled-by-default extension loading.
- Raw SQLitePCL `sqlite3*` handle interop is intentionally unsupported. `SqliteConnection.Handle` returns `null` rather than exposing a fake SQLite handle.
- `PRAGMA read_uncommitted` is tracked as connection-local state for API compatibility, but Turso does not currently implement SQLite shared-cache dirty reads.
- `SqliteBlob` preserves fixed-length blob stream behavior through SQL reads and writes. It is not yet backed by a native incremental-blob storage handle.
- SQLite virtual-table modules such as FTS3/FTS5 are not built in unless provided by a Turso extension/module.
- Async methods currently use the base ADO.NET behavior rather than a dedicated async native path.

## Entity Framework Core

`Turso.EntityFrameworkCore.Sqlite` adds a `UseTurso` provider hook for local, direct remote, and embedded-replica Turso databases. It reuses EF Core SQLite's LINQ translation pipeline and executes generated SQL through the `Turso.Data.Sqlite` facade.

```bash
dotnet add package Turso.EntityFrameworkCore.Sqlite
```

```C#
using Microsoft.EntityFrameworkCore;

public sealed class AppDbContext : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseTurso("Data Source=app.db");
}
```

You can also pass an existing Turso SQLite-compatible connection:

```C#
using Microsoft.EntityFrameworkCore;
using Turso.Data.Sqlite;

await using var connection = new SqliteConnection("Data Source=app.db");
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseTurso(connection)
    .Options;
```

The provider supports normal EF Core CRUD, generated keys, transactions, migrations, and schema creation through `EnsureCreated` and `EnsureCreatedAsync`. Use the same remote and replica connection strings shown above with `UseTurso`.

Direct remote connections cannot run EF's client-side SQLite helpers, including `REGEXP`, decimal `ef_*` functions, and the `EF_DECIMAL` collation; queries that need them fail before SQL is sent. `EnsureDeleted` cannot delete a direct remote database and points callers to the Turso platform API. For embedded replicas, `EnsureDeleted` removes only the local replica and its sidecar files, never the remote database.
