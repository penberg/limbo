using System.Data.Common;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using AwesomeAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Turso.Data.Sqlite;
using Turso.EntityFrameworkCore.Sqlite.Storage.Internal;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoEfRemoteReplicaTests
{
    [Test]
    public async Task DirectRemoteClonesAllSettingsAndRejectsLocalSqlHelpers()
    {
        using var handler = new ScriptedEfRemoteHandler();
        using var handlerScope = UseRemoteHandler(handler);
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = "https://example.test",
            AuthToken = "secret",
            ReadYourWrites = false,
            DefaultTimeout = 17,
            Tls = true,
        };
        await using var context = CreateContext(builder.ConnectionString);

        var connection = context.Database.GetDbConnection().Should().BeOfType<SqliteConnection>().Subject;
        connection.IsDirectRemote.Should().BeTrue();
        connection.IsManaged.Should().BeTrue();

#pragma warning disable EF1001
        var relationalConnection = context.GetService<ISqliteRelationalConnection>();
        await using (var clone = relationalConnection.CreateReadOnlyConnection())
#pragma warning restore EF1001
        {
            var cloneOptions = new SqliteConnectionStringBuilder(clone.ConnectionString);
            cloneOptions.DataSource.Should().Be(builder.DataSource);
            cloneOptions.AuthToken.Should().Be(builder.AuthToken);
            cloneOptions.ReadYourWrites.Should().Be(builder.ReadYourWrites);
            cloneOptions.DefaultTimeout.Should().Be(builder.DefaultTimeout);
            cloneOptions.Tls.Should().Be(builder.Tls);
            cloneOptions.Mode.Should().Be(SqliteOpenMode.ReadOnly);
            cloneOptions.Pooling.Should().BeFalse();
            clone.DbConnection.Should().BeOfType<SqliteConnection>()
                .Which.IsDirectRemote.Should().BeTrue();
        }

        await context.Items
            .Where(item => Regex.IsMatch(item.Name, "^remote"))
            .Invoking(query => query.ToListAsync())
            .Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*REGEXP*");

        await context.Items
            .Where(item => item.Amount + 1m > 2m)
            .Invoking(query => query.ToListAsync())
            .Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*ef_*");

        await context.Items
            .OrderBy(item => item.Amount)
            .Invoking(query => query.ToListAsync())
            .Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*EF_DECIMAL*");

        handler.SqlStatements.Should().BeEmpty();

        await context.Database.ExecuteSqlRawAsync("SELECT ef_sum FROM data");
        await context.Database.ExecuteSqlRawAsync("SELECT EF_DECIMAL FROM data");
        await context.Database.ExecuteSqlRawAsync("SELECT REGEXP FROM data");
        handler.SqlStatements.Should().Equal(
            "SELECT ef_sum FROM data",
            "SELECT EF_DECIMAL FROM data",
            "SELECT REGEXP FROM data");
    }

    [Test]
    public async Task DirectRemoteSupportsMigrationsGeneratedKeysCrudAndTransactions()
    {
        using var handler = new ScriptedEfRemoteHandler();
        using var handlerScope = UseRemoteHandler(handler);
        await using var context = CreateContext(
            "Data Source=https://example.test;Auth Token=secret;Read Your Writes=False");
        var creator = context.GetService<IRelationalDatabaseCreator>();

        await creator.CreateAsync();
        handler.SqlStatements.Should().BeEmpty();
        (await creator.ExistsAsync()).Should().BeTrue();
        handler.SqlStatements.Should().Equal("SELECT 1");

        await context.Database.MigrateAsync();
        (await context.Database.GetAppliedMigrationsAsync())
            .Should().ContainSingle()
            .Which.Should().Be(EfRemoteReplicaMigration.MigrationId);

        var item = new EfRemoteReplicaItem { Name = "one", Amount = 1.25m };
        context.Items.Add(item);
        await context.SaveChangesAsync();
        item.Id.Should().BeGreaterThan(0);

        item.Name = "updated";
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
        (await context.Items.SingleAsync()).Name.Should().Be("updated");

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Items.Add(new EfRemoteReplicaItem { Name = "rolled back", Amount = 2m });
            await context.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        context.ChangeTracker.Clear();
        (await context.Items.CountAsync(item => item.Name == "rolled back")).Should().Be(0);

        var stored = await context.Items.SingleAsync();
        context.Items.Remove(stored);
        await context.SaveChangesAsync();
        (await context.Items.CountAsync()).Should().Be(0);

        var requestCount = handler.SqlStatements.Count;
        await creator.Invoking(value => value.DeleteAsync())
            .Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*platform API*");
        handler.SqlStatements.Should().HaveCount(requestCount);

        handler.SqlStatements.Should().Contain(sql => sql.Contains("__EFMigrationsHistory", StringComparison.Ordinal));
        handler.SqlStatements.Should().Contain(sql => sql.Contains("RETURNING", StringComparison.OrdinalIgnoreCase));
        handler.SqlStatements.Should().Contain(sql => sql.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase));
        handler.SqlStatements.Should().Contain(sql => sql.StartsWith("SAVEPOINT", StringComparison.OrdinalIgnoreCase));
        handler.SqlStatements.Should().Contain(sql => sql.StartsWith("RELEASE SAVEPOINT", StringComparison.OrdinalIgnoreCase));
        handler.SqlStatements.Should().Contain(sql => sql.StartsWith("ROLLBACK", StringComparison.OrdinalIgnoreCase));
    }

    [Test]
    public async Task DirectRemoteEnsureCreatedCreatesTablesAndEnsureDeletedRejectsRemoteDeletion()
    {
        using var handler = new ScriptedEfRemoteHandler();
        using var handlerScope = UseRemoteHandler(handler);
        await using var context = CreateContext(
            "Data Source=https://example.test;Auth Token=secret;Read Your Writes=False");

        (await context.Database.EnsureCreatedAsync()).Should().BeTrue();
        (await context.Database.EnsureCreatedAsync()).Should().BeFalse();
        (await context.Database.GetService<IRelationalDatabaseCreator>().HasTablesAsync()).Should().BeTrue();

        await context.Database.Invoking(database => database.EnsureDeletedAsync())
            .Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*platform API*");

        handler.SqlStatements.Should().Contain(sql => sql.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
        handler.SqlStatements.Should().NotContain(sql => sql.Contains("PRAGMA journal_mode", StringComparison.OrdinalIgnoreCase));
    }

    [Test]
    public async Task ExternalDirectRemoteConnectionHonorsContextOwnership()
    {
        using var handler = new ScriptedEfRemoteHandler();
        using var handlerScope = UseRemoteHandler(handler);
        const string connectionString =
            "Data Source=https://example.test;Auth Token=secret;Read Your Writes=False";

        await using var unownedConnection = new SqliteConnection(connectionString);
        await unownedConnection.OpenAsync();
        await using (var context = CreateContext(unownedConnection, contextOwnsConnection: false))
            (await context.Database.CanConnectAsync()).Should().BeTrue();
        unownedConnection.State.Should().Be(System.Data.ConnectionState.Open);

        var ownedConnection = new SqliteConnection(connectionString);
        await ownedConnection.OpenAsync();
        await using (var context = CreateContext(ownedConnection, contextOwnsConnection: true))
            (await context.Database.CanConnectAsync()).Should().BeTrue();
        ownedConnection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Test]
    public async Task DeferredBootstrapReplicaSupportsMigrationsCrudAndDeletesOnlyReplicaFiles()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "ef-replica-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var replicaPath = Path.Combine(directory, "replica.db");
        var unrelatedPath = Path.Combine(directory, "keep.txt");
        using var syncScope = RejectSyncRequests();
        try
        {
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = "https://example.test",
                ReplicaPath = replicaPath,
                BootstrapIfEmpty = false,
                Pooling = false,
                SyncClientName = "ef-replica-test",
            };
            await using var context = CreateContext(builder.ConnectionString);
            var connection = context.Database.GetDbConnection().Should().BeOfType<SqliteConnection>().Subject;
            connection.IsReplica.Should().BeTrue();
            connection.IsManaged.Should().BeTrue();

#pragma warning disable EF1001
            var relationalConnection = context.GetService<ISqliteRelationalConnection>();
            await using (var clone = relationalConnection.CreateReadOnlyConnection())
#pragma warning restore EF1001
            {
                var cloneOptions = new SqliteConnectionStringBuilder(clone.ConnectionString);
                cloneOptions.DataSource.Should().Be(builder.DataSource);
                cloneOptions.ReplicaPath.Should().Be(builder.ReplicaPath);
                cloneOptions.BootstrapIfEmpty.Should().BeFalse();
                cloneOptions.SyncClientName.Should().Be(builder.SyncClientName);
                clone.DbConnection.Should().BeOfType<SqliteConnection>()
                    .Which.IsReplica.Should().BeTrue();
            }

            var creator = context.GetService<IRelationalDatabaseCreator>();
            (await creator.ExistsAsync()).Should().BeFalse();
            await creator.CreateAsync();
            (await creator.ExistsAsync()).Should().BeTrue();
            (await creator.HasTablesAsync()).Should().BeFalse();

            await context.Database.MigrateAsync();
            context.Database.GetMigrations()
                .Should().ContainSingle()
                .Which.Should().Be(EfRemoteReplicaMigration.MigrationId);
            File.Exists(replicaPath).Should().BeTrue();
            (await context.Database.GetAppliedMigrationsAsync())
                .Should().ContainSingle()
                .Which.Should().Be(EfRemoteReplicaMigration.MigrationId);

            var item = new EfRemoteReplicaItem { Name = "replica", Amount = 3.5m };
            context.Items.Add(item);
            await context.SaveChangesAsync();
            item.Id.Should().BeGreaterThan(0);

            item.Name = "updated replica";
            await context.SaveChangesAsync();
            context.ChangeTracker.Clear();
            (await context.Items.SingleAsync()).Name.Should().Be("updated replica");
            (await context.Items
                    .Where(value => value.Amount > 3m)
                    .OrderBy(value => value.Amount)
                    .SingleAsync())
                .Amount.Should().Be(3.5m);
            (await context.Items
                    .Where(value => Regex.IsMatch(value.Name, "^updated"))
                    .SingleAsync())
                .Name.Should().Be("updated replica");

            await using (var transaction = await context.Database.BeginTransactionAsync())
            {
                context.Items.Add(new EfRemoteReplicaItem { Name = "rolled back", Amount = 4m });
                await context.SaveChangesAsync();
                await transaction.RollbackAsync();
            }

            context.ChangeTracker.Clear();
            (await context.Items.CountAsync(item => item.Name == "rolled back")).Should().Be(0);

            await context.Database.CloseConnectionAsync();
            await File.WriteAllTextAsync(unrelatedPath, "keep");
            var sidecars = CreateReplicaSidecars(replicaPath);

            (await context.Database.EnsureDeletedAsync()).Should().BeTrue();

            File.Exists(replicaPath).Should().BeFalse();
            sidecars.Should().OnlyContain(path => !File.Exists(path));
            File.Exists(unrelatedPath).Should().BeTrue();
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task MemoryReplicaExistsWithoutTouchingTheNetworkOrFiles()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "ef-memory-replica-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var unrelatedPath = Path.Combine(directory, "keep.txt");
        await File.WriteAllTextAsync(unrelatedPath, "keep");
        using var syncScope = RejectSyncRequests();
        try
        {
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = "https://example.test",
                ReplicaPath = ":memory:",
                BootstrapIfEmpty = false,
                Pooling = false,
            };
            await using var context = CreateContext(builder.ConnectionString);
            context.Database.GetDbConnection().Should().BeOfType<SqliteConnection>()
                .Which.IsReplica.Should().BeTrue();

            var creator = context.GetService<IRelationalDatabaseCreator>();
            creator.Exists().Should().BeTrue();
            (await creator.ExistsAsync()).Should().BeTrue();

            creator.Delete();
            await creator.DeleteAsync();

            File.Exists(unrelatedPath).Should().BeTrue();
            Directory.EnumerateFiles(directory).Should().ContainSingle();
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task LocalDatabaseKeepsSqliteHelpersAndTursoHistoryRepository()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "ef-local-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var databasePath = Path.Combine(directory, "local.db");
        var unrelatedPath = Path.Combine(directory, "keep.txt");
        try
        {
            await using var context = CreateContext($"Data Source={databasePath};Pooling=False");
            context.Database.GetDbConnection().Should().BeOfType<SqliteConnection>()
                .Which.IsLocal.Should().BeTrue();

#pragma warning disable EF1001
            context.GetService<IHistoryRepository>().Should().BeOfType<TursoSqliteHistoryRepository>();
#pragma warning restore EF1001

            var creator = context.GetService<IRelationalDatabaseCreator>();
            (await creator.ExistsAsync()).Should().BeFalse();
            await creator.CreateAsync();
            (await creator.ExistsAsync()).Should().BeTrue();
            (await creator.HasTablesAsync()).Should().BeFalse();

            await context.Database.MigrateAsync();
            (await creator.HasTablesAsync()).Should().BeTrue();

            context.Items.Add(new EfRemoteReplicaItem { Name = "local", Amount = 3.5m });
            await context.SaveChangesAsync();
            context.ChangeTracker.Clear();

            (await context.Items
                    .Where(item => Regex.IsMatch(item.Name, "^loc"))
                    .SingleAsync())
                .Name.Should().Be("local");
            (await context.Items
                    .Where(item => item.Amount + 1m > 4m)
                    .OrderBy(item => item.Amount)
                    .SingleAsync())
                .Amount.Should().Be(3.5m);

            await context.Database.CloseConnectionAsync();
            await File.WriteAllTextAsync(unrelatedPath, "keep");
            (await context.Database.EnsureDeletedAsync()).Should().BeTrue();

            File.Exists(databasePath).Should().BeFalse();
            File.Exists(databasePath + "-wal").Should().BeFalse();
            File.Exists(databasePath + "-shm").Should().BeFalse();
            File.Exists(unrelatedPath).Should().BeTrue();
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static EfRemoteReplicaDbContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<EfRemoteReplicaDbContext>()
            .UseTurso(
                connectionString,
                sqlite => sqlite.MigrationsAssembly(typeof(EfRemoteReplicaMigration).Assembly.FullName))
            .Options;
        return new EfRemoteReplicaDbContext(options);
    }

    private static EfRemoteReplicaDbContext CreateContext(
        SqliteConnection connection,
        bool contextOwnsConnection)
    {
        var options = new DbContextOptionsBuilder<EfRemoteReplicaDbContext>()
            .UseTurso(
                connection,
                contextOwnsConnection,
                sqlite => sqlite.MigrationsAssembly(typeof(EfRemoteReplicaMigration).Assembly.FullName))
            .Options;
        return new EfRemoteReplicaDbContext(options);
    }

    private static string[] CreateReplicaSidecars(string replicaPath)
    {
        var sidecars = new[]
        {
            replicaPath + "-journal",
            replicaPath + "-wal",
            replicaPath + "-shm",
            Path.ChangeExtension(replicaPath, ".db-log"),
            replicaPath + "-wal-revert",
            replicaPath + "-info",
            replicaPath + "-changes",
            replicaPath + "-replace-base-apply",
            replicaPath + "-replace-base-apply-test.backup",
        };
        foreach (var sidecar in sidecars)
            File.WriteAllText(sidecar, "sidecar");
        return sidecars;
    }

    private static IDisposable UseRemoteHandler(HttpMessageHandler handler)
    {
        global::Turso.TursoConnection.RemoteMessageHandlerFactory = () => handler;
        return new Scope(() => global::Turso.TursoConnection.RemoteMessageHandlerFactory = null);
    }

    private static IDisposable RejectSyncRequests()
    {
        global::Turso.TursoConnection.SyncHttpClientFactory =
            () => new HttpClient(new UnexpectedSyncHandler());
        return new Scope(() => global::Turso.TursoConnection.SyncHttpClientFactory = null);
    }

    private sealed class UnexpectedSyncHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => throw new AssertionException($"Deferred bootstrap unexpectedly requested {request.RequestUri}.");
    }

    private sealed class Scope(Action dispose) : IDisposable
    {
        public void Dispose() => dispose();
    }

    private sealed class ScriptedEfRemoteHandler : HttpMessageHandler
    {
        private readonly HashSet<string> _tables = new(StringComparer.Ordinal);
        private readonly List<RemoteItem> _items = [];
        private List<RemoteItem>? _transactionItems;
        private long _nextId = 1;
        private int _lastAffected;
        private string? _migrationId;

        public List<string> SqlStatements { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var requestBody = await request.Content!.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            using var document = JsonDocument.Parse(requestBody);
            var results = new JsonArray();
            foreach (var pipelineRequest in document.RootElement.GetProperty("requests").EnumerateArray())
            {
                var type = pipelineRequest.GetProperty("type").GetString();
                results.Add(type switch
                {
                    "execute" => Ok(
                        "execute",
                        new JsonObject
                        {
                            ["result"] = Execute(pipelineRequest.GetProperty("stmt")),
                        }),
                    "close" => Ok("close"),
                    _ => throw new AssertionException($"Unexpected remote pipeline request '{type}'."),
                });
            }

            var response = new JsonObject
            {
                ["baton"] = "ef-session",
                ["results"] = results,
            };
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(response.ToJsonString(), Encoding.UTF8, "application/json"),
            };
        }

        protected override void Dispose(bool disposing)
        {
        }

        private JsonObject Execute(JsonElement statement)
        {
            var sql = statement.GetProperty("sql").GetString()!.Trim();
            SqlStatements.Add(sql);
            var arguments = GetArguments(statement);

            if (sql.StartsWith("SELECT 1", StringComparison.OrdinalIgnoreCase))
                return Scalar("value", "INTEGER", Integer(1));

            if (sql.Contains("sqlite_master", StringComparison.OrdinalIgnoreCase))
            {
                var count = sql.Contains("__EFMigrationsHistory", StringComparison.Ordinal)
                    ? (_tables.Contains("__EFMigrationsHistory") ? 1 : 0)
                    : _tables.Count;
                return Scalar("COUNT(*)", "INTEGER", Integer(count));
            }

            if (sql.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
            {
                var table = Regex.Match(
                    sql,
                    "CREATE TABLE(?: IF NOT EXISTS)?\\s+\"(?<name>[^\"]+)\"",
                    RegexOptions.IgnoreCase).Groups["name"].Value;
                table.Should().NotBeEmpty();
                _tables.Add(table);
                return Empty();
            }

            if (sql.StartsWith("DROP TABLE", StringComparison.OrdinalIgnoreCase))
            {
                var table = Regex.Match(
                    sql,
                    "DROP TABLE(?: IF EXISTS)?\\s+\"(?<name>[^\"]+)\"",
                    RegexOptions.IgnoreCase).Groups["name"].Value;
                _tables.Remove(table);
                return Empty();
            }

            if (sql.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
            {
                _transactionItems = _items.Select(static item => item with { }).ToList();
                return Empty();
            }

            if (sql.StartsWith("ROLLBACK", StringComparison.OrdinalIgnoreCase))
            {
                if (_transactionItems is not null)
                {
                    _items.Clear();
                    _items.AddRange(_transactionItems);
                }
                _transactionItems = null;
                return Empty();
            }

            if (sql.StartsWith("COMMIT", StringComparison.OrdinalIgnoreCase))
            {
                _transactionItems = null;
                return Empty();
            }

            if (sql.StartsWith("SAVEPOINT", StringComparison.OrdinalIgnoreCase)
                || sql.StartsWith("RELEASE SAVEPOINT", StringComparison.OrdinalIgnoreCase))
            {
                return Empty();
            }

            if (sql.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("__EFMigrationsLock", StringComparison.Ordinal))
            {
                _lastAffected = 1;
                return Scalar("Id", "INTEGER", Integer(1), affectedRows: 1, lastInsertRowId: "1");
            }

            if (sql.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("__EFMigrationsLock", StringComparison.Ordinal))
            {
                _lastAffected = 1;
                return Empty(1);
            }

            if (sql.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("__EFMigrationsHistory", StringComparison.Ordinal))
            {
                _migrationId = EfRemoteReplicaMigration.MigrationId;
                _lastAffected = 1;
                return Empty(1);
            }

            if (sql.Contains("FROM \"__EFMigrationsHistory\"", StringComparison.OrdinalIgnoreCase))
            {
                return _migrationId is null
                    ? Result(
                        [("MigrationId", "TEXT"), ("ProductVersion", "TEXT")],
                        [])
                    : Result(
                        [("MigrationId", "TEXT"), ("ProductVersion", "TEXT")],
                        [[Text(_migrationId), Text("9.0.9")]]);
            }

            if (sql.StartsWith("INSERT INTO \"RemoteItems\"", StringComparison.OrdinalIgnoreCase))
            {
                var values = GetColumnValues(sql, arguments);
                var item = new RemoteItem(
                    _nextId++,
                    values["Name"],
                    decimal.Parse(values["Amount"], CultureInfo.InvariantCulture));
                _items.Add(item);
                _lastAffected = 1;
                return sql.Contains("RETURNING", StringComparison.OrdinalIgnoreCase)
                    ? Scalar("Id", "INTEGER", Integer(item.Id), 1, item.Id.ToString(CultureInfo.InvariantCulture))
                    : Empty(1, item.Id.ToString(CultureInfo.InvariantCulture));
            }

            if (sql.StartsWith("UPDATE \"RemoteItems\"", StringComparison.OrdinalIgnoreCase))
            {
                var item = _items.Single();
                var name = arguments.Values.First(value => value.Type == "text").Value;
                _items[_items.IndexOf(item)] = item with { Name = name };
                _lastAffected = 1;
                return sql.Contains("RETURNING", StringComparison.OrdinalIgnoreCase)
                    ? Scalar("1", "INTEGER", Integer(1), affectedRows: 1)
                    : Empty(1);
            }

            if (sql.StartsWith("DELETE FROM \"RemoteItems\"", StringComparison.OrdinalIgnoreCase))
            {
                _lastAffected = _items.Count == 0 ? 0 : 1;
                if (_items.Count > 0)
                    _items.RemoveAt(0);
                return sql.Contains("RETURNING", StringComparison.OrdinalIgnoreCase)
                    ? Scalar("1", "INTEGER", Integer(1), affectedRows: _lastAffected)
                    : Empty(_lastAffected);
            }

            if (sql.Contains("FROM \"RemoteItems\"", StringComparison.OrdinalIgnoreCase))
            {
                if (sql.Contains("COUNT(*)", StringComparison.OrdinalIgnoreCase))
                {
                    var nameFilter = arguments.Values.FirstOrDefault()?.Value;
                    if (nameFilter is null
                        && sql.Contains("'rolled back'", StringComparison.Ordinal))
                    {
                        nameFilter = "rolled back";
                    }
                    var count = nameFilter is null ? _items.Count : _items.Count(item => item.Name == nameFilter);
                    return Scalar("COUNT(*)", "INTEGER", Integer(count));
                }

                var item = _items.Single();
                return Result(
                    [("Id", "INTEGER"), ("Amount", "TEXT"), ("Name", "TEXT")],
                    [[
                        Integer(item.Id),
                        Text(item.Amount.ToString(CultureInfo.InvariantCulture)),
                        Text(item.Name),
                    ]]);
            }

            if (sql.StartsWith("SELECT changes()", StringComparison.OrdinalIgnoreCase))
                return Scalar("changes()", "INTEGER", Integer(_lastAffected));
            if (sql.StartsWith("SELECT last_insert_rowid()", StringComparison.OrdinalIgnoreCase))
                return Scalar("last_insert_rowid()", "INTEGER", Integer(_nextId - 1));
            if (sql.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase))
                return Empty();
            if (sql is "SELECT ef_sum FROM data"
                or "SELECT EF_DECIMAL FROM data"
                or "SELECT REGEXP FROM data")
            {
                return Empty();
            }

            throw new AssertionException($"Unexpected EF SQL:\n{sql}");
        }

        private static Dictionary<string, RemoteValue> GetArguments(JsonElement statement)
            => statement.GetProperty("named_args")
                .EnumerateArray()
                .ToDictionary(
                    argument => argument.GetProperty("name").GetString()!,
                    argument =>
                    {
                        var value = argument.GetProperty("value");
                        var type = value.GetProperty("type").GetString()!;
                        var text = type == "null"
                            ? string.Empty
                            : value.GetProperty("value").ToString();
                        return new RemoteValue(type, text);
                    },
                    StringComparer.Ordinal);

        private static Dictionary<string, string> GetColumnValues(
            string sql,
            IReadOnlyDictionary<string, RemoteValue> arguments)
        {
            var match = Regex.Match(
                sql,
                "INSERT INTO \"RemoteItems\"\\s*\\((?<columns>[^)]+)\\)\\s*VALUES\\s*\\((?<values>[^)]+)\\)",
                RegexOptions.IgnoreCase);
            match.Success.Should().BeTrue();
            var columns = match.Groups["columns"].Value
                .Split(',')
                .Select(value => value.Trim().Trim('"'))
                .ToArray();
            var values = match.Groups["values"].Value
                .Split(',')
                .Select(value => value.Trim())
                .ToArray();
            return columns.Zip(values).ToDictionary(
                pair => pair.First,
                pair => arguments[pair.Second].Value,
                StringComparer.Ordinal);
        }

        private static JsonObject Ok(string responseType, JsonObject? content = null)
        {
            var response = content ?? new JsonObject();
            response["type"] = responseType;
            return new JsonObject
            {
                ["type"] = "ok",
                ["response"] = response,
            };
        }

        private static JsonObject Empty(int affectedRows = 0, string? lastInsertRowId = null)
            => Result([], [], affectedRows, lastInsertRowId);

        private static JsonObject Scalar(
            string name,
            string declaredType,
            JsonObject value,
            int affectedRows = 0,
            string? lastInsertRowId = null)
            => Result([(name, declaredType)], [[value]], affectedRows, lastInsertRowId);

        private static JsonObject Result(
            IReadOnlyList<(string Name, string DeclaredType)> columns,
            IReadOnlyList<IReadOnlyList<JsonObject>> rows,
            int affectedRows = 0,
            string? lastInsertRowId = null)
        {
            var jsonColumns = new JsonArray();
            foreach (var column in columns)
            {
                jsonColumns.Add(
                    new JsonObject
                    {
                        ["name"] = column.Name,
                        ["decltype"] = column.DeclaredType,
                    });
            }

            var jsonRows = new JsonArray();
            foreach (var row in rows)
            {
                var jsonRow = new JsonArray();
                foreach (var value in row)
                    jsonRow.Add(value);
                jsonRows.Add(jsonRow);
            }

            return new JsonObject
            {
                ["cols"] = jsonColumns,
                ["rows"] = jsonRows,
                ["affected_row_count"] = affectedRows,
                ["last_insert_rowid"] = lastInsertRowId,
            };
        }

        private static JsonObject Integer(long value)
            => new()
            {
                ["type"] = "integer",
                ["value"] = value.ToString(CultureInfo.InvariantCulture),
            };

        private static JsonObject Text(string value)
            => new()
            {
                ["type"] = "text",
                ["value"] = value,
            };

        private sealed record RemoteValue(string Type, string Value);

        private sealed record RemoteItem(long Id, string Name, decimal Amount);
    }
}

public sealed class EfRemoteReplicaDbContext(DbContextOptions<EfRemoteReplicaDbContext> options)
    : DbContext(options)
{
    public DbSet<EfRemoteReplicaItem> Items => Set<EfRemoteReplicaItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EfRemoteReplicaItem>(
            entity =>
            {
                entity.ToTable("RemoteItems");
                entity.HasKey(item => item.Id);
                entity.Property(item => item.Id).ValueGeneratedOnAdd();
                entity.Property(item => item.Name).IsRequired();
                entity.Property(item => item.Amount).HasColumnType("TEXT");
            });
    }
}

public sealed class EfRemoteReplicaItem
{
    public long Id { get; set; }

    public string Name { get; set; } = "";

    public decimal Amount { get; set; }
}

[DbContext(typeof(EfRemoteReplicaDbContext))]
[Migration(MigrationId)]
public sealed class EfRemoteReplicaMigration : Migration
{
    public const string MigrationId = "20260825153300_RemoteReplica";

    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "RemoteItems",
            columns: table => new
            {
                Id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                Name = table.Column<string>(type: "TEXT", nullable: false),
                Amount = table.Column<decimal>(type: "TEXT", nullable: false),
            },
            constraints: table => table.PrimaryKey("PK_RemoteItems", value => value.Id));
    }

    protected override void Down(MigrationBuilder migrationBuilder)
        => migrationBuilder.DropTable(name: "RemoteItems");
}
