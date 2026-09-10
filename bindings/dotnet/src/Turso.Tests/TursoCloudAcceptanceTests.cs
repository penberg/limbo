using System.Data;
using AwesomeAssertions;
using Microsoft.EntityFrameworkCore;
using FacadeConnection = Turso.Data.Sqlite.SqliteConnection;

namespace Turso.Tests;

[NonParallelizable]
public sealed class TursoCloudAcceptanceTests
{
    [Test]
    public async Task AutomaticSyncPullsRemoteChanges()
    {
        var (remoteUrl, authToken) = GetCloudCredentials();
        var tableName = "dotnet_interval_" + Guid.NewGuid().ToString("N");
        var replicaDirectory = NewReplicaDirectory("interval");
        var replicaPath = Path.Combine(replicaDirectory, "replica.db");
        var remoteConnectionString = $"Data Source={remoteUrl};Auth Token={authToken}";

        await using var remote = new TursoConnection(remoteConnectionString);
        await remote.OpenAsync();
        try
        {
            await ExecuteNonQueryAsync(remote, $"CREATE TABLE {tableName}(id INTEGER PRIMARY KEY, value INTEGER)");
            await ExecuteNonQueryAsync(remote, $"INSERT INTO {tableName} VALUES (1, 1)");
            await using var replica = new TursoConnection(
                remoteConnectionString
                + $";Replica Path={replicaPath};Pooling=False;Sync Interval=1");
            var synchronized = new TaskCompletionSource<TursoAutomaticSyncStatus>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            replica.AutomaticSyncStatusChanged += (_, args) =>
            {
                if (args.Status.LastPullAppliedChanges == true)
                    synchronized.TrySetResult(args.Status);
            };
            await replica.OpenAsync();

            await ExecuteNonQueryAsync(remote, $"UPDATE {tableName} SET value = 2 WHERE id = 1");
            var status = await synchronized.Task.WaitAsync(TimeSpan.FromSeconds(10));

            status.LastSuccess.Should().NotBeNull();
            await using var verify = replica.CreateCommand();
            verify.CommandText = $"SELECT value FROM {tableName} WHERE id = 1";
            (await verify.ExecuteScalarAsync()).Should().Be(2L);
        }
        finally
        {
            await ExecuteNonQueryAsync(remote, $"DROP TABLE IF EXISTS {tableName}");
            Directory.Delete(replicaDirectory, recursive: true);
        }
    }

    [Test]
    public async Task SqliteFacadeAndEfQueryDirectRemoteAndReplica()
    {
        var (remoteUrl, authToken) = GetCloudCredentials();
        var tableName = "dotnet_facade_" + Guid.NewGuid().ToString("N");
        var replicaDirectory = NewReplicaDirectory("facade-ef");
        var replicaPath = Path.Combine(replicaDirectory, "replica.db");
        var remoteConnectionString = $"Data Source={remoteUrl};Auth Token={authToken}";
        var replicaConnectionString =
            remoteConnectionString + $";Replica Path={replicaPath};Pooling=True";

        await using var remote = new TursoConnection(remoteConnectionString);
        await remote.OpenAsync();
        try
        {
            await ExecuteNonQueryAsync(remote, $"CREATE TABLE {tableName}(value TEXT)");
            await ExecuteNonQueryAsync(remote, $"INSERT INTO {tableName} VALUES ('facade-ef')");

            await using (var facadeRemote = new FacadeConnection(remoteConnectionString))
            {
                await facadeRemote.OpenAsync();
                await using var command = facadeRemote.CreateCommand();
                command.CommandText = $"SELECT value FROM {tableName}";
                (await command.ExecuteScalarAsync()).Should().Be("facade-ef");
            }

            await using (var remoteContext = new DbContext(
                             new DbContextOptionsBuilder()
                                 .UseTurso(remoteConnectionString)
                                 .Options))
            {
#pragma warning disable EF1002 // tableName is generated from a fixed prefix and Guid("N").
                var value = await remoteContext.Database
                    .SqlQueryRaw<string>($"SELECT value AS Value FROM {tableName}")
                    .SingleAsync();
#pragma warning restore EF1002
                value.Should().Be("facade-ef");
            }

            await using (var facadeReplica = new FacadeConnection(replicaConnectionString))
            {
                await facadeReplica.OpenAsync();
                await using var command = facadeReplica.CreateCommand();
                command.CommandText = $"SELECT value FROM {tableName}";
                (await command.ExecuteScalarAsync()).Should().Be("facade-ef");
            }

            await using (var replicaContext = new DbContext(
                             new DbContextOptionsBuilder()
                                 .UseTurso(replicaConnectionString)
                                 .Options))
            {
#pragma warning disable EF1002 // tableName is generated from a fixed prefix and Guid("N").
                var value = await replicaContext.Database
                    .SqlQueryRaw<string>($"SELECT value AS Value FROM {tableName}")
                    .SingleAsync();
#pragma warning restore EF1002
                value.Should().Be("facade-ef");
            }
        }

        finally
        {
            await DropTableAsync(remote, tableName);
            Directory.Delete(replicaDirectory, recursive: true);
        }
    }

    private static async Task ExecuteNonQueryAsync(TursoConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropTableAsync(TursoConnection remote, string tableName)
    {
        if (remote.State != ConnectionState.Open)
            await remote.OpenAsync();
        await ExecuteNonQueryAsync(remote, $"DROP TABLE IF EXISTS {tableName}");
    }

    private static (string RemoteUrl, string AuthToken) GetCloudCredentials()
    {
        var remoteUrl = Environment.GetEnvironmentVariable("TURSO_REMOTE_URL");
        var authToken = Environment.GetEnvironmentVariable("TURSO_AUTH_TOKEN");
        if (string.IsNullOrWhiteSpace(remoteUrl) || string.IsNullOrWhiteSpace(authToken))
            Assert.Ignore("Set TURSO_REMOTE_URL and TURSO_AUTH_TOKEN to run the Turso Cloud acceptance tests.");

        return (remoteUrl, authToken);
    }

    private static string NewReplicaDirectory(string suffix)
    {
        var path = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            $"turso-cloud-{suffix}-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
