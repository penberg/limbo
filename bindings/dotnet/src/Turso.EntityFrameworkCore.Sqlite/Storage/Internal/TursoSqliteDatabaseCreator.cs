using System.Data;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Sqlite.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using TursoSqliteConnection = Turso.Data.Sqlite.SqliteConnection;
using TursoSqliteConnectionStringBuilder = Turso.Data.Sqlite.SqliteConnectionStringBuilder;
using TursoSqliteException = Turso.Data.Sqlite.SqliteException;
using TursoSqliteOpenMode = Turso.Data.Sqlite.SqliteOpenMode;

namespace Turso.EntityFrameworkCore.Sqlite.Storage.Internal;

public class TursoSqliteDatabaseCreator(
    RelationalDatabaseCreatorDependencies dependencies,
    ISqliteRelationalConnection connection,
    IRawSqlCommandBuilder rawSqlCommandBuilder)
    : RelationalDatabaseCreator(dependencies)
{
    private const int SQLITE_CANTOPEN = 14;
    private const string ProbeSql = "SELECT 1;";
    private const string HasTablesSql =
        "SELECT COUNT(*) FROM \"sqlite_master\" "
        + "WHERE \"type\" = 'table' AND \"rootpage\" IS NOT NULL "
        + "AND \"name\" NOT LIKE 'sqlite_%' "
        + "AND \"name\" NOT LIKE 'turso_%' "
        + "AND \"name\" NOT LIKE '__turso_%';";
    private const string EnableWalSql = "PRAGMA journal_mode = 'wal';";

    public override void Create()
    {
        var options = GetConnectionOptions();
        if (options.IsDirectRemote)
            return;

        Dependencies.Connection.Open();
        try
        {
            if (options.IsLocal)
                ExecuteNonQuery(EnableWalSql, Dependencies.Connection);
        }
        finally
        {
            Dependencies.Connection.Close();
        }
    }

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = GetConnectionOptions();
        if (options.IsDirectRemote)
            return;

        await Dependencies.Connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (options.IsLocal)
            {
                await ExecuteNonQueryAsync(EnableWalSql, Dependencies.Connection, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            await Dependencies.Connection.CloseAsync().ConfigureAwait(false);
        }
    }

    public override bool Exists()
    {
        var options = GetConnectionOptions();
        if (IsMemory(options))
            return true;
        if (options.IsReplica)
            return File.Exists(options.ReplicaPath);

        using var readOnlyConnection = connection.CreateReadOnlyConnection();
        try
        {
            readOnlyConnection.Open(errorsExpected: true);
            if (options.IsDirectRemote)
                _ = ExecuteScalar(ProbeSql, readOnlyConnection);
        }
        catch (TursoSqliteException ex) when (ex.SqliteErrorCode == SQLITE_CANTOPEN)
        {
            return false;
        }
        finally
        {
            readOnlyConnection.Close();
        }

        return true;
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = GetConnectionOptions();
        if (IsMemory(options))
            return true;
        if (options.IsReplica)
            return File.Exists(options.ReplicaPath);

        await using var readOnlyConnection = connection.CreateReadOnlyConnection();
        try
        {
            await readOnlyConnection.OpenAsync(cancellationToken, errorsExpected: true).ConfigureAwait(false);
            if (options.IsDirectRemote)
            {
                _ = await ExecuteScalarAsync(ProbeSql, readOnlyConnection, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (TursoSqliteException ex) when (ex.SqliteErrorCode == SQLITE_CANTOPEN)
        {
            return false;
        }
        finally
        {
            await readOnlyConnection.CloseAsync().ConfigureAwait(false);
        }

        return true;
    }

    public override bool HasTables()
        => Convert.ToInt64(ExecuteScalar(HasTablesSql, Dependencies.Connection)) != 0;

    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
        => Convert.ToInt64(
            await ExecuteScalarAsync(HasTablesSql, Dependencies.Connection, cancellationToken)
                .ConfigureAwait(false)) != 0;

    public override void Delete()
    {
        var options = GetConnectionOptions();
        ThrowIfDirectRemoteDelete(options);

        var dbConnection = Dependencies.Connection.DbConnection;
        var wasOpen = dbConnection.State == ConnectionState.Open;
        var path = options.IsReplica
            ? options.ReplicaPath
            : GetOpenLocalPath();

        Dependencies.Connection.Close();
        if (dbConnection.State != ConnectionState.Closed)
            dbConnection.Close();
        DeleteDatabaseFiles(path, options.IsReplica);

        if (wasOpen && options.IsLocal)
            Dependencies.Connection.Open();
    }

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = GetConnectionOptions();
        ThrowIfDirectRemoteDelete(options);

        var dbConnection = Dependencies.Connection.DbConnection;
        var wasOpen = dbConnection.State == ConnectionState.Open;
        string path;
        if (options.IsReplica)
        {
            path = options.ReplicaPath;
        }
        else
        {
            if (!wasOpen)
                await Dependencies.Connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            path = dbConnection.DataSource;
        }

        await Dependencies.Connection.CloseAsync().ConfigureAwait(false);
        if (dbConnection.State != ConnectionState.Closed)
            await dbConnection.CloseAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        DeleteDatabaseFiles(path, options.IsReplica);

        if (wasOpen && options.IsLocal)
            await Dependencies.Connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    private TursoSqliteConnectionStringBuilder GetConnectionOptions()
        => new(connection.ConnectionString);

    private string GetOpenLocalPath()
    {
        var dbConnection = Dependencies.Connection.DbConnection;
        if (dbConnection.State != ConnectionState.Open)
            Dependencies.Connection.Open();
        return dbConnection.DataSource;
    }

    private object? ExecuteScalar(string sql, IRelationalConnection relationalConnection)
        => rawSqlCommandBuilder.Build(sql)
            .ExecuteScalar(CreateCommandParameters(relationalConnection));

    private Task<object?> ExecuteScalarAsync(
        string sql,
        IRelationalConnection relationalConnection,
        CancellationToken cancellationToken)
        => rawSqlCommandBuilder.Build(sql)
            .ExecuteScalarAsync(CreateCommandParameters(relationalConnection), cancellationToken);

    private void ExecuteNonQuery(string sql, IRelationalConnection relationalConnection)
        => rawSqlCommandBuilder.Build(sql)
            .ExecuteNonQuery(CreateCommandParameters(relationalConnection));

    private Task<int> ExecuteNonQueryAsync(
        string sql,
        IRelationalConnection relationalConnection,
        CancellationToken cancellationToken)
        => rawSqlCommandBuilder.Build(sql)
            .ExecuteNonQueryAsync(CreateCommandParameters(relationalConnection), cancellationToken);

    private RelationalCommandParameterObject CreateCommandParameters(IRelationalConnection relationalConnection)
        => new(
            relationalConnection,
            null,
            null,
            null,
            Dependencies.CommandLogger,
            CommandSource.Migrations);

    private static bool IsMemory(TursoSqliteConnectionStringBuilder options)
        => options.IsLocal
            && (options.DataSource.Equals(":memory:", StringComparison.OrdinalIgnoreCase)
                || options.Mode == TursoSqliteOpenMode.Memory)
           || options.IsReplica
            && options.ReplicaPath.Equals(":memory:", StringComparison.OrdinalIgnoreCase);

    private static void ThrowIfDirectRemoteDelete(TursoSqliteConnectionStringBuilder options)
    {
        if (options.IsDirectRemote)
        {
            throw new NotSupportedException(
                "Deleting a direct remote Turso database is not supported. "
                + "Use the Turso platform API to delete the remote database.");
        }
    }

    private static void DeleteDatabaseFiles(string path, bool includeReplicaSidecars)
    {
        if (string.IsNullOrWhiteSpace(path)
            || path.Equals(":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        TursoSqliteConnection.ClearAllPools();
        var fullPath = Path.GetFullPath(path);
        File.Delete(fullPath);
        File.Delete(fullPath + "-journal");
        File.Delete(fullPath + "-wal");
        File.Delete(fullPath + "-shm");

        if (!includeReplicaSidecars)
            return;

        File.Delete(Path.ChangeExtension(fullPath, ".db-log"));
        File.Delete(fullPath + "-wal-revert");
        File.Delete(fullPath + "-info");
        File.Delete(fullPath + "-changes");
        File.Delete(fullPath + "-replace-base-apply");

        var directory = Path.GetDirectoryName(fullPath);
        if (directory is null || !Directory.Exists(directory))
            return;

        var fileName = Path.GetFileName(fullPath);
        foreach (var backup in Directory.EnumerateFiles(
                     directory,
                     fileName + "-replace-base-apply-*.backup",
                     SearchOption.TopDirectoryOnly))
        {
            File.Delete(backup);
        }
    }
}

public sealed class TursoSqliteHistoryRepository(HistoryRepositoryDependencies dependencies)
    : SqliteHistoryRepository(dependencies)
{
    private static readonly TimeSpan MigrationLockRetryDelay = TimeSpan.FromSeconds(1);

    protected override string LockTableName { get; } = "__EFMigrationsLock";

    protected override string ExistsSql
    {
        get
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));
            return "SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"name\" = "
                   + stringTypeMapping.GenerateSqlLiteral(TableName)
                   + " COLLATE NOCASE AND \"type\" = 'table';";
        }
    }

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        if (!IsDirectRemote)
            return base.AcquireDatabaseLock();

        Dependencies.MigrationsLogger.AcquiringMigrationLock();
        CreateLockTableCommand().ExecuteNonQuery(CreateCommandParameters());

        var retryDelay = MigrationLockRetryDelay;
        while (true)
        {
            var relationalCommandParameters = CreateCommandParameters();
            var migrationLock = new SqliteMigrationDatabaseLock(
                CreateDeleteLockCommand(),
                relationalCommandParameters,
                this);
            if (CreateInsertLockCommand(DateTimeOffset.UtcNow)
                .ExecuteScalar(relationalCommandParameters) is not null)
            {
                return migrationLock;
            }

            Thread.Sleep(retryDelay);
            if (retryDelay < TimeSpan.FromMinutes(1))
                retryDelay += retryDelay;
        }
    }

    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default)
    {
        if (!IsDirectRemote)
            return await base.AcquireDatabaseLockAsync(cancellationToken).ConfigureAwait(false);

        Dependencies.MigrationsLogger.AcquiringMigrationLock();
        await CreateLockTableCommand()
            .ExecuteNonQueryAsync(CreateCommandParameters(), cancellationToken)
            .ConfigureAwait(false);

        var retryDelay = MigrationLockRetryDelay;
        while (true)
        {
            var relationalCommandParameters = CreateCommandParameters();
            var migrationLock = new SqliteMigrationDatabaseLock(
                CreateDeleteLockCommand(),
                relationalCommandParameters,
                this);
            if (await CreateInsertLockCommand(DateTimeOffset.UtcNow)
                    .ExecuteScalarAsync(relationalCommandParameters, cancellationToken)
                    .ConfigureAwait(false) is not null)
            {
                return migrationLock;
            }

            await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            if (retryDelay < TimeSpan.FromMinutes(1))
                retryDelay += retryDelay;
        }
    }

    private bool IsDirectRemote
        => Dependencies.Connection.DbConnection is TursoSqliteConnection { IsDirectRemote: true };

    private IRelationalCommand CreateLockTableCommand()
        => Dependencies.RawSqlCommandBuilder.Build(
            $"""
             CREATE TABLE IF NOT EXISTS "{LockTableName}" (
                 "Id" INTEGER NOT NULL CONSTRAINT "PK_{LockTableName}" PRIMARY KEY,
                 "Timestamp" TEXT NOT NULL
             );
             """);

    private IRelationalCommand CreateInsertLockCommand(DateTimeOffset timestamp)
    {
        var timestampLiteral = Dependencies.TypeMappingSource.GetMapping(typeof(DateTimeOffset))
            .GenerateSqlLiteral(timestamp);
        return Dependencies.RawSqlCommandBuilder.Build(
            $"""
             INSERT OR IGNORE INTO "{LockTableName}"("Id", "Timestamp") VALUES(1, {timestampLiteral})
             RETURNING "Id";
             """);
    }

    private IRelationalCommand CreateDeleteLockCommand()
        => Dependencies.RawSqlCommandBuilder.Build(
            $"""
             DELETE FROM "{LockTableName}";
             """);

    private RelationalCommandParameterObject CreateCommandParameters()
        => new(
            Dependencies.Connection,
            null,
            null,
            Dependencies.CurrentContext.Context,
            Dependencies.CommandLogger,
            CommandSource.Migrations);
}
