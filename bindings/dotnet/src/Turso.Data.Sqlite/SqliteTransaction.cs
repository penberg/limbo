using System.Data;
using System.Data.Common;

namespace Turso.Data.Sqlite;

public class SqliteTransaction : DbTransaction
{
    private SqliteConnection? _connection;
    private readonly IsolationLevel _isolationLevel;
    private readonly global::Turso.TursoTransaction? _managedTransaction;
    private bool _completed;
    private bool _externalRollback;

    internal SqliteTransaction(SqliteConnection connection, IsolationLevel isolationLevel, bool deferred)
    {
        _connection = connection;
        _isolationLevel = NormalizeIsolationLevel(connection, isolationLevel, deferred);

        if (_isolationLevel == IsolationLevel.ReadUncommitted)
            connection.ReadUncommitted = true;

        if (connection.IsManagedConnection)
        {
            try
            {
                _managedTransaction = new global::Turso.TursoTransaction(
                    connection.ManagedConnection,
                    _isolationLevel,
                    deferred);
            }
            catch (Turso.Raw.Public.TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }

            return;
        }

        Execute(_isolationLevel == IsolationLevel.Serializable && !deferred ? "BEGIN IMMEDIATE;" : "BEGIN;");
    }

    public override IsolationLevel IsolationLevel => _isolationLevel;

    public override bool SupportsSavepoints => true;

    protected override DbConnection? DbConnection => Connection;

    public new virtual SqliteConnection? Connection => _connection;

    internal bool IsCompleted => _completed;

    internal bool WasRolledBackExternally => _externalRollback;

    internal global::Turso.TursoTransaction? ManagedTransaction => _managedTransaction;

    public override void Commit()
    {
        ThrowIfCompleted();
        if (_externalRollback)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);

        if (_managedTransaction is not null)
        {
            try
            {
                _managedTransaction.Commit();
            }
            catch (global::Turso.TursoRemoteSqlException ex) when (ex.IsStreamExpired)
            {
                Complete();
                throw SqliteCommand.ToSqliteException(ex);
            }
            catch (Turso.Raw.Public.TursoException ex)
            {
                if (_managedTransaction.IsCompleted)
                    Complete();
                throw SqliteCommand.ToSqliteException(ex);
            }
            catch
            {
                if (_managedTransaction.IsCompleted)
                    Complete();
                throw;
            }
        }
        else
        {
            Execute("COMMIT;");
        }

        Complete();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfCompleted();
        if (_externalRollback)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (_managedTransaction is null)
        {
            Commit();
            return;
        }

        try
        {
            await _managedTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (global::Turso.TursoRemoteSqlException ex) when (ex.IsStreamExpired)
        {
            Complete();
            throw SqliteCommand.ToSqliteException(ex);
        }
        catch (Turso.Raw.Public.TursoException ex)
        {
            if (_managedTransaction.IsCompleted)
                Complete();
            throw SqliteCommand.ToSqliteException(ex);
        }
        catch
        {
            if (_managedTransaction.IsCompleted)
                Complete();
            throw;
        }

        Complete();
    }

    public override void Rollback()
    {
        ThrowIfCompleted();
        try
        {
            if (!_externalRollback)
            {
                if (_managedTransaction is not null)
                    _managedTransaction.Rollback();
                else
                    Execute("ROLLBACK;");
            }
        }
        catch (Turso.Raw.Public.TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex);
        }
        finally
        {
            Complete();
        }
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfCompleted();
        try
        {
            if (!_externalRollback)
            {
                if (_managedTransaction is not null)
                    await _managedTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                else
                    Execute("ROLLBACK;");
            }
        }
        catch (Turso.Raw.Public.TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex);
        }
        finally
        {
            Complete();
        }
    }

    public override void Save(string savepointName)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        ExecuteTransactionCommand("SAVEPOINT " + QuoteIdentifier(savepointName) + ";");
    }

    public override Task SaveAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        return ExecuteTransactionCommandAsync(
            "SAVEPOINT " + QuoteIdentifier(savepointName) + ";",
            cancellationToken);
    }

    public override void Rollback(string savepointName)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        ExecuteTransactionCommand("ROLLBACK TO SAVEPOINT " + QuoteIdentifier(savepointName) + ";");
    }

    public override Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        return ExecuteTransactionCommandAsync(
            "ROLLBACK TO SAVEPOINT " + QuoteIdentifier(savepointName) + ";",
            cancellationToken);
    }

    public override void Release(string savepointName)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        ExecuteTransactionCommand("RELEASE SAVEPOINT " + QuoteIdentifier(savepointName) + ";");
    }

    public override Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(savepointName);
        ThrowIfCompleted();
        return ExecuteTransactionCommandAsync(
            "RELEASE SAVEPOINT " + QuoteIdentifier(savepointName) + ";",
            cancellationToken);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed && _managedTransaction is not null)
        {
            try
            {
                _managedTransaction.Dispose();
            }
            catch (Turso.Raw.Public.TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
            finally
            {
                Complete();
            }
        }
        else if (disposing && !_completed && _connection is { State: ConnectionState.Open })
            Rollback();
        else if (disposing && _connection is not null && ReferenceEquals(_connection.Transaction, this))
            _connection.Transaction = null;

        base.Dispose(disposing);
    }

    internal void MarkCompletedExternally(bool rolledBack)
    {
        if (_managedTransaction is not null)
        {
            throw new InvalidOperationException(
                "Managed transactions cannot be completed through raw transaction-control SQL.");
        }

        if (rolledBack)
        {
            _externalRollback = true;
            return;
        }

        Complete();
    }

    private void Complete()
    {
        var connection = _connection;
        if (connection is null)
        {
            _completed = true;
            return;
        }

        connection.Transaction = null;
        if (_isolationLevel == IsolationLevel.ReadUncommitted)
            connection.ReadUncommitted = false;

        _completed = true;
        _connection = null;
    }

    private void ThrowIfCompleted()
    {
        if (_completed || _connection is null || _connection.State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
    }

    private static IsolationLevel NormalizeIsolationLevel(SqliteConnection connection, IsolationLevel isolationLevel, bool deferred)
    {
        if ((isolationLevel == IsolationLevel.ReadUncommitted && (!connection.IsSharedCache || !deferred))
            || isolationLevel == IsolationLevel.ReadCommitted
            || isolationLevel == IsolationLevel.RepeatableRead
            || isolationLevel == IsolationLevel.Unspecified)
        {
            return IsolationLevel.Serializable;
        }

        if (isolationLevel == IsolationLevel.Serializable || isolationLevel == IsolationLevel.ReadUncommitted)
            return isolationLevel;

        throw new ArgumentException(Properties.Resources.InvalidIsolationLevel(isolationLevel));
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private void Execute(string sql)
    {
        var connection = _connection;
        if (connection is null)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = this;
        command.ExecuteNonQuery();
    }

    private void ExecuteTransactionCommand(string sql)
    {
        if (_managedTransaction is null)
        {
            Execute(sql);
            return;
        }

        try
        {
            using var command = new global::Turso.TursoCommand(_connection!.ManagedConnection)
            {
                CommandText = sql,
                Transaction = _managedTransaction,
            };
            command.ExecuteNonQuery();
        }
        catch (Turso.Raw.Public.TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex, sql);
        }
    }

    private async Task ExecuteTransactionCommandAsync(string sql, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_managedTransaction is null)
        {
            Execute(sql);
            return;
        }

        try
        {
            await using var command = new global::Turso.TursoCommand(_connection!.ManagedConnection)
            {
                CommandText = sql,
                Transaction = _managedTransaction,
            };
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Turso.Raw.Public.TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex, sql);
        }
    }

}
