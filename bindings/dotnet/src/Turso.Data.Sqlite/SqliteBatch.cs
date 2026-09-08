using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Turso.Raw.Public;

namespace Turso.Data.Sqlite;

public sealed class SqliteBatch : DbBatch
{
    private readonly SqliteBatchCommandCollection _batchCommands = new();
    private SqliteConnection? _connection;
    private SqliteTransaction? _transaction;
    private global::Turso.TursoBatch? _activeBatch;
    private int _timeout = 30;

    public SqliteBatch()
    {
    }

    public SqliteBatch(SqliteConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
        _transaction = connection.Transaction;
        _timeout = connection.DefaultTimeout;
    }

    public new SqliteBatchCommandCollection BatchCommands => _batchCommands;

    protected override DbBatchCommandCollection DbBatchCommands => _batchCommands;

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set
        {
            _connection = value as SqliteConnection
                          ?? (value is null
                              ? null
                              : throw new ArgumentException(
                                  "Connection must be a SqliteConnection.",
                                  nameof(value)));
            if (_connection is not null)
                _timeout = _connection.DefaultTimeout;
        }
    }

    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = value as SqliteTransaction
                             ?? (value is null
                                 ? null
                                 : throw new ArgumentException(
                                     "Transaction must be a SqliteTransaction.",
                                     nameof(value)));
    }

    public override int Timeout
    {
        get => _timeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _timeout = value;
        }
    }

    public override void Cancel()
    {
        _activeBatch?.Cancel();
    }

    public override int ExecuteNonQuery()
    {
        var state = BuildBatch();
        if (state.Batch is null)
            return SetRecordsAffected(state);

        using (state.Batch)
        {
            WaitForOpenReader(state.Statements);
            _activeBatch = state.Batch;
            try
            {
                var recordsAffected = state.Batch.ExecuteNonQuery();
                SetRecordsAffected(state);
                return recordsAffected;
            }
            catch (TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
            finally
            {
                _activeBatch = null;
            }
        }
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = BuildBatch();
        if (state.Batch is null)
            return SetRecordsAffected(state);

        await using (state.Batch)
        {
            await WaitForOpenReaderAsync(state.Statements, cancellationToken).ConfigureAwait(false);
            _activeBatch = state.Batch;
            try
            {
                var recordsAffected = await state.Batch
                    .ExecuteNonQueryAsync(cancellationToken)
                    .ConfigureAwait(false);
                SetRecordsAffected(state);
                return recordsAffected;
            }
            catch (TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
            finally
            {
                _activeBatch = null;
            }
        }
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        return ReadScalar(reader);
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var reader = await ExecuteDbDataReaderAsync(
                CommandBehavior.Default,
                cancellationToken)
            .ConfigureAwait(false);
        return await ReadScalarAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    public override void Prepare()
    {
        var state = BuildBatch(preparing: true);
        if (state.Batch is null)
            return;

        using (state.Batch)
        {
            try
            {
                state.Batch.Prepare();
            }
            catch (TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
        }
    }

    public override async Task PrepareAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = BuildBatch(preparing: true);
        if (state.Batch is null)
            return;

        await using (state.Batch)
        {
            try
            {
                await state.Batch.PrepareAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
        }
    }

    protected override DbBatchCommand CreateDbBatchCommand()
        => new SqliteBatchCommand();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteReaderAsyncCore(behavior, CancellationToken.None).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
        => await ExecuteReaderAsyncCore(behavior, cancellationToken).ConfigureAwait(false);

    private async Task<SqliteDataReader> ExecuteReaderAsyncCore(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = BuildBatch();
        if (state.Batch is null)
        {
            SetRecordsAffected(state);
            var emptyOwner = CreateReaderOwner();
            var closeCallback = emptyOwner.OwnBufferedReader();
            return new SqliteDataReader(
                emptyOwner,
                new ManagedSqliteExecution([], 0, HadResultSet: false),
                behavior,
                closeCallback);
        }

        await using (state.Batch)
        {
            await WaitForOpenReaderAsync(state.Statements, cancellationToken).ConfigureAwait(false);
            _activeBatch = state.Batch;
            try
            {
                await using var reader = await state.Batch
                    .ExecuteReaderAsync(behavior & ~CommandBehavior.CloseConnection, cancellationToken)
                    .ConfigureAwait(false);
                var results = new List<ManagedSqliteResult>(state.Statements.Count);
                var statementIndex = 0;
                do
                {
                    if (statementIndex >= state.Statements.Count)
                    {
                        throw new SqliteException(
                            Properties.Resources.SqliteNativeError(
                                1,
                                $"Managed batch returned more than {state.Statements.Count} results."),
                            1);
                    }

                    var statement = state.Statements[statementIndex];
                    var result = await SqliteCommand
                        .BufferManagedResultAsync(reader, statement.Sql, cancellationToken)
                        .ConfigureAwait(false);
                    results.Add(result with
                    {
                        RecordsAffected = state.ManagedCommands[statementIndex].RecordsAffected,
                    });
                    statementIndex++;
                }
                while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

                if (statementIndex != state.Statements.Count)
                {
                    throw new SqliteException(
                        Properties.Resources.SqliteNativeError(
                            1,
                            $"Managed batch returned {statementIndex} results for {state.Statements.Count} statements."),
                        1);
                }

                var recordsAffected = SetRecordsAffected(state);
                var owner = CreateReaderOwner();
                var closeCallback = owner.OwnBufferedReader();
                return new SqliteDataReader(
                    owner,
                    new ManagedSqliteExecution(
                        results,
                        recordsAffected,
                        results.Any(static result => result.Columns.Count > 0)),
                    behavior,
                    closeCallback);
            }
            catch (TursoException ex)
            {
                throw SqliteCommand.ToSqliteException(ex);
            }
            finally
            {
                _activeBatch = null;
            }
        }
    }

    private BatchState BuildBatch(bool preparing = false)
    {
        var connection = ValidateBatch();
        var managedBatch = new global::Turso.TursoBatch(connection.ManagedConnection)
        {
            Timeout = Timeout,
            Transaction = _transaction?.ManagedTransaction,
        };
        var statements = new List<ManagedSqliteStatement>();
        var managedCommands = new List<global::Turso.TursoBatchCommand>();
        var mappings = new List<BatchCommandMapping>(_batchCommands.Count);
        try
        {
            foreach (var batchCommand in _batchCommands.Items)
            {
                using var command = new SqliteCommand(batchCommand.CommandText, connection, _transaction);
                foreach (SqliteParameter parameter in batchCommand.Parameters)
                    command.Parameters.Add(parameter);
                command.ValidateManagedParameterValues();

                var commandStatements = ManagedSqliteStatementParser.Parse(batchCommand.CommandText);
                if (commandStatements.Count == 0)
                    throw new InvalidOperationException("Batch command text must contain a SQL statement.");
                if (commandStatements.Any(static statement => statement.IsTransactionControl))
                {
                    throw new InvalidOperationException(
                        "Transaction-control SQL is not supported in a SqliteBatch. "
                        + "Use SqliteConnection.BeginTransaction and SqliteTransaction instead.");
                }
                if (connection.IsReadOnly
                    && commandStatements.Any(SqliteCommand.IsWriteStatement))
                {
                    throw new SqliteException(
                        Properties.Resources.SqliteNativeError(8, "attempt to write a readonly database"),
                        8);
                }

                var firstManagedIndex = managedCommands.Count;
                foreach (var statement in commandStatements)
                {
                    string sql;
                    if (preparing)
                    {
                        sql = SqliteCommand.RewriteFacadeStatement(statement.Sql, connection);
                    }
                    else if (command.TryHandleFacadeStatement(statement.Sql, out sql))
                    {
                        continue;
                    }

                    using var managedCommand = command.CreateManagedCommand(statement, sql);
                    var managedBatchCommand = new global::Turso.TursoBatchCommand(managedCommand.CommandText);
                    foreach (global::Turso.TursoParameter parameter in managedCommand.Parameters)
                        managedBatchCommand.Parameters.Add(parameter);
                    managedBatch.BatchCommands.Add(managedBatchCommand);
                    managedCommands.Add(managedBatchCommand);
                    statements.Add(statement);
                }

                mappings.Add(
                    new BatchCommandMapping(
                        batchCommand,
                        firstManagedIndex,
                        managedCommands.Count - firstManagedIndex));
            }

            if (managedCommands.Count == 0)
            {
                managedBatch.Dispose();
                return new BatchState(null, statements, managedCommands, mappings);
            }

            return new BatchState(managedBatch, statements, managedCommands, mappings);
        }
        catch
        {
            managedBatch.Dispose();
            throw;
        }
    }

    private SqliteConnection ValidateBatch()
    {
        var connection = _connection
                         ?? throw new InvalidOperationException(
                             "Connection must be set before executing a batch.");
        if (!connection.IsManagedConnection)
        {
            throw new NotSupportedException(
                "SQLite facade batches are available only for direct remote or embedded replica connections.");
        }
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.CallRequiresOpenConnection("ExecuteBatch"));
        if (_transaction is { IsCompleted: true })
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (_transaction is not null && !ReferenceEquals(_transaction.Connection, connection))
            throw new InvalidOperationException(Properties.Resources.TransactionConnectionMismatch);
        if (connection.Transaction is not null
            && !ReferenceEquals(_transaction, connection.Transaction))
        {
            throw new InvalidOperationException(Properties.Resources.TransactionRequired);
        }
        if (_batchCommands.Count == 0)
            throw new InvalidOperationException("Batch must contain at least one command.");

        return connection;
    }

    private int SetRecordsAffected(BatchState state)
    {
        var total = 0;
        foreach (var mapping in state.Mappings)
        {
            var commandTotal = 0;
            for (var index = 0; index < mapping.Count; index++)
            {
                var recordsAffected = state.ManagedCommands[mapping.FirstIndex + index].RecordsAffected;
                if (recordsAffected > 0)
                    commandTotal = checked(commandTotal + recordsAffected);
            }

            mapping.Command.SetRecordsAffected(commandTotal);
            total = checked(total + commandTotal);
        }

        return total;
    }

    private SqliteCommand CreateReaderOwner()
        => new(_connection) { Transaction = _transaction };

    private void WaitForOpenReader(IReadOnlyList<ManagedSqliteStatement> statements)
    {
        if (_connection?.HasOpenReader != true || !statements.Any(SqliteCommand.IsWriteStatement))
            return;

        Thread.Sleep(TimeSpan.FromSeconds(Timeout));
        throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
    }

    private async Task WaitForOpenReaderAsync(
        IReadOnlyList<ManagedSqliteStatement> statements,
        CancellationToken cancellationToken)
    {
        if (_connection?.HasOpenReader != true || !statements.Any(SqliteCommand.IsWriteStatement))
            return;

        await Task.Delay(TimeSpan.FromSeconds(Timeout), cancellationToken).ConfigureAwait(false);
        throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
    }

    private static object? ReadScalar(DbDataReader reader)
    {
        do
        {
            if (reader.FieldCount > 0 && reader.Read())
                return reader.GetValue(0);
        } while (reader.NextResult());

        return null;
    }

    private static async Task<object?> ReadScalarAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        do
        {
            if (reader.FieldCount > 0
                && await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return reader.GetValue(0);
            }
        } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

        return null;
    }

    private sealed record BatchCommandMapping(
        SqliteBatchCommand Command,
        int FirstIndex,
        int Count);

    private sealed record BatchState(
        global::Turso.TursoBatch? Batch,
        IReadOnlyList<ManagedSqliteStatement> Statements,
        IReadOnlyList<global::Turso.TursoBatchCommand> ManagedCommands,
        IReadOnlyList<BatchCommandMapping> Mappings);
}

public sealed class SqliteBatchCommand : DbBatchCommand
{
    private readonly SqliteParameterCollection _parameters = new();
    private string _commandText = string.Empty;
    private int _recordsAffected = -1;

    public SqliteBatchCommand()
    {
    }

    public SqliteBatchCommand(string commandText)
    {
        CommandText = commandText;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? string.Empty;
    }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new ArgumentException(Properties.Resources.InvalidCommandType(value));
        }
    }

    public new SqliteParameterCollection Parameters => _parameters;

    protected override DbParameterCollection DbParameterCollection => _parameters;

    public override int RecordsAffected => _recordsAffected;

    public override bool CanCreateParameter => true;

    public override DbParameter CreateParameter()
        => new SqliteParameter();

    internal void SetRecordsAffected(int recordsAffected)
    {
        _recordsAffected = recordsAffected;
    }
}

public sealed class SqliteBatchCommandCollection : DbBatchCommandCollection
{
    private readonly List<SqliteBatchCommand> _commands = [];

    public override int Count => _commands.Count;

    public override bool IsReadOnly => false;

    internal IReadOnlyList<SqliteBatchCommand> Items => _commands;

    public override void Add(DbBatchCommand item)
        => _commands.Add(RequireCommand(item));

    public override void Clear()
        => _commands.Clear();

    public override bool Contains(DbBatchCommand item)
        => item is SqliteBatchCommand command && _commands.Contains(command);

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        for (var index = 0; index < _commands.Count; index++)
            array[arrayIndex + index] = _commands[index];
    }

    public override IEnumerator<DbBatchCommand> GetEnumerator()
        => _commands.GetEnumerator();

    public override int IndexOf(DbBatchCommand item)
        => item is SqliteBatchCommand command ? _commands.IndexOf(command) : -1;

    public override void Insert(int index, DbBatchCommand item)
        => _commands.Insert(index, RequireCommand(item));

    public override bool Remove(DbBatchCommand item)
        => item is SqliteBatchCommand command && _commands.Remove(command);

    public override void RemoveAt(int index)
        => _commands.RemoveAt(index);

    protected override DbBatchCommand GetBatchCommand(int index)
        => _commands[index];

    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand)
        => _commands[index] = RequireCommand(batchCommand);

    private static SqliteBatchCommand RequireCommand(DbBatchCommand command)
        => command as SqliteBatchCommand
           ?? throw new ArgumentException(
               "Batch command must be a SqliteBatchCommand.",
               nameof(command));
}
