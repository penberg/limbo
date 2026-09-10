using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Data.Sqlite;

public class SqliteCommand : DbCommand
{
    private SqliteConnection? _connection;
    private SqliteTransaction? _transaction;
    private TursoStatementHandle? _statement;
    private string _commandText = string.Empty;
    private int _commandTimeout = 30;
    private bool _hasOpenReader;
    private global::Turso.TursoCommand? _activeManagedCommand;

    public SqliteCommand()
    {
    }

    public SqliteCommand(string? commandText)
    {
        CommandText = commandText;
    }

    public SqliteCommand(SqliteConnection? connection)
    {
        Connection = connection;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection)
        : this(commandText)
    {
        Connection = connection;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection, SqliteTransaction? transaction)
        : this(commandText, connection)
    {
        Transaction = transaction;
    }

    public SqliteCommand(string? commandText, SqliteConnection? connection, DbTransaction? transaction)
        : this(commandText, connection)
    {
        Transaction = transaction as SqliteTransaction
                      ?? (transaction is null ? null : throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction)));
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set
        {
            ThrowIfReaderOpen(nameof(CommandText));
            _commandText = value ?? string.Empty;
        }
    }

    public override int CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _commandTimeout = value;
        }
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

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    public new SqliteConnection? Connection
    {
        get => _connection;
        set
        {
            ThrowIfReaderOpen(nameof(Connection));
            _connection = value;
            if (value is not null)
            {
                _commandTimeout = value.DefaultTimeout;
                _transaction ??= value.Transaction;
            }
        }
    }

    public new SqliteParameterCollection Parameters { get; } = new();

    public new SqliteTransaction? Transaction
    {
        get => _transaction;
        set
        {
            ThrowIfReaderOpen(nameof(Transaction));
            _transaction = value;
        }
    }

    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = value as SqliteConnection
                            ?? (value is null ? null : throw new ArgumentException("Connection must be a SqliteConnection.", nameof(value)));
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => Transaction;
        set => Transaction = value as SqliteTransaction
                            ?? (value is null ? null : throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(value)));
    }

    public override void Cancel()
    {
        _activeManagedCommand?.Cancel();
    }

    public override int ExecuteNonQuery()
    {
        using var reader = Execute("ExecuteNonQuery");
        while (reader.Read())
        {
        }

        reader.Close();
        if (IsTransactionControlCommand(CommandText))
            Connection?.Transaction?.MarkCompletedExternally(IsRollbackCommand(CommandText));

        return reader.RecordsAffected;
    }

    public override object? ExecuteScalar()
    {
        using var reader = Execute("ExecuteScalar");
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public override void Prepare()
    {
        EnsureExecutable("Prepare");
        if (Connection!.IsManagedConnection)
        {
            PrepareManagedAsync(CancellationToken.None).GetAwaiter().GetResult();
            return;
        }

        var statements = SplitStatements(CommandText);
        if (statements.Count != 1)
        {
            _statement?.Dispose();
            _statement = null;
            return;
        }

        TursoStatementHandle? preparedStatement = null;
        try
        {
            preparedStatement = PrepareSingleStatement(statements[0]);
            _statement?.Dispose();
            _statement = preparedStatement;
            preparedStatement = null;
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex);
        }
        finally
        {
            preparedStatement?.Dispose();
        }
    }

    public override Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureExecutable("Prepare");
        return Connection!.IsManagedConnection
            ? PrepareManagedAsync(cancellationToken)
            : base.PrepareAsync(cancellationToken);
    }

    protected override DbParameter CreateDbParameter() => new SqliteParameter();

    public new SqliteDataReader ExecuteReader() => Execute("ExecuteReader");

    public new SqliteDataReader ExecuteReader(CommandBehavior behavior) => Execute("ExecuteReader", behavior);

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => Execute("ExecuteReader", behavior);

    public new Task<SqliteDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        => ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    public new async Task<SqliteDataReader> ExecuteReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
        => (SqliteDataReader)await ExecuteDbDataReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Connection?.IsManagedConnection != true)
            return ExecuteNonQuery();

        await using var reader = await ExecuteManagedAsync("ExecuteNonQuery", CommandBehavior.Default, cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
        }

        await reader.CloseAsync().ConfigureAwait(false);
        return reader.RecordsAffected;
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Connection?.IsManagedConnection != true)
            return ExecuteScalar();

        await using var reader = await ExecuteManagedAsync("ExecuteScalar", CommandBehavior.Default, cancellationToken)
            .ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? reader.GetValue(0)
            : null;
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Connection?.IsManagedConnection != true)
            return Execute("ExecuteReader", behavior);

        return await ExecuteManagedAsync("ExecuteReader", behavior, cancellationToken).ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _activeManagedCommand?.Dispose();
            _statement?.Dispose();
        }

        base.Dispose(disposing);
    }

    private SqliteDataReader Execute(string method, CommandBehavior behavior = CommandBehavior.Default)
    {
        EnsureExecutable(method);
        if (Connection!.IsManagedConnection)
        {
            return ExecuteManagedAsync(method, behavior, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }

        if (IsEmptyCommand(CommandText))
        {
            _hasOpenReader = true;
            Connection?.ReaderOpened();
            return new SqliteDataReader(this, -1, behavior, CloseReader);
        }

        if (Connection?.HasOpenReader == true && IsWriteCommand(CommandText))
        {
            Thread.Sleep(TimeSpan.FromSeconds(CommandTimeout));
            throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
        }
        if (Connection?.IsReadOnly == true && IsWriteCommand(CommandText))
            throw new SqliteException(Properties.Resources.SqliteNativeError(8, "attempt to write a readonly database"), 8);

        var recordsAffected = 0;
        var statements = SplitStatements(CommandText);
        try
        {
            for (var i = 0; i < statements.Count; i++)
            {
                if (TryHandleFacadeStatement(statements[i], out var sql))
                    continue;

                var statement = PrepareSingleStatement(sql);
                if (TursoBindings.GetFieldCount(statement) > 0)
                {
                    _hasOpenReader = true;
                    Connection?.ReaderOpened();
                    return new SqliteDataReader(this, statement, statements[i], statements.Skip(i + 1).ToList(), recordsAffected, behavior, CloseReader);
                }

                while (TursoBindings.Read(statement))
                {
                }

                if (CountsRowsAffected(statements[i]))
                    recordsAffected += TursoBindings.RowsAffected(statement);
                statement.Dispose();
            }
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex);
        }

        _hasOpenReader = true;
        Connection?.ReaderOpened();
        return new SqliteDataReader(this, recordsAffected, behavior, CloseReader);
    }

    private async Task PrepareManagedAsync(CancellationToken cancellationToken)
    {
        var statements = GetManagedStatements();
        ValidateManagedParameterValues();
        foreach (var statement in statements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sql = RewriteFacadeStatement(statement.Sql, Connection!);

            using var command = CreateManagedCommand(statement, sql);
            _activeManagedCommand = command;
            try
            {
                await command.PrepareAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (TursoException ex)
            {
                throw ToSqliteException(ex, statement.Sql);
            }
            finally
            {
                _activeManagedCommand = null;
            }
        }
    }

    private async Task<SqliteDataReader> ExecuteManagedAsync(
        string method,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        EnsureExecutable(method);
        var statements = GetManagedStatements();
        if (Connection!.HasOpenReader && statements.Any(IsWriteStatement))
        {
            await Task.Delay(TimeSpan.FromSeconds(CommandTimeout), cancellationToken).ConfigureAwait(false);
            throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
        }
        if (Connection!.IsReadOnly && statements.Any(IsWriteStatement))
        {
            throw new SqliteException(
                Properties.Resources.SqliteNativeError(8, "attempt to write a readonly database"),
                8);
        }
        if (statements.Count == 0)
        {
            _hasOpenReader = true;
            Connection!.ReaderOpened();
            return new SqliteDataReader(this, -1, behavior, CloseReader);
        }

        ValidateManagedParameterValues();
        var results = new List<ManagedSqliteResult>();
        var recordsAffected = 0;
        var hadResultSet = false;
        foreach (var statement in statements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryHandleFacadeStatement(statement.Sql, out var sql))
                continue;

            using var command = CreateManagedCommand(statement, sql);
            _activeManagedCommand = command;
            try
            {
                await using var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.Default, cancellationToken)
                    .ConfigureAwait(false);
                var result = await BufferManagedResultAsync(reader, statement.Sql, cancellationToken)
                    .ConfigureAwait(false);
                if (result.Columns.Count > 0)
                {
                    results.Add(result);
                    hadResultSet = true;
                }

                if (CountsRowsAffected(statement) && result.RecordsAffected > 0)
                    recordsAffected = checked(recordsAffected + result.RecordsAffected);
            }
            catch (TursoException ex)
            {
                throw ToSqliteException(ex, statement.Sql);
            }
            finally
            {
                _activeManagedCommand = null;
            }
        }

        _hasOpenReader = true;
        Connection!.ReaderOpened();
        return new SqliteDataReader(
            this,
            new ManagedSqliteExecution(results, recordsAffected, hadResultSet),
            behavior,
            CloseReader);
    }

    internal static async Task<ManagedSqliteResult> BufferManagedResultAsync(
        DbDataReader reader,
        string sql,
        CancellationToken cancellationToken)
    {
        var columns = new List<ManagedSqliteColumn>(reader.FieldCount);
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            columns.Add(
                new ManagedSqliteColumn(
                    reader.GetName(ordinal),
                    reader.GetDataTypeName(ordinal),
                    reader.GetFieldType(ordinal)));
        }

        var rows = new List<object?[]>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new object?[reader.FieldCount];
            for (var ordinal = 0; ordinal < row.Length; ordinal++)
                row[ordinal] = reader.GetValue(ordinal);
            rows.Add(row);
        }

        return new ManagedSqliteResult(sql, columns, rows, reader.RecordsAffected);
    }

    internal global::Turso.TursoCommand CreateManagedCommand(
        ManagedSqliteStatement statement,
        string? sql = null)
    {
        var command = new global::Turso.TursoCommand(Connection!.ManagedConnection)
        {
            CommandText = sql ?? statement.Sql,
            CommandTimeout = CommandTimeout,
            Transaction = Transaction?.ManagedTransaction,
        };

        try
        {
            AddManagedParameters(command, statement);
            return command;
        }
        catch
        {
            command.Dispose();
            throw;
        }
    }

    private IReadOnlyList<ManagedSqliteStatement> GetManagedStatements()
    {
        var statements = ManagedSqliteStatementParser.Parse(CommandText);
        if (statements.Any(statement => statement.IsTransactionControl && !CanExecuteSavepointStatement(statement)))
        {
            throw new InvalidOperationException(
                "Transaction-control SQL is not supported on a managed SqliteConnection. "
                + "Use SqliteConnection.BeginTransaction and SqliteTransaction instead.");
        }

        if (statements.Count > 1
            && Connection!.IsDirectRemote
            && !Connection.ManagedReadYourWrites
            && Transaction is null)
        {
            throw new NotSupportedException(
                "Multi-statement SqliteCommand execution requires Read Your Writes=True "
                + "or an explicit SqliteTransaction on a direct remote connection.");
        }

        return statements;
    }

    private bool CanExecuteSavepointStatement(ManagedSqliteStatement statement)
    {
        if (Transaction is null)
            return false;

        if (statement.FirstKeyword is "SAVEPOINT" or "RELEASE")
            return true;

        if (statement.FirstKeyword != "ROLLBACK")
            return false;

        var sql = statement.Sql.TrimStart();
        if (!sql.StartsWith("ROLLBACK", StringComparison.OrdinalIgnoreCase))
            return false;
        sql = sql["ROLLBACK".Length..].TrimStart();
        if (sql.StartsWith("TRANSACTION", StringComparison.OrdinalIgnoreCase))
            sql = sql["TRANSACTION".Length..].TrimStart();
        return sql.StartsWith("TO", StringComparison.OrdinalIgnoreCase);
    }

    internal void ValidateManagedParameterValues()
    {
        for (var index = 0; index < Parameters.Count; index++)
        {
            var parameter = Parameters[index];
            if (string.IsNullOrEmpty(parameter.ParameterName))
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.ParameterName)));
            if (!parameter.HasValue)
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.Value)));
        }
    }

    internal void AddManagedParameters(
        global::Turso.TursoCommand command,
        ManagedSqliteStatement statement)
    {
        var mapped = new Dictionary<string, SqliteParameter>(StringComparer.Ordinal);
        for (var index = 0; index < Parameters.Count; index++)
        {
            var parameter = Parameters[index];
            var parameterName = parameter.ParameterName;
            string? matchedName = null;
            if (statement.ParameterNames.Contains(parameterName, StringComparer.Ordinal))
            {
                matchedName = parameterName;
            }
            else if (!IsPrefixed(parameterName))
            {
                foreach (var candidate in statement.ParameterNames)
                {
                    if (!IsPrefixed(candidate)
                        || !candidate.AsSpan(1).Equals(parameterName.AsSpan(), StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (matchedName is not null)
                        throw new InvalidOperationException(Properties.Resources.AmbiguousParameterName(parameterName));
                    matchedName = candidate;
                }
            }

            if (matchedName is not null)
                mapped[matchedName] = parameter;
        }

        foreach (var parameterName in statement.ParameterNames)
        {
            if (!mapped.TryGetValue(parameterName, out var parameter))
                throw new InvalidOperationException(Properties.Resources.MissingParameters(parameterName));

            command.Parameters.Add(parameter.ToManagedParameter(parameterName));
        }
    }

    private void ThrowIfReaderOpen(string property)
    {
        if (_hasOpenReader)
            throw new InvalidOperationException(Properties.Resources.SetRequiresNoOpenReader(property));
    }

    private void EnsureExecutable(string method)
    {
        if (_hasOpenReader)
            throw new InvalidOperationException(Properties.Resources.DataReaderOpen);
        if (Connection is null || Connection.State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.CallRequiresOpenConnection(method));
        if (Transaction is { IsCompleted: true } or { WasRolledBackExternally: true })
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (Transaction is not null && !ReferenceEquals(Transaction.Connection, Connection))
            throw new InvalidOperationException(Properties.Resources.TransactionConnectionMismatch);

        var connectionTransaction = Connection.Transaction;
        if (connectionTransaction is null || ReferenceEquals(Transaction, connectionTransaction))
            return;
        if (connectionTransaction.IsCompleted)
            throw new InvalidOperationException(Properties.Resources.TransactionCompleted);
        if (!IsTransactionControlCommand(CommandText))
            throw new InvalidOperationException(Properties.Resources.TransactionRequired);
    }

    private void CloseReader()
    {
        _hasOpenReader = false;
        Connection?.ReaderClosed();
    }

    internal Action OwnBufferedReader()
    {
        _hasOpenReader = true;
        Connection?.ReaderOpened();
        return () =>
        {
            CloseReader();
            Dispose();
        };
    }

    internal TursoStatementHandle PrepareSingleStatement(string sql)
    {
        var connection = Connection!;
        sql = RewriteFacadeStatement(sql, connection);
        TursoStatementHandle statement;
        try
        {
            statement = TursoBindings.PrepareStatement(connection.DatabaseHandle, sql);
        }
        catch (TursoException ex)
        {
            throw ToSqliteException(ex, sql);
        }

        try
        {
            BindParameters(statement);
            return statement;
        }
        catch
        {
            statement.Dispose();
            throw;
        }
    }

    private void BindParameters(TursoStatementHandle statement)
    {
        var parameterCount = TursoBindings.GetParameterCount(statement);
        var boundParameters = new bool[parameterCount + 1];

        for (var i = 0; i < Parameters.Count; i++)
        {
            var parameter = Parameters[i];
            if (string.IsNullOrEmpty(parameter.ParameterName))
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.ParameterName)));
            if (!parameter.HasValue)
                throw new InvalidOperationException(Properties.Resources.RequiresSet(nameof(parameter.Value)));

            var parameterIndex = FindParameterIndex(statement, parameter.ParameterName, parameterCount);
            if (parameterIndex == 0)
                continue;

            TursoBindings.BindParameter(statement, parameterIndex, parameter.ToTursoValue());
            boundParameters[parameterIndex] = true;
        }

        for (var i = 1; i <= parameterCount; i++)
        {
            if (!boundParameters[i])
            {
                var parameterName = TursoBindings.GetParameterName(statement, i);
                throw new InvalidOperationException(
                    parameterName is null
                        ? Properties.Resources.MissingParameters(i)
                        : Properties.Resources.MissingParameters(parameterName));
            }
        }
    }

    private static bool IsEmptyCommand(string commandText)
    {
        foreach (var line in commandText.Split('\n'))
        {
            var trimmedLine = line.Trim();
            if (trimmedLine.Length != 0 && !trimmedLine.StartsWith("--", StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static bool IsTransactionControlCommand(string commandText)
    {
        var trimmed = commandText.TrimStart();
        return IsRollbackCommand(trimmed) || IsCommitCommand(trimmed);
    }

    private static bool IsRollbackCommand(string commandText)
    {
        var tail = GetCommandTail(commandText, "ROLLBACK");
        return tail is not null
               && !tail.StartsWith("TO", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsCommitCommand(string commandText)
        => GetCommandTail(commandText, "COMMIT") is not null;

    private static string? GetCommandTail(string commandText, string command)
    {
        var trimmed = commandText.TrimStart();
        if (!trimmed.StartsWith(command, StringComparison.OrdinalIgnoreCase))
            return null;
        if (trimmed.Length > command.Length && char.IsLetterOrDigit(trimmed[command.Length]))
            return null;

        return trimmed[command.Length..].TrimStart();
    }

    private static bool IsWriteCommand(string commandText)
        => ManagedSqliteStatementParser.Parse(commandText).Any(IsWriteStatement);

    internal static bool IsWriteStatement(ManagedSqliteStatement statement)
        => statement.FirstKeyword is "CREATE" or "DROP" or "ALTER"
            or "INSERT" or "UPDATE" or "DELETE" or "REPLACE"
            or "VACUUM" or "ATTACH" or "DETACH" or "REINDEX" or "ANALYZE"
           || statement.FirstKeyword == "WITH" && IsWithDmlStatement(statement.Sql)
           || statement.FirstKeyword == "PRAGMA" && IsWritablePragma(statement.Sql);

    private static bool IsWritablePragma(string sql)
    {
        if (Regex.IsMatch(sql, @"\bPRAGMA\b[\s\S]*=", RegexOptions.IgnoreCase))
            return true;

        var match = Regex.Match(
            sql,
            @"\bPRAGMA\s+(?:(?:[A-Za-z_][A-Za-z0-9_]*)\.)?(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(",
            RegexOptions.IgnoreCase);
        return match.Success && !ReadOnlyPragmaFunctions.Contains(match.Groups["name"].Value);
    }

    private static readonly HashSet<string> ReadOnlyPragmaFunctions =
        new(StringComparer.OrdinalIgnoreCase)
        {
            "foreign_key_check",
            "foreign_key_list",
            "index_info",
            "index_list",
            "index_xinfo",
            "integrity_check",
            "quick_check",
            "table_info",
            "table_list",
            "table_xinfo",
        };

    internal bool TryHandleFacadeStatement(string sql, out string rewrittenSql)
    {
        var connection = Connection!;
        var normalized = NormalizeSql(sql);
        if (TryParseReadUncommittedSetter(normalized, out var enabled))
        {
            connection.ReadUncommitted = enabled;
            rewrittenSql = EmptyResultSql;
            return true;
        }

        rewrittenSql = RewriteUnsupportedPragmas(normalized, sql, connection);
        return false;
    }

    private const string EmptyResultSql = "SELECT 1 WHERE 0";

    internal static string RewriteFacadeStatement(string sql, SqliteConnection connection)
        => RewriteUnsupportedPragmas(NormalizeSql(sql), sql, connection);

    private static string RewriteUnsupportedPragmas(string normalized, string sql, SqliteConnection connection)
    {
        if (normalized.Equals("PRAGMA recursive_triggers", StringComparison.OrdinalIgnoreCase))
            return "SELECT " + (connection.RecursiveTriggers ? "1" : "0");
        if (TryParseReadUncommittedSetter(normalized, out _))
            return EmptyResultSql;
        if (normalized.Equals("PRAGMA read_uncommitted", StringComparison.OrdinalIgnoreCase))
            return "SELECT " + (connection.ReadUncommitted ? "1" : "0");
        if (normalized.Equals("PRAGMA compile_options", StringComparison.OrdinalIgnoreCase))
            return "SELECT CAST(NULL AS TEXT) AS compile_options WHERE 0";
        if (normalized.IndexOf("pragma_compile_options", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return normalized.IndexOf("count", StringComparison.OrdinalIgnoreCase) >= 0
                ? "SELECT 0"
                : "SELECT CAST(NULL AS TEXT) AS compile_options WHERE 0";
        }

        return sql;
    }

    private static string NormalizeSql(string sql)
        => sql.Trim().TrimEnd(';').Trim();

    private static bool TryParseReadUncommittedSetter(string normalized, out bool enabled)
    {
        enabled = false;
        const string prefix = "PRAGMA read_uncommitted";
        if (!normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;
        if (normalized.Length == prefix.Length)
            return false;

        var value = normalized[prefix.Length..].TrimStart();
        if (value.StartsWith("=", StringComparison.Ordinal))
            value = value[1..].Trim();
        else if (value.StartsWith("(", StringComparison.Ordinal) && value.EndsWith(")", StringComparison.Ordinal))
            value = value[1..^1].Trim();
        else
            return false;

        enabled = ParsePragmaEnabled(value);
        return true;
    }

    private static bool ParsePragmaEnabled(string value)
    {
        value = value.Trim('\'', '"');
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number)
            ? number != 0
            : value.Equals("ON", StringComparison.OrdinalIgnoreCase)
              || value.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
              || value.Equals("YES", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool CountsRowsAffected(string commandText)
    {
        var firstStatement = SplitStatements(commandText).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(firstStatement))
            return false;

        var trimmed = firstStatement.TrimStart();
        return trimmed.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)
               || trimmed.StartsWith("REPLACE", StringComparison.OrdinalIgnoreCase)
               || IsWithDmlStatement(trimmed);
    }

    private static bool CountsRowsAffected(ManagedSqliteStatement statement)
        => statement.FirstKeyword is "INSERT" or "UPDATE" or "DELETE" or "REPLACE"
           || statement.FirstKeyword == "WITH"
           && Regex.IsMatch(
               statement.Sql,
               @"\)\s*(INSERT|UPDATE|DELETE|REPLACE)\b",
               RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static bool IsWithDmlStatement(string trimmedStatement)
        => Regex.IsMatch(
            trimmedStatement,
            @"\)(?:\s|--[^\r\n]*(?:\r?\n|$)|/\*[\s\S]*?\*/)*(INSERT|UPDATE|DELETE|REPLACE)\b",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static List<string> SplitStatements(string commandText)
    {
        var statements = new List<string>();
        var current = new StringBuilder();
        var inSingleQuote = false;
        var inDoubleQuote = false;
        var inLineComment = false;

        for (var i = 0; i < commandText.Length; i++)
        {
            var c = commandText[i];
            var next = i + 1 < commandText.Length ? commandText[i + 1] : '\0';

            if (inLineComment)
            {
                current.Append(c);
                if (c == '\n')
                    inLineComment = false;
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && c == '-' && next == '-')
            {
                inLineComment = true;
                current.Append(c);
                continue;
            }

            if (c == '\'' && !inDoubleQuote)
            {
                current.Append(c);
                if (inSingleQuote && next == '\'')
                {
                    current.Append(next);
                    i++;
                    continue;
                }

                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (c == '"' && !inSingleQuote)
                inDoubleQuote = !inDoubleQuote;

            if (c == ';' && !inSingleQuote && !inDoubleQuote)
            {
                AddStatement(statements, current);
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        AddStatement(statements, current);
        return statements;
    }

    private static void AddStatement(List<string> statements, StringBuilder current)
    {
        var statement = current.ToString().Trim();
        if (statement.Length != 0 && !IsEmptyCommand(statement))
            statements.Add(statement);
    }

    internal static SqliteException ToSqliteException(TursoException ex, string? sql = null)
    {
        if (ex is global::Turso.TursoRemoteSqlException remoteException)
        {
            var (errorCode, extendedErrorCode) = GetRemoteSqliteErrorCodes(
                remoteException.RemoteErrorCode);
            var remoteMessage = string.IsNullOrWhiteSpace(remoteException.RemoteErrorMessage)
                ? remoteException.Message
                : remoteException.RemoteErrorMessage;
            if (sql is not null)
                remoteMessage = PreserveNoSuchTableCase(remoteMessage, sql);

            return new SqliteException(
                Properties.Resources.SqliteNativeError(errorCode, remoteMessage),
                errorCode,
                extendedErrorCode);
        }

        var message = ex.Message;
        foreach (var prefix in new[] { "Unable to prepare statement: Parse error: ", "Parse error: " })
        {
            if (message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                message = message[prefix.Length..];
                break;
            }
        }
        if (message.StartsWith("Extension error: ", StringComparison.OrdinalIgnoreCase))
            message = message["Extension error: ".Length..];
        if (message.StartsWith("Error: cannot use aggregate, window functions or reference other tables in WHERE clause of CREATE INDEX", StringComparison.Ordinal))
            message = "non-deterministic functions prohibited in partial index WHERE clauses";
        const string sqliteErrorPrefix = "__turso_sqlite_error__:";
        if (message.StartsWith(sqliteErrorPrefix, StringComparison.Ordinal))
        {
            var codeEnd = message.IndexOf(':', sqliteErrorPrefix.Length);
            if (codeEnd > sqliteErrorPrefix.Length
                && int.TryParse(message[sqliteErrorPrefix.Length..codeEnd], NumberStyles.Integer, CultureInfo.InvariantCulture, out var errorCode))
            {
                var sqliteMessage = message[(codeEnd + 1)..];
                return new SqliteException(Properties.Resources.SqliteNativeError(errorCode, sqliteMessage), errorCode);
            }
        }

        if (sql is not null)
            message = PreserveNoSuchTableCase(message, sql);

        return new SqliteException(Properties.Resources.SqliteNativeError(1, message), 1);
    }

    private static (int ErrorCode, int ExtendedErrorCode) GetRemoteSqliteErrorCodes(
        string? remoteErrorCode)
    {
        if (string.IsNullOrWhiteSpace(remoteErrorCode))
            return (1, 1);

        var code = remoteErrorCode.ToUpperInvariant();
        var extended = code switch
        {
            "SQLITE_BUSY_RECOVERY" => 261,
            "SQLITE_BUSY_SNAPSHOT" => 517,
            "SQLITE_BUSY_TIMEOUT" => 773,
            "SQLITE_LOCKED_SHAREDCACHE" => 262,
            "SQLITE_LOCKED_VTAB" => 518,
            "SQLITE_READONLY_RECOVERY" => 264,
            "SQLITE_READONLY_CANTLOCK" => 520,
            "SQLITE_READONLY_ROLLBACK" => 776,
            "SQLITE_READONLY_DBMOVED" => 1032,
            "SQLITE_READONLY_CANTINIT" => 1288,
            "SQLITE_READONLY_DIRECTORY" => 1544,
            "SQLITE_CONSTRAINT_CHECK" => 275,
            "SQLITE_CONSTRAINT_COMMITHOOK" => 531,
            "SQLITE_CONSTRAINT_FOREIGNKEY" => 787,
            "SQLITE_CONSTRAINT_FUNCTION" => 1043,
            "SQLITE_CONSTRAINT_NOTNULL" => 1299,
            "SQLITE_CONSTRAINT_PRIMARYKEY" => 1555,
            "SQLITE_CONSTRAINT_TRIGGER" => 1811,
            "SQLITE_CONSTRAINT_UNIQUE" => 2067,
            "SQLITE_CONSTRAINT_VTAB" => 2323,
            "SQLITE_CONSTRAINT_ROWID" => 2579,
            "SQLITE_CONSTRAINT_PINNED" => 2835,
            "SQLITE_CONSTRAINT_DATATYPE" => 3091,
            "SQLITE_ABORT_ROLLBACK" => 516,
            _ => 0,
        };
        if (extended != 0)
            return (extended & 0xff, extended);

        var primary = code switch
        {
            "SQLITE_OK" => 0,
            "SQLITE_ERROR" => 1,
            "SQLITE_INTERNAL" => 2,
            "SQLITE_PERM" => 3,
            "SQLITE_ABORT" => 4,
            "SQLITE_BUSY" => 5,
            "SQLITE_LOCKED" => 6,
            "SQLITE_NOMEM" => 7,
            "SQLITE_READONLY" => 8,
            "SQLITE_INTERRUPT" => 9,
            "SQLITE_IOERR" => 10,
            "SQLITE_CORRUPT" => 11,
            "SQLITE_NOTFOUND" => 12,
            "SQLITE_FULL" => 13,
            "SQLITE_CANTOPEN" => 14,
            "SQLITE_PROTOCOL" => 15,
            "SQLITE_EMPTY" => 16,
            "SQLITE_SCHEMA" => 17,
            "SQLITE_TOOBIG" => 18,
            "SQLITE_CONSTRAINT" => 19,
            "SQLITE_MISMATCH" => 20,
            "SQLITE_MISUSE" => 21,
            "SQLITE_NOLFS" => 22,
            "SQLITE_AUTH" => 23,
            "SQLITE_FORMAT" => 24,
            "SQLITE_RANGE" => 25,
            "SQLITE_NOTADB" => 26,
            "SQLITE_NOTICE" => 27,
            "SQLITE_WARNING" => 28,
            _ when code.StartsWith("SQLITE_CONSTRAINT_", StringComparison.Ordinal) => 19,
            _ when code.StartsWith("SQLITE_BUSY_", StringComparison.Ordinal) => 5,
            _ when code.StartsWith("SQLITE_LOCKED_", StringComparison.Ordinal) => 6,
            _ when code.StartsWith("SQLITE_READONLY_", StringComparison.Ordinal) => 8,
            _ when code.StartsWith("SQLITE_IOERR_", StringComparison.Ordinal) => 10,
            _ when code.StartsWith("SQLITE_CORRUPT_", StringComparison.Ordinal) => 11,
            _ when code.StartsWith("SQLITE_CANTOPEN_", StringComparison.Ordinal) => 14,
            _ when code.StartsWith("SQLITE_ABORT_", StringComparison.Ordinal) => 4,
            _ => 1,
        };
        return (primary, primary);
    }

    private static string PreserveNoSuchTableCase(string message, string sql)
    {
        const string noSuchTable = "no such table: ";
        if (!message.StartsWith(noSuchTable, StringComparison.OrdinalIgnoreCase))
            return message;

        var tableName = message[noSuchTable.Length..];
        var sqlSpan = sql.AsSpan();
        for (var i = 0; i <= sqlSpan.Length - tableName.Length; i++)
        {
            if (MemoryExtensions.Equals(sqlSpan.Slice(i, tableName.Length), tableName, StringComparison.OrdinalIgnoreCase))
                return noSuchTable + sql.Substring(i, tableName.Length);
        }

        return message;
    }

    private static int FindParameterIndex(TursoStatementHandle statement, string parameterName, int parameterCount)
    {
        var index = FindExactParameterIndex(statement, parameterName, parameterCount);
        if (index != 0 || IsPrefixed(parameterName))
            return index;

        foreach (var prefix in new[] { '@', '$', ':' })
        {
            var prefixedIndex = FindExactParameterIndex(statement, prefix + parameterName, parameterCount);
            if (prefixedIndex == 0)
                continue;

            if (index != 0)
                throw new InvalidOperationException(Properties.Resources.AmbiguousParameterName(parameterName));

            index = prefixedIndex;
        }

        return index;
    }

    private static int FindExactParameterIndex(TursoStatementHandle statement, string parameterName, int parameterCount)
    {
        for (var i = 1; i <= parameterCount; i++)
        {
            if (string.Equals(TursoBindings.GetParameterName(statement, i), parameterName, StringComparison.Ordinal))
                return i;
        }

        return 0;
    }

    private static bool IsPrefixed(string parameterName)
        => parameterName.Length > 0 && parameterName[0] is '@' or '$' or ':';
}
