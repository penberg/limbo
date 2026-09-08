using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Turso;
using Turso.Raw.Public;
using Turso.Raw.Public.Handles;

namespace Turso.Data.Sqlite;

public partial class SqliteConnection : DbConnection
{
    private const int SQLITE_ERROR = 1;
    private const int SQLITE_CANTOPEN = 14;
    private static readonly object SharedMemoryLock = new();
    private static readonly Dictionary<string, int> SharedMemoryReferences = new(StringComparer.OrdinalIgnoreCase);

    private TursoDatabaseHandle? _database;
    private TursoConnection? _managedConnection;
    private bool _ownsManagedConnection;
    private bool _wrapsManagedConnection;
    private SqliteConnectionStringBuilder _connectionOptions = new();
    private bool _disposed;
    private int? _defaultTimeout;
    private int _openReaderCount;
    private string? _dataSource;
    private bool _readOnly;
    private bool _recursiveTriggers;
    private bool _readUncommitted;
    private string? _sharedMemoryPath;
    private bool _extensionsEnabled;
    private readonly List<(string File, string? Proc)> _pendingExtensions = [];

    public SqliteConnection()
    {
    }

    public SqliteConnection(string? connectionString)
    {
        ConnectionString = connectionString;
    }

    public SqliteConnection(TursoConnection connection, bool ownsConnection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var options = new SqliteConnectionStringBuilder(connection.ConnectionString);
        if (options.IsLocal)
        {
            throw new ArgumentException(
                "Only direct remote and embedded replica Turso connections can be wrapped.",
                nameof(connection));
        }

        _connectionOptions = options;
        _managedConnection = connection;
        _ownsManagedConnection = ownsConnection;
        _wrapsManagedConnection = true;
        _readOnly = options.Mode == SqliteOpenMode.ReadOnly;
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionOptions.ConnectionString;
        set
        {
            if (State == ConnectionState.Open)
                throw new InvalidOperationException(Properties.Resources.ConnectionStringRequiresClosedConnection);

            var options = new SqliteConnectionStringBuilder(value);
            if (_wrapsManagedConnection && options.IsLocal)
            {
                throw new InvalidOperationException(
                    "A SqliteConnection wrapping a TursoConnection must remain a direct remote or embedded replica connection.");
            }

            ConfigureManagedConnection(options);
            _connectionOptions = options;
            _defaultTimeout = null;
        }
    }

    public override string Database => _managedConnection?.Database ?? "main";

    public override string DataSource => _managedConnection?.DataSource ?? _dataSource ?? _connectionOptions.DataSource;

    public int DefaultTimeout
    {
        get => _defaultTimeout ?? _connectionOptions.DefaultTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _defaultTimeout = value;
        }
    }

    /// <summary>
    ///     Raw SQLitePCL <c>sqlite3*</c> interop is not supported by the Turso-backed provider.
    /// </summary>
    public virtual dynamic? Handle => null;

    public SqliteTransaction? Transaction { get; internal set; }

    public bool IsLocal => _connectionOptions.IsLocal;

    public bool IsDirectRemote => _connectionOptions.IsDirectRemote;

    public bool IsRemote => _connectionOptions.IsRemote;

    public bool IsReplica => _connectionOptions.IsReplica;

    public bool IsManaged => IsRemote;

    public TursoSyncDatabase? SyncDatabase => _managedConnection?.SyncDatabase;

    internal bool IsSharedCache => _connectionOptions.Cache == SqliteCacheMode.Shared;

    internal bool IsManagedConnection => _managedConnection is not null;

    private bool HasNativeCallbackHandle =>
        _database is not null || IsReplica && State == ConnectionState.Open;

    internal TursoConnection ManagedConnection => _managedConnection
        ?? throw new InvalidOperationException("The connection does not use the managed Turso backend.");

    internal bool ReadUncommitted
    {
        get => _readUncommitted;
        set => _readUncommitted = value;
    }

    // The SQLite version Turso tracks (SQLITE_VERSION in core/dialect/sqlite.rs).
    public override string ServerVersion => "3.50.4";

    public override ConnectionState State => _managedConnection?.State
        ?? (_database is null ? ConnectionState.Closed : ConnectionState.Open);

    public override bool CanCreateBatch => _managedConnection?.CanCreateBatch == true;

    protected override DbProviderFactory DbProviderFactory => SqliteFactory.Instance;

    public override void Open()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_managedConnection is not null)
        {
            var managedOriginalState = State;
            try
            {
                _managedConnection.Open();
                RegisterManagedReplicaCallbacks();
            }
            finally
            {
                NotifyManagedStateChange(managedOriginalState);
            }

            return;
        }

        if (_database is not null)
            throw new InvalidOperationException("The connection is already open.");
        if (!string.IsNullOrEmpty(_connectionOptions.Password))
            throw new InvalidOperationException(Properties.Resources.EncryptionNotSupported("e_sqlite3"));

        var originalState = State;
        var filename = NormalizeDataSource(_connectionOptions);
        var readOnly = _connectionOptions.Mode == SqliteOpenMode.ReadOnly;
        var sharedMemoryPath = IsSharedMemory(_connectionOptions) ? RegisterSharedMemoryFile(filename) : null;
        try
        {
            _database = TursoBindings.OpenDatabase(filename);
            _dataSource = filename;
            _readOnly = readOnly;
            _sharedMemoryPath = sharedMemoryPath;
            ApplyExtensionSettings();
            ApplyConnectionOptions();
            RegisterScalarFunctions();
            RegisterAggregateFunctions();
            RegisterCollations();
            LoadPendingExtensions();
            OnStateChange(new StateChangeEventArgs(originalState, State));
        }
        catch (TursoException ex)
        {
            CleanupFailedOpen(sharedMemoryPath);
            throw ToSqliteException(ex);
        }
        catch (SqliteException)
        {
            CleanupFailedOpen(sharedMemoryPath);
            throw;
        }
    }

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_managedConnection is not null)
            return OpenManagedAsync(cancellationToken);

        cancellationToken.ThrowIfCancellationRequested();
        Open();
        return Task.CompletedTask;
    }

    public override void Close()
    {
        if (_managedConnection is not null)
        {
            var managedOriginalState = State;
            System.Runtime.ExceptionServices.ExceptionDispatchInfo? transactionFailure = null;
            try
            {
                if (Transaction is { IsCompleted: false } transaction)
                {
                    try
                    {
                        transaction.Dispose();
                    }
                    catch (Exception ex)
                    {
                        transactionFailure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
                    }
                }

                FreeNativeFunctionContexts();
                _managedConnection.Close();
            }
            finally
            {
                NotifyManagedStateChange(managedOriginalState);
            }

            transactionFailure?.Throw();
            return;
        }

        if (_database is null)
            return;

        var originalState = State;
        _database.Dispose();
        _database = null;
        FreeNativeFunctionContexts();
        _dataSource = null;
        _readOnly = false;
        _recursiveTriggers = false;
        _readUncommitted = false;
        if (_sharedMemoryPath is not null)
        {
            ReleaseSharedMemoryFile(_sharedMemoryPath);
            _sharedMemoryPath = null;
        }
        OnStateChange(new StateChangeEventArgs(originalState, State));
    }

    public override void ChangeDatabase(string databaseName)
    {
        if (_managedConnection is not null)
        {
            _managedConnection.ChangeDatabase(databaseName);
            return;
        }

        throw new NotSupportedException("Changing databases is not supported.");
    }

    public void Sync()
    {
        GetManagedConnectionForSync().Sync();
    }

    public Task SyncAsync(CancellationToken cancellationToken = default)
    {
        return GetManagedConnectionForSync().SyncAsync(cancellationToken);
    }

    public override DataTable GetSchema()
        => GetSchema(DbMetaDataCollectionNames.MetaDataCollections, null);

    public override DataTable GetSchema(string collectionName)
        => GetSchema(collectionName, null);

    public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
    {
        ThrowIfManagedLocalOnly(nameof(GetSchema));

        if (string.Equals(collectionName, DbMetaDataCollectionNames.MetaDataCollections, StringComparison.OrdinalIgnoreCase))
        {
            ValidateRestrictions(collectionName, restrictionValues, 0);
            var table = new DataTable(DbMetaDataCollectionNames.MetaDataCollections);
            table.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
            table.Columns.Add(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int));
            table.Columns.Add(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int));
            table.Rows.Add(DbMetaDataCollectionNames.MetaDataCollections, 0, 0);
            table.Rows.Add(DbMetaDataCollectionNames.ReservedWords, 0, 0);
            table.Rows.Add("Tables", 4, 4);
            table.Rows.Add("Columns", 4, 4);
            return table;
        }

        if (string.Equals(collectionName, DbMetaDataCollectionNames.ReservedWords, StringComparison.OrdinalIgnoreCase))
        {
            ValidateRestrictions(collectionName, restrictionValues, 0);
            var table = new DataTable(DbMetaDataCollectionNames.ReservedWords);
            table.Columns.Add(DbMetaDataColumnNames.ReservedWord, typeof(string));
            foreach (var word in new[] { "ABORT", "ALTER", "CREATE", "DELETE", "DROP", "INSERT", "SELECT", "UPDATE" })
                table.Rows.Add(word);

            return table;
        }

        if (string.Equals(collectionName, "Tables", StringComparison.OrdinalIgnoreCase))
            return GetTablesSchema(collectionName, restrictionValues);

        if (string.Equals(collectionName, "Columns", StringComparison.OrdinalIgnoreCase))
            return GetColumnsSchema(collectionName, restrictionValues);

        throw new ArgumentException(Properties.Resources.UnknownCollection(collectionName));
    }

    public static void ClearAllPools()
    {
    }

    public static void ClearPool(SqliteConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
    }

    public new virtual SqliteTransaction BeginTransaction()
        => BeginTransaction(IsolationLevel.Unspecified);

    public virtual SqliteTransaction BeginTransaction(bool deferred)
        => BeginTransaction(IsolationLevel.Unspecified, deferred);

    public new virtual SqliteTransaction BeginTransaction(IsolationLevel isolationLevel)
        => BeginTransaction(isolationLevel, deferred: isolationLevel == IsolationLevel.ReadUncommitted);

    public virtual SqliteTransaction BeginTransaction(IsolationLevel isolationLevel, bool deferred)
    {
        if (State != ConnectionState.Open)
            throw new InvalidOperationException(Properties.Resources.CallRequiresOpenConnection(nameof(BeginTransaction)));
        if (Transaction is not null)
            throw new InvalidOperationException(Properties.Resources.ParallelTransactionsNotSupported);

        Transaction = new SqliteTransaction(this, isolationLevel, deferred);
        return Transaction;
    }

    public virtual void CreateCollation(string name, Comparison<string>? comparison)
    {
        RegisterCollation(name, comparison is null ? null : (left, right) => comparison(left, right));
    }

    public virtual void CreateCollation<T>(string name, T state, Func<T, string, string, int>? comparison)
    {
        RegisterCollation(name, comparison is null ? null : (left, right) => comparison(state, left, right));
    }

    public virtual void CreateFunction<TResult>(string name, Func<TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 0, isDeterministic, function is null ? null : _ => function());
    }

    public virtual void CreateFunction<T1, TResult>(string name, Func<T1, TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 1, isDeterministic, function is null ? null : args => InvokeTypedFunction(name, function, args));
    }

    public virtual void CreateFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 2, isDeterministic, function is null ? null : args => InvokeTypedFunction(name, function, args));
    }

    public virtual void CreateFunction<TState, TResult>(string name, TState state, Func<TState, TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 0, isDeterministic, function is null ? null : args => InvokeTypedFunction(state, function, args));
    }

    public virtual void CreateFunction<TState, T1, TResult>(string name, TState state, Func<TState, T1, TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 1, isDeterministic, function is null ? null : args => InvokeTypedFunction(name, state, function, args));
    }

    public virtual void CreateFunction<TState, T1, T2, TResult>(string name, TState state, Func<TState, T1, T2, TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, 2, isDeterministic, function is null ? null : args => InvokeTypedFunction(name, state, function, args));
    }

    public virtual void CreateFunction<TResult>(string name, Func<object?[], TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, -1, isDeterministic, function is null ? null : args => function(args));
    }

    public virtual void CreateFunction<TState, TResult>(string name, TState state, Func<TState, object?[], TResult>? function, bool isDeterministic = false)
    {
        RegisterScalarFunction(name, -1, isDeterministic, function is null ? null : args => function(state, args));
    }

    public virtual void CreateAggregate<TAccumulate>(string name, Func<TAccumulate?, TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 0, isDeterministic, default(TAccumulate), func is null ? null : (accumulator, args) => InvokeNullableAggregateStep(func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<T1, TAccumulate>(string name, Func<TAccumulate?, T1, TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 1, isDeterministic, default(TAccumulate), func is null ? null : (accumulator, args) => InvokeNullableAggregateStep(name, func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<TAccumulate>(string name, Func<TAccumulate?, object?[], TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, -1, isDeterministic, default(TAccumulate), func is null ? null : (accumulator, args) => InvokeNullableAggregateStep(func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<TAccumulate>(string name, TAccumulate seed, Func<TAccumulate, TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 0, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<T1, TAccumulate>(string name, TAccumulate seed, Func<TAccumulate, T1, TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 1, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(name, func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<TAccumulate>(string name, TAccumulate seed, Func<TAccumulate, object?[], TAccumulate>? func, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, -1, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(func, accumulator, args), accumulator => accumulator);
    }

    public virtual void CreateAggregate<TAccumulate, TResult>(string name, TAccumulate seed, Func<TAccumulate, TAccumulate>? func, Func<TAccumulate, TResult>? resultSelector, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 0, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(func, accumulator, args), accumulator => InvokeResultSelector(resultSelector!, accumulator));
    }

    public virtual void CreateAggregate<T1, TAccumulate, TResult>(string name, TAccumulate seed, Func<TAccumulate, T1, TAccumulate>? func, Func<TAccumulate, TResult>? resultSelector, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 1, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(name, func, accumulator, args), accumulator => InvokeResultSelector(resultSelector!, accumulator));
    }

    public virtual void CreateAggregate<T1, T2, TAccumulate, TResult>(string name, TAccumulate seed, Func<TAccumulate, T1, T2, TAccumulate>? func, Func<TAccumulate, TResult>? resultSelector, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, 2, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(name, func, accumulator, args), accumulator => InvokeResultSelector(resultSelector!, accumulator));
    }

    public virtual void CreateAggregate<TAccumulate, TResult>(string name, TAccumulate seed, Func<TAccumulate, object?[], TAccumulate>? func, Func<TAccumulate, TResult>? resultSelector, bool isDeterministic = false)
    {
        RegisterAggregateFunction(name, -1, isDeterministic, seed, func is null ? null : (accumulator, args) => InvokeSeededAggregateStep(func, accumulator, args), accumulator => InvokeResultSelector(resultSelector!, accumulator));
    }

    public virtual void EnableExtensions(bool enable = true)
    {
        ThrowIfManagedLocalOnly(nameof(EnableExtensions));
        _extensionsEnabled = enable;
        if (_database is not null)
            TursoBindings.EnableLoadExtension(DatabaseHandle, enable);
    }

    public virtual void LoadExtension(string file, string? proc = null)
    {
        ThrowIfManagedLocalOnly(nameof(LoadExtension));
        ArgumentNullException.ThrowIfNull(file);
        if (proc is not null)
            throw new NotSupportedException("Custom extension entry points are not yet supported by the Turso SQLite-compatible provider.");

        if (_database is null)
        {
            _pendingExtensions.Add((file, proc));
            return;
        }

        LoadExtensionCore(file, proc);
    }

    public virtual void BackupDatabase(SqliteConnection destination)
        => BackupDatabase(destination, "main", "main");

    public virtual void BackupDatabase(SqliteConnection destination, string destinationName, string sourceName)
    {
        ThrowIfManagedLocalOnly(nameof(BackupDatabase));
        if (_database is null)
            throw new InvalidOperationException(Properties.Resources.CallRequiresOpenConnection("BackupDatabase"));
        ArgumentNullException.ThrowIfNull(destination);
        if (Transaction is not null)
            throw new SqliteException(Properties.Resources.SqliteNativeError(5, "database is locked"), 5);
        if (destination.State != ConnectionState.Open)
            destination.Open();

        foreach (var createSql in GetSchemaSql())
            destination.ExecuteNonQuery(createSql);

        foreach (var tableName in GetUserTableNames())
            CopyTableRows(destination, tableName);
    }

    public new virtual SqliteCommand CreateCommand() => new(this) { Transaction = Transaction };

    protected override DbCommand CreateDbCommand() => CreateCommand();

    protected override DbBatch CreateDbBatch()
    {
        if (_managedConnection is null)
        {
            throw new NotSupportedException(
                "SQLite facade batches are available only for direct remote or embedded replica connections.");
        }

        return new SqliteBatch(this);
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => BeginTransaction(isolationLevel);

    protected override ValueTask<DbTransaction> BeginDbTransactionAsync(
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<DbTransaction>(BeginTransaction(isolationLevel));
    }

    protected override void Dispose(bool disposing)
    {
        if (_managedConnection is not null)
        {
            ExceptionDispatchInfo? failure = null;
            if (disposing
                && !_disposed
                && Transaction is { IsCompleted: false } transaction)
            {
                try
                {
                    transaction.Dispose();
                }
                catch (Exception exception)
                {
                    failure = ExceptionDispatchInfo.Capture(exception);
                }
            }

            if (disposing && !_disposed && _ownsManagedConnection)
            {
                var originalState = State;
                try
                {
                    FreeNativeFunctionContexts();
                    _managedConnection.Dispose();
                }
                catch (Exception exception) when (failure is not null)
                {
                    // Preserve the transaction cleanup failure.
                    _ = exception;
                }
                catch (Exception exception)
                {
                    failure = ExceptionDispatchInfo.Capture(exception);
                }
                finally
                {
                    NotifyManagedStateChange(originalState);
                }
            }

            _disposed = true;
            base.Dispose(disposing);
            failure?.Throw();
            return;
        }

        if (disposing)
            Close();

        _disposed = true;
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        if (_managedConnection is null)
            return base.DisposeAsync();

        return DisposeManagedAsync();
    }

    private async ValueTask DisposeManagedAsync()
    {
        ExceptionDispatchInfo? failure = null;
        if (!_disposed && Transaction is { IsCompleted: false } transaction)
        {
            try
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure = ExceptionDispatchInfo.Capture(exception);
            }
        }

        if (!_disposed && _ownsManagedConnection)
        {
            var originalState = State;
            try
            {
                FreeNativeFunctionContexts();
                await ManagedConnection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (failure is not null)
            {
                // Preserve the transaction cleanup failure.
                _ = exception;
            }
            catch (Exception exception)
            {
                failure = ExceptionDispatchInfo.Capture(exception);
            }
            finally
            {
                NotifyManagedStateChange(originalState);
            }
        }

        _disposed = true;
        await base.DisposeAsync().ConfigureAwait(false);
        failure?.Throw();
    }

    internal TursoDatabaseHandle DatabaseHandle
    {
        get
        {
            if (_managedConnection is not null)
            {
                if (IsReplica)
                    return ManagedConnection.Turso;
                throw new NotSupportedException(
                    "SQLite native handles are not available for direct remote connections.");
            }

            return _database ?? throw new InvalidOperationException("The connection is not open.");
        }
    }

    internal bool HasOpenReader => _openReaderCount > 0;

    internal bool IsReadOnly => _readOnly;

    internal bool RecursiveTriggers => _recursiveTriggers;

    internal bool ManagedReadYourWrites => _connectionOptions.ReadYourWrites;

    internal void ReaderOpened() => _openReaderCount++;

    internal void ReaderClosed()
    {
        if (_openReaderCount > 0)
            _openReaderCount--;
    }

    internal void ExecuteNonQuery(string sql)
    {
        using var command = new SqliteCommand(sql, this);
        command.ExecuteNonQuery();
    }

    private List<string> GetSchemaSql()
    {
        var schema = new List<string>();
        using var command = new SqliteCommand("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 ELSE 3 END;", this);
        using var reader = command.ExecuteReader();
        while (reader.Read())
            schema.Add(reader.GetString(0));

        return schema;
    }

    private List<string> GetUserTableNames()
    {
        var tables = new List<string>();
        using var command = new SqliteCommand("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name;", this);
        using var reader = command.ExecuteReader();
        while (reader.Read())
            tables.Add(reader.GetString(0));

        return tables;
    }

    private DataTable GetTablesSchema(string collectionName, string?[]? restrictionValues)
    {
        EnsureOpen();
        ValidateRestrictions(collectionName, restrictionValues, 4);
        var table = new DataTable("Tables");
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("TABLE_TYPE", typeof(string));

        var tableNameRestriction = GetRestriction(restrictionValues, 2);
        using var command = CreateCommand();
        command.CommandText = "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
                              + (tableNameRestriction is null ? "" : " AND name COLLATE NOCASE = $table")
                              + " ORDER BY name;";
        if (tableNameRestriction is not null)
            command.Parameters.AddWithValue("$table", tableNameRestriction);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var type = reader.GetString(1).Equals("view", StringComparison.OrdinalIgnoreCase) ? "VIEW" : "BASE TABLE";
            var tableName = GetDeclaredSchemaObjectName(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2));
            table.Rows.Add("main", DBNull.Value, tableName, type);
        }

        return table;
    }

    private DataTable GetColumnsSchema(string collectionName, string?[]? restrictionValues)
    {
        EnsureOpen();
        ValidateRestrictions(collectionName, restrictionValues, 4);
        var table = new DataTable("Columns");
        table.Columns.Add("TABLE_CATALOG", typeof(string));
        table.Columns.Add("TABLE_SCHEMA", typeof(string));
        table.Columns.Add("TABLE_NAME", typeof(string));
        table.Columns.Add("COLUMN_NAME", typeof(string));
        table.Columns.Add("ORDINAL_POSITION", typeof(int));
        table.Columns.Add("COLUMN_DEFAULT", typeof(string));
        table.Columns.Add("IS_NULLABLE", typeof(bool));
        table.Columns.Add("DATA_TYPE", typeof(string));

        var tableNameRestriction = GetRestriction(restrictionValues, 2);
        var columnNameRestriction = GetRestriction(restrictionValues, 3);
        var tableNames = tableNameRestriction is null ? GetUserTableNames() : [tableNameRestriction];
        foreach (var tableName in tableNames)
        {
            using var command = CreateCommand();
            command.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var columnName = reader.GetString(1);
                if (columnNameRestriction is not null
                    && !string.Equals(columnNameRestriction, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                table.Rows.Add(
                    "main",
                    DBNull.Value,
                    tableName,
                    columnName,
                    reader.GetInt32(0),
                    reader.IsDBNull(4) ? DBNull.Value : reader.GetValue(4),
                    reader.GetInt64(3) == 0,
                    reader.GetString(2));
            }
        }

        return table;
    }

    private void CopyTableRows(SqliteConnection destination, string tableName)
    {
        using var select = new SqliteCommand("SELECT * FROM " + QuoteIdentifier(tableName) + ";", this);
        using var reader = select.ExecuteReader();
        while (reader.Read())
        {
            using var insert = destination.CreateCommand();
            var parameterNames = Enumerable.Range(0, reader.FieldCount).Select(i => "$p" + i).ToArray();
            insert.CommandText = "INSERT INTO " + QuoteIdentifier(tableName) + " VALUES (" + string.Join(", ", parameterNames) + ");";
            for (var i = 0; i < reader.FieldCount; i++)
                insert.Parameters.AddWithValue(parameterNames[i], reader.GetValue(i));

            insert.ExecuteNonQuery();
        }
    }

    internal void EnsureOpen()
    {
        if (State != ConnectionState.Open)
            throw new InvalidOperationException("The connection is not open.");
    }

    private void ConfigureManagedConnection(SqliteConnectionStringBuilder options)
    {
        if (options.IsLocal)
        {
            if (_managedConnection is not null && _ownsManagedConnection)
                _managedConnection.Dispose();

            _managedConnection = null;
            _ownsManagedConnection = false;
            _readOnly = false;
            return;
        }

        var connectionString = options.GetTursoConnectionString();
        _readOnly = options.Mode == SqliteOpenMode.ReadOnly;
        if (_managedConnection is null)
        {
            _managedConnection = new TursoConnection(connectionString);
            _ownsManagedConnection = true;
            return;
        }

        _managedConnection.ConnectionString = connectionString;
    }

    private async Task OpenManagedAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var originalState = State;
        try
        {
            await ManagedConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            RegisterManagedReplicaCallbacks();
        }
        finally
        {
            NotifyManagedStateChange(originalState);
        }
    }

    private TursoConnection GetManagedConnectionForSync()
    {
        if (_managedConnection is null)
            throw new NotSupportedException("Sync requires an embedded replica connection.");

        return _managedConnection;
    }

    private void NotifyManagedStateChange(ConnectionState originalState)
    {
        var currentState = State;
        if (currentState != originalState)
            OnStateChange(new StateChangeEventArgs(originalState, currentState));
    }

    private void ThrowIfManagedLocalOnly(string operation)
    {
        if (_managedConnection is not null)
        {
            throw new NotSupportedException(
                $"{operation} is not available for direct remote or embedded replica connections.");
        }
    }

    private void ThrowIfDirectRemote(string operation)
    {
        if (IsDirectRemote)
        {
            throw new NotSupportedException(
                $"{operation} is not available for direct remote connections.");
        }
    }

    private void RegisterManagedReplicaCallbacks()
    {
        if (!IsReplica)
            return;

        using var syncOperation = ManagedConnection.EnterSyncOperation();
        RegisterScalarFunctions();
        RegisterAggregateFunctions();
        RegisterCollations();
    }

    private void ApplyConnectionOptions()
    {
        if (_connectionOptions.ForeignKeys.HasValue)
            ExecuteNonQuery("PRAGMA foreign_keys = " + (_connectionOptions.ForeignKeys.Value ? "1" : "0") + ";");
        if (_connectionOptions.RecursiveTriggers)
            _recursiveTriggers = true;
    }

    private void ApplyExtensionSettings()
    {
        if (_extensionsEnabled)
            TursoBindings.EnableLoadExtension(DatabaseHandle, true);
    }

    private void LoadPendingExtensions()
    {
        foreach (var (file, proc) in _pendingExtensions)
            LoadExtensionCore(file, proc);
        _pendingExtensions.Clear();
    }

    private void LoadExtensionCore(string file, string? proc)
    {
        try
        {
            TursoBindings.LoadExtension(DatabaseHandle, file);
        }
        catch (TursoException ex)
        {
            throw SqliteCommand.ToSqliteException(ex);
        }
    }

    private void CleanupFailedOpen(string? sharedMemoryPath)
    {
        _database?.Dispose();
        _database = null;
        FreeNativeFunctionContexts();
        _dataSource = null;
        _readOnly = false;
        _sharedMemoryPath = null;
        if (sharedMemoryPath is not null)
            ReleaseSharedMemoryFile(sharedMemoryPath);
    }

    private static void ValidateRestrictions(string collectionName, string?[]? restrictionValues, int maxRestrictions)
    {
        if (restrictionValues is not null && restrictionValues.Length > maxRestrictions)
            throw new ArgumentException(Properties.Resources.TooManyRestrictions(collectionName));
    }

    private static string? GetRestriction(string?[]? restrictionValues, int index)
        => restrictionValues is not null && restrictionValues.Length > index && !string.IsNullOrEmpty(restrictionValues[index])
            ? restrictionValues[index]
            : null;

    private static string GetDeclaredSchemaObjectName(string storedName, string type, string? createSql)
    {
        if (string.IsNullOrWhiteSpace(createSql))
            return storedName;

        var index = 0;
        if (!TryReadKeyword(createSql, ref index, "CREATE"))
            return storedName;

        _ = TryReadKeyword(createSql, ref index, "TEMP")
            || TryReadKeyword(createSql, ref index, "TEMPORARY");

        var expectedType = type.Equals("view", StringComparison.OrdinalIgnoreCase) ? "VIEW" : "TABLE";
        if (!TryReadKeyword(createSql, ref index, expectedType))
            return storedName;

        var beforeIf = index;
        if (TryReadKeyword(createSql, ref index, "IF"))
        {
            if (!TryReadKeyword(createSql, ref index, "NOT")
                || !TryReadKeyword(createSql, ref index, "EXISTS"))
            {
                index = beforeIf;
            }
        }

        return TryReadSchemaObjectName(createSql, ref index, out var objectName)
            ? objectName
            : storedName;
    }

    private static bool TryReadKeyword(string sql, ref int index, string keyword)
    {
        SkipSqlWhitespace(sql, ref index);
        if (sql.Length - index < keyword.Length
            || !sql.AsSpan(index, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var end = index + keyword.Length;
        if (end < sql.Length && IsSqlIdentifierPart(sql[end]))
            return false;

        index = end;
        return true;
    }

    private static bool TryReadSchemaObjectName(string sql, ref int index, [NotNullWhen(true)] out string? objectName)
    {
        objectName = null;
        do
        {
            if (!TryReadIdentifier(sql, ref index, out var part))
                return objectName is not null;

            objectName = part;
            SkipSqlWhitespace(sql, ref index);
            if (index >= sql.Length || sql[index] != '.')
                return true;

            index++;
        }
        while (true);
    }

    private static bool TryReadIdentifier(string sql, ref int index, [NotNullWhen(true)] out string? identifier)
    {
        identifier = null;
        SkipSqlWhitespace(sql, ref index);
        if (index >= sql.Length)
            return false;

        var quote = sql[index];
        if (quote is '"' or '\'' or '`')
        {
            index++;
            var start = index;
            var builder = new System.Text.StringBuilder();
            while (index < sql.Length)
            {
                if (sql[index] == quote)
                {
                    if (index + 1 < sql.Length && sql[index + 1] == quote)
                    {
                        builder.Append(sql.AsSpan(start, index - start));
                        builder.Append(quote);
                        index += 2;
                        start = index;
                        continue;
                    }

                    builder.Append(sql.AsSpan(start, index - start));
                    index++;
                    identifier = builder.ToString();
                    return true;
                }

                index++;
            }

            return false;
        }

        if (quote == '[')
        {
            index++;
            var start = index;
            while (index < sql.Length && sql[index] != ']')
                index++;
            if (index >= sql.Length)
                return false;

            identifier = sql[start..index];
            index++;
            return true;
        }

        var tokenStart = index;
        while (index < sql.Length && IsSqlIdentifierPart(sql[index]))
            index++;

        if (index == tokenStart)
            return false;

        identifier = sql[tokenStart..index];
        return true;
    }

    private static void SkipSqlWhitespace(string sql, ref int index)
    {
        while (index < sql.Length && char.IsWhiteSpace(sql[index]))
            index++;
    }

    private static bool IsSqlIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_' || value == '$';

    private static string NormalizeDataSource(SqliteConnectionStringBuilder options)
    {
        var dataSource = options.DataSource;
        if (string.IsNullOrEmpty(dataSource))
            return ":memory:";
        if (options.Vfs is { Length: > 0 } vfs && !IsSupportedVfs(vfs))
            throw new SqliteException(Properties.Resources.SqliteNativeError(SQLITE_ERROR, "no such vfs: " + vfs), SQLITE_ERROR);
        if (dataSource == ":memory:")
            return dataSource;
        if (options.Mode == SqliteOpenMode.Memory)
            return options.Cache == SqliteCacheMode.Shared && dataSource.Length > 0
                ? GetSharedMemoryFile(dataSource)
                : ":memory:";
        if (dataSource.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            return NormalizeUriDataSource(dataSource);

        const string dataDirectory = "|DataDirectory|";
        if (dataSource.StartsWith(dataDirectory, StringComparison.OrdinalIgnoreCase))
        {
            var baseDirectory = AppDomain.CurrentDomain.GetData("DataDirectory") as string
                                ?? AppContext.BaseDirectory;
            dataSource = Path.Combine(baseDirectory, dataSource[dataDirectory.Length..].TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }

        var filename = Path.IsPathRooted(dataSource)
            ? dataSource
            : Path.Combine(AppContext.BaseDirectory, dataSource);

        if ((options.Mode == SqliteOpenMode.ReadOnly || options.Mode == SqliteOpenMode.ReadWrite) && !File.Exists(filename))
            throw new SqliteException(Properties.Resources.SqliteNativeError(SQLITE_CANTOPEN, "unable to open database file"), SQLITE_CANTOPEN);

        return filename;
    }

    private static string NormalizeUriDataSource(string dataSource)
    {
        var queryStart = dataSource.IndexOf('?', StringComparison.Ordinal);
        var path = queryStart < 0 ? dataSource[5..] : dataSource[5..queryStart];
        var query = queryStart < 0 ? string.Empty : dataSource[(queryStart + 1)..];
        foreach (var part in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var pieces = part.Split('=', 2);
            if (!pieces[0].Equals("mode", StringComparison.OrdinalIgnoreCase))
                continue;

            var mode = pieces.Length == 2 ? pieces[1] : string.Empty;
            if (!mode.Equals("ro", StringComparison.OrdinalIgnoreCase)
                && !mode.Equals("rw", StringComparison.OrdinalIgnoreCase)
                && !mode.Equals("rwc", StringComparison.OrdinalIgnoreCase)
                && !mode.Equals("memory", StringComparison.OrdinalIgnoreCase))
                throw new SqliteException(Properties.Resources.SqliteNativeError(SQLITE_ERROR, "no such access mode: " + mode), SQLITE_ERROR);
            if (mode.Equals("memory", StringComparison.OrdinalIgnoreCase))
                return ":memory:";
            if ((mode.Equals("ro", StringComparison.OrdinalIgnoreCase) || mode.Equals("rw", StringComparison.OrdinalIgnoreCase)) && !File.Exists(path))
                throw new SqliteException(Properties.Resources.SqliteNativeError(SQLITE_CANTOPEN, "unable to open database file"), SQLITE_CANTOPEN);
        }

        return Path.IsPathRooted(path)
            ? path
            : Path.Combine(AppContext.BaseDirectory, path);
    }

    private static bool IsSupportedVfs(string vfs)
        => vfs.Equals("win32-longpath", StringComparison.OrdinalIgnoreCase)
           || vfs.Equals("unix-dotfile", StringComparison.OrdinalIgnoreCase);

    private static string GetSharedMemoryFile(string dataSource)
    {
        var sanitized = string.Join("_", dataSource.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));
        if (sanitized.Length == 0)
            sanitized = Math.Abs(dataSource.GetHashCode(StringComparison.Ordinal)).ToString(CultureInfo.InvariantCulture);

        return Path.Combine(Path.GetTempPath(), "turso-dotnet-shared-" + sanitized + ".db");
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static bool IsSharedMemory(SqliteConnectionStringBuilder options)
        => options.Mode == SqliteOpenMode.Memory && options.Cache == SqliteCacheMode.Shared && options.DataSource.Length > 0;

    private static string RegisterSharedMemoryFile(string path)
    {
        lock (SharedMemoryLock)
        {
            if (!SharedMemoryReferences.TryGetValue(path, out var references))
            {
                if (File.Exists(path))
                    File.Delete(path);
                SharedMemoryReferences[path] = 1;
            }
            else
            {
                SharedMemoryReferences[path] = references + 1;
            }

            return path;
        }
    }

    private static void ReleaseSharedMemoryFile(string path)
    {
        lock (SharedMemoryLock)
        {
            if (!SharedMemoryReferences.TryGetValue(path, out var references))
                return;

            if (references > 1)
            {
                SharedMemoryReferences[path] = references - 1;
                return;
            }

            SharedMemoryReferences.Remove(path);
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static SqliteException ToSqliteException(TursoException exception)
    {
        var message = exception.Message;
        return new SqliteException(Properties.Resources.SqliteNativeError(SQLITE_ERROR, message), SQLITE_ERROR);
    }
}
