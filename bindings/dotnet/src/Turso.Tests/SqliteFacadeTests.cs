using System.Data;
using System.Data.Common;
using AwesomeAssertions;
using Turso.Data.Sqlite;

namespace Turso.Tests;

public class SqliteFacadeTests
{
    [Test]
    public void FactoryCreatesSqliteFacadeObjects()
    {
        DbProviderFactory factory = SqliteFactory.Instance;

        factory.CreateConnection().Should().BeOfType<SqliteConnection>();
        factory.CreateCommand().Should().BeOfType<SqliteCommand>();
        factory.CreateParameter().Should().BeOfType<SqliteParameter>();
        factory.CreateConnectionStringBuilder().Should().BeOfType<SqliteConnectionStringBuilder>();
        factory.CanCreateBatch.Should().BeTrue();
        factory.CreateBatch().Should().BeOfType<SqliteBatch>();
        factory.CreateBatchCommand().Should().BeOfType<SqliteBatchCommand>();
    }

    [Test]
    public void ConnectionStringBuilderParsesSqliteAliasesAndEnums()
    {
        var builder = new SqliteConnectionStringBuilder("Filename=:memory:;Mode=Memory;Cache=Shared;Command Timeout=7;DateTimeKind=Utc;DateTimeFormat=Ticks;BinaryGUID=False;Version=3");

        builder.DataSource.Should().Be(":memory:");
        builder.Mode.Should().Be(SqliteOpenMode.Memory);
        builder.Cache.Should().Be(SqliteCacheMode.Shared);
        builder.DefaultTimeout.Should().Be(7);
        builder.DateTimeKind.Should().Be(DateTimeKind.Utc);
        builder.DateTimeFormat.Should().Be("Ticks");
        builder.BinaryGUID.Should().BeFalse();
        builder.Version.Should().Be(3);
        builder["DataSource"].Should().Be(":memory:");
    }

    [Test]
    public void ConnectionStringBuilderRoundTripsManagedConnectionKeywords()
    {
        var builder = new SqliteConnectionStringBuilder(
            "DataSource=libsql://example.turso.io;AuthToken=token;ReplicaPath=replica.db;"
            + "ReadYourWrites=False;SyncInterval=5;SyncClientName=client;SyncLongPollTimeout=6;"
            + "BootstrapIfEmpty=False;PartialBootstrapPrefix=7;PartialBootstrapQuery=SELECT 1;"
            + "PartialSyncSegmentSize=8;PartialSyncPrefetch=True;RemoteEncryptionCipher=aes256gcm;"
            + "RemoteEncryptionKey=key;PushOperationsThreshold=9;PullBytesThreshold=10;"
            + "ForceLogicalMvccPull=True;SyncExperimentalFeatures=feature;TLS=True");

        builder.AuthToken.Should().Be("token");
        builder.ReplicaPath.Should().Be("replica.db");
        builder.ReadYourWrites.Should().BeFalse();
        builder.SyncInterval.Should().Be(5);
        builder.SyncClientName.Should().Be("client");
        builder.SyncLongPollTimeout.Should().Be(6);
        builder.BootstrapIfEmpty.Should().BeFalse();
        builder.PartialBootstrapPrefix.Should().Be(7);
        builder.PartialBootstrapQuery.Should().Be("SELECT 1");
        builder.PartialSyncSegmentSize.Should().Be(8);
        builder.PartialSyncPrefetch.Should().BeTrue();
        builder.RemoteEncryptionCipher.Should().Be("aes256gcm");
        builder.RemoteEncryptionKey.Should().Be("key");
        builder.PushOperationsThreshold.Should().Be(9);
        builder.PullBytesThreshold.Should().Be(10);
        builder.ForceLogicalMvccPull.Should().BeTrue();
        builder.SyncExperimentalFeatures.Should().Be("feature");
        builder.Tls.Should().BeTrue();
        builder.IsReplica.Should().BeTrue();
        builder.IsDirectRemote.Should().BeFalse();
        builder.IsRemote.Should().BeTrue();
        builder.IsLocal.Should().BeFalse();

        var roundTripped = new SqliteConnectionStringBuilder(builder.ConnectionString);
        roundTripped.ConnectionString.Should().Be(builder.ConnectionString);
        roundTripped.ReplicaPath.Should().Be("replica.db");
        roundTripped.PartialSyncSegmentSize.Should().Be(8);
        roundTripped.Tls.Should().BeTrue();

        using var connection = new SqliteConnection(roundTripped.ConnectionString);
        connection.IsLocal.Should().BeFalse();
        connection.IsDirectRemote.Should().BeFalse();
        connection.IsRemote.Should().BeTrue();
        connection.IsReplica.Should().BeTrue();
        connection.IsManaged.Should().BeTrue();
    }

    [TestCase(":memory:", true, false, false)]
    [TestCase("local.db", true, false, false)]
    [TestCase("file:local.db", true, false, false)]
    [TestCase("https://example.turso.io", false, true, false)]
    [TestCase("libsql://example.turso.io", false, true, false)]
    public void ConnectionStringBuilderClassifiesDataSource(
        string dataSource,
        bool isLocal,
        bool isDirectRemote,
        bool isReplica)
    {
        var builder = new SqliteConnectionStringBuilder { DataSource = dataSource };

        builder.IsLocal.Should().Be(isLocal);
        builder.IsDirectRemote.Should().Be(isDirectRemote);
        builder.IsReplica.Should().Be(isReplica);

        builder.ReplicaPath = isLocal ? "ignored.db" : "replica.db";
        builder.IsReplica.Should().Be(!isLocal);
    }

    [Test]
    public async Task ManagedConnectionDelegatesLifecycleAndState()
    {
        await using var connection = new SqliteConnection(
            "Data Source=https://example.turso.io;Auth Token=token;Default Timeout=7");
        var transitions = new List<(ConnectionState Original, ConnectionState Current)>();
        connection.StateChange += (_, args) => transitions.Add((args.OriginalState, args.CurrentState));

        connection.IsLocal.Should().BeFalse();
        connection.IsDirectRemote.Should().BeTrue();
        connection.IsRemote.Should().BeTrue();
        connection.IsReplica.Should().BeFalse();
        connection.IsManaged.Should().BeTrue();
        connection.CanCreateBatch.Should().BeTrue();
        connection.DataSource.Should().Be("https://example.turso.io");
        connection.DefaultTimeout.Should().Be(7);
        connection.State.Should().Be(ConnectionState.Closed);

        await connection.OpenAsync();
        connection.State.Should().Be(ConnectionState.Open);
        connection.DataSource.Should().Be("https://example.turso.io");
        connection.SyncDatabase.Should().BeNull();
        Assert.Throws<NotSupportedException>(connection.Sync);
        Assert.ThrowsAsync<NotSupportedException>(() => connection.SyncAsync());

        connection.Close();
        connection.State.Should().Be(ConnectionState.Closed);
        connection.Open();
        connection.Close();
        transitions.Should().Equal(
            (ConnectionState.Closed, ConnectionState.Open),
            (ConnectionState.Open, ConnectionState.Closed),
            (ConnectionState.Closed, ConnectionState.Open),
            (ConnectionState.Open, ConnectionState.Closed));
    }

    [Test]
    public async Task WrappedTursoConnectionHonorsExplicitOwnership()
    {
        var borrowed = new TursoConnection("Data Source=https://example.turso.io");
        borrowed.Open();
        await using (var wrapper = new SqliteConnection(borrowed, ownsConnection: false))
        {
            wrapper.State.Should().Be(ConnectionState.Open);
            wrapper.IsDirectRemote.Should().BeTrue();
        }

        borrowed.State.Should().Be(ConnectionState.Open);
        borrowed.Close();

        var owned = new TursoConnection("Data Source=https://example.turso.io");
        owned.Open();
        var owningWrapper = new SqliteConnection(owned, ownsConnection: true);
        await owningWrapper.DisposeAsync();

        owned.State.Should().Be(ConnectionState.Closed);
        Assert.Throws<ObjectDisposedException>(owned.Open);
    }

    [Test]
    public void ManagedConnectionRejectsLocalOnlyFacadeApis()
    {
        using var connection = new SqliteConnection("Data Source=https://example.turso.io");

        Assert.Throws<NotSupportedException>(() => connection.GetSchema());
        Assert.Throws<NotSupportedException>(() => connection.EnableExtensions());
        Assert.Throws<NotSupportedException>(() => connection.CreateFunction("custom", () => 1));

        using var local = new SqliteConnection("Data Source=:memory:");
        local.CanCreateBatch.Should().BeFalse();
        Assert.Throws<NotSupportedException>(() => local.CreateBatch());
    }

    [Test]
    public void ExecutesCommonAdoNetQuery()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using (var create = new SqliteCommand("CREATE TABLE t(id INTEGER, name TEXT)", connection))
        {
            create.ExecuteNonQuery().Should().Be(0);
        }

        using (var insert = new SqliteCommand("INSERT INTO t VALUES ($id, $name)", connection))
        {
            insert.Parameters.Add("$id", SqliteType.Integer).Value = 1;
            insert.Parameters.Add("$name", SqliteType.Text).Value = "alice";
            insert.ExecuteNonQuery().Should().Be(1);
        }

        using var select = new SqliteCommand("SELECT id, name FROM t", connection);
        using var reader = select.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(1);
        reader.GetString(1).Should().Be("alice");
        reader.Read().Should().BeFalse();
        reader.Dispose();
        connection.State.Should().Be(ConnectionState.Closed);
    }

    [Test]
    public async Task AsyncMethodsHonorPreCanceledToken()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        using var connection = new SqliteConnection("Data Source=:memory:");

        Assert.ThrowsAsync<OperationCanceledException>(() => connection.OpenAsync(cancellation.Token))!
            .CancellationToken.Should().Be(cancellation.Token);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";

        Assert.ThrowsAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cancellation.Token))!
            .CancellationToken.Should().Be(cancellation.Token);
    }

    [Test]
    public void SchemaCollectionsReportTablesAndColumns()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("""
            CREATE TABLE Customers(
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL DEFAULT ''
            );
            CREATE VIEW CustomerNames AS SELECT Name FROM Customers;
            """);

        var collections = connection.GetSchema();
        collections.Rows.Cast<DataRow>().Select(row => row[DbMetaDataColumnNames.CollectionName])
            .Should().Contain(["Tables", "Columns"]);

        var tables = connection.GetSchema("Tables");
        tables.Rows.Cast<DataRow>().Select(row => row["TABLE_NAME"])
            .Should().Contain(["Customers", "CustomerNames"]);

        var columns = connection.GetSchema("Columns", [null, null, "Customers"]);
        columns.Rows.Cast<DataRow>().Select(row => row["COLUMN_NAME"])
            .Should().Equal("Id", "Name");
        columns.Rows.Cast<DataRow>().Single(row => (string)row["COLUMN_NAME"] == "Name")["IS_NULLABLE"]
            .Should().Be(false);
    }

    [Test]
    public void TypedGettersConvertTextBackedValues()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var reader = connection.ExecuteReader("SELECT '42', '1', 'true', 42, 3.5;");
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(42);
        reader.GetBoolean(1).Should().BeTrue();
        reader.GetBoolean(2).Should().BeTrue();
        reader.GetString(3).Should().Be("42");
        reader.GetString(4).Should().Be("3.5");
    }

    [Test]
    public void ExecuteNonQueryReportsRowsAffectedForCteDmlAndReturning()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Id INTEGER PRIMARY KEY, Value TEXT); INSERT INTO Data VALUES (1, 'a');");

        connection.ExecuteNonQuery("WITH Target AS (SELECT 1 AS Id) UPDATE Data SET Value = 'b' WHERE Id = (SELECT Id FROM Target);")
            .Should().Be(1);
        connection.ExecuteNonQuery("UPDATE Data SET Value = 'c' WHERE Id = 1 RETURNING Id;")
            .Should().Be(1);
    }

    [Test]
    public void ReadOnlyConnectionRejectsCteDml()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "readonly-cte.db");
        using (var connection = new SqliteConnection($"Data Source={path}"))
        {
            connection.Open();
            connection.ExecuteNonQuery("CREATE TABLE Data(Id INTEGER PRIMARY KEY, Value TEXT); INSERT INTO Data VALUES (1, 'a');");
        }

        using var readOnly = new SqliteConnection($"Data Source={path};Mode=ReadOnly");
        readOnly.Open();

        var exception = Assert.Throws<SqliteException>(() => readOnly.ExecuteNonQuery("""
            WITH Target AS (SELECT 1 AS Id)
            UPDATE Data SET Value = 'b' WHERE Id = (SELECT Id FROM Target);
            """));
        exception!.SqliteErrorCode.Should().Be(8);
    }

    [Test]
    public void ReadOnlyConnectionRejectsWriteAfterReadInBatch()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "readonly-batch.db");
        using (var connection = new SqliteConnection($"Data Source={path}"))
        {
            connection.Open();
            connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT);");
        }

        using var readOnly = new SqliteConnection($"Data Source={path};Mode=ReadOnly");
        readOnly.Open();

        var exception = Assert.Throws<SqliteException>(() => readOnly.ExecuteNonQuery("""
            SELECT 1;
            INSERT INTO Data VALUES ('blocked');
            """));
        exception!.SqliteErrorCode.Should().Be(8);
    }

    [Test]
    public void StateChangeFiresForOpenAndClose()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        var transitions = new List<(ConnectionState Original, ConnectionState Current)>();
        connection.StateChange += (_, args) => transitions.Add((args.OriginalState, args.CurrentState));

        connection.Open();
        connection.Close();

        transitions.Should().Equal(
            (ConnectionState.Closed, ConnectionState.Open),
            (ConnectionState.Open, ConnectionState.Closed));
    }

    [Test]
    public void ConnectionReportsActualDataSourceWhenOpen()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "local.db");
        using var connection = new SqliteConnection($"Data Source={path}");

        connection.Open();

        connection.DataSource.Should().Be(path);
    }

    [Test]
    public void OpenModeReadOnlyRejectsWrites()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "readonly.db");
        using (var connection = new SqliteConnection($"Data Source={path}"))
        {
            connection.Open();
            connection.ExecuteNonQuery("CREATE TABLE Data(Value);");
        }

        using var readOnly = new SqliteConnection($"Data Source={path};Mode=ReadOnly");
        readOnly.Open();

        var exception = Assert.Throws<SqliteException>(() => readOnly.ExecuteNonQuery("INSERT INTO Data VALUES (1);"));
        exception.SqliteErrorCode.Should().Be(8);
    }

    [Test]
    public void SharedMemoryConnectionsUseSameBackingStore()
    {
        var connectionString = "Data Source=turso-shared-test;Mode=Memory;Cache=Shared";
        using var first = new SqliteConnection(connectionString);
        using var second = new SqliteConnection(connectionString);

        first.Open();
        first.ExecuteNonQuery("CREATE TABLE Data(Value); INSERT INTO Data VALUES ('shared');");
        second.Open();

        second.ExecuteScalar<string>("SELECT Value FROM Data;").Should().Be("shared");
    }

    [Test]
    public void ConnectionStringPragmasAreApplied()
    {
        using var connection = new SqliteConnection("Data Source=:memory:;Foreign Keys=True;Recursive Triggers=True");
        connection.Open();

        connection.ExecuteScalar<long>("PRAGMA foreign_keys;").Should().Be(1);
        connection.ExecuteScalar<long>("PRAGMA recursive_triggers;").Should().Be(1);
    }

    [Test]
    public void ReadUncommittedTransactionSetsAndResetsPragma()
    {
        using var connection = new SqliteConnection("Data Source=:memory:;Cache=Shared");
        connection.Open();

        using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
            transaction.IsolationLevel.Should().Be(IsolationLevel.ReadUncommitted);
            connection.ExecuteScalar<long>("PRAGMA read_uncommitted;").Should().Be(1);
        }

        connection.ExecuteScalar<long>("PRAGMA read_uncommitted;").Should().Be(0);
    }

    [Test]
    public void UnsupportedIsolationLevelsArePromotedOrRejectedLikeSqlite()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using (var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
            transaction.IsolationLevel.Should().Be(IsolationLevel.Serializable);
            connection.ExecuteScalar<long>("PRAGMA read_uncommitted;").Should().Be(0);
        }

        Assert.Throws<ArgumentException>(() => connection.BeginTransaction(IsolationLevel.Chaos));
        Assert.Throws<ArgumentException>(() => connection.BeginTransaction(IsolationLevel.Snapshot));
    }

    [Test]
    public void TransactionSavepointsAndExternalRollbackMatchSqliteFacade()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value INTEGER);");

        using (var transaction = connection.BeginTransaction())
        {
            transaction.Save("before_insert");
            connection.ExecuteNonQuery("INSERT INTO Data VALUES (1);");
            transaction.Rollback("before_insert");
            transaction.Release("before_insert");
            transaction.Commit();
        }

        connection.ExecuteScalar<long>("SELECT COUNT(*) FROM Data;").Should().Be(0);

        var rolledBack = connection.BeginTransaction();
        connection.ExecuteNonQuery("ROLLBACK;");
        rolledBack.Rollback();
        Assert.Throws<InvalidOperationException>(() => rolledBack.Rollback());
    }

    [Test]
    public void ExternalRollbackPreventsUsingAmbientTransaction()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var transaction = connection.BeginTransaction();
        connection.ExecuteNonQuery("ROLLBACK;");

        Assert.Throws<InvalidOperationException>(() => connection.ExecuteNonQuery("SELECT 1;"))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.TransactionCompleted);
        transaction.Rollback();
    }

    [Test]
    public void RawSqlitePclHandleInteropIsUnsupported()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");

        ((object?)connection.Handle).Should().BeNull();
        connection.Open();
        ((object?)connection.Handle).Should().BeNull();
    }

    [Test]
    public void ReaderFieldTypeUsesExpressionAndDeclaredTypesBeforeRead()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using (var reader = connection.ExecuteReader("SELECT 1, 3.14, 'test', X'0102', NULL;"))
        {
            reader.GetFieldType(0).Should().Be(typeof(long));
            reader.GetFieldType(1).Should().Be(typeof(double));
            reader.GetFieldType(2).Should().Be(typeof(string));
            reader.GetFieldType(3).Should().Be(typeof(byte[]));
            reader.GetFieldType(4).Should().Be(typeof(byte[]));
            Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetFieldType(5))!
                .ParamName.Should().Be("ordinal");
        }

        connection.ExecuteNonQuery("CREATE TABLE Typed(TextValue TEXT, IntegerValue INTEGER, RealValue REAL, BlobValue BLOB, NumericValue NUMERIC);");
        using (var typed = connection.ExecuteReader("SELECT TextValue, IntegerValue, RealValue, BlobValue, NumericValue FROM Typed;"))
        {
            typed.GetFieldType(0).Should().Be(typeof(string));
            typed.GetFieldType(1).Should().Be(typeof(long));
            typed.GetFieldType(2).Should().Be(typeof(double));
            typed.GetFieldType(3).Should().Be(typeof(byte[]));
            typed.GetFieldType(4).Should().Be(typeof(string));
        }
    }

    [Test]
    public void ReaderDisposeStopsDrainingWhenLaterResultErrors()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value INTEGER);");

        using (var reader = connection.ExecuteReader("SELECT 1; SELECT fail('boom'); INSERT INTO Data VALUES (1);"))
        {
            Assert.DoesNotThrow(() => reader.Dispose());
        }

        connection.ExecuteScalar<long>("SELECT COUNT(*) FROM Data;").Should().Be(0);
    }

    [Test]
    public void ReaderNextResultErrorStopsRemainingStatements()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value INTEGER NOT NULL);");

        using (var reader = connection.ExecuteReader("SELECT 1; INSERT INTO Data VALUES (1); INSERT INTO Data VALUES (NULL); INSERT INTO Data VALUES (2);"))
        {
            Assert.Throws<SqliteException>(() => reader.NextResult())!
                .Message.Should().Contain("constraint failed");
        }

        connection.ExecuteScalar<long>("SELECT COUNT(*) FROM Data;").Should().Be(1);
    }

    [Test]
    public void DataTableLoadHandlesNullsInAliasedJoinColumns()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("""
            CREATE TABLE Member (
              ID INTEGER,
              Lastname TEXT NOT NULL,
              Firstname TEXT NOT NULL,
              Type INTEGER,
              Hidden INTEGER,
              PRIMARY KEY (ID AUTOINCREMENT)
            );
            CREATE TABLE Types (
              ID INTEGER,
              Description TEXT NOT NULL,
              Hidden INTEGER,
              PRIMARY KEY (ID AUTOINCREMENT)
            );
            INSERT INTO Types (Description) VALUES ('Administrator');
            INSERT INTO Types (Description) VALUES ('User');
            INSERT INTO Member (Lastname, Firstname, Type, Hidden) VALUES ('Mustermann', 'Max', 1, 0);
            INSERT INTO Member (Lastname, Firstname, Type, Hidden) VALUES ('Muller', 'Willhelm', NULL, 0);
            """);

        using var command = new SqliteCommand("""
            SELECT
              Member.ID AS ID,
              Member.Lastname,
              Member.Firstname,
              Types.ID AS TypeID,
              Types.Description AS Type,
              Member.Hidden
            FROM Member
            LEFT OUTER JOIN Types ON Types.ID = Member.Type;
            """, connection);
        using var dataReader = command.ExecuteReader();
        var table = new DataTable();

        Assert.DoesNotThrow(() => table.Load(dataReader));
        table.Columns["Type"]!.DataType.Should().Be(typeof(string));
    }

    [Test]
    public void GetSchemaReturnsMetadataCollectionsAndReservedWords()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");

        var collections = connection.GetSchema();
        collections.TableName.Should().Be(DbMetaDataCollectionNames.MetaDataCollections);
        collections.Rows.Cast<DataRow>().Select(row => row[DbMetaDataColumnNames.CollectionName])
            .Should().Contain(DbMetaDataCollectionNames.ReservedWords);

        var reservedWords = connection.GetSchema(DbMetaDataCollectionNames.ReservedWords);
        reservedWords.Rows.Cast<DataRow>().Select(row => row[DbMetaDataColumnNames.ReservedWord])
            .Should().Contain("SELECT");
    }

    [Test]
    public void BackupDatabaseCopiesSchemaAndRows()
    {
        using var source = new SqliteConnection("Data Source=:memory:");
        using var destination = new SqliteConnection("Data Source=:memory:");
        source.Open();
        destination.Open();
        source.ExecuteNonQuery("CREATE TABLE Person(Name TEXT); INSERT INTO Person VALUES ('Waldo');");

        source.BackupDatabase(destination);

        destination.ExecuteScalar<string>("SELECT Name FROM Person;").Should().Be("Waldo");
    }

    [Test]
    public void SqliteBlobReadsAndWritesExistingBlobColumn()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value BLOB); INSERT INTO Data(rowid, Value) VALUES (1, X'0102');");

        using (var blob = new SqliteBlob(connection, "Data", "Value", 1))
        {
            blob.Length.Should().Be(2);
            blob.ReadByte().Should().Be(1);
            blob.Position = 0;
            blob.Write([3], 0, 1);
        }

        connection.ExecuteScalar<byte[]>("SELECT Value FROM Data WHERE rowid = 1;").Should().Equal([3, 2]);
    }

    [Test]
    public void SqliteBlobHonorsReadOnlyMode()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value BLOB); INSERT INTO Data(rowid, Value) VALUES (1, X'0102');");

        using var blob = new SqliteBlob(connection, "Data", "Value", 1, readOnly: true);

        blob.CanWrite.Should().BeFalse();
        Assert.Throws<NotSupportedException>(() => blob.Write([3], 0, 1))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.WriteNotSupported);
    }

    [Test]
    public void AdvancedApisValidateNames()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");

        Assert.Throws<ArgumentNullException>(() => connection.CreateFunction(null!, () => 1))!
            .ParamName.Should().Be("name");
        Assert.Throws<ArgumentNullException>(() => connection.CreateAggregate<int>(null!, value => value))!
            .ParamName.Should().Be("name");
        Assert.Throws<ArgumentNullException>(() => connection.CreateCollation(null!, null))!
            .ParamName.Should().Be("name");
    }

    [Test]
    public void ScalarFunctionWorksWhenRegisteredBeforeOpen()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.CreateFunction("test", 1L, (long state, long x, int y) => $"{state} {x} {y}");
        connection.Open();

        connection.ExecuteScalar<string>("SELECT test(2, 3);").Should().Be("1 2 3");
    }

    [Test]
    public void ScalarFunctionSupportsVariadicArgumentsAndBlobValues()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateFunction("test", args => string.Join(", ", args.Select(arg => arg?.GetType().Name ?? "(null)")));

        connection.ExecuteScalar<string>("SELECT test(1, 3.1, 'A', X'7E57', NULL);")
            .Should().Be("Int64, Double, String, Byte[], (null)");
    }

    [Test]
    public void ScalarFunctionReportsNullForNonNullableParameters()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateFunction("test", (long x) => x);

        var exception = Assert.Throws<SqliteException>(() => connection.ExecuteScalar<long>("SELECT test(NULL);"))!;

        exception.SqliteErrorCode.Should().Be(1);
        exception.Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(1, Data.Sqlite.Properties.Resources.UDFCalledWithNull("test", 0)));
    }

    [Test]
    public void ScalarFunctionPreservesDecimalPrecision()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        const decimal expected = 1234567890123456789012345678m;

        connection.CreateFunction("test", () => expected);

        using var reader = connection.ExecuteReader("SELECT test();");
        reader.Read().Should().BeTrue();
        reader.GetDecimal(0).Should().Be(expected);
    }

    [Test]
    public void DbTypeDecimalParametersPreservePrecision()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT);");
        const decimal expected = 1234567890123456789012345678m;

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO Data VALUES ($value);";
            var parameter = insert.CreateParameter();
            parameter.ParameterName = "$value";
            parameter.DbType = DbType.Decimal;
            parameter.Value = expected;
            insert.Parameters.Add(parameter);
            insert.ExecuteNonQuery();
        }

        using var reader = connection.ExecuteReader("SELECT Value FROM Data;");
        reader.Read().Should().BeTrue();
        reader.GetDecimal(0).Should().Be(expected);
    }

    [Test]
    public void ScalarFunctionPropagatesSqliteExceptionCode()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateFunction<long>("test", () => throw new SqliteException("Test", 200));

        var exception = Assert.Throws<SqliteException>(() => connection.ExecuteScalar<long>("SELECT test();"))!;

        exception.SqliteErrorCode.Should().Be(200);
        exception.Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(200, "Test"));
    }

    [Test]
    public void ScalarFunctionCanBeRemoved()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateFunction("test", () => 1L);
        connection.ExecuteScalar<long>("SELECT test();").Should().Be(1);

        connection.CreateFunction("test", default(Func<long>));

        Assert.Throws<SqliteException>(() => connection.ExecuteScalar<long>("SELECT test();"))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(1, "no such function: test"));
    }

    [Test]
    public void AggregateFunctionWorksWhenRegisteredBeforeOpen()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.CreateAggregate(
            "concat_test",
            "A",
            (string accumulator, string value, long suffix) => accumulator + value + suffix,
            accumulator => accumulator + "Z");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT, Suffix INTEGER); INSERT INTO Data VALUES ('X', 1);");

        connection.ExecuteScalar<string>("SELECT concat_test(Value, Suffix) FROM Data;")
            .Should().Be("AX1Z");
    }

    [Test]
    public void AggregateFunctionSupportsVariadicArgumentsAndNoRows()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT);");
        connection.CreateAggregate("join_test", "seed", (string accumulator, object?[] args) => accumulator + string.Join("|", args), accumulator => accumulator + ":done");

        connection.ExecuteScalar<string>("SELECT join_test(Value) FROM Data;")
            .Should().Be("seed:done");
    }

    [Test]
    public void AggregateFunctionPropagatesSqliteExceptionCode()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT); INSERT INTO Data VALUES ('X');");
        connection.CreateAggregate("fail_test", "seed", (string _) => throw new SqliteException("Test", 200));

        var exception = Assert.Throws<SqliteException>(() => connection.ExecuteScalar<string>("SELECT fail_test() FROM Data;"))!;

        exception.SqliteErrorCode.Should().Be(200);
        exception.Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(200, "Test"));
        connection.ExecuteScalar<long>("SELECT 1;").Should().Be(1);
    }

    [Test]
    public void AggregateFunctionFinalizerErrorLeavesConnectionUsable()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT); INSERT INTO Data VALUES ('X');");
        connection.CreateAggregate<string, string, string>(
            "final_fail_test",
            "seed",
            (string accumulator, string value) => accumulator + value,
            _ => throw new SqliteException("Final failed", 201));

        var exception = Assert.Throws<SqliteException>(() => connection.ExecuteScalar<string>("SELECT final_fail_test(Value) FROM Data;"))!;

        exception.SqliteErrorCode.Should().Be(201);
        exception.Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(201, "Final failed"));
        connection.ExecuteScalar<long>("SELECT 1;").Should().Be(1);
    }

    [Test]
    public void AggregateFunctionCanBeRemoved()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT); INSERT INTO Data VALUES ('X');");
        connection.CreateAggregate("remove_test", (string? accumulator, string value) => accumulator + value);
        connection.ExecuteScalar<string>("SELECT remove_test(Value) FROM Data;").Should().Be("X");

        connection.CreateAggregate("remove_test", default(Func<string?, string, string>));

        Assert.Throws<SqliteException>(() => connection.ExecuteScalar<string>("SELECT remove_test(Value) FROM Data;"))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(1, "no such function: remove_test"));
    }

    [Test]
    public void CollationWorksWhenRegisteredBeforeOpen()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.CreateCollation("test_nocase", StringComparer.OrdinalIgnoreCase.Compare);
        connection.Open();

        connection.ExecuteScalar<long>("SELECT 'abc' = 'ABC' COLLATE test_nocase;").Should().Be(1);
    }

    [Test]
    public void CollationStateOverloadCanBeRemoved()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateCollation("remove_nocase", StringComparer.OrdinalIgnoreCase, static (comparer, left, right) => comparer.Compare(left, right));
        connection.ExecuteScalar<long>("SELECT 'abc' = 'ABC' COLLATE remove_nocase;").Should().Be(1);

        connection.CreateCollation<string>("remove_nocase", "", null);

        var exception = Assert.Throws<SqliteException>(() => connection.ExecuteScalar<long>("SELECT 'abc' = 'ABC' COLLATE remove_nocase;"))!;
        exception.Message.Should().Be(Data.Sqlite.Properties.Resources.SqliteNativeError(1, "no such collation sequence: remove_nocase"));
    }

    [Test]
    public void CustomCollationCanBeUsedInExpressionsAndOrderingButNotSchema()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.CreateCollation("reverse_text", static (left, right) => -string.CompareOrdinal(left, right));

        connection.ExecuteScalar<long>("SELECT 'b' > 'a' COLLATE reverse_text;").Should().Be(0);
        connection.ExecuteNonQuery("CREATE TABLE Data(Value TEXT); INSERT INTO Data VALUES ('a'), ('b');");
        connection.ExecuteScalar<string>("SELECT Value FROM Data ORDER BY Value COLLATE reverse_text LIMIT 1;")
            .Should().Be("b");
        Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery("CREATE TABLE Collated(Value TEXT COLLATE reverse_text);"))!
            .Message.Should().Contain("custom collations are not supported in schema definitions");
        Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery("CREATE INDEX Data_Value_Custom ON Data(Value COLLATE reverse_text);"))!
            .Message.Should().Contain("custom collations are not supported in indexes");
    }

    [Test]
    public void EnableExtensionsControlsSqlLoadExtension()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var sql = "SELECT load_extension('unknown');";

        var disabled = Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery(sql))!.Message;
        connection.EnableExtensions();
        var enabled = Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery(sql))!.Message;
        connection.EnableExtensions(false);
        var disabledAgain = Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery(sql))!.Message;

        enabled.Should().NotBe(disabled);
        disabledAgain.Should().Be(disabled);
    }

    [Test]
    public void CompileOptionsCompatibilityIsHandledByFacade()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        connection.ExecuteScalar<long>("SELECT COUNT(*) FROM pragma_compile_options WHERE compile_options = 'OMIT_LOAD_EXTENSION';").Should().Be(0);

        using var reader = connection.ExecuteReader("PRAGMA compile_options;");
        reader.GetName(0).Should().Be("compile_options");
        reader.Read().Should().BeFalse();
    }

    [Test]
    public void LoadExtensionUsesNativeLoaderEvenWhenSqlFunctionDisabled()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.EnableExtensions(false);
        var disabled = Assert.Throws<SqliteException>(() => connection.ExecuteNonQuery("SELECT load_extension('unknown');"))!.Message;

        var loadError = Assert.Throws<SqliteException>(() => connection.LoadExtension("unknown"))!.Message;

        loadError.Should().NotBe(disabled);
    }

    [Test]
    public void LoadExtensionWhenClosedRunsOnNextOpen()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.LoadExtension("unknown");

        var exception = Assert.Throws<SqliteException>(connection.Open)!;

        exception.Message.Should().NotBe(Data.Sqlite.Properties.Resources.SqliteNativeError(1, "runtime extension loading is disabled"));
    }

    [Test]
    public void ExecuteReaderSkipsDmlStatementsBeforeFirstResultSet()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE t(value INTEGER);");

        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO t VALUES (1); SELECT value FROM t;";

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(1);
    }

    [Test]
    public void ExecuteScalarCanReuseCommandAfterPrepareFailure()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM dual;";

        var exception = Assert.Throws<SqliteException>(() => command.ExecuteScalar());
        exception.SqliteErrorCode.Should().Be(1);

        connection.ExecuteNonQuery("CREATE TABLE dual(dummy TEXT); INSERT INTO dual VALUES ('X');");

        command.ExecuteScalar().Should().Be(1);
    }

    [Test]
    public void OpenReaderBlocksWriteCommandUntilTimeout()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE t(value INTEGER); INSERT INTO t VALUES (1);");

        using var reader = connection.ExecuteReader("SELECT value FROM t;");
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE t;";
        command.CommandTimeout = 0;

        var exception = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());
        exception.SqliteErrorCode.Should().Be(5);
    }

    [Test]
    public void SqlTextIsMarshaledAsUtf8()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        connection.ExecuteScalar<string>("SELECT 'têst';").Should().Be("têst");
    }

    [Test]
    public void TypedDateTimeGettersThrowSqliteNullMessage()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var reader = connection.ExecuteReader("SELECT NULL;");
        reader.Read().Should().BeTrue();

        Assert.Throws<InvalidOperationException>(() => reader.GetDateTime(0))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.CalledOnNullValue(0));
        Assert.Throws<InvalidOperationException>(() => reader.GetDateTimeOffset(0))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.CalledOnNullValue(0));
    }

    [Test]
    public void GetFieldValueConvertsDateOnlyAndTimeOnly()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var reader = connection.ExecuteReader(
            "SELECT '2014-04-15', julianday('2014-04-15'), '13:10:15', '13:10:15.5', '1.0e-2', X'0E7E0DDC5D364849AB9B8CA8056BF93A', CAST('dc0d7e0e-365d-4948-ab9b-8ca8056bf93a' AS BLOB), '3.14';");
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<DateOnly>(0).Should().Be(new DateOnly(2014, 4, 15));
        reader.GetFieldValue<DateOnly>(1).Should().Be(new DateOnly(2014, 4, 15));
        reader.GetFieldValue<TimeOnly>(2).Should().Be(new TimeOnly(13, 10, 15));
        reader.GetFieldValue<TimeOnly>(3).Should().Be(new TimeOnly(13, 10, 15, 500));
        reader.GetFieldValue<decimal>(4).Should().Be(0.01m);
        reader.GetFieldValue<Guid>(5).Should().Be(new Guid("dc0d7e0e-365d-4948-ab9b-8ca8056bf93a"));
        reader.GetFieldValue<Guid>(6).Should().Be(new Guid("dc0d7e0e-365d-4948-ab9b-8ca8056bf93a"));
        reader.GetDouble(7).Should().Be(3.14d);
    }

    [Test]
    public void GetValueMaterializesDeclaredGuidColumns()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery(
            """
            CREATE TABLE ValuesWithGuids (
                TextGuid GUID,
                BlobGuid UNIQUEIDENTIFIER,
                RawBlob BLOB
            );
            INSERT INTO ValuesWithGuids VALUES (
                'dc0d7e0e-365d-4948-ab9b-8ca8056bf93a',
                X'0E7E0DDC5D364849AB9B8CA8056BF93A',
                X'0E7E0DDC5D364849AB9B8CA8056BF93A'
            );
            """);

        using var reader = connection.ExecuteReader("SELECT TextGuid, BlobGuid, RawBlob FROM ValuesWithGuids;");
        reader.Read().Should().BeTrue();

        var expected = new Guid("dc0d7e0e-365d-4948-ab9b-8ca8056bf93a");
        reader.GetFieldType(0).Should().Be(typeof(Guid));
        reader.GetFieldType(1).Should().Be(typeof(Guid));
        reader.GetValue(0).Should().Be(expected);
        reader.GetValue(1).Should().Be(expected);
        reader.GetValue(2).Should().BeOfType<byte[]>();

        var schema = reader.GetSchemaTable();
        schema.Rows[0][SchemaTableColumn.DataType].Should().Be(typeof(Guid));
        schema.Rows[1][SchemaTableColumn.DataType].Should().Be(typeof(Guid));
        schema.Rows[2][SchemaTableColumn.DataType].Should().Be(typeof(byte[]));
    }

    [Test]
    public void GetFieldValueThrowsForNullTypedValues()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var reader = connection.ExecuteReader("SELECT NULL;");
        reader.Read().Should().BeTrue();

        Assert.Throws<InvalidOperationException>(() => reader.GetFieldValue<byte[]>(0))!
            .Message.Should().Be(Data.Sqlite.Properties.Resources.CalledOnNullValue(0));
    }

    [Test]
    public void PrepareDoesNotRejectMultiStatementBatches()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA read_uncommitted = 1;
            CREATE TABLE t(value INTEGER);
            INSERT INTO t VALUES (1);
            SELECT value FROM t;
            """;

        command.Prepare();
        command.ExecuteScalar().Should().Be(1L);
    }

    [Test]
    public void GetDataTypeNameWorksBeforeRead()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Person (Name nvarchar(4000));");

        using (var literalReader = connection.ExecuteReader("SELECT 1, 3.14, 'test', X'7E57', NULL;"))
        {
            literalReader.GetDataTypeName(0).Should().Be("INTEGER");
            literalReader.GetDataTypeName(1).Should().Be("REAL");
            literalReader.GetDataTypeName(2).Should().Be("TEXT");
            literalReader.GetDataTypeName(3).Should().Be("BLOB");
            literalReader.GetDataTypeName(4).Should().Be("BLOB");
        }

        using var columnReader = connection.ExecuteReader("SELECT Name FROM Person;");
        columnReader.GetDataTypeName(0).Should().Be("nvarchar");
    }

    [Test]
    public void OrdinalLookupMatchesSqliteErrors()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using (var reader = connection.ExecuteReader("SELECT 1 AS Id, 2 AS ID;"))
        {
            Assert.Throws<InvalidOperationException>(() => reader.GetOrdinal("id"))!
                .Message.Should().Contain(Data.Sqlite.Properties.Resources.AmbiguousColumnName("id", "Id", "ID"));
        }

        using var missingReader = connection.ExecuteReader("SELECT 1;");
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => missingReader.GetOrdinal("Name"));
        exception!.ParamName.Should().Be("name");
        exception.ActualValue.Should().Be("Name");
    }

    [Test]
    public void GetSchemaTableReturnsSqliteColumnMetadata()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery(
            """
            CREATE TABLE Person (
                ID INTEGER PRIMARY KEY,
                Code INT UNIQUE,
                LastName TEXT NOT NULL
            );
            INSERT INTO Person VALUES (1, 123, 'Smith');
            """);

        using var reader = connection.ExecuteReader("SELECT LastName, ID, Code, ID + 1 AS IncID FROM Person;");
        var schema = reader.GetSchemaTable();

        schema.Rows.Count.Should().Be(4);
        schema.Rows[0][SchemaTableColumn.BaseTableName].Should().Be("Person");
        schema.Rows[0][SchemaTableColumn.BaseColumnName].Should().Be("LastName");
        schema.Rows[0][SchemaTableColumn.DataType].Should().Be(typeof(string));
        schema.Rows[0]["DataTypeName"].Should().Be("TEXT");
        schema.Rows[0][SchemaTableColumn.AllowDBNull].Should().Be(false);
        schema.Rows[1][SchemaTableColumn.IsKey].Should().Be(true);
        schema.Rows[2][SchemaTableColumn.IsUnique].Should().Be(true);
        schema.Rows[3][SchemaTableColumn.IsExpression].Should().Be(true);
        schema.Rows[3][SchemaTableColumn.BaseTableName].Should().Be(DBNull.Value);
    }

    [Test]
    public void GetSchemaTableUnescapesQuotedBaseTableNames()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery(@"CREATE TABLE ""Bad""""Table""(Value);");

        using var reader = connection.ExecuteReader(@"SELECT * FROM ""Bad""""Table"";");
        var schema = reader.GetSchemaTable();

        schema.Rows[0][SchemaTableColumn.BaseTableName].Should().Be(@"Bad""Table");
    }

    [Test]
    public void GetSchemaTableInfersTypeForTypelessColumns()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE Test(Value); INSERT INTO Test VALUES (NULL), ('A');");

        using var reader = connection.ExecuteReader("SELECT Value FROM Test;");
        var schema = reader.GetSchemaTable();

        schema.Rows[0][SchemaTableColumn.DataType].Should().Be(typeof(string));
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName());
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose()
        {
            if (Directory.Exists(Path))
                Directory.Delete(Path, recursive: true);
        }
    }
}
