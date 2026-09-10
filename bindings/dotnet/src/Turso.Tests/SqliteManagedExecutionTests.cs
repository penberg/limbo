using System.Data;
using System.Data.Common;
using System.Net;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Turso.Data.Sqlite;

namespace Turso.Tests;

[NonParallelizable]
public sealed class SqliteManagedExecutionTests
{
    [Test]
    public async Task ReplicaFacadePropagatesItsEffectivePoolingDefault()
    {
        var directory = Path.Combine(
            Path.GetTempPath(),
            "turso-sqlite-pooling-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var sqliteBuilder = new SqliteConnectionStringBuilder
        {
            DataSource = "https://example.test",
            ReplicaPath = Path.Combine(directory, "replica.db"),
            BootstrapIfEmpty = false,
        };

        try
        {
            await using var first = new SqliteConnection(sqliteBuilder.ConnectionString);
            await using var second = new SqliteConnection(sqliteBuilder.ConnectionString);

            await first.OpenAsync();
            await second.OpenAsync();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("-- no statement")]
    [TestCase("/* no statement */")]
    public void DirectRemoteEmptyCommandReturnsMinusOneWithoutSendingARequest(string sql)
    {
        using var handler = new ScriptedPipelineHandler();
        using var handlerScope = UseRemoteHandler(handler);
        using var connection = new SqliteConnection("Data Source=https://example.test");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.ExecuteNonQuery().Should().Be(-1);
        handler.RequestBodies.Should().BeEmpty();
    }

    [TestCase("/* leading comment */ INSERT INTO items VALUES (1)")]
    [TestCase("PRAGMA user_version = 1")]
    [TestCase("PRAGMA user_version(1)")]
    [TestCase("WITH value(x) AS (SELECT 1) /* comment */ INSERT INTO items SELECT x FROM value")]
    public async Task ReadOnlyManagedBatchRejectsWritesBeforeSendingThem(string sql)
    {
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Mode=ReadOnly");
        await connection.OpenAsync();
        await using var batch = (SqliteBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new SqliteBatchCommand(sql));

        var action = async () => await batch.ExecuteNonQueryAsync(CancellationToken.None);

        await action.Should().ThrowAsync<SqliteException>()
            .Where(exception => exception.SqliteErrorCode == 8);
    }

    [Test]
    public void DirectRemoteTriggerColumnNamedEndStaysInOneStatement()
    {
        using var handler = new ScriptedPipelineHandler(EmptyResult(), CloseResult());
        using var handlerScope = UseRemoteHandler(handler);
        using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER copy_value AFTER INSERT ON items BEGIN
                INSERT INTO audit(end) VALUES (NEW.value);
            END;
            """;

        command.ExecuteNonQuery();
        connection.Close();

        handler.RequestBodies.Should().HaveCount(2);
        GetExecuteStatement(handler.RequestBodies[0]).GetProperty("sql").GetString()
            .Should().Contain("INSERT INTO audit(end)")
            .And.EndWith("END");
    }

    [Test]
    public async Task DirectRemoteSplitsTriggerBodiesAndFiltersParametersPerStatement()
    {
        using var handler = new ScriptedPipelineHandler(
            IntegerResult(1),
            EmptyResult(),
            IntegerResult(3),
            CloseResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT $first /* ; @ignored */;
            CREATE TRIGGER update_items AFTER INSERT ON items BEGIN
                INSERT INTO audit VALUES (':ignored', @trigger);
                UPDATE items
                SET value = CASE WHEN value = ';' THEN 'x' ELSE value END;
            END;
            -- ; $ignored
            SELECT @second, "$ignored", `@ignored`, [@ignored];
            """;
        command.Parameters.AddWithValue("$first", 1);
        command.Parameters.AddWithValue("@trigger", 2);
        command.Parameters.AddWithValue("@second", 3);
        command.Parameters.AddWithValue("$unused", 4);

        await using var reader = (SqliteDataReader)await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(1);
        (await reader.ReadAsync()).Should().BeFalse();
        (await reader.NextResultAsync()).Should().BeTrue();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(3);
        (await reader.NextResultAsync()).Should().BeFalse();
        reader.RecordsAffected.Should().Be(-1);
        await reader.CloseAsync();
        await connection.CloseAsync();

        handler.RequestBodies.Should().HaveCount(4);
        var statements = handler.RequestBodies
            .Take(3)
            .Select(GetExecuteStatement)
            .ToArray();
        statements[0].GetProperty("sql").GetString().Should().Contain("SELECT $first");
        GetNamedArgumentNames(statements[0]).Should().Equal("$first");
        statements[1].GetProperty("sql").GetString().Should()
            .Contain("INSERT INTO audit")
            .And.Contain("UPDATE items")
            .And.EndWith("END");
        GetNamedArgumentNames(statements[1]).Should().Equal("@trigger");
        statements[2].GetProperty("sql").GetString().Should().Contain("SELECT @second");
        GetNamedArgumentNames(statements[2]).Should().Equal("@second");
        GetRequestBaton(handler.RequestBodies[1]).Should().Be("session");
        GetRequestBaton(handler.RequestBodies[2]).Should().Be("session");
    }

    [Test]
    public void DirectRemoteMultiStatementRejectsStatelessExecution()
    {
        using var handler = new ScriptedPipelineHandler();
        using var handlerScope = UseRemoteHandler(handler);
        using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1; SELECT 2;";

        command.Invoking(static value => value.ExecuteReader())
            .Should().Throw<NotSupportedException>()
            .WithMessage("*Read Your Writes=True*");
        handler.RequestBodies.Should().BeEmpty();
    }

    [Test]
    public async Task ManagedPrepareMapsParametersWithoutExecutingAndRejectsAmbiguousNames()
    {
        using var handler = new ScriptedPipelineHandler();
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT :first; SELECT $second;";
        command.Parameters.AddWithValue("first", 1);
        command.Parameters.AddWithValue("second", 2);

        await command.PrepareAsync();

        handler.RequestBodies.Should().BeEmpty();
        command.CommandText = "SELECT :value, $value";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("value", 1);
        await command.Invoking(value => value.PrepareAsync())
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*ambiguous*");
        command.CommandText = "SELECT @missing";
        command.Parameters.Clear();
        await command.Invoking(value => value.PrepareAsync())
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*@missing*");
        handler.RequestBodies.Should().BeEmpty();
    }

    [Test]
    public async Task DirectRemoteParametersAndReaderUseSqliteConversions()
    {
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        using var handler = new ScriptedPipelineHandler(
            ExecuteResult(
                """
                {
                  "cols": [
                    { "name": "at", "decltype": "TEXT" },
                    { "name": "elapsed", "decltype": "REAL" },
                    { "name": "id", "decltype": "GUID" },
                    { "name": "enabled", "decltype": "INTEGER" },
                    { "name": "missing", "decltype": "TEXT" }
                  ],
                  "rows": [[
                    { "type": "text", "value": "2025-01-02 03:04:05+02:00" },
                    { "type": "float", "value": 0.5 },
                    { "type": "text", "value": "00112233-4455-6677-8899-aabbccddeeff" },
                    { "type": "integer", "value": "1" },
                    { "type": "null" }
                  ]],
                  "affected_row_count": 0,
                  "last_insert_rowid": null
                }
                """,
                includeClose: true));
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT $text, :amount, @blob, $id";
        command.Parameters.Add("text", SqliteType.Text, 3).Value = "abcdef";
        command.Parameters.Add("amount", SqliteType.Text).Value = 12.5m;
        command.Parameters.Add("blob", SqliteType.Blob, 2).Value = new byte[] { 1, 2, 3 };
        command.Parameters.Add("id", SqliteType.Text).Value = guid;

        await using var reader = (SqliteDataReader)await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetDateTime(0).Should().Be(
            new DateTime(2025, 1, 2, 1, 4, 5, DateTimeKind.Utc));
        reader.GetDateTimeOffset(0).Offset.Should().Be(TimeSpan.FromHours(2));
        reader.GetTimeSpan(1).Should().Be(TimeSpan.FromHours(12));
        reader.GetGuid(2).Should().Be(guid);
        reader.GetValue(2).Should().Be(guid);
        reader.GetBoolean(3).Should().BeTrue();
        reader.IsDBNull(4).Should().BeTrue();
        reader.Invoking(value => value.GetString(4))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*ordinal 4*");
        reader.GetDataTypeName(2).Should().Be("GUID");
        var schema = reader.GetSchemaTable();
        schema.Rows[2][SchemaTableColumn.DataType].Should().Be(typeof(Guid));
        schema.Rows[2]["DataTypeName"].Should().Be("GUID");

        var statement = GetExecuteStatement(handler.RequestBodies.Single());
        var arguments = statement.GetProperty("named_args")
            .EnumerateArray()
            .ToDictionary(
                argument => argument.GetProperty("name").GetString()!,
                argument => argument.GetProperty("value"));
        arguments.Keys.Should().Equal("$text", ":amount", "@blob", "$id");
        arguments["$text"].GetProperty("value").GetString().Should().Be("abc");
        arguments["$text"].TryGetProperty("base64", out _).Should().BeFalse();
        arguments[":amount"].GetProperty("value").GetString().Should().Be("12.5");
        arguments["@blob"].GetProperty("base64").GetString().Should().Be("AQI=");
        arguments["@blob"].TryGetProperty("value", out _).Should().BeFalse();
        arguments["$id"].GetProperty("value").GetString().Should()
            .Be("00112233-4455-6677-8899-AABBCCDDEEFF");
    }

    [Test]
    public void DirectRemoteTranslatesSqliteErrorCodes()
    {
        using var handler = new ScriptedPipelineHandler(
            ErrorResult(
                "SQLITE_CONSTRAINT_FOREIGNKEY",
                "FOREIGN KEY constraint failed",
                includeClose: true));
        using var handlerScope = UseRemoteHandler(handler);
        using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO child VALUES (1)";

        var exception = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());

        exception!.SqliteErrorCode.Should().Be(19);
        exception.SqliteExtendedErrorCode.Should().Be(787);
        exception.Message.Should().Contain("FOREIGN KEY constraint failed");
    }

    [Test]
    public void DirectRemotePreservesAbortRollbackExtendedCode()
    {
        using var handler = new ScriptedPipelineHandler(
            ErrorResult(
                "SQLITE_ABORT_ROLLBACK",
                "transaction rolled back",
                includeClose: true));
        using var handlerScope = UseRemoteHandler(handler);
        using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE items SET value = 1";

        var exception = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());

        exception!.SqliteErrorCode.Should().Be(4);
        exception.SqliteExtendedErrorCode.Should().Be(516);
    }

    [Test]
    public async Task ManagedTransactionOwnsControlSqlAndSavepoints()
    {
        using var handler = new ScriptedPipelineHandler(
            EmptyResult(),
            EmptyResult(),
            EmptyResult(),
            EmptyResult(),
            EmptyResult(),
            CloseResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "COMMIT";
            await command.Invoking(value => value.ExecuteNonQueryAsync())
                .Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*Transaction-control SQL*");
        }

        await transaction.SaveAsync("save \" one");
        await transaction.RollbackAsync("save \" one");
        await transaction.ReleaseAsync("save \" one");
        await transaction.CommitAsync();

        handler.RequestBodies.Take(5).Select(GetExecuteSql).Should().Equal(
            "BEGIN IMMEDIATE",
            "SAVEPOINT \"save \"\" one\";",
            "ROLLBACK TO SAVEPOINT \"save \"\" one\";",
            "RELEASE SAVEPOINT \"save \"\" one\";",
            "COMMIT");
        GetRequestTypes(handler.RequestBodies[^1]).Should().Equal("close");
    }

    [Test]
    public async Task DirectRemoteOpenReaderBlocksWritesAndHonorsCancellation()
    {
        using var handler = new ScriptedPipelineHandler(IntegerResult(1), CloseResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        await connection.OpenAsync();
        await using var select = connection.CreateCommand();
        select.CommandText = "SELECT 1";
        await using var reader = await select.ExecuteReaderAsync();
        await using var write = connection.CreateCommand();
        write.CommandText = "UPDATE items SET value = 2";
        write.CommandTimeout = 30;
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        var action = async () => await write.ExecuteNonQueryAsync(cancellation.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
        handler.RequestBodies.Should().HaveCount(1);
    }

    [Test]
    public async Task DirectRemoteOpenReaderBlocksBatchWrites()
    {
        using var handler = new ScriptedPipelineHandler(IntegerResult(1), CloseResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=True");
        await connection.OpenAsync();
        await using var select = connection.CreateCommand();
        select.CommandText = "SELECT 1";
        await using var reader = await select.ExecuteReaderAsync();
        await using var batch = (SqliteBatch)connection.CreateBatch();
        batch.Timeout = 0;
        batch.BatchCommands.Add(new SqliteBatchCommand("UPDATE items SET value = 2"));

        var action = async () => await batch.ExecuteNonQueryAsync(CancellationToken.None);

        await action.Should().ThrowAsync<SqliteException>()
            .Where(exception => exception.SqliteErrorCode == 5);
        handler.RequestBodies.Should().HaveCount(1);
    }

    [Test]
    public async Task DirectRemoteBatchFlattensStatementsAndPreservesResults()
    {
        using var handler = new ScriptedPipelineHandler(BatchResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection(
            "Data Source=https://example.test;Read Your Writes=False");
        await connection.OpenAsync();
        await using var batch = (SqliteBatch)connection.CreateBatch();
        var insertAndSelect = new SqliteBatchCommand(
            "INSERT INTO items VALUES ($one); SELECT $two;");
        insertAndSelect.Parameters.AddWithValue("$one", 1);
        insertAndSelect.Parameters.AddWithValue("$two", "two");
        batch.BatchCommands.Add(insertAndSelect);
        var update = new SqliteBatchCommand("UPDATE items SET value = @value");
        update.Parameters.AddWithValue("@value", 3);
        batch.BatchCommands.Add(update);

        await using var reader = await batch.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeFalse();
        (await reader.NextResultAsync()).Should().BeTrue();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("two");
        (await reader.NextResultAsync()).Should().BeTrue();
        (await reader.ReadAsync()).Should().BeFalse();
        (await reader.NextResultAsync()).Should().BeFalse();
        reader.RecordsAffected.Should().Be(3);
        insertAndSelect.RecordsAffected.Should().Be(1);
        update.RecordsAffected.Should().Be(2);

        using var document = JsonDocument.Parse(handler.RequestBodies.Single());
        var steps = document.RootElement.GetProperty("requests")[0]
            .GetProperty("batch")
            .GetProperty("steps");
        steps.GetArrayLength().Should().Be(3);
        GetNamedArgumentNames(steps[0].GetProperty("stmt")).Should().Equal("$one");
        GetNamedArgumentNames(steps[1].GetProperty("stmt")).Should().Equal("$two");
        GetNamedArgumentNames(steps[2].GetProperty("stmt")).Should().Equal("@value");
    }

    [Test]
    public async Task DirectRemoteBatchReaderBlocksCommandWrites()
    {
        using var handler = new ScriptedPipelineHandler(BatchResult());
        using var handlerScope = UseRemoteHandler(handler);
        await using var connection = new SqliteConnection("Data Source=https://example.test");
        await connection.OpenAsync();
        await using var batch = (SqliteBatch)connection.CreateBatch();
        batch.BatchCommands.Add(new SqliteBatchCommand(
            "INSERT INTO items VALUES (1); SELECT value FROM items"));
        batch.BatchCommands.Add(new SqliteBatchCommand("UPDATE items SET value = 2"));
        await using var reader = await batch.ExecuteReaderAsync();
        await using var write = connection.CreateCommand();
        write.CommandText = "DELETE FROM items";
        write.CommandTimeout = 0;

        var action = async () => await write.ExecuteNonQueryAsync();

        await action.Should().ThrowAsync<SqliteException>()
            .Where(exception => exception.SqliteErrorCode == 5);
        handler.RequestBodies.Should().HaveCount(1);
    }

    [Test]
    public async Task DeferredBootstrapReplicaUsesManagedCommandsTransactionsAndBatch()
    {
        var directory = Path.Combine(
            TestContext.CurrentContext.WorkDirectory,
            "sqlite-managed-replica-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var replicaPath = Path.Combine(directory, "replica.db");
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = "https://example.test",
                ReplicaPath = replicaPath,
                BootstrapIfEmpty = false,
                Pooling = false,
            };
            await using var connection = new SqliteConnection(builder.ConnectionString);
            await connection.OpenAsync();
            connection.CreateFunction<int, int>("double_value", value => value * 2);
            using (var function = connection.CreateCommand())
            {
                function.CommandText = "SELECT double_value(2)";
                function.ExecuteScalar().Should().Be(4L);
            }
            await using (var setup = connection.CreateCommand())
            {
                setup.CommandText = """
                    CREATE TABLE items(value INTEGER);
                    INSERT INTO items VALUES ($first);
                    SELECT value FROM items;
                    """;
                setup.Parameters.AddWithValue("$first", 1);
                await using var reader = await setup.ExecuteReaderAsync();
                (await reader.ReadAsync()).Should().BeTrue();
                reader.GetInt64(0).Should().Be(1);
                reader.RecordsAffected.Should().Be(1);
            }

            await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
            {
                await using var insert = connection.CreateCommand();
                insert.Transaction = transaction;
                insert.CommandText = "INSERT INTO items VALUES (2)";
                (await insert.ExecuteNonQueryAsync()).Should().Be(1);
                await transaction.SaveAsync("before-three");
                insert.CommandText = "INSERT INTO items VALUES (3)";
                (await insert.ExecuteNonQueryAsync()).Should().Be(1);
                await transaction.RollbackAsync("before-three");
                await transaction.ReleaseAsync("before-three");
                await transaction.CommitAsync();
            }

            await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
            {
                await using var insert = connection.CreateCommand();
                insert.Transaction = transaction;
                insert.CommandText = "INSERT INTO items VALUES (99)";
                (await insert.ExecuteNonQueryAsync()).Should().Be(1);
                await transaction.RollbackAsync();
            }

            await using var batch = (SqliteBatch)connection.CreateBatch();
            batch.BatchCommands.Add(new SqliteBatchCommand("INSERT INTO items VALUES (4)"));
            batch.BatchCommands.Add(new SqliteBatchCommand("SELECT SUM(value) FROM items"));
            await using var batchReader = await batch.ExecuteReaderAsync();
            (await batchReader.ReadAsync()).Should().BeFalse();
            (await batchReader.NextResultAsync()).Should().BeTrue();
            (await batchReader.ReadAsync()).Should().BeTrue();
            batchReader.GetInt64(0).Should().Be(7);
            batchReader.RecordsAffected.Should().Be(1);
            await batchReader.DisposeAsync();

            await using var scalarBatch = (SqliteBatch)connection.CreateBatch();
            scalarBatch.BatchCommands.Add(new SqliteBatchCommand("INSERT INTO items VALUES (5)"));
            scalarBatch.BatchCommands.Add(new SqliteBatchCommand("SELECT MAX(value) FROM items"));
            (await scalarBatch.ExecuteScalarAsync(CancellationToken.None)).Should().Be(5L);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static IDisposable UseRemoteHandler(HttpMessageHandler handler)
    {
        TursoConnection.RemoteMessageHandlerFactory = () => handler;
        return new Scope(() => TursoConnection.RemoteMessageHandlerFactory = null);
    }

    private static JsonElement GetExecuteStatement(string requestBody)
    {
        using var document = JsonDocument.Parse(requestBody);
        return document.RootElement.GetProperty("requests")[0].GetProperty("stmt").Clone();
    }

    private static string GetExecuteSql(string requestBody)
        => GetExecuteStatement(requestBody).GetProperty("sql").GetString()!;

    private static string? GetRequestBaton(string requestBody)
    {
        using var document = JsonDocument.Parse(requestBody);
        return document.RootElement.GetProperty("baton").GetString();
    }

    private static string[] GetRequestTypes(string requestBody)
    {
        using var document = JsonDocument.Parse(requestBody);
        return document.RootElement.GetProperty("requests")
            .EnumerateArray()
            .Select(request => request.GetProperty("type").GetString()!)
            .ToArray();
    }

    private static string[] GetNamedArgumentNames(JsonElement statement)
        => statement.GetProperty("named_args")
            .EnumerateArray()
            .Select(argument => argument.GetProperty("name").GetString()!)
            .ToArray();

    private static string IntegerResult(long value)
        => ExecuteResult(
            $$"""
            {
              "cols": [{ "name": "value", "decltype": "INTEGER" }],
              "rows": [[{ "type": "integer", "value": "{{value}}" }]],
              "affected_row_count": 0,
              "last_insert_rowid": null
            }
            """);

    private static string EmptyResult(int affectedRows = 0)
        => ExecuteResult(
            $$"""
            {
              "cols": [],
              "rows": [],
              "affected_row_count": {{affectedRows}},
              "last_insert_rowid": null
            }
            """);

    private static string ExecuteResult(
        string statementResult,
        bool includeClose = false)
    {
        var closeResult = includeClose
            ? """
              ,
              {
                "type": "ok",
                "response": { "type": "close" }
              }
              """
            : string.Empty;
        return $$"""
                 {
                   "baton": "session",
                   "results": [
                     {
                       "type": "ok",
                       "response": {
                         "type": "execute",
                         "result": {{statementResult}}
                       }
                     }{{closeResult}}
                   ]
                 }
                 """;
    }

    private static string ErrorResult(
        string code,
        string message,
        bool includeClose)
    {
        var closeResult = includeClose
            ? """
              ,
              {
                "type": "ok",
                "response": { "type": "close" }
              }
              """
            : string.Empty;
        return $$"""
                 {
                   "results": [
                     {
                       "type": "error",
                       "error": { "message": "{{message}}", "code": "{{code}}" }
                     }{{closeResult}}
                   ]
                 }
                 """;
    }

    private static string CloseResult()
        => """
           {
             "results": [
               {
                 "type": "ok",
                 "response": { "type": "close" }
               }
             ]
           }
           """;

    private static string BatchResult()
        => """
           {
             "results": [
               {
                 "type": "ok",
                 "response": {
                   "type": "batch",
                   "result": {
                     "step_results": [
                       {
                         "cols": [],
                         "rows": [],
                         "affected_row_count": 1,
                         "last_insert_rowid": "1"
                       },
                       {
                         "cols": [{ "name": "value", "decltype": "TEXT" }],
                         "rows": [[{ "type": "text", "value": "two" }]],
                         "affected_row_count": 0,
                         "last_insert_rowid": null
                       },
                       {
                         "cols": [],
                         "rows": [],
                         "affected_row_count": 2,
                         "last_insert_rowid": null
                       }
                     ],
                     "step_errors": [null, null, null]
                   }
                 }
               },
               {
                 "type": "ok",
                 "response": { "type": "close" }
               }
             ]
           }
           """;

    private sealed class ScriptedPipelineHandler(params string[] responses) : HttpMessageHandler
    {
        private readonly Queue<string> _responses = new(responses);

        public List<string> RequestBodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestBodies.Add(
                await request.Content!.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
            if (_responses.Count == 0)
                throw new InvalidOperationException("No scripted pipeline response remains.");

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    _responses.Dequeue(),
                    Encoding.UTF8,
                    "application/json"),
            };
        }
    }

    private sealed class Scope(Action dispose) : IDisposable
    {
        public void Dispose()
        {
            dispose();
        }
    }
}
