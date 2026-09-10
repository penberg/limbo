using System.Data.Common;

namespace Turso.Data.Sqlite;

public sealed class SqliteFactory : DbProviderFactory
{
    public static readonly SqliteFactory Instance = new();

    private SqliteFactory()
    {
    }

    public override bool CanCreateBatch => true;

    public override DbBatch CreateBatch() => new SqliteBatch();

    public override DbBatchCommand CreateBatchCommand() => new SqliteBatchCommand();

    public override DbCommand CreateCommand() => new SqliteCommand();

    public override DbConnection CreateConnection() => new SqliteConnection();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new SqliteConnectionStringBuilder();

    public override DbParameter CreateParameter() => new SqliteParameter();
}
