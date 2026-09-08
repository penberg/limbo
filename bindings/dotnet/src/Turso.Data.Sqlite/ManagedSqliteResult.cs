namespace Turso.Data.Sqlite;

internal sealed record ManagedSqliteColumn(string Name, string DataTypeName, Type FieldType);

internal sealed record ManagedSqliteResult(
    string Sql,
    IReadOnlyList<ManagedSqliteColumn> Columns,
    IReadOnlyList<object?[]> Rows,
    int RecordsAffected);

internal sealed record ManagedSqliteExecution(
    IReadOnlyList<ManagedSqliteResult> Results,
    int RecordsAffected,
    bool HadResultSet);
