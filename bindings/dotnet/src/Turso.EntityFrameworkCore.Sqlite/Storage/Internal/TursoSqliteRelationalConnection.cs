using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using TursoSqliteConnection = Turso.Data.Sqlite.SqliteConnection;
using TursoSqliteConnectionStringBuilder = Turso.Data.Sqlite.SqliteConnectionStringBuilder;
using TursoSqliteOpenMode = Turso.Data.Sqlite.SqliteOpenMode;

namespace Turso.EntityFrameworkCore.Sqlite.Storage.Internal;

public class TursoSqliteRelationalConnection : SqliteRelationalConnection
{
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;
    private readonly IDiagnosticsLogger<DbLoggerCategory.Infrastructure> _logger;
    private readonly int? _commandTimeout;

    public TursoSqliteRelationalConnection(
        RelationalConnectionDependencies dependencies,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IDiagnosticsLogger<DbLoggerCategory.Infrastructure> logger)
        : base(dependencies, rawSqlCommandBuilder, logger)
    {
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
        _logger = logger;

        var relationalOptions = RelationalOptionsExtension.Extract(dependencies.ContextOptions);
        _commandTimeout = relationalOptions.CommandTimeout;
        if (relationalOptions.Connection is TursoSqliteConnection connection)
            InitializeTursoConnection(connection);
    }

    protected override DbConnection CreateDbConnection()
    {
        var connection = new TursoSqliteConnection(GetValidatedConnectionString());
        InitializeTursoConnection(connection);
        return connection;
    }

    public override ISqliteRelationalConnection CreateReadOnlyConnection()
    {
        var connectionStringBuilder = new TursoSqliteConnectionStringBuilder(GetValidatedConnectionString())
        {
            Mode = TursoSqliteOpenMode.ReadOnly,
            Pooling = false
        };

        var contextOptions = new DbContextOptionsBuilder()
            .UseTurso(connectionStringBuilder.ToString())
            .Options;

        return new TursoSqliteRelationalConnection(
            Dependencies with { ContextOptions = contextOptions },
            _rawSqlCommandBuilder,
            _logger);
    }

    private void InitializeTursoConnection(TursoSqliteConnection connection)
    {
        if (_commandTimeout.HasValue)
            connection.DefaultTimeout = _commandTimeout.Value;

        if (connection.IsDirectRemote)
            return;

        connection.CreateFunction<string, string, bool?>(
            "regexp",
            (pattern, input) => input is null || pattern is null
                ? null
                : Regex.IsMatch(input, pattern, RegexOptions.None, TimeSpan.FromMilliseconds(1000)),
            isDeterministic: true);

        connection.CreateFunction(
            "ef_mod",
            (decimal? dividend, decimal? divisor) => divisor == 0m ? null : dividend % divisor,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_add",
            (decimal? left, decimal? right) => left + right,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_divide",
            (decimal? dividend, decimal? divisor) => divisor == 0m ? null : dividend / divisor,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_compare",
            (decimal? left, decimal? right) => left.HasValue && right.HasValue
                ? decimal.Compare(left.Value, right.Value)
                : default(int?),
            isDeterministic: true);

        connection.CreateFunction(
            "ef_multiply",
            (decimal? left, decimal? right) => left * right,
            isDeterministic: true);

        connection.CreateFunction(
            "ef_negate",
            (decimal? value) => -value,
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_avg",
            seed: (0m, 0ul),
            ((decimal Sum, ulong Count) accumulator, decimal? value) => value is null
                ? accumulator
                : (accumulator.Sum + value.Value, accumulator.Count + 1),
            ((decimal Sum, ulong Count) accumulator) => accumulator.Count == 0
                ? default(decimal?)
                : accumulator.Sum / accumulator.Count,
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_max",
            seed: null,
            (decimal? max, decimal? value) => max is null
                ? value
                : value is null
                    ? max
                    : decimal.Max(max.Value, value.Value),
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_min",
            seed: null,
            (decimal? min, decimal? value) => min is null
                ? value
                : value is null
                    ? min
                    : decimal.Min(min.Value, value.Value),
            isDeterministic: true);

        connection.CreateAggregate(
            "ef_sum",
            seed: null,
            (decimal? sum, decimal? value) => value is null
                ? sum
                : sum is null
                    ? value
                    : sum.Value + value.Value,
            isDeterministic: true);

        connection.CreateCollation(
            "EF_DECIMAL",
            (left, right) => decimal.Compare(
                decimal.Parse(left, NumberStyles.Number, CultureInfo.InvariantCulture),
                decimal.Parse(right, NumberStyles.Number, CultureInfo.InvariantCulture)));
    }
}

internal sealed class TursoSqliteCommandInterceptor : DbCommandInterceptor
{
    public static TursoSqliteCommandInterceptor Instance { get; } = new();

    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result)
    {
        Validate(command);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Validate(command);
        return ValueTask.FromResult(result);
    }

    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result)
    {
        Validate(command);
        return result;
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        Validate(command);
        return ValueTask.FromResult(result);
    }

    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        Validate(command);
        return result;
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        Validate(command);
        return ValueTask.FromResult(result);
    }

    private static void Validate(DbCommand command)
    {
        if (command.Connection is not TursoSqliteConnection { IsDirectRemote: true })
            return;

        var unsupportedToken = FindUnsupportedToken(command.CommandText);
        if (unsupportedToken is not null)
        {
            throw new NotSupportedException(
                $"Direct remote Turso connections do not support the client-side SQLite helper '{unsupportedToken}'. "
                + "Use SQL supported by the remote database or use a local database.");
        }
    }

    private static string? FindUnsupportedToken(string sql)
    {
        string? previousIdentifier = null;
        for (var index = 0; index < sql.Length;)
        {
            if (TrySkipTriviaOrString(sql, ref index))
                continue;

            if (!IsIdentifierStart(sql[index]))
            {
                index++;
                continue;
            }

            var start = index++;
            while (index < sql.Length && IsIdentifierPart(sql[index]))
                index++;

            var token = sql[start..index];
            var next = NextSignificantCharacter(sql, index);
            if ((IsDecimalHelper(token) && next == '(')
                || (token.Equals("EF_DECIMAL", StringComparison.OrdinalIgnoreCase)
                    && previousIdentifier?.Equals("COLLATE", StringComparison.OrdinalIgnoreCase) == true)
                || (token.Equals("REGEXP", StringComparison.OrdinalIgnoreCase)
                    && IsRegexpOperator(sql, start, previousIdentifier, next)))
            {
                return token;
            }

            previousIdentifier = token;
        }

        return null;
    }

    private static char NextSignificantCharacter(string sql, int index)
    {
        while (index < sql.Length && char.IsWhiteSpace(sql[index]))
            index++;
        return index < sql.Length ? sql[index] : '\0';
    }

    private static bool IsRegexpOperator(
        string sql,
        int tokenStart,
        string? previousIdentifier,
        char next)
    {
        if (next == '(')
            return true;

        var previousIndex = tokenStart - 1;
        while (previousIndex >= 0 && char.IsWhiteSpace(sql[previousIndex]))
            previousIndex--;
        if (previousIndex < 0 || sql[previousIndex] is ',' or '.' or '(')
            return false;
        if (sql[previousIndex] is '"' or ']' or '`' or '\'' or ')' or '?'
            || char.IsAsciiDigit(sql[previousIndex]))
        {
            return true;
        }

        return previousIdentifier is not null
               && !IsExpressionIntroducer(previousIdentifier);
    }

    private static bool IsExpressionIntroducer(string token)
        => token.Equals("SELECT", StringComparison.OrdinalIgnoreCase)
           || token.Equals("WHERE", StringComparison.OrdinalIgnoreCase)
           || token.Equals("AND", StringComparison.OrdinalIgnoreCase)
           || token.Equals("OR", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ON", StringComparison.OrdinalIgnoreCase)
           || token.Equals("WHEN", StringComparison.OrdinalIgnoreCase)
           || token.Equals("THEN", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ELSE", StringComparison.OrdinalIgnoreCase)
           || token.Equals("AS", StringComparison.OrdinalIgnoreCase)
           || token.Equals("BY", StringComparison.OrdinalIgnoreCase)
           || token.Equals("SET", StringComparison.OrdinalIgnoreCase)
           || token.Equals("VALUES", StringComparison.OrdinalIgnoreCase)
           || token.Equals("RETURNING", StringComparison.OrdinalIgnoreCase);

    private static bool TrySkipTriviaOrString(string sql, ref int index)
    {
        if (sql[index] == '\'')
        {
            SkipDelimited(sql, ref index, '\'');
            return true;
        }

        if (sql[index] == '"')
        {
            SkipDelimited(sql, ref index, '"');
            return true;
        }

        if (sql[index] == '`')
        {
            SkipDelimited(sql, ref index, '`');
            return true;
        }

        if (sql[index] == '[')
        {
            SkipDelimited(sql, ref index, ']');
            return true;
        }

        if (sql[index] == '-' && index + 1 < sql.Length && sql[index + 1] == '-')
        {
            index += 2;
            while (index < sql.Length && sql[index] is not ('\r' or '\n'))
                index++;
            return true;
        }

        if (sql[index] == '/' && index + 1 < sql.Length && sql[index + 1] == '*')
        {
            index += 2;
            while (index + 1 < sql.Length && (sql[index] != '*' || sql[index + 1] != '/'))
                index++;
            index = Math.Min(index + 2, sql.Length);
            return true;
        }

        return false;
    }

    private static void SkipDelimited(string sql, ref int index, char closing)
    {
        index++;
        while (index < sql.Length)
        {
            if (sql[index] != closing)
            {
                index++;
                continue;
            }

            if (index + 1 < sql.Length && sql[index + 1] == closing)
            {
                index += 2;
                continue;
            }

            index++;
            return;
        }
    }

    private static bool IsDecimalHelper(string token)
        => token.Equals("ef_mod", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_add", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_divide", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_compare", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_multiply", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_negate", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_avg", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_max", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_min", StringComparison.OrdinalIgnoreCase)
           || token.Equals("ef_sum", StringComparison.OrdinalIgnoreCase);

    private static bool IsIdentifierStart(char value)
        => value is '_' || char.IsLetter(value);

    private static bool IsIdentifierPart(char value)
        => value is '_' || char.IsLetterOrDigit(value);
}
