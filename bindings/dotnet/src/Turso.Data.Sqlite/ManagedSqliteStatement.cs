using System.Text;

namespace Turso.Data.Sqlite;

internal sealed record ManagedSqliteStatement(
    string Sql,
    IReadOnlyList<string> ParameterNames,
    string FirstKeyword)
{
    public bool IsTransactionControl
        => FirstKeyword is "BEGIN" or "COMMIT" or "END" or "ROLLBACK" or "SAVEPOINT" or "RELEASE";
}

internal static class ManagedSqliteStatementParser
{
    public static IReadOnlyList<ManagedSqliteStatement> Parse(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);

        var statements = new List<ManagedSqliteStatement>();
        var text = new StringBuilder();
        var parameters = new List<string>();
        var parameterSet = new HashSet<string>(StringComparer.Ordinal);
        var leadingKeywords = new List<string>(3);
        var hasSql = false;
        var isTrigger = false;
        var triggerBodyStarted = false;
        var triggerBodyDepth = 0;
        var triggerStatementStart = false;

        for (var index = 0; index < sql.Length; index++)
        {
            var current = sql[index];
            var next = index + 1 < sql.Length ? sql[index + 1] : '\0';

            if (current == '-' && next == '-')
            {
                var end = index + 2;
                while (end < sql.Length && sql[end] != '\n')
                    end++;

                text.Append(sql, index, end - index);
                index = end - 1;
                continue;
            }

            if (current == '/' && next == '*')
            {
                var end = index + 2;
                while (end + 1 < sql.Length && (sql[end] != '*' || sql[end + 1] != '/'))
                    end++;

                end = end + 1 < sql.Length ? end + 2 : sql.Length;
                text.Append(sql, index, end - index);
                index = end - 1;
                continue;
            }

            if (current is '\'' or '"' or '`' or '[')
            {
                var end = ReadQuoted(sql, index, current);
                text.Append(sql, index, end - index);
                index = end - 1;
                hasSql = true;
                if (triggerBodyStarted)
                    triggerStatementStart = false;
                continue;
            }

            if (TryReadParameter(sql, index, out var parameterEnd))
            {
                var parameterName = sql[index..parameterEnd];
                text.Append(parameterName);
                if (parameterSet.Add(parameterName))
                    parameters.Add(parameterName);
                index = parameterEnd - 1;
                hasSql = true;
                if (triggerBodyStarted)
                    triggerStatementStart = false;
                continue;
            }

            if (IsIdentifierStart(current))
            {
                var end = index + 1;
                while (end < sql.Length && IsIdentifierPart(sql[end]))
                    end++;

                var token = sql[index..end];
                text.Append(token);
                hasSql = true;
                TrackKeyword(
                    token,
                    leadingKeywords,
                    ref isTrigger,
                    ref triggerBodyStarted,
                    ref triggerBodyDepth,
                    ref triggerStatementStart);
                index = end - 1;
                continue;
            }

            if (current == ';'
                && (!isTrigger || triggerBodyStarted && triggerBodyDepth == 0))
            {
                AddStatement(statements, text, parameters, leadingKeywords, hasSql);
                Reset(
                    text,
                    parameters,
                    parameterSet,
                    leadingKeywords,
                    ref hasSql,
                    ref isTrigger,
                    ref triggerBodyStarted,
                    ref triggerBodyDepth,
                    ref triggerStatementStart);
                continue;
            }

            text.Append(current);
            if (!char.IsWhiteSpace(current) && current != ';')
            {
                hasSql = true;
                if (triggerBodyStarted)
                    triggerStatementStart = false;
            }
            else if (current == ';' && triggerBodyStarted && triggerBodyDepth > 0)
            {
                triggerStatementStart = true;
            }
        }

        AddStatement(statements, text, parameters, leadingKeywords, hasSql);
        return statements;
    }

    private static int ReadQuoted(string sql, int start, char opening)
    {
        var closing = opening == '[' ? ']' : opening;
        var index = start + 1;
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

            return index + 1;
        }

        return sql.Length;
    }

    private static bool TryReadParameter(string sql, int start, out int end)
    {
        end = start;
        var prefix = sql[start];
        if (prefix == '?')
        {
            end = start + 1;
            while (end < sql.Length && char.IsAsciiDigit(sql[end]))
                end++;
            return true;
        }

        if (prefix is not (':' or '@' or '$')
            || start + 1 >= sql.Length
            || !IsParameterNamePart(sql[start + 1]))
        {
            return false;
        }

        end = start + 2;
        while (end < sql.Length && IsParameterNamePart(sql[end]))
            end++;

        if (prefix != '$')
            return true;

        while (end + 2 < sql.Length
               && sql[end] == ':'
               && sql[end + 1] == ':'
               && IsParameterNamePart(sql[end + 2]))
        {
            end += 3;
            while (end < sql.Length && IsParameterNamePart(sql[end]))
                end++;
        }

        if (end < sql.Length && sql[end] == '(')
        {
            var close = sql.IndexOf(')', end + 1);
            if (close >= 0)
                end = close + 1;
        }

        return true;
    }

    private static void TrackKeyword(
        string token,
        List<string> leadingKeywords,
        ref bool isTrigger,
        ref bool triggerBodyStarted,
        ref int triggerBodyDepth,
        ref bool triggerStatementStart)
    {
        var keyword = token.ToUpperInvariant();
        if (leadingKeywords.Count < 3)
        {
            leadingKeywords.Add(keyword);
            isTrigger = leadingKeywords is ["CREATE", "TRIGGER", ..]
                or ["CREATE", "TEMP", "TRIGGER", ..]
                or ["CREATE", "TEMPORARY", "TRIGGER", ..];
        }

        if (!isTrigger)
            return;

        if (!triggerBodyStarted)
        {
            if (keyword == "BEGIN")
            {
                triggerBodyStarted = true;
                triggerBodyDepth = 1;
                triggerStatementStart = true;
            }

            return;
        }

        if (keyword == "CASE")
        {
            triggerBodyDepth++;
            triggerStatementStart = false;
        }
        else if (keyword == "END")
        {
            if (triggerBodyDepth > 1)
                triggerBodyDepth--;
            else if (triggerStatementStart)
                triggerBodyDepth = 0;
            triggerStatementStart = false;
        }
        else
        {
            triggerStatementStart = false;
        }
    }

    private static void AddStatement(
        List<ManagedSqliteStatement> statements,
        StringBuilder text,
        List<string> parameters,
        List<string> leadingKeywords,
        bool hasSql)
    {
        if (!hasSql)
            return;

        var statement = text.ToString().Trim();
        if (statement.Length == 0)
            return;

        statements.Add(
            new ManagedSqliteStatement(
                statement,
                parameters.ToArray(),
                leadingKeywords.Count == 0 ? string.Empty : leadingKeywords[0]));
    }

    private static void Reset(
        StringBuilder text,
        List<string> parameters,
        HashSet<string> parameterSet,
        List<string> leadingKeywords,
        ref bool hasSql,
        ref bool isTrigger,
        ref bool triggerBodyStarted,
        ref int triggerBodyDepth,
        ref bool triggerStatementStart)
    {
        text.Clear();
        parameters.Clear();
        parameterSet.Clear();
        leadingKeywords.Clear();
        hasSql = false;
        isTrigger = false;
        triggerBodyStarted = false;
        triggerBodyDepth = 0;
        triggerStatementStart = false;
    }

    private static bool IsIdentifierStart(char value)
        => value == '_' || char.IsLetter(value);

    private static bool IsIdentifierPart(char value)
        => value == '_' || char.IsLetterOrDigit(value);

    private static bool IsParameterNamePart(char value)
        => value == '_' || char.IsLetterOrDigit(value);
}
