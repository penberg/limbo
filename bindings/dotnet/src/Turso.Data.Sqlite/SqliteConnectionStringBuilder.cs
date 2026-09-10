using System.Collections;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Turso.Data.Sqlite;

public class SqliteConnectionStringBuilder : DbConnectionStringBuilder
{
    private static readonly string[] CanonicalKeywords =
    [
        "Data Source",
        "Mode",
        "Cache",
        "Password",
        "Foreign Keys",
        "Recursive Triggers",
        "Default Timeout",
        "Pooling",
        "Vfs",
        "Auth Token",
        "Replica Path",
        "Read Your Writes",
        "Sync Interval",
        "Sync Client Name",
        "Sync Long Poll Timeout",
        "Bootstrap If Empty",
        "Partial Bootstrap Prefix",
        "Partial Bootstrap Query",
        "Partial Sync Segment Size",
        "Partial Sync Prefetch",
        "Remote Encryption Cipher",
        "Remote Encryption Key",
        "Push Operations Threshold",
        "Pull Bytes Threshold",
        "Force Logical MVCC Pull",
        "Sync Experimental Features",
        "Tls",
        "DateTimeKind",
        "DateTimeFormat",
        "BinaryGUID",
        "Version",
    ];

    private static readonly Dictionary<string, string> KeywordMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Data Source"] = "Data Source",
        ["DataSource"] = "Data Source",
        ["Filename"] = "Data Source",
        ["Mode"] = "Mode",
        ["Cache"] = "Cache",
        ["Password"] = "Password",
        ["Foreign Keys"] = "Foreign Keys",
        ["ForeignKeys"] = "Foreign Keys",
        ["Recursive Triggers"] = "Recursive Triggers",
        ["RecursiveTriggers"] = "Recursive Triggers",
        ["Default Timeout"] = "Default Timeout",
        ["DefaultTimeout"] = "Default Timeout",
        ["Command Timeout"] = "Default Timeout",
        ["CommandTimeout"] = "Default Timeout",
        ["Pooling"] = "Pooling",
        ["Vfs"] = "Vfs",
        ["Auth Token"] = "Auth Token",
        ["AuthToken"] = "Auth Token",
        ["Authentication Token"] = "Auth Token",
        ["AuthenticationToken"] = "Auth Token",
        ["Replica Path"] = "Replica Path",
        ["ReplicaPath"] = "Replica Path",
        ["Read Your Writes"] = "Read Your Writes",
        ["ReadYourWrites"] = "Read Your Writes",
        ["Sync Interval"] = "Sync Interval",
        ["SyncInterval"] = "Sync Interval",
        ["Sync Client Name"] = "Sync Client Name",
        ["SyncClientName"] = "Sync Client Name",
        ["Sync Long Poll Timeout"] = "Sync Long Poll Timeout",
        ["SyncLongPollTimeout"] = "Sync Long Poll Timeout",
        ["Bootstrap If Empty"] = "Bootstrap If Empty",
        ["BootstrapIfEmpty"] = "Bootstrap If Empty",
        ["Partial Bootstrap Prefix"] = "Partial Bootstrap Prefix",
        ["PartialBootstrapPrefix"] = "Partial Bootstrap Prefix",
        ["Partial Bootstrap Query"] = "Partial Bootstrap Query",
        ["PartialBootstrapQuery"] = "Partial Bootstrap Query",
        ["Partial Sync Segment Size"] = "Partial Sync Segment Size",
        ["PartialSyncSegmentSize"] = "Partial Sync Segment Size",
        ["Partial Sync Prefetch"] = "Partial Sync Prefetch",
        ["PartialSyncPrefetch"] = "Partial Sync Prefetch",
        ["Remote Encryption Cipher"] = "Remote Encryption Cipher",
        ["RemoteEncryptionCipher"] = "Remote Encryption Cipher",
        ["Remote Encryption Key"] = "Remote Encryption Key",
        ["RemoteEncryptionKey"] = "Remote Encryption Key",
        ["Push Operations Threshold"] = "Push Operations Threshold",
        ["PushOperationsThreshold"] = "Push Operations Threshold",
        ["Pull Bytes Threshold"] = "Pull Bytes Threshold",
        ["PullBytesThreshold"] = "Pull Bytes Threshold",
        ["Force Logical MVCC Pull"] = "Force Logical MVCC Pull",
        ["ForceLogicalMvccPull"] = "Force Logical MVCC Pull",
        ["Sync Experimental Features"] = "Sync Experimental Features",
        ["SyncExperimentalFeatures"] = "Sync Experimental Features",
        ["Tls"] = "Tls",
        ["TLS"] = "Tls",
        ["DateTimeKind"] = "DateTimeKind",
        ["Date Time Kind"] = "DateTimeKind",
        ["DateTimeFormat"] = "DateTimeFormat",
        ["Date Time Format"] = "DateTimeFormat",
        ["BinaryGUID"] = "BinaryGUID",
        ["BinaryGuid"] = "BinaryGUID",
        ["Binary GUID"] = "BinaryGUID",
        ["Version"] = "Version",
    };

    public SqliteConnectionStringBuilder()
    {
    }

    public string AuthToken
    {
        get => GetString("Auth Token");
        set => SetString("Auth Token", value);
    }

    public string ReplicaPath
    {
        get => GetString("Replica Path");
        set => SetString("Replica Path", value);
    }

    public bool ReadYourWrites
    {
        get => GetBool("Read Your Writes", true);
        set => this["Read Your Writes"] = value;
    }

    public int SyncInterval
    {
        get => GetInt("Sync Interval", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Sync Interval"] = value;
        }
    }

    public string SyncClientName
    {
        get => GetString("Sync Client Name");
        set => SetString("Sync Client Name", value);
    }

    public int SyncLongPollTimeout
    {
        get => GetInt("Sync Long Poll Timeout", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Sync Long Poll Timeout"] = value;
        }
    }

    public bool BootstrapIfEmpty
    {
        get => GetBool("Bootstrap If Empty", true);
        set => this["Bootstrap If Empty"] = value;
    }

    public int PartialBootstrapPrefix
    {
        get => GetInt("Partial Bootstrap Prefix", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Partial Bootstrap Prefix"] = value;
        }
    }

    public string PartialBootstrapQuery
    {
        get => GetString("Partial Bootstrap Query");
        set => SetString("Partial Bootstrap Query", value);
    }

    public long PartialSyncSegmentSize
    {
        get => GetLong("Partial Sync Segment Size", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Partial Sync Segment Size"] = value;
        }
    }

    public bool PartialSyncPrefetch
    {
        get => GetBool("Partial Sync Prefetch");
        set => this["Partial Sync Prefetch"] = value;
    }

    public string RemoteEncryptionCipher
    {
        get => GetString("Remote Encryption Cipher");
        set => SetString("Remote Encryption Cipher", value);
    }

    public string RemoteEncryptionKey
    {
        get => GetString("Remote Encryption Key");
        set => SetString("Remote Encryption Key", value);
    }

    public long PushOperationsThreshold
    {
        get => GetLong("Push Operations Threshold", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Push Operations Threshold"] = value;
        }
    }

    public long PullBytesThreshold
    {
        get => GetLong("Pull Bytes Threshold", 0);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Pull Bytes Threshold"] = value;
        }
    }

    public bool ForceLogicalMvccPull
    {
        get => GetBool("Force Logical MVCC Pull");
        set => this["Force Logical MVCC Pull"] = value;
    }

    public string SyncExperimentalFeatures
    {
        get => GetString("Sync Experimental Features");
        set => SetString("Sync Experimental Features", value);
    }

    public bool? Tls
    {
        get => GetNullableBool("Tls");
        set => SetNullable("Tls", value);
    }

    public SqliteConnectionStringBuilder(string? connectionString)
    {
        ConnectionString = connectionString ?? string.Empty;
    }

    public bool IsLocal => !IsDirectRemote && !IsReplica;

    public bool IsDirectRemote
    {
        get
        {
            var options = TursoConnectionOptions.Parse(GetTursoConnectionString());
            return options.IsRemote && !options.IsReplica;
        }
    }

    public bool IsRemote => IsDirectRemote || IsReplica;

    public bool IsReplica => TursoConnectionOptions.Parse(GetTursoConnectionString()).IsReplica;

    public string DataSource
    {
        get => GetString("Data Source");
        set => SetString("Data Source", value);
    }

    private long GetLong(string keyword, long defaultValue)
    {
        return base.TryGetValue(keyword, out var value)
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : defaultValue;
    }

    public SqliteOpenMode Mode
    {
        get => GetEnum("Mode", SqliteOpenMode.ReadWriteCreate);
        set => this["Mode"] = value;
    }

    public SqliteCacheMode Cache
    {
        get => GetEnum("Cache", SqliteCacheMode.Default);
        set => this["Cache"] = value;
    }

    public string Password
    {
        get => GetString("Password");
        set => SetString("Password", value);
    }

    public bool? ForeignKeys
    {
        get => GetNullableBool("Foreign Keys");
        set => SetNullable("Foreign Keys", value);
    }

    public bool RecursiveTriggers
    {
        get => GetBool("Recursive Triggers");
        set => this["Recursive Triggers"] = value;
    }

    public int DefaultTimeout
    {
        get => GetInt("Default Timeout", 30);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Default Timeout"] = value;
        }
    }

    public bool Pooling
    {
        get => GetBool("Pooling", true);
        set => this["Pooling"] = value;
    }

    public string? Vfs
    {
        get => GetString("Vfs");
        set => SetString("Vfs", value);
    }

    public DateTimeKind DateTimeKind
    {
        get => GetEnum("DateTimeKind", System.DateTimeKind.Unspecified);
        set => this["DateTimeKind"] = value;
    }

    public string DateTimeFormat
    {
        get => GetString("DateTimeFormat");
        set => SetString("DateTimeFormat", value);
    }

    public bool BinaryGUID
    {
        get => GetBool("BinaryGUID", true);
        set => this["BinaryGUID"] = value;
    }

    public int Version
    {
        get => GetInt("Version", 3);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            this["Version"] = value;
        }
    }

    public override ICollection Keys => new ReadOnlyCollection<string>(CanonicalKeywords);

    public override ICollection Values => new ReadOnlyCollection<object?>(CanonicalKeywords.Select(GetValueOrDefault).ToArray());

    [AllowNull]
    public override object this[string keyword]
    {
        get
        {
            var normalizedKeyword = NormalizeKeyword(keyword);
            return (base.TryGetValue(normalizedKeyword, out var value)
                ? ConvertFromStoredValue(normalizedKeyword, value)
                : GetValueOrDefault(normalizedKeyword))!;
        }
        set
        {
            var normalizedKeyword = NormalizeKeyword(keyword);
            if (value is null)
            {
                Remove(normalizedKeyword);
                return;
            }

            base[normalizedKeyword] = ConvertToStoredValue(normalizedKeyword, value);
        }
    }

    public override bool ContainsKey(string keyword) => KeywordMap.ContainsKey(keyword);

    public override bool Remove(string keyword)
    {
        if (!KeywordMap.TryGetValue(keyword, out var normalizedKeyword))
            return false;

        return base.Remove(normalizedKeyword);
    }

#pragma warning disable CS8765
    public override bool TryGetValue(string keyword, out object? value)
#pragma warning restore CS8765
    {
        if (!KeywordMap.TryGetValue(keyword, out var normalizedKeyword))
        {
            value = null;
            return false;
        }

        var result = base.TryGetValue(normalizedKeyword, out var storedValue)
            ? ConvertFromStoredValue(normalizedKeyword, storedValue)
            : GetValueOrDefault(normalizedKeyword);
        value = result;
        return true;
    }

    internal string GetTursoConnectionString()
    {
        var builder = new DbConnectionStringBuilder();
        foreach (var keyword in ManagedKeywords)
        {
            if (base.TryGetValue(keyword, out var value))
                builder[keyword] = value;
        }
        if (!string.IsNullOrWhiteSpace(ReplicaPath) && !builder.ContainsKey("Pooling"))
            builder["Pooling"] = Pooling;

        return builder.ConnectionString;
    }

    private static readonly string[] ManagedKeywords =
    [
        "Data Source",
        "Default Timeout",
        "Pooling",
        "Auth Token",
        "Replica Path",
        "Read Your Writes",
        "Sync Interval",
        "Sync Client Name",
        "Sync Long Poll Timeout",
        "Bootstrap If Empty",
        "Partial Bootstrap Prefix",
        "Partial Bootstrap Query",
        "Partial Sync Segment Size",
        "Partial Sync Prefetch",
        "Remote Encryption Cipher",
        "Remote Encryption Key",
        "Push Operations Threshold",
        "Pull Bytes Threshold",
        "Force Logical MVCC Pull",
        "Sync Experimental Features",
        "Tls",
    ];

    private static string NormalizeKeyword(string keyword)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyword);
        if (KeywordMap.TryGetValue(keyword, out var normalizedKeyword))
            return normalizedKeyword;

        throw new ArgumentException(Properties.Resources.KeywordNotSupported(keyword));
    }

    private string GetString(string keyword)
    {
        return base.TryGetValue(keyword, out var value)
            ? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
            : string.Empty;
    }

    private void SetString(string keyword, string? value)
    {
        if (value is null)
            Remove(keyword);
        else
            this[keyword] = value;
    }

    private bool GetBool(string keyword, bool defaultValue = false)
    {
        return base.TryGetValue(keyword, out var value)
            ? Convert.ToBoolean(value, CultureInfo.InvariantCulture)
            : defaultValue;
    }

    private bool? GetNullableBool(string keyword)
    {
        return base.TryGetValue(keyword, out var value)
            ? Convert.ToBoolean(value, CultureInfo.InvariantCulture)
            : null;
    }

    private int GetInt(string keyword, int defaultValue)
    {
        return base.TryGetValue(keyword, out var value)
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : defaultValue;
    }

    private TEnum GetEnum<TEnum>(string keyword, TEnum defaultValue)
        where TEnum : struct
    {
        if (!base.TryGetValue(keyword, out var value))
            return defaultValue;

        if (value is TEnum typedValue)
        {
            if (!Enum.IsDefined(typeof(TEnum), typedValue))
                throw new ArgumentOutOfRangeException(nameof(value), value, Properties.Resources.InvalidEnumValue(typeof(TEnum), typedValue));

            return typedValue;
        }

        if (value is string stringValue && Enum.TryParse<TEnum>(stringValue, ignoreCase: true, out var parsedValue))
            return parsedValue;

        return (TEnum)Enum.ToObject(typeof(TEnum), Convert.ToInt32(value, CultureInfo.InvariantCulture));
    }

    private void SetNullable<T>(string keyword, T? value)
        where T : struct
    {
        if (value.HasValue)
            this[keyword] = value.Value;
        else
            Remove(keyword);
    }

    private static object? ConvertToStoredValue(string keyword, object value)
    {
        return keyword switch
        {
            "Mode" => ConvertOpenMode(value),
            "Cache" => ConvertCacheMode(value),
            "Foreign Keys" => ConvertToNullableBoolean(value),
            "Recursive Triggers" or "Pooling" or "BinaryGUID"
                or "Read Your Writes" or "Bootstrap If Empty" or "Partial Sync Prefetch"
                or "Force Logical MVCC Pull" => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
            "Default Timeout" or "Version" or "Sync Interval" or "Sync Long Poll Timeout"
                or "Partial Bootstrap Prefix" => Convert.ToInt32(value, CultureInfo.InvariantCulture),
            "Partial Sync Segment Size" or "Push Operations Threshold" or "Pull Bytes Threshold"
                => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            "Tls" => ConvertToNullableBoolean(value),
            "DateTimeKind" => ConvertDateTimeKind(value),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };
    }

    private static object? ConvertFromStoredValue(string keyword, object value)
    {
        return keyword switch
        {
            "Mode" => ConvertOpenMode(value),
            "Cache" => ConvertCacheMode(value),
            "Foreign Keys" => ConvertToNullableBoolean(value)!,
            "DateTimeKind" => ConvertDateTimeKind(value),
            _ => value,
        };
    }

    private object? GetValueOrDefault(string keyword)
    {
        return keyword switch
        {
            "Data Source" => string.Empty,
            "Mode" => SqliteOpenMode.ReadWriteCreate,
            "Cache" => SqliteCacheMode.Default,
            "Password" => string.Empty,
            "Foreign Keys" => null!,
            "Recursive Triggers" => false,
            "Default Timeout" => 30,
            "Pooling" => true,
            "Vfs" => null!,
            "Auth Token" => string.Empty,
            "Replica Path" => string.Empty,
            "Read Your Writes" => true,
            "Sync Interval" => 0,
            "Sync Client Name" => string.Empty,
            "Sync Long Poll Timeout" => 0,
            "Bootstrap If Empty" => true,
            "Partial Bootstrap Prefix" => 0,
            "Partial Bootstrap Query" => string.Empty,
            "Partial Sync Segment Size" => 0L,
            "Partial Sync Prefetch" => false,
            "Remote Encryption Cipher" => string.Empty,
            "Remote Encryption Key" => string.Empty,
            "Push Operations Threshold" => 0L,
            "Pull Bytes Threshold" => 0L,
            "Force Logical MVCC Pull" => false,
            "Sync Experimental Features" => string.Empty,
            "Tls" => null!,
            "DateTimeKind" => System.DateTimeKind.Unspecified,
            "DateTimeFormat" => string.Empty,
            "BinaryGUID" => true,
            "Version" => 3,
            _ => throw new ArgumentException(Properties.Resources.KeywordNotSupported(keyword)),
        };
    }

    private static TEnum ConvertEnum<TEnum>(object value)
        where TEnum : struct
    {
        if (value is TEnum typedValue)
            return typedValue;

        if (value is string stringValue)
            return Enum.Parse<TEnum>(stringValue, ignoreCase: true);

        if (value.GetType().IsEnum && value is not TEnum)
            throw new ArgumentException(Properties.Resources.ConvertFailed(value.GetType(), typeof(TEnum)));

        var enumValue = (TEnum)Enum.ToObject(typeof(TEnum), value);
        if (!Enum.IsDefined(typeof(TEnum), enumValue))
            throw new ArgumentOutOfRangeException(nameof(value), value, Properties.Resources.InvalidEnumValue(typeof(TEnum), enumValue));

        return enumValue;
    }

    private static bool? ConvertToNullableBoolean(object value)
        => value is null or string { Length: 0 }
            ? null
            : Convert.ToBoolean(value, CultureInfo.InvariantCulture);

    private static SqliteOpenMode ConvertOpenMode(object value)
    {
        var mode = ConvertEnum<SqliteOpenMode>(value);
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(value), value, Properties.Resources.InvalidEnumValue(typeof(SqliteOpenMode), mode));

        return mode;
    }

    private static SqliteCacheMode ConvertCacheMode(object value)
    {
        var mode = ConvertEnum<SqliteCacheMode>(value);
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(value), value, Properties.Resources.InvalidEnumValue(typeof(SqliteCacheMode), mode));

        return mode;
    }

    private static DateTimeKind ConvertDateTimeKind(object value)
    {
        var kind = ConvertEnum<DateTimeKind>(value);
        if (!Enum.IsDefined(kind))
            throw new ArgumentOutOfRangeException(nameof(value), value, Properties.Resources.InvalidEnumValue(typeof(DateTimeKind), kind));

        return kind;
    }
}
