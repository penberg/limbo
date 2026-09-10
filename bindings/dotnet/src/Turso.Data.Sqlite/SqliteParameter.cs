using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Turso.Raw.Public.Value;

namespace Turso.Data.Sqlite;

public class SqliteParameter : DbParameter
{
    private string _parameterName = string.Empty;
    private string _sourceColumn = string.Empty;
    private object? _value;
    private int? _size;
    private bool _hasValue;
    private DbType _dbType = DbType.String;
    private SqliteType? _sqliteType;

    public SqliteParameter()
    {
    }

    public SqliteParameter(string? name, object? value)
    {
        ParameterName = name;
        Value = value;
    }

    public SqliteParameter(string? name, SqliteType type)
    {
        ParameterName = name;
        SqliteType = type;
    }

    public SqliteParameter(string? name, SqliteType type, int size)
        : this(name, type)
    {
        Size = size;
    }

    public SqliteParameter(string? name, SqliteType type, int size, string? sourceColumn)
        : this(name, type, size)
    {
        SourceColumn = sourceColumn;
    }

    public override DbType DbType
    {
        get => _dbType;
        set
        {
            _dbType = value;
            _sqliteType = value switch
            {
                DbType.Binary => SqliteType.Blob,
                DbType.Byte or DbType.Boolean or DbType.Int16 or DbType.Int32 or DbType.Int64 or DbType.SByte
                    or DbType.UInt16 or DbType.UInt32 or DbType.UInt64 => SqliteType.Integer,
                DbType.Double or DbType.Single => SqliteType.Real,
                _ => SqliteType.Text,
            };
        }
    }

    public SqliteType SqliteType
    {
        get => _sqliteType ?? InferSqliteType(Value);
        set
        {
            _sqliteType = value;
            _dbType = value switch
            {
                SqliteType.Integer => DbType.Int64,
                SqliteType.Real => DbType.Double,
                SqliteType.Blob => DbType.Binary,
                _ => DbType.String,
            };
        }
    }

    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
                throw new ArgumentException(Properties.Resources.InvalidParameterDirection(value));
        }
    }

    public override bool IsNullable { get; set; }

    [AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }

    [AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    public override object? Value
    {
        get => _value;
        set
        {
            _value = value;
            _hasValue = true;
        }
    }

    public override bool SourceColumnNullMapping { get; set; }

    public override int Size
    {
        get => _size
            ?? (Value is string stringValue
                ? stringValue.Length
                : Value is byte[] bytes
                    ? bytes.Length
                    : 0);
        set
        {
            if (value < -1)
                throw new ArgumentOutOfRangeException(nameof(value), value, message: null);

            _size = value;
        }
    }

    public override void ResetDbType()
        => ResetSqliteType();

    public virtual void ResetSqliteType()
    {
        _sqliteType = null;
        _dbType = DbType.String;
    }

    internal bool HasValue => _hasValue;

    internal TursoValue ToTursoValue()
    {
        if (Value is null || Value == DBNull.Value)
            return TursoValue.Null();

        return SqliteType switch
        {
            SqliteType.Integer => TursoValue.Int(ToInt64(Value)),
            SqliteType.Real => TursoValue.Real(ToDouble(Value)),
            SqliteType.Blob => TursoValue.Blob(ToBytes(Value)),
            SqliteType.Text => TursoValue.String(ApplySize(ToInvariantString(Value))),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    internal global::Turso.TursoParameter ToManagedParameter(string parameterName)
    {
        var value = ToTursoValue();
        return new global::Turso.TursoParameter(parameterName, value.ValueType switch
        {
            TursoValueType.Null or TursoValueType.Empty => DBNull.Value,
            TursoValueType.Integer => value.IntValue,
            TursoValueType.Real => value.RealValue,
            TursoValueType.Text => value.StringValue,
            TursoValueType.Blob => value.BlobValue,
            _ => throw new ArgumentOutOfRangeException()
        })
        {
            IsNullable = IsNullable,
            SourceColumn = SourceColumn,
            SourceColumnNullMapping = SourceColumnNullMapping,
        };
    }

    private static SqliteType InferSqliteType(object? value)
    {
        return value switch
        {
            null or DBNull => SqliteType.Text,
            byte[] or Memory<byte> or ReadOnlyMemory<byte> => SqliteType.Blob,
            Enum => SqliteType.Integer,
            float or double => SqliteType.Real,
            bool or byte or sbyte or short or ushort or int or uint or long or ulong => SqliteType.Integer,
            _ => SqliteType.Text,
        };
    }

    private string ToInvariantString(object value)
    {
        return value switch
        {
            object when value.GetType() == typeof(object) => throw new InvalidOperationException(Properties.Resources.UnknownDataType(value.GetType())),
            DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
            DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", CultureInfo.InvariantCulture),
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimeOnly timeOnly => timeOnly.Ticks % TimeSpan.TicksPerSecond == 0
                ? timeOnly.ToString("HH:mm:ss", CultureInfo.InvariantCulture)
                : timeOnly.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
            TimeSpan timeSpan => timeSpan.ToString("c", CultureInfo.InvariantCulture),
            decimal decimalValue => decimal.Truncate(decimalValue) == decimalValue
                ? decimalValue.ToString("0.0", CultureInfo.InvariantCulture)
                : decimalValue.ToString(CultureInfo.InvariantCulture),
            Guid guid => guid.ToString("D", CultureInfo.InvariantCulture).ToUpperInvariant(),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };
    }

    private byte[] ToBytes(object value)
    {
        return value switch
        {
            byte[] bytes => ApplySize(bytes),
            Memory<byte> memory => ApplySize(memory.ToArray()),
            ReadOnlyMemory<byte> memory => ApplySize(memory.ToArray()),
            Guid guid => guid.ToByteArray(),
            _ => throw new InvalidOperationException(Properties.Resources.UnknownDataType(value.GetType())),
        };
    }

    private byte[] ApplySize(byte[] bytes)
    {
        return _size is > -1 && _size < bytes.Length
            ? bytes[.._size.Value]
            : bytes;
    }

    private string ApplySize(string value)
    {
        return _size is > -1 && _size < value.Length
            ? value[.._size.Value]
            : value;
    }

    private static long ToInt64(object value)
    {
        return value is Enum enumValue
            ? Convert.ToInt64(enumValue, CultureInfo.InvariantCulture)
            : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static double ToDouble(object value)
    {
        var result = value switch
        {
            DateTime dateTime => dateTime.ToOADate() + 2415018.5,
            DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime.ToOADate() + 2415018.5,
            DateOnly dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue).ToOADate() + 2415018.5,
            TimeOnly timeOnly => timeOnly.ToTimeSpan().TotalDays,
            TimeSpan timeSpan => timeSpan.TotalDays,
            _ => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        };

        if (double.IsNaN(result))
            throw new InvalidOperationException(Properties.Resources.CannotStoreNaN);

        return result;
    }
}
