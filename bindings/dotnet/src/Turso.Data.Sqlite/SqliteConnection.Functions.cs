using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Turso.Raw.Public;

namespace Turso.Data.Sqlite;

public partial class SqliteConnection
{
    private static readonly TursoScalarFunctionCallback ScalarFunctionCallback = InvokeScalarFunction;
    private static readonly TursoContextDestructorCallback ContextDestructorCallback = NoopContextDestructor;
    private static readonly TursoValueDestructorCallback ValueDestructorCallback = DestroyFunctionValue;
    private readonly Dictionary<string, ScalarFunctionRegistration> _scalarFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<GCHandle> _nativeFunctionContexts = [];

    private void RegisterScalarFunction(string name, int argc, bool isDeterministic, Func<object?[], object?>? function)
    {
        ThrowIfDirectRemote("Custom scalar functions");
        ArgumentNullException.ThrowIfNull(name);
        if (function is null)
        {
            _scalarFunctions.Remove(name);
            if (HasNativeCallbackHandle)
            {
                using var syncOperation = _managedConnection?.EnterSyncOperation();
                TursoBindings.UnregisterFunction(DatabaseHandle, name);
            }
            return;
        }

        var registration = new ScalarFunctionRegistration(name, argc, isDeterministic, function);
        _scalarFunctions[name] = registration;
        if (HasNativeCallbackHandle)
        {
            using var syncOperation = _managedConnection?.EnterSyncOperation();
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
        }
    }

    private void RegisterScalarFunctions()
    {
        foreach (var registration in _scalarFunctions.Values)
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
    }

    private void FreeNativeFunctionContexts()
    {
        foreach (var handle in _nativeFunctionContexts)
        {
            if (handle.Target is AggregateFunctionRegistration aggregate)
                aggregate.FreeInvocations();
            if (handle.IsAllocated)
                handle.Free();
        }

        _nativeFunctionContexts.Clear();
    }

    private static object? InvokeTypedFunction<T1, TResult>(string name, Func<T1, TResult> function, object?[] args)
        => function(ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeTypedFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> function, object?[] args)
        => function(ConvertArgument<T1>(name, args[0], 0), ConvertArgument<T2>(name, args[1], 1));

    private static object? InvokeTypedFunction<TState, TResult>(TState state, Func<TState, TResult> function, object?[] args)
        => function(state);

    private static object? InvokeTypedFunction<TState, T1, TResult>(string name, TState state, Func<TState, T1, TResult> function, object?[] args)
        => function(state, ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeTypedFunction<TState, T1, T2, TResult>(string name, TState state, Func<TState, T1, T2, TResult> function, object?[] args)
        => function(state, ConvertArgument<T1>(name, args[0], 0), ConvertArgument<T2>(name, args[1], 1));

    private static T ConvertArgument<T>(string functionName, object? value, int ordinal)
    {
        if (value is null or DBNull)
        {
            if (!typeof(T).IsValueType || Nullable.GetUnderlyingType(typeof(T)) is not null)
                return default!;

            throw new InvalidOperationException(Properties.Resources.UDFCalledWithNull(functionName, ordinal));
        }

        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        if (targetType == typeof(object))
            return (T)value;
        if (targetType == typeof(byte[]) && value is byte[] bytes)
            return (T)(object)bytes;
        if (targetType == typeof(string))
            return (T)(object)Convert.ToString(value, CultureInfo.InvariantCulture)!;
        if (targetType == typeof(bool))
            return (T)(object)Convert.ToBoolean(value, CultureInfo.InvariantCulture);

        return (T)Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
    }

    private static TursoExtensionValue InvokeScalarFunction(IntPtr context, int argc, IntPtr argv, IntPtr contextDestructor, IntPtr valueDestructor)
    {
        try
        {
            var registration = (ScalarFunctionRegistration?)GCHandle.FromIntPtr(context).Target
                ?? throw new ObjectDisposedException(nameof(ScalarFunctionRegistration));
            var args = ReadArguments(argc, argv);
            return CreateResult(registration.Invoke(args));
        }
        catch (SqliteException ex)
        {
            return CreateError("__turso_sqlite_error__:" + ex.SqliteErrorCode.ToString(CultureInfo.InvariantCulture) + ":" + ex.Message);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static void NoopContextDestructor(IntPtr context)
    {
    }

    private static void DestroyFunctionValue(IntPtr result)
    {
        if (result == IntPtr.Zero)
            return;

        var value = Marshal.PtrToStructure<TursoExtensionValue>(result);
        FreeExtensionValue(value);
    }

    private static object?[] ReadArguments(int argc, IntPtr argv)
    {
        if (argc == 0)
            return [];

        var args = new object?[argc];
        var size = Marshal.SizeOf<TursoExtensionValue>();
        for (var i = 0; i < argc; i++)
        {
            var value = Marshal.PtrToStructure<TursoExtensionValue>(IntPtr.Add(argv, i * size));
            args[i] = value.ValueType switch
            {
                TursoExtensionValueType.Null => null,
                TursoExtensionValueType.Integer => value.Value.IntValue,
                TursoExtensionValueType.Float => value.Value.RealValue,
                TursoExtensionValueType.Text => ReadText(value.Value.TextValue),
                TursoExtensionValueType.Blob => ReadBlob(value.Value.BlobValue),
                _ => null
            };
        }

        return args;
    }

    private static string ReadText(IntPtr textValuePtr)
    {
        if (textValuePtr == IntPtr.Zero)
            return string.Empty;

        var textValue = Marshal.PtrToStructure<ExtensionTextValue>(textValuePtr);
        if (textValue.Text == IntPtr.Zero || textValue.Length == 0)
            return string.Empty;

        var bytes = new byte[textValue.Length];
        Marshal.Copy(textValue.Text, bytes, 0, bytes.Length);
        return Encoding.UTF8.GetString(bytes);
    }

    private static byte[] ReadBlob(IntPtr blobValuePtr)
    {
        if (blobValuePtr == IntPtr.Zero)
            return [];

        var blobValue = Marshal.PtrToStructure<ExtensionBlobValue>(blobValuePtr);
        if (blobValue.Data == IntPtr.Zero || blobValue.Length == 0)
            return [];

        var bytes = new byte[checked((int)blobValue.Length)];
        Marshal.Copy(blobValue.Data, bytes, 0, bytes.Length);
        return bytes;
    }

    private static TursoExtensionValue CreateResult(object? value)
    {
        if (value is null or DBNull)
            return new TursoExtensionValue { ValueType = TursoExtensionValueType.Null };

        return value switch
        {
            bool boolValue => CreateInteger(boolValue ? 1 : 0),
            byte byteValue => CreateInteger(byteValue),
            sbyte sbyteValue => CreateInteger(sbyteValue),
            short shortValue => CreateInteger(shortValue),
            ushort ushortValue => CreateInteger(ushortValue),
            int intValue => CreateInteger(intValue),
            uint uintValue => CreateInteger(uintValue),
            long longValue => CreateInteger(longValue),
            float floatValue => CreateReal(floatValue),
            double doubleValue => CreateReal(doubleValue),
            decimal decimalValue => CreateText(decimalValue.ToString(CultureInfo.InvariantCulture)),
            byte[] bytes => CreateBlob(bytes),
            _ => CreateText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
        };
    }

    private static TursoExtensionValue CreateInteger(long value)
        => new() { ValueType = TursoExtensionValueType.Integer, Value = new TursoExtensionValueUnion { IntValue = value } };

    private static TursoExtensionValue CreateReal(double value)
        => new() { ValueType = TursoExtensionValueType.Float, Value = new TursoExtensionValueUnion { RealValue = value } };

    private static TursoExtensionValue CreateText(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var text = new ExtensionTextValue { Subtype = 0, Text = AllocBytes(bytes), Length = checked((uint)bytes.Length) };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionTextValue>());
        Marshal.StructureToPtr(text, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Text, Value = new TursoExtensionValueUnion { TextValue = ptr } };
    }

    private static TursoExtensionValue CreateBlob(byte[] bytes)
    {
        var blob = new ExtensionBlobValue { Data = AllocBytes(bytes), Length = (ulong)bytes.Length };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionBlobValue>());
        Marshal.StructureToPtr(blob, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Blob, Value = new TursoExtensionValueUnion { BlobValue = ptr } };
    }

    private static TursoExtensionValue CreateError(string message)
    {
        var text = CreateText(message);
        var error = new ExtensionErrorValue { Code = 14, Message = text.Value.TextValue };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionErrorValue>());
        Marshal.StructureToPtr(error, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Error, Value = new TursoExtensionValueUnion { ErrorValue = ptr } };
    }

    private static IntPtr AllocBytes(byte[] bytes)
    {
        if (bytes.Length == 0)
            return IntPtr.Zero;

        var data = Marshal.AllocHGlobal(bytes.Length);
        Marshal.Copy(bytes, 0, data, bytes.Length);
        return data;
    }

    private static void FreeExtensionValue(TursoExtensionValue value)
    {
        switch (value.ValueType)
        {
            case TursoExtensionValueType.Text:
                FreeText(value.Value.TextValue);
                break;
            case TursoExtensionValueType.Blob:
                FreeBlob(value.Value.BlobValue);
                break;
            case TursoExtensionValueType.Error:
                FreeError(value.Value.ErrorValue);
                break;
        }
    }

    private static void FreeText(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
            return;

        var text = Marshal.PtrToStructure<ExtensionTextValue>(ptr);
        if (text.Text != IntPtr.Zero)
            Marshal.FreeHGlobal(text.Text);
        Marshal.FreeHGlobal(ptr);
    }

    private static void FreeBlob(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
            return;

        var blob = Marshal.PtrToStructure<ExtensionBlobValue>(ptr);
        if (blob.Data != IntPtr.Zero)
            Marshal.FreeHGlobal(blob.Data);
        Marshal.FreeHGlobal(ptr);
    }

    private static void FreeError(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
            return;

        var error = Marshal.PtrToStructure<ExtensionErrorValue>(ptr);
        FreeText(error.Message);
        Marshal.FreeHGlobal(ptr);
    }

    private sealed class ScalarFunctionRegistration(string name, int argc, bool isDeterministic, Func<object?[], object?> invoke)
    {
        public object? Invoke(object?[] args) => invoke(args);

        public GCHandle Register(Turso.Raw.Public.Handles.TursoDatabaseHandle database)
        {
            var handle = GCHandle.Alloc(this);
            try
            {
                TursoBindings.RegisterScalarFunction(
                    database,
                    name,
                    argc,
                    isDeterministic,
                    GCHandle.ToIntPtr(handle),
                    ScalarFunctionCallback,
                    ContextDestructorCallback,
                    ValueDestructorCallback);
                return handle;
            }
            catch
            {
                handle.Free();
                throw;
            }
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionTextValue
    {
        public int Subtype;
        public IntPtr Text;
        public uint Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionBlobValue
    {
        public IntPtr Data;
        public ulong Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionErrorValue
    {
        public int Code;
        public IntPtr Message;
    }
}
