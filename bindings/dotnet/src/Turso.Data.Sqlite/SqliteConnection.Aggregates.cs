using System.Runtime.InteropServices;
using Turso.Raw.Public;

namespace Turso.Data.Sqlite;

public partial class SqliteConnection
{
    private static readonly TursoAggregateInitCallback AggregateInitCallback = InitializeAggregate;
    private static readonly TursoAggregateStepCallback AggregateStepCallback = StepAggregate;
    private static readonly TursoAggregateFinalCallback AggregateFinalCallback = FinalizeAggregate;
    private static readonly TursoContextDestructorCallback AggregateDestructorCallback = DestroyAggregate;
    private readonly Dictionary<string, AggregateFunctionRegistration> _aggregateFunctions = new(StringComparer.OrdinalIgnoreCase);

    private void RegisterAggregateFunction(string name, int argc, bool isDeterministic, object? seed, Func<object?, object?[], object?>? step, Func<object?, object?> resultSelector)
    {
        ThrowIfDirectRemote("Custom aggregate functions");
        ArgumentNullException.ThrowIfNull(name);
        if (step is null)
        {
            _aggregateFunctions.Remove(name);
            if (HasNativeCallbackHandle)
            {
                using var syncOperation = _managedConnection?.EnterSyncOperation();
                TursoBindings.UnregisterFunction(DatabaseHandle, name);
            }
            return;
        }

        var registration = new AggregateFunctionRegistration(name, argc, isDeterministic, seed, step, resultSelector);
        _aggregateFunctions[name] = registration;
        if (HasNativeCallbackHandle)
        {
            using var syncOperation = _managedConnection?.EnterSyncOperation();
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
        }
    }

    private void RegisterAggregateFunctions()
    {
        foreach (var registration in _aggregateFunctions.Values)
            _nativeFunctionContexts.Add(registration.Register(DatabaseHandle));
    }

    private static object? InvokeNullableAggregateStep<TAccumulate>(Func<TAccumulate?, TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate?)accumulator);

    private static object? InvokeNullableAggregateStep<T1, TAccumulate>(string name, Func<TAccumulate?, T1, TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate?)accumulator, ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeNullableAggregateStep<TAccumulate>(Func<TAccumulate?, object?[], TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate?)accumulator, args);

    private static object? InvokeSeededAggregateStep<TAccumulate>(Func<TAccumulate, TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate)accumulator!);

    private static object? InvokeSeededAggregateStep<T1, TAccumulate>(string name, Func<TAccumulate, T1, TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate)accumulator!, ConvertArgument<T1>(name, args[0], 0));

    private static object? InvokeSeededAggregateStep<T1, T2, TAccumulate>(string name, Func<TAccumulate, T1, T2, TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate)accumulator!, ConvertArgument<T1>(name, args[0], 0), ConvertArgument<T2>(name, args[1], 1));

    private static object? InvokeSeededAggregateStep<TAccumulate>(Func<TAccumulate, object?[], TAccumulate> function, object? accumulator, object?[] args)
        => function((TAccumulate)accumulator!, args);

    private static object? InvokeResultSelector<TAccumulate, TResult>(Func<TAccumulate, TResult> resultSelector, object? accumulator)
        => resultSelector((TAccumulate)accumulator!);

    private static IntPtr InitializeAggregate(IntPtr context)
    {
        var registration = (AggregateFunctionRegistration?)GCHandle.FromIntPtr(context).Target
            ?? throw new ObjectDisposedException(nameof(AggregateFunctionRegistration));
        return registration.CreateInvocationHandle();
    }

    private static TursoExtensionValue StepAggregate(IntPtr context, IntPtr aggregateContext, int argc, IntPtr argv)
    {
        try
        {
            var invocation = (AggregateInvocation?)GCHandle.FromIntPtr(aggregateContext).Target
                ?? throw new ObjectDisposedException(nameof(AggregateInvocation));
            invocation.Step(ReadArguments(argc, argv));
            return CreateResult(null);
        }
        catch (SqliteException ex)
        {
            return CreateError("__turso_sqlite_error__:" + ex.SqliteErrorCode.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + ex.Message);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static TursoExtensionValue FinalizeAggregate(IntPtr context, IntPtr aggregateContext)
    {
        try
        {
            var invocation = (AggregateInvocation?)GCHandle.FromIntPtr(aggregateContext).Target
                ?? throw new ObjectDisposedException(nameof(AggregateInvocation));
            return CreateResult(invocation.FinalizeResult());
        }
        catch (SqliteException ex)
        {
            return CreateError("__turso_sqlite_error__:" + ex.SqliteErrorCode.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + ex.Message);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static void DestroyAggregate(IntPtr aggregateContext)
    {
        if (aggregateContext == IntPtr.Zero)
            return;

        var handle = GCHandle.FromIntPtr(aggregateContext);
        if (handle.Target is AggregateInvocation invocation)
            invocation.Registration.FreeInvocation(handle);
        else if (handle.IsAllocated)
            handle.Free();
    }

    private sealed class AggregateFunctionRegistration(
        string name,
        int argc,
        bool isDeterministic,
        object? seed,
        Func<object?, object?[], object?> step,
        Func<object?, object?> resultSelector)
    {
        private readonly List<GCHandle> _invocations = [];

        public IntPtr CreateInvocationHandle()
        {
            var handle = GCHandle.Alloc(new AggregateInvocation(this, seed, step, resultSelector));
            lock (_invocations)
            {
                _invocations.Add(handle);
            }

            return GCHandle.ToIntPtr(handle);
        }

        public void FreeInvocation(GCHandle handle)
        {
            lock (_invocations)
            {
                _invocations.Remove(handle);
            }

            if (handle.IsAllocated)
                handle.Free();
        }

        public void FreeInvocations()
        {
            lock (_invocations)
            {
                foreach (var handle in _invocations)
                {
                    if (handle.IsAllocated)
                        handle.Free();
                }

                _invocations.Clear();
            }
        }

        public GCHandle Register(Turso.Raw.Public.Handles.TursoDatabaseHandle database)
        {
            var handle = GCHandle.Alloc(this);
            try
            {
                TursoBindings.RegisterAggregateFunction(
                    database,
                    name,
                    argc,
                    isDeterministic,
                    GCHandle.ToIntPtr(handle),
                    AggregateInitCallback,
                    AggregateStepCallback,
                    AggregateFinalCallback,
                    ContextDestructorCallback,
                    AggregateDestructorCallback,
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

    private sealed class AggregateInvocation(AggregateFunctionRegistration registration, object? seed, Func<object?, object?[], object?> step, Func<object?, object?> resultSelector)
    {
        private object? _accumulator = seed;

        public AggregateFunctionRegistration Registration { get; } = registration;

        public void Step(object?[] args)
        {
            _accumulator = step(_accumulator, args);
        }

        public object? FinalizeResult()
            => resultSelector(_accumulator);
    }
}
