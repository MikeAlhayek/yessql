using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

#nullable enable

namespace YesSql.Data;

/// <summary>
/// Ensures that concurrent calls for the same key share the same work.
/// </summary>
/// <remarks>
/// The pending work is stored as a non-generic <see cref="Task"/> so that each call can use its own result type, without
/// wrapping the work in an additional async method to convert (and box) its result to a common type.
/// </remarks>
internal sealed class WorkDispatcher<TKey> where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, Task> _workers = new();

    public async Task<TValue> ScheduleAsync<TValue, TState>(TKey key, TState state, Func<TKey, TState, Task<TValue>> valueFactory)
    {
        ArgumentNullException.ThrowIfNull(key);

        while (true)
        {
            if (_workers.TryGetValue(key, out var task))
            {
                // The same key is expected to always produce the same type of result. If it doesn't,
                // the work is not shared instead of failing.
                if (task is Task<TValue> typedTask)
                {
                    return await typedTask;
                }

                return await valueFactory(key, state);
            }

            // This is the task that we'll return to all waiters. We'll complete it when the factory is complete
            var tcs = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (_workers.TryAdd(key, tcs.Task))
            {
                try
                {
                    var value = await valueFactory(key, state);
                    tcs.TrySetResult(value);
                    return value;
                }
                catch (Exception ex)
                {
                    // Make sure all waiters see the exception
                    tcs.SetException(ex);

                    throw;
                }
                finally
                {
                    // We remove the entry if the factory failed so it's not a permanent failure
                    // and future gets can retry (this could be a pluggable policy)
                    _workers.TryRemove(key, out _);
                }
            }
        }
    }
}
