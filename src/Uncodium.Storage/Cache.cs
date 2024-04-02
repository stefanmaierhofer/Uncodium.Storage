/*
    Copyright (C) 2014-2024. Stefan Maierhofer. http://github.com/stefanmaierhofer.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

using System.Linq;

namespace Uncodium.Storage;

public sealed class Cache<K, T> : IDisposable, IAsyncDisposable
    where K : notnull
{
    private readonly string _displayName;
    private readonly Dictionary<K, T> _cache = [];
    private readonly HashSet<K> _dirty = [];
    private readonly bool _asyncWriteBack;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _lock = new();
    private readonly int _writeBackTaskWaitMilliSeconds = 5000;

    private readonly Func<K, Ct, ValueTask<T?>> _getAsync;
    private readonly Func<K, T, Ct, ValueTask> _setAsync;
    private readonly Func<K, Ct, ValueTask> _deleteAsync;
    private readonly Func<T, T, bool> _equals;

    public Cache(
        string displayName,
        bool asyncWriteBack,
        Func<K, Ct, ValueTask<T?>> getAsync,
        Func<K, T, Ct, ValueTask> setAsync,
        Func<K, Ct, ValueTask> deleteAsync,
        Func<T, T, bool> equals
        )
    {
        _displayName = displayName;
        _asyncWriteBack = asyncWriteBack;
        _getAsync = getAsync;
        _setAsync = setAsync;
        _deleteAsync = deleteAsync;
        _equals = equals;

        if (asyncWriteBack)
        {
            Task.Run(WriteBackTask);
        }
    }

    public async Task FlushAsync(Ct ct = default)
    {
        while (_dirty.Count > 0)
        {
            K key;
            T item;
            lock (_lock)
            {
                if (_dirty.Count == 0) return;
                key = _dirty.First();
                _dirty.Remove(key);
                item = _cache[key];
            }

            await _setAsync(key, item, ct); // write to backing location
        }
    }

    private async Task WriteBackTask()
    {
#if DEBUG
        Console.WriteLine($"[{DateTimeOffset.UtcNow}][Cache][{_displayName}][WriteBackTask] started");
#endif

        var ct = _cts.Token;

        try
        {
            K key = default!;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    bool wait = false;
                    lock (_lock)
                    {
                        if (_dirty.Count > 0)
                        {
                            key = _dirty.First();
                        }
                        else
                        {
                            wait = true;
                        }
                    }

                    if (wait)
                    {
#if DEBUG
                        //Console.WriteLine($"[{DateTimeOffset.UtcNow}][Cache][{_displayName}][WriteBackTask] waiting {_writeBackTaskWaitMilliSeconds} ms");
#endif

                        await Task.Delay(_writeBackTaskWaitMilliSeconds, ct);
                    }
                    else
                    {
                        T item;
                        lock (_lock)
                        {
                            item = _cache[key];
                            _dirty.Remove(key);
                        }

                        await _setAsync(key, item, ct); // write to backing location

#if DEBUG
                        Console.WriteLine($"[{DateTimeOffset.UtcNow}][Cache][{_displayName}][WriteBackTask] wrote item to backing location (key={key}).");
#endif
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"[{DateTimeOffset.UtcNow}][ERROR][Cache][{_displayName}][WriteBackTask] Failed to process next item. " +
                        $"Error cb63262e-1d54-42ea-8ddf-87eb7f0ca428.\n" +
                        $"{e} "
                        );
                }
            }

            await FlushAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(
                $"[{DateTimeOffset.UtcNow}][ERROR][Cache][{_displayName}][WriteBackTask] WriteBackTask failed completely. " +
                $"Error ff8492ec-8d79-4d05-b840-9e43a9c218b3.\n" +
                $"{e} "
                );
        }
    }

    public async Task<T?> GetAsync(K key, Ct ct = default)
    {
        _cts.Token.ThrowIfCancellationRequested();

        // cache hit?
        lock (_lock)
        {
            if (_cache.TryGetValue(key, out var item))
            {
                return item;
            }
        }

        // cache miss!
        {
            var item = await _getAsync(key, ct);
            if (item != null)
            {
                lock (_lock) _cache.Add(key, item);
            }
            return item;
        }
    }

    public async Task SetAsync(K key, T value, Ct ct = default)
    {
        _cts.Token.ThrowIfCancellationRequested();

        T? old;
        bool exists;
        lock (_lock) exists = _cache.TryGetValue(key, out old);

        if (exists && _equals(old!, value))
        {
            // identical item already in cache -> do nothing
            return;
        }

        // update cache
        lock (_lock) _cache[key] = value;

        // write item to backing destination (TODO: lazy/async writeback with user-specified maximum rate)
        if (_asyncWriteBack)
        {
            // mark dirty -> the asynchronous background task will pick this up and write item to the backing location
            lock (_lock) _dirty.Add(key);
        }
        else
        {
            // immediately (synchronously) write item to backing location
            await _setAsync(key, value, ct);
        }
    }

    public async Task DeleteAsync(K key, Ct ct)
    {
        _cts.Token.ThrowIfCancellationRequested();

        // remove from cache
        lock (_lock) _cache.Remove(key);

        // remove from backing destination
        await _deleteAsync(key, ct);
    }

    public IReadOnlyList<T> Values
    {
        get
        {
            lock (_lock) return [.. _cache.Values];
        }
    }

    public void Dispose()
    {
        if (_cts.IsCancellationRequested) return;
        _cts.Cancel();
    }

    public ValueTask DisposeAsync()
    {
        if (_cts.IsCancellationRequested) return default;
        _cts.Cancel();
        return default;
    }
}
