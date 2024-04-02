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

using System.Runtime.CompilerServices;

namespace Uncodium.Storage;

public static class RecordStore
{
    private record Config(int Version, DateTimeOffset Created, string Type)
    {
        public const int CURRENT_VERSION = 1;
    }

    public static async Task<RecordStore<T>> CreateAsync<T>(
        IStorageBucket bucket,
        BucketPath prefix,
        Func<T, BucketPath> key,
        Func<T, T>? migrate,
        Func<T, T, bool> equals,
        bool asyncWriteBack = true,
        Ct ct = default
        )
        where T : notnull
    {
        var store = new RecordStore<T>(bucket, prefix, key, migrate, equals, asyncWriteBack);

        var config = await bucket.TryReadJsonAsync<Config>(prefix + "config.json", ct: ct);
        if (config == null)
        {
            await bucket.WriteJsonAsync(
                file: prefix + "config.json",
                data: new Config(
                    Version: Config.CURRENT_VERSION,
                    Created: DateTimeOffset.UtcNow,
                    Type: typeof(T).FullName ?? typeof(T).Name
                    ),
                writeMode: WriteMode.CreateOrFail,
                ct: ct
                );
        }
        else if (config.Version != Config.CURRENT_VERSION)
        {
            throw new Exception($"Internal error 647fa8fd-c82d-4c57-8f29-0d0593c0a2c7.");
        }

        return store;
    }
}

public class RecordStore<T> : IDisposable, IAsyncDisposable
    where T : notnull
{
    private readonly BucketPath _prefix;
    private readonly IStorageBucket _bucket;

    private readonly Func<T, BucketPath> _key;
    private readonly Func<T, T>? _migrate;
    private readonly Func<T, T, bool> _equals;
    private readonly Cache<BucketPath, T> _cache;

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _cache.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return _cache.DisposeAsync();
    }

    internal RecordStore(
        IStorageBucket bucket,
        BucketPath prefix,
        Func<T, BucketPath> key, Func<T, T>? migrate,
        Func<T, T, bool> equals,
        bool asyncWriteBack = true
        )
    {
        _bucket = bucket;
        _prefix = prefix.ToAbsolute();
        _key = key;
        _migrate = migrate;
        _equals = equals;
        _cache = new(
            displayName: typeof(T).FullName ?? typeof(T).Name,
            asyncWriteBack: asyncWriteBack,
            getAsync: _bucket.TryReadJsonAsync<T>,
            setAsync: (k, v, ct) => _bucket.WriteJsonAsync(k, v!, writeMode: WriteMode.CreateOrReplace, ct),
            deleteAsync: (path, ct) => _bucket.DeleteAsync(path, recursive: true, ct: ct),
            equals: _equals
            );
    }

    /// <summary>
    /// Load record from disk.
    /// Returns null if record does not exist.
    /// </summary>
    public async Task<T?> TryLoadAsync(BucketPath id, Ct ct = default)
    {
        var key = BuildKey(id);

        var item = await _cache.GetAsync(key, ct);

        if (_migrate != null && item != null)
        {
            var itemMigrated = _migrate(item);
            if (_equals(item, itemMigrated))
            {
                return item;
            }
            else
            {
                await SaveAsync(itemMigrated, ct);
                return itemMigrated;
            }
        }

        return item;
    }

    /// <summary>
    /// Load record from disk.
    /// Returns null if record does not exist.
    /// </summary>
    public Task<T?> TryLoadAsync(string id, Ct ct = default)
        => TryLoadAsync(new BucketPath(id), ct);

    /// <summary>
    /// Save record to disk.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task<T> SaveAsync(T x, Ct ct = default)
    {
        var key = BuildKey(_key(x));
        await _cache.SetAsync(key, x, ct);
        return x;
    }

    /// <summary>
    /// Delete record on disk.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task DeleteAsync(BucketPath id, Ct ct = default)
    {
        var key = BuildKey(id);
        await _cache.DeleteAsync(key, ct);
    }

    /// <summary>
    /// Delete record on disk.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task DeleteAsync(string id, Ct ct = default)
        => DeleteAsync(new BucketPath(id), ct);

    public async IAsyncEnumerable<T> ListAsync([EnumeratorCancellation] Ct ct = default)
    {
        await _cache.FlushAsync(ct);

        var filePaths = _bucket.ListAsync(_prefix, recursive: true, ct);
        var excludeConfigName = _prefix + "config.json";
        await foreach (var filePath in filePaths)
        {
            if (filePath.IsDirectory || filePath.Path == excludeConfigName) continue;
            var item = await _cache.GetAsync(filePath.Path, ct) ?? throw new Exception(
                $"Failed to load {filePath.ToJsonString()}. " +
                $"Error 0c2a3af0-37f5-431b-8edc-b875ff642c8c."
                );
            yield return item;
        }
    }

    /// <summary>
    /// Ensures that all cached items (AT THE MOMENT THIS FUNCTION IS CALLED) are guaranteed to be written to backing storage.
    /// </summary>
    public Task FlushAsync(Ct ct = default) => _cache.FlushAsync(ct);

    #region Helpers

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal BucketPath BuildKey(BucketPath path)
    {
        if (path.IsAbsolute) throw new Exception($"Expected relative path. Error 689a1e91-8f4c-44f5-a4ef-7d911d7267e8.");
        if (path.Segments.Length > 0 && path.Segments[0].Length > 2)
        {
            var p = path.Segments[0];
            return _prefix + $"{p[0]}{p[1]}/{path}.json";
        }
        else
        {
            return _prefix + "_/" + path + ".json";
        }
    }

    #endregion
}

