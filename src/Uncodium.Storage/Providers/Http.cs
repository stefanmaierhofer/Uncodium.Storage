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

using System.IO;
using System.Net.Http;

namespace Uncodium.Storage;

public class HttpStorageProvider : IStorageProvider
{
    /// <inheritdoc/>
    public string Name => throw new NotImplementedException();

    /// <inheritdoc/>
    public Permissions Permissions => throw new NotImplementedException();

    /// <inheritdoc/>
    public Task DeleteStorageBucketAsync(string bucketName, Ct ct = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> GetStorageBucketAsync(string bucketName, string? path, Permissions mode, TimeSpan ttl, Ct ct = default)
    {
        throw new NotImplementedException();
    }
}

public class HttpStorageBucket : IStorageBucket
{
    private readonly HttpClient _client;
    private readonly BucketPath _prefix = BucketPath.Empty;
    private readonly string? _tail;

    private HttpStorageBucket(Permissions mode, HttpClient client, BucketPath prefix, string? tail)
    {
        Permissions = mode;
        _client = client;
        _prefix = prefix;
        _tail = tail;
    }

    public HttpStorageBucket(string url)
    {
        string baseAddress = url;

        var i = url.IndexOf("{0}");
        if (i >= 0)
        {
#if NETSTANDARD2_0
            baseAddress = url.Substring(0, i);
            _tail = url.Substring(i + 3);
#else
            baseAddress = url[..i];
            _tail = url[(i + 3)..];
#endif
        }
        else
        {
            i = url.IndexOf('?');
            if (i >= 0)
            {
#if NETSTANDARD2_0
                baseAddress = url.Substring(0, i);
                _tail = url.Substring(i);
#else
                baseAddress = url[..i];
                _tail = url[i..];
#endif
            }
        }

#if NETSTANDARD2_0
        if (!baseAddress.EndsWith("/")) { baseAddress += "/"; }
#else
        if (!baseAddress.EndsWith('/')) { baseAddress += "/"; }
#endif

        _client = new HttpClient() { BaseAddress = new Uri(baseAddress) };
    }

    #region IStorageBucket

    /// <inheritdoc/>
    public Permissions Permissions { get; private set; } = Permissions.Read | Permissions.List;

    /// <inheritdoc/>
    public ValueTask WriteAsync(BucketPath file, byte[] data, WriteMode writeMode, Ct ct = default)
    {
        throw new NotSupportedException($"Error f5fdccfe-fa92-4ef3-ba15-314995ee0563.");
    }

    /// <inheritdoc/>
    public Task<Stream> GetWriteStreamAsync(BucketPath file, WriteMode writeMode, Ct ct = default)
    {
        throw new NotSupportedException($"Error c20b78de-f094-4845-aab3-4edd080e2302.");
    }

    /// <inheritdoc/>
    public async Task<bool> ExistsAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error ae19be37-4ec7-424d-b292-fd79f1d313db.");

        var requestUri = (_prefix + file.ToRelative()).ToString();
        if (_tail != null) requestUri += _tail;

        var request = new HttpRequestMessage(HttpMethod.Head, requestUri);
        var response = await _client.SendAsync(request, cancellationToken: ct);
        return response.IsSuccessStatusCode;
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 3ba97b84-8d7f-4ebd-9d0e-a45bb4029595.");

        var requestUri = (_prefix + file.ToRelative()).ToString();
        if (_tail != null) requestUri += _tail;

        var response = await _client.GetAsync(requestUri, ct);
        response.EnsureSuccessStatusCode();

#if NETSTANDARD2_0
        return await response.Content.ReadAsByteArrayAsync();
#else
        return await response.Content.ReadAsByteArrayAsync(ct);
#endif
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadSliceAsync(BucketPath file, long offset, long size, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error f1916c78-cf0a-43fd-95d4-2af2b357d89e.");

        var requestUri = (_prefix + file.ToRelative()).ToString();
        if (_tail != null) requestUri += _tail;

        var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        request.Headers.Add("Range", $"bytes={offset}-{offset + size - 1}");
        var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken: ct);
#if NETSTANDARD2_0
        var buffer = await response.Content.ReadAsByteArrayAsync();
#else
        var buffer = await response.Content.ReadAsByteArrayAsync(ct);
#endif
        return buffer;
    }

    /// <inheritdoc/>
    public async Task<Stream> GetReadStreamAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 7f79c62e-170f-40f4-ad3f-5f5dcb466fc9.");

        var requestUri = file.ToString();
        if (_tail != null) requestUri += _tail;

#if NETSTANDARD2_0
        return await _client.GetStreamAsync(requestUri);
#else
        return await _client.GetStreamAsync(requestUri, ct);
#endif
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<FileSystemItem> ListAsync(BucketPath path, bool recursive, Ct ct = default)
    {
        throw new NotSupportedException($"Error 946977b4-06ff-4969-b49d-87fe43fa3e48.");
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(BucketPath path, bool recursive, Ct ct = default)
    {
        throw new NotSupportedException($"Error a9213613-fc5c-4c2c-9fbc-c1ea5c498f22.");
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> ChangeDirAsync(BucketPath directory, Ct ct = default)
    {
        directory = directory.ToRelative();
        var newPrefix = _prefix + directory.ToRelative();
        var result = new HttpStorageBucket(Permissions, _client, newPrefix, _tail);
        return Task.FromResult<IStorageBucket>(result);
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> WithPermissionsAsync(Permissions newPermissions)
    {
        if (newPermissions.HasFlag(Permissions.Read) && !Permissions.HasFlag(Permissions.Read)) throw new Exception(
            $"Can't add Read permission. Error f3dec98b-55d3-496f-9767-860e3a82d946."
            );

        if (newPermissions.HasFlag(Permissions.Write) && !Permissions.HasFlag(Permissions.Write)) throw new Exception(
            $"Can't add Write permission. Error 0fb98e31-84b7-4719-b765-e1222c77976e."
            );

        if (newPermissions.HasFlag(Permissions.Delete) && !Permissions.HasFlag(Permissions.Delete)) throw new Exception(
            $"Can't add Delete permission. Error a4225024-db1e-4ce4-a2dc-b1750176ec7f."
            );

        if (newPermissions.HasFlag(Permissions.List) && !Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Can't add List permission. Error 6cdc5e86-f024-480d-b07b-f2d1aca22eb3."
            );

        return Task.FromResult<IStorageBucket>(new HttpStorageBucket(newPermissions, _client, _prefix, _tail));
    }

    #endregion
}
