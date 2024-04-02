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

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Uncodium.Storage;

/// <summary>
/// https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#permissions-for-a-directory-container-or-blob
/// </summary>
[Flags]
public enum AzureSasPermissions
{
    None                    = 0,

    Read                    = 1 << 0,
    Add                     = 1 << 1,
    Create                  = 1 << 2,
    Write                   = 1 << 3,
    Delete                  = 1 << 4,
    DeleteVersion           = 1 << 5,
    PermantDelete           = 1 << 6,
    List                    = 1 << 7,
    Tags                    = 1 << 8,
    Find                    = 1 << 9,
    Move                    = 1 << 10,
    Execute                 = 1 << 11,
    Ownership               = 1 << 12,
    Permissions             = 1 << 13,
    SetImmutabilityPolicy   = 1 << 14,
}

public class AzureStorageProvider : IStorageProvider
{
    private readonly Config _config;

    public record Config(string Name, string ConnectionString, Permissions Permissions = Permissions.All);

    public AzureStorageProvider(Config config)
    {
        var csPerms = AzureSasUtils.ParseSasPermissionsFromConnectionString(config.ConnectionString).ToUncodiumPermissions();
        _config = config with { Permissions = config.Permissions & csPerms };
    }

    #region IStorageProvider

    /// <inheritdoc/>
    public string Name => _config.Name;

    /// <inheritdoc/>
    public Permissions Permissions => _config.Permissions;

    /// <inheritdoc/>
    public async Task DeleteStorageBucketAsync(string bucketName, Ct ct = default)
    {
        var blobContainerClient = GetBlobContainerClient(bucketName);
        await blobContainerClient.DeleteAsync(cancellationToken: ct);
    }

    /// <inheritdoc/>
    public async Task<IStorageBucket> GetStorageBucketAsync(string bucketName, string? path, Permissions mode, TimeSpan ttl, Ct ct = default)
    {
#if NETSTANDARD2_0
        var ts = bucketName.Replace('\\', '/').Split(['/'], StringSplitOptions.RemoveEmptyEntries);
#else
        var ts = bucketName.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
#endif
        if (ts.Length > 1)
        {
            bucketName = ts[0];
#if NETSTANDARD2_0
            var rest = string.Join("/", ts.Skip(1));
#else
            var rest = string.Join('/', ts[1..]);
#endif
            if (string.IsNullOrWhiteSpace(path))
            {
                path = rest;
            }
            else
            {
                path = System.IO.Path.Combine(rest, path).Replace('\\', '/');
            }
        }

        var blobContainerClient = GetBlobContainerClient(bucketName);

        await blobContainerClient.CreateIfNotExistsAsync(Azure.Storage.Blobs.Models.PublicAccessType.None, cancellationToken: ct);

        var azurePerms = _config.Permissions.ToAzureBlobContainerSasPermissions();

        var sasUrl = AzureSasUtils.GetServiceSasUriForContainer(
            blobContainerClient,
            azurePerms,
            DateTimeOffset.UtcNow + ttl
            );

        return new AzureStorageBucket(sasUrl, prefix: path);
    }

#endregion

    #region Internal

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private BlobContainerClient GetBlobContainerClient(string bucketName)
    {
        return new BlobContainerClient(_config.ConnectionString, bucketName);
    }

    #endregion
}

public class AzureStorageBucket : IStorageBucket
{
    private readonly Uri _sas;
    private readonly string? _prefix;
    private readonly BlobContainerClient _client;

    private AzureStorageBucket(Uri sas, string? prefix, BlobContainerClient client, Permissions permissions)
    {
        _sas = sas;
        _prefix = prefix;
        _client = client;
        Permissions = permissions;
    }

    public AzureStorageBucket(Uri sas, string? prefix)
        : this(sas, prefix != null ? new BucketPath(prefix) : null)
    {
    }

    public AzureStorageBucket(string sasUrl, string? prefix)
        : this(new Uri(sasUrl), prefix != null ? new BucketPath(prefix) : null)
    {
    }

    public AzureStorageBucket(Uri sas, BucketPath? prefix)
    {
        _sas = sas;
        _prefix = prefix?.ToRelative().ToString();
        _client = new BlobContainerClient(_sas);

        Permissions = AzureSasUtils.ParseSasPermissions(_sas).ToUncodiumPermissions();
    }

    public AzureStorageBucket(Uri sas) : this(sas, prefix: default(BucketPath)) { }

    public AzureStorageBucket(string sasUrl) : this(sasUrl, prefix: default) { }

    #region IStorageBucket

    public Permissions Permissions { get; }

    /// <inheritdoc/>
    public async ValueTask WriteAsync(BucketPath file, byte[] buffer, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Write}. Error 03112817-aed6-4e22-a92e-8f5e38f29a35.");

        var blobName = GetBlobName(file);

        switch (writeMode)
        {
            case WriteMode.CreateOrReplace:
                {
                    var blobClient = _client.GetBlobClient(blobName);
                    await blobClient.UploadAsync(new BinaryData(buffer), overwrite: true, cancellationToken: ct);
                    break;
                }

            case WriteMode.CreateOrFail:
                {
                    var blobClient = _client.GetBlobClient(blobName);
                    var options = new BlobUploadOptions()
                    {
                        Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All }
                    };
                    await blobClient.UploadAsync(new BinaryData(buffer), options, ct);
                    break;
                }

            case WriteMode.AppendOrCreate:
                {
                    var blobClient = _client.GetAppendBlobClient(blobName);
                    await blobClient.CreateIfNotExistsAsync(cancellationToken: ct);
                    using var stream = new MemoryStream(buffer);
                    await blobClient.AppendBlockAsync(stream, cancellationToken: ct);
                    break;
                }

            //case WriteMode.AppendOrFail:
            //    {
            //        var blobClient = _client.GetAppendBlobClient(blobName);
            //        using var stream = new MemoryStream(buffer);
            //        await blobClient.AppendBlockAsync(stream, cancellationToken: ct);
            //        break;
            //    }

            default:
                throw new Exception($"Unknown write mode {writeMode}. Error a0a32fb9-fc44-400b-947c-44eed369e090.");
        }
    }

    /// <inheritdoc/>
    public async Task<Stream> GetWriteStreamAsync(BucketPath file, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Read}. Error 88195fc1-283c-4d33-bfbd-6218228f83cc.");

        var blobName = GetBlobName(file);
        var blobClient = _client.GetBlobClient(blobName);
        var stream = await blobClient.OpenReadAsync(cancellationToken: ct);
        return stream;
    }

    /// <inheritdoc/>
    public async Task<bool> ExistsAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 87252407-dba4-4dfc-8e1c-862a8d9fa1dc.");

        var blobName = GetBlobName(file);
        var blobClient = _client.GetBlobClient(blobName);
        var response = await blobClient.ExistsAsync(cancellationToken: ct);
        return response.Value;
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 7513d8d0-33d9-4395-826a-d4f202ea8d4a.");

        var blobName = GetBlobName(file);
        var blobClient = _client.GetBlobClient(blobName);
        var response = await blobClient.DownloadContentAsync(cancellationToken: ct);
        return response.Value.Content.ToArray();
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadSliceAsync(BucketPath file, long offset, long size, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error b1a7c181-226b-4795-85de-13d088665576.");

        var blobName = GetBlobName(file);
        var blobClient = _client.GetBlobClient(blobName);
        var options = new BlobDownloadOptions { Range = new HttpRange(offset, size) };
        var response = await blobClient.DownloadContentAsync(options, ct);
        return response.Value.Content.ToArray();

    }

    /// <inheritdoc/>
    public async Task<Stream> GetReadStreamAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error f38462aa-2da1-4c8b-a9de-85feff45907d.");

        var blobName = GetBlobName(file);
        var blobClient = _client.GetBlobClient(blobName);
        var response = await blobClient.DownloadStreamingAsync(cancellationToken: ct);
        return response.Value.Content;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<FileSystemItem> ListAsync(BucketPath path, bool recursive, [EnumeratorCancellation] Ct ct = default)
    {
        const string DELIMITER = "/";

        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Missing permission {Permissions.List}. " +
            $"Error b281db87-bd12-448e-9607-2c6f1f82d046."
            );

        var prefix = GetBlobName(path);
        var trimCount = _prefix?.Length ?? 0;

        if (recursive)
        {
            await foreach (var blob in _client.GetBlobsAsync(prefix: prefix, cancellationToken: ct))
            {
                var p = blob.Name;
#if NETSTANDARD2_0
                if (trimCount > 0) p = p.Substring(trimCount);
#else
                if (trimCount > 0) p = p[trimCount..];
#endif
                yield return new FileSystemItem(
                    Path: new BucketPath(p),
                    IsDirectory: false,
                    SizeInBytes: blob.Properties.ContentLength,
                    Created: blob.Properties.CreatedOn,
                    LastModified: blob.Properties.LastModified
                    );
            }
        }
        else
        {
            prefix += DELIMITER;
            await foreach (var e in _client.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: DELIMITER, cancellationToken: ct))
            {
                if (e.IsBlob)
                {
                    var blob = e.Blob;
                    var p = blob.Name;
#if NETSTANDARD2_0
                    if (trimCount > 0) p = p.Substring(trimCount);
#else
                    if (trimCount > 0) p = p[trimCount..];
#endif
                    yield return new FileSystemItem(
                        Path: new BucketPath(p),
                        IsDirectory: false,
                        SizeInBytes: blob.Properties.ContentLength,
                        Created: blob.Properties.CreatedOn,
                        LastModified: blob.Properties.LastModified
                        );
                }
                else
                {
                    var p = e.Prefix;
#if NETSTANDARD2_0
                    if (trimCount > 0) p = p.Substring(trimCount);
#else
                    if (trimCount > 0) p = p[trimCount..];
#endif
                    yield return new FileSystemItem(
                        Path: new BucketPath(p),
                        IsDirectory: true,
                        SizeInBytes: null,
                        Created: null,
                        LastModified: null
                        );
                }
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(BucketPath path, bool recursive, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Delete)) throw new Exception($"Missing permission {Permissions.Read}. Error 7247cdbd-044f-40a0-aeed-55f278d28c1a.");

        if (recursive)
        {
            var prefix = GetBlobName(path);
            await foreach (var blob in _client.GetBlobsAsync(prefix: prefix, cancellationToken: ct))
            {
                var blobClient = _client.GetBlobClient(blob.Name);
                await blobClient.DeleteIfExistsAsync(cancellationToken: ct);
            }
        }
        else
        {
            var blobName = GetBlobName(path);
            var blobClient = _client.GetBlobClient(blobName);
            var response = await blobClient.DeleteIfExistsAsync(cancellationToken: ct);
            if (response.Value == false)
            {
                await foreach (var e in ListAsync(path, recursive: false, ct))
                {
                    throw new Exception(
                        $"Directory is not empty ({path}) and parameter 'recursive' is set to false. " +
                        $"Error 4253359c-ce7f-43c6-83df-ec0a8f94ea82."
                        );
                }
            }
        }
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> ChangeDirAsync(BucketPath directory, Ct ct = default)
    {
        directory = directory.ToRelative();
        var newPrefix = (_prefix + directory.ToRelative()).ToString();
        var result = new AzureStorageBucket(_sas, newPrefix, _client, Permissions);
        return Task.FromResult<IStorageBucket>(result);
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> WithPermissionsAsync(Permissions newPermissions)
    {
        if (newPermissions.HasFlag(Permissions.Read) && !Permissions.HasFlag(Permissions.Read)) throw new Exception(
            $"Can't add Read permission. Error 97b8c2c1-5f63-41da-a1bc-ac170620712d."
            );

        if (newPermissions.HasFlag(Permissions.Write) && !Permissions.HasFlag(Permissions.Write)) throw new Exception(
            $"Can't add Write permission. Error d2463cbf-c304-4a39-8fda-1024cc36a64f."
            );

        if (newPermissions.HasFlag(Permissions.Delete) && !Permissions.HasFlag(Permissions.Delete)) throw new Exception(
            $"Can't add Delete permission. Error b9c707c2-ea50-473d-b8e5-b09a5d064b3a."
            );

        if (newPermissions.HasFlag(Permissions.List) && !Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Can't add List permission. Error beae7a5f-ee84-411b-9e87-de674ad63252."
            );

        return Task.FromResult<IStorageBucket>(new AzureStorageBucket(_sas, _prefix, _client, newPermissions));
    }

    #endregion

    #region helpers

    /// <summary>
    /// Construct the actual blob name to use in Azure APIs from a path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetBlobName(BucketPath path)
        => (_prefix + path.ToRelative()).ToString();

    #endregion
}