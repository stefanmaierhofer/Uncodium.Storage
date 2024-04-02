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
using System.Reflection;
using System.Text.Json;
using System.Web;

namespace Uncodium.Storage;

/// <summary>
/// 
/// </summary>
[Flags]
public enum Permissions
{
    None   = 0,
    All    = Read | Write | Delete | List,

    Read   = 1,
    Write  = 2,
    Delete = 4,
    List   = 8,
}

/// <summary>
/// 
/// </summary>
public enum WriteMode
{
    /// <summary>
    /// Specifies that the storage system should create a new file.
    /// If the file already exists, it will be replaced.
    /// This requires Permissions.Write permission.
    /// </summary>
    CreateOrReplace = 1,

    /// <summary>
    /// Specifies that the storage system should create a new file.
    /// This requires Permissions.Write permission.
    /// If the file already exists, an exception is thrown.
    /// </summary>
    CreateOrFail = 2,

    /// <summary>
    /// Appends to the file if it exists, or creates a new file.
    /// This requires Permissions.Write permission.
    /// 
    AppendOrCreate = 3,
}

public record FileSystemItem(BucketPath Path, bool IsDirectory, long? SizeInBytes, DateTimeOffset? Created, DateTimeOffset? LastModified)
{
    public override string ToString() => Path.ToString();

    public FileSystemItemDto ToDto() => new(Path.ToString(), IsDirectory, SizeInBytes, Created, LastModified);

    public static FileSystemItem FromDto(FileSystemItemDto dto) => new((BucketPath)dto.Path, dto.IsDirectory, dto.SizeInBytes, dto.Created, dto.LastModified);
}

public record FileSystemItemDto(string Path, bool IsDirectory, long? SizeInBytes, DateTimeOffset? Created, DateTimeOffset? LastModified);

/// <summary>
/// Uncodium storage.
/// Implementations for local file system, in-memory, Azure blob containers, HTTP, and more, are available.
/// </summary>
public interface IStorageBucket
{
    /// <summary>
    /// Which operations are allowed in this storage bucket (read, list, write, ...).
    /// </summary>
    Permissions Permissions { get; }

    /// <summary>
    /// Write byte buffer to file.
    /// </summary>
    ValueTask WriteAsync(BucketPath file, byte[] data, WriteMode writeMode, Ct ct = default);

    /// <summary>
    /// Get write stream for file.
    /// </summary>
    Task<Stream> GetWriteStreamAsync(BucketPath file, WriteMode writeMode, Ct ct = default);

    /// <summary>
    /// True if file exists.
    /// </summary>
    Task<bool> ExistsAsync(BucketPath file, Ct ct = default);

    /// <summary>
    /// Return file content as byte array.
    /// </summary>
    Task<byte[]> ReadAsync(BucketPath file, Ct ct = default);

    /// <summary>
    /// Return partial file content as byte array.
    /// </summary>
    Task<byte[]> ReadSliceAsync(BucketPath file, long offset, long size, Ct ct = default);

    /// <summary>
    /// Get read stream for file.
    /// </summary>
    Task<Stream> GetReadStreamAsync(BucketPath file, Ct ct = default);

    /// <summary>
    /// Delete file or directory.
    /// If the path does not exist, no exception is thrown.
    /// </summary>
    ValueTask DeleteAsync(BucketPath path, bool recursive, Ct ct = default);

    /// <summary>
    /// List files in given path.
    /// If recursive is false, then files and directories will be listed.
    /// If recursive is true, then only files will be listed, but recursively for all subdirectories.
    /// </summary>
    IAsyncEnumerable<FileSystemItem> ListAsync(BucketPath path, bool recursive, Ct ct = default);

    /// <summary>
    /// Return a new storage bucket, which is rooted at the given path relative to the current root.
    /// Directories along the path will be created if they do not yet exist.
    /// </summary>
    Task<IStorageBucket> ChangeDirAsync(BucketPath directory, Ct ct = default);

    /// <summary>
    /// Return a new storage bucket with updated permissions.
    /// Permissions can only be removed.
    /// If a permission is added, an exception is thrown.
    /// </summary>
    Task<IStorageBucket> WithPermissionsAsync(Permissions newPermissions);
}

public static class StorageBucket
{
    public static async Task<IStorageBucket> ResolveAsync(object o, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        switch (o)
        {
            case IStorageBucket bucket:
                {
                    return bucket;
                }

            case Uri uri:
                {
                    return await ResolveAsync(uri, ct);
                }

            case "inmemory":
                {
                    return new InMemoryStorageBucket(Permissions.All);
                }

            case string s:
                {
                    // try to resolve string as uri
                    return await ResolveAsync(new Uri(s), ct);
                }

            case JsonElement j:
                {
                    return await ResolveAsync(j.GetString()!, ct);
                }

            default:
                {
                    throw new Exception(
                        $"Failed to resolve storage bucket from \"{o?.GetType().FullName}\". " +
                        $"Error 79744298-bfdb-4026-b2b6-b7a04954a837."
                        );
                }
        }
    }

    public static async Task<IStorageBucket> ResolveAsync(Uri uri, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var pq = HttpUtility.ParseQueryString(uri.Query)["p"];
        var p = pq != null ? ParsePermissions(pq) : Permissions.All;

        switch (uri.Scheme)
        {
            case "inmemory":
                {
                    return new InMemoryStorageBucket(p);
                }

            case "file":
                {
                    return new FileSystemStorageBucket(uri.AbsolutePath, p);
                }

            case "azure":
                {
                    var a = Assembly.Load("Uncodium.Storage.Azure");
                    var t = a.GetType("Uncodium.Storage.AzureStorageBucket")!;
                    var ctr = t.GetConstructor([typeof(string)]) ?? throw new Exception(
                        "Failed to get Uncodium.Storage.AzureStorageBucket type. Error f0ffc1cc-af40-466f-9c83-158ecbc84de7."
                        );

#if NETSTANDARD2_0
                    var sas = "https" + uri.ToString().Substring("azure".Length);
#else
                    var sas = "https" + uri.ToString()["azure".Length..];
#endif

                    var o = ctr.Invoke([sas]);
                    return (IStorageBucket)o;
                }

            case "lookup":
                {
                    await Task.Delay(0, ct);
                    throw new Exception($"Failed to look up storage bucket \"{uri}\". Error a3cb4cde-e8ec-4df9-86dd-6c9d80027cec.");
                }

            default:
                {
                    throw new Exception(
                        $"Unknown URI scheme \"{uri.Scheme}\". " +
                        $"Error 1b8423a6-2bdf-4ce7-9493-f1001d1cc137."
                        );
                }
        }
    }

    private static readonly Dictionary<char, Permissions> _char2Permission = new()
    {
        { 'a', Permissions.All },
        { 'd', Permissions.Delete },
        { 'l', Permissions.List },
        { 'r', Permissions.Read },
        { 'w', Permissions.Write },
    };
    public static Permissions ParsePermissions(string s)
    {
        var p = Permissions.None;
        foreach (var c in s)
        {
            if (_char2Permission.TryGetValue(c, out var x))
            {
                p |= x;
            }
            else
            {
                throw new Exception(
                    $"Invalid character '{c}' in permission string \"{s}\". " +
                    $"Error d5c63303-6745-4917-8ce1-59d4ce4652f5."
                    );
            }
        }
        return p;
    }
}