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
using System.Text;
using System.Text.Json;

namespace Uncodium.Storage;

public static class IStorageBucketExtensions
{
    /// <summary>
    /// Write byte buffer to file.
    /// </summary>
    public static ValueTask WriteAsync(this IStorageBucket self, string file, byte[] data, WriteMode writeMode, Ct ct = default)
        => self.WriteAsync(new BucketPath(file), data, writeMode, ct);

    /// <summary>
    /// Write string to file.
    /// </summary>
    public static ValueTask WriteAsync(this IStorageBucket self, string file, string data, WriteMode writeMode, Ct ct = default)
        => self.WriteAsync(new BucketPath(file), Encoding.UTF8.GetBytes(data), writeMode, ct);

    /// <summary>
    /// Write JSON-serialized object to file.
    /// </summary>
    public static ValueTask WriteJsonAsync<T>(this IStorageBucket self, string file, T data, WriteMode writeMode, Ct ct = default) where T : notnull
        => self.WriteAsync(new BucketPath(file), data.ToJsonString(), writeMode, ct);

    /// <summary>
    /// Write string to file.
    /// </summary>
    public static ValueTask WriteAsync(this IStorageBucket self, BucketPath file, string data, WriteMode writeMode, Ct ct = default)
        => self.WriteAsync(file, Encoding.UTF8.GetBytes(data), writeMode, ct);

    /// <summary>
    /// Write JSON-serialized object to file.
    /// </summary>
    public static ValueTask WriteJsonAsync<T>(this IStorageBucket self, BucketPath file, T data, WriteMode writeMode, Ct ct = default) where T : notnull
        => self.WriteAsync(file, data.ToJsonString(), writeMode, ct);

    /// <summary>
    /// Get write stream for file.
    /// </summary>
    public static Task<Stream> GetWriteStreamAsync(this IStorageBucket self, string file, WriteMode writeMode, Ct ct = default)
        => self.GetWriteStreamAsync(new BucketPath(file), writeMode, ct);

    /// <summary>
    /// True if file exists.
    /// </summary>
    public static Task<bool> ExistsAsync(this IStorageBucket self, string file, Ct ct = default)
        => self.ExistsAsync(new BucketPath(file), ct);

    /// <summary>
    /// Return full file as byte array.
    /// </summary>
    public static Task<byte[]> ReadAsync(this IStorageBucket self, string file, Ct ct = default)
        => self.ReadAsync(new BucketPath(file), ct);

    /// <summary>
    /// Return full file as string.
    /// </summary>
    public static async Task<string> ReadStringAsync(this IStorageBucket self, BucketPath file, Ct ct = default)
    {
        var buffer = await self.ReadAsync(file, ct);
        return Encoding.UTF8.GetString(buffer);
    }

    /// <summary>
    /// Return full file as string.
    /// </summary>
    public static Task<string> ReadStringAsync(this IStorageBucket self, string file, Ct ct = default)
        => ReadStringAsync(self, new BucketPath(file), ct);

    /// <summary>
    /// Deserializes JSON content of given file to a T.
    /// Returns null, if this is not possible, e.g. key does not exist, or deserialization fails.
    /// </summary>
    public static async ValueTask<T?> TryReadJsonAsync<T>(this IStorageBucket self, BucketPath file, Ct ct = default)
    {
        try
        {
            using var stream = await self.GetReadStreamAsync(file, ct);
            return await JsonSerializer.DeserializeAsync<T>(stream, JsonUtils.JsonOptions, ct);
        }
        catch
        {
            return default;
        }
    }

    /// <summary>
    /// Deserializes JSON content of given file to a T.
    /// Returns null, if this is not possible, e.g. key does not exist, or deserialization fails.
    /// </summary>
    public static ValueTask<T?> TryReadJsonAsync<T>(this IStorageBucket self, string file, Ct ct = default)
        => TryReadJsonAsync<T>(self, new BucketPath(file), ct);

    /// <summary>
    /// Return partial file as byte array.
    /// </summary>
    public static Task<byte[]> ReadSliceAsync(this IStorageBucket self, string file, long offset, long size, Ct ct = default)
        => self.ReadSliceAsync(new BucketPath(file), offset, size, ct);

    /// <summary>
    /// Get read stream for file.
    /// </summary>
    public static Task<Stream> GetReadStreamAsync(this IStorageBucket self, string file, Ct ct = default)
        => self.GetReadStreamAsync(new BucketPath(file), ct);

    /// <summary>
    /// Delete file or directory.
    /// If the path does not exist, no exception is thrown.
    /// </summary>
    public static ValueTask DeleteAsync(this IStorageBucket self, string path, bool recursive, Ct ct = default)
        => self.DeleteAsync(new BucketPath(path), recursive, ct);

    /// <summary>
    /// List all files in given directory.
    /// If recursive is true, then also recursively list all files in subdirectories.
    /// If recursive is false, then directories in the given path will also be returned.
    /// </summary>
    public static IAsyncEnumerable<FileSystemItem> ListAsync(this IStorageBucket self, string path, bool recursive, Ct ct = default)
        => self.ListAsync(new BucketPath(path), recursive, ct);

    /// <summary>
    /// List all files.
    /// If recursive is true, then also recursively list all files in subdirectories.
    /// If recursive is false, then directories in current path will also be returned.
    /// </summary>
    public static IAsyncEnumerable<FileSystemItem> ListAsync(this IStorageBucket self, bool recursive, Ct ct = default)
        => self.ListAsync(BucketPath.Root, recursive, ct);

    /// <summary>
    /// Return a new storage bucket, which is rooted at the given directory.
    /// This acts similar to 'cd' in file systems, but without the ability to go back up to the parent directory.
    /// </summary>
    public static Task<IStorageBucket> ChangeDirAsync(this IStorageBucket self, string directory, Ct ct = default)
        => self.ChangeDirAsync(new BucketPath(directory), ct);
}
