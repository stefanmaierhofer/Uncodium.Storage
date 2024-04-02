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
using System.Linq;
using System.Runtime.CompilerServices;

namespace Uncodium.Storage;

public class FileSystemStorageProvider : IStorageProvider
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

public class FileSystemStorageBucket : IStorageBucket
{
    private readonly DirectoryInfo _root;

    public FileSystemStorageBucket(DirectoryInfo directory, Permissions permissions)
    {
        _root = directory;
        if (!_root.Exists) _root.Create();

        Permissions = permissions;
        BaseDir = _root.FullName;
    }

    public FileSystemStorageBucket(string directory, Permissions permissions) 
        : this(new DirectoryInfo(directory), permissions)
    {
    }

    public string BaseDir { get; }

    #region IStorageBucket

    /// <inheritdoc/>
    public Permissions Permissions { get; }

    /// <inheritdoc/>
    public async ValueTask WriteAsync(BucketPath file, byte[] buffer, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Write}. Error be32660d-c7b9-4574-a008-17ce8fdeb93e.");

        file = file.ToRelative();

        var dir = GetOrCreateDir(file.GetPathWithoutLastSegment());
        var key = file.LastSegmentAsString;
        var fileInfo = new FileInfo(Path.Combine(dir.FullName, key));

        if (fileInfo.Exists)
        {
            // file already exists ...

            switch (writeMode)
            {
                case WriteMode.CreateOrReplace:
                    {
#if NETSTANDARD2_0
                        File.WriteAllBytes(fileInfo.FullName, buffer);
#else
                        await File.WriteAllBytesAsync(fileInfo.FullName, buffer, ct);
#endif
                        break;
                    }

                case WriteMode.CreateOrFail:
                    throw new Exception($"File {file} already exists. Error 2ae2f92e-eb05-4e87-83d8-4de1b2489e26.");


                case WriteMode.AppendOrCreate:
                    {
                        using var f = fileInfo.Open(FileMode.Append, FileAccess.Write, FileShare.None);
#if NETSTANDARD2_0
                        await f.WriteAsync(buffer, offset: 0, count: buffer.Length, ct);
#else
                        await f.WriteAsync(buffer, ct);
#endif
                        break;
                    }

                //case WriteMode.AppendOrFail:
                //    throw new Exception($"File {file} already exists. Error 0553837e-64f3-4f9f-805d-d772bf679fc5.");

                default:
                    throw new Exception($"Unknown write mode {writeMode}. Error e1885378-8fe7-4615-8b6a-f489eb9c693a.");
            }
        }
        else
        {
            // file does not exist ...

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable IDE0066 // Convert switch statement to expression
            switch (writeMode)
            {
                case WriteMode.CreateOrReplace:
                case WriteMode.CreateOrFail:
                case WriteMode.AppendOrCreate:
#if NETSTANDARD2_0
                    File.WriteAllBytes(fileInfo.FullName, buffer);
#else
                    await File.WriteAllBytesAsync(fileInfo.FullName, buffer, ct);
#endif
                    break;
                default:
                    throw new Exception($"Expected write mode CreateOrReplace, or CreateOrFail, or AppendOrCreate, but found {writeMode}. Error 162441af-b1f4-401e-897d-077cdee9f3d0.");
            };
#pragma warning restore IDE0079 // Remove unnecessary suppression
        }
    }

    /// <inheritdoc/>
    public Task<Stream> GetWriteStreamAsync(BucketPath file, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Write}. Error 8202509b-ce2f-4004-a6e1-1105c88144d8.");

        file = file.ToRelative();

        var dir = GetOrCreateDir(file.GetPathWithoutLastSegment());
        var key = file.LastSegmentAsString;
        var fileInfo = new FileInfo(Path.Combine(dir.FullName, key));

        if (fileInfo.Exists)
        {
            // file already exists ...

            switch (writeMode)
            {
                case WriteMode.CreateOrReplace:
                    {
                        Stream f = fileInfo.Open(FileMode.Create, FileAccess.Write, FileShare.None);
                        return Task.FromResult(f);
                    }

                case WriteMode.CreateOrFail:
                    throw new Exception($"File {file} already exists. Error a4f1461d-7a94-478b-9a6b-60d47143ff63.");


                case WriteMode.AppendOrCreate:
                    {
                        Stream f = fileInfo.Open(FileMode.Append, FileAccess.Write, FileShare.None);
                        return Task.FromResult(f);
                    }

                //case WriteMode.AppendOrFail:
                //    throw new Exception($"File {file} already exists. Error 342d38ed-ca0b-4c63-b4a0-82fd0be7c746.");

                default:
                    throw new Exception($"Unknown write mode {writeMode}. Error 5ac992ad-9192-4d3c-9d02-30d719299bc6.");
            }
        }
        else
        {
            // file does not exist ...

            switch (writeMode)
            {
                case WriteMode.CreateOrReplace:
                case WriteMode.CreateOrFail:
                case WriteMode.AppendOrCreate:
                    {
                        Stream f = fileInfo.Open(FileMode.Create, FileAccess.Write, FileShare.None);
                        return Task.FromResult(f);
                    }
                default:
                    throw new Exception($"Expected write mode CreateOrReplace, or CreateOrFail, or AppendOrCreate, but found {writeMode}. Error 8d4aeaee-3b01-4b11-8873-0f6f63bd7cd5.");
            };
        }
    }

    /// <inheritdoc/>
    public Task<bool> ExistsAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error b546adc5-273b-48b3-aaf1-08c63c6377c0.");

        var fileInfo = GenFileInfo(file);
        return Task.FromResult(fileInfo.Exists);
    }

    /// <inheritdoc/>
    public Task<byte[]> ReadAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 91b1286a-575b-4196-b496-3227ce2c7135.");

        var path = GenFileInfo(file);

#if NETSTANDARD2_0
        var buffer = Task.FromResult(File.ReadAllBytes(path.FullName));
#else
        var buffer = File.ReadAllBytesAsync(path.FullName, cancellationToken: ct);
#endif

        return buffer;
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadSliceAsync(BucketPath file, long offset, long size, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 43661271-ee41-4967-b76f-7b87e5f1dba8.");

        var path = GenFileInfo(file);

        using var stream = path.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
        stream.Seek(offset, SeekOrigin.Begin);
        var buffer = new byte[size];
        var p = buffer.AsMemory();

#if NETSTANDARD2_0
        while (p.Length > 0) p = p.Slice(await stream.ReadAsync(buffer, offset: 0, count: p.Length, cancellationToken: ct));
#else
        while (p.Length > 0) p = p[await stream.ReadAsync(buffer, cancellationToken: ct)..];
#endif

        return buffer;
    }

    /// <inheritdoc/>
    public Task<Stream> GetReadStreamAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error b73a46e8-b76a-4dbe-8895-3b79696f98f1.");

        var path = GenFileInfo(file);

        if (path.Exists)
        {
            var stream = path.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
            return Task.FromResult<Stream>(stream);
        }
        else
        {
            throw new Exception($"File does not exist ({path}). Error 9ed44136-4094-4bd4-a83b-be8e1dfa72a0.");
        }
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(BucketPath file, bool recursive, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Delete)) throw new Exception($"Missing permission {Permissions.Delete}. Error 64af46cd-8560-48b9-b6b8-9659101849af.");

        var fileInfo = GenFileInfo(file);

        BucketPath path;
        if (fileInfo.Exists)
        {
            fileInfo.Delete();
            path = file.GetPathWithoutLastSegment();
        }
        else if (Directory.Exists(fileInfo.FullName))
        {
            Directory.Delete(fileInfo.FullName, recursive);
            path = file;
        }
        else
        {
#if NETSTANDARD2_0
            return new(Task.CompletedTask);
#else
            return ValueTask.CompletedTask;
#endif
        }

        try
        {
            path = path.ToRelative();
            while (path.Segments.Length > 0)
            {
                var dirInfo = new DirectoryInfo(Path.Combine(_root.FullName, path.ToString()));
                
                if (dirInfo.EnumerateFileSystemInfos().Any())
                {
                    break;
                }
                else
                {
                    dirInfo.Delete();
                    path = path.GetPathWithoutLastSegment();
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"[WARNING] FileSystemStorageService.DeleteAsync({file}, recursive: {recursive}): {e.Message}");
        }

#if NETSTANDARD2_0
        return new(Task.CompletedTask);
#else
        return ValueTask.CompletedTask;
#endif
    }

    /// <inheritdoc/>
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    public async IAsyncEnumerable<FileSystemItem> ListAsync(BucketPath path, bool recursive, [EnumeratorCancellation] Ct ct = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Missing permission {Permissions.List}. " +
            $"Error 96eacf3e-72f6-4af0-b08d-63a15b2e44b3."
            );

        // path is a dir?
        var dirInfo = GenDirInfo(path);
        //var offset = dirInfo.FullName.Length;
        var offset = _root.FullName.Length;

        BucketPath P(string s)
        {
#if NETSTANDARD2_0
            return new BucketPath(s.Substring(offset));
#else
            return new BucketPath(s[offset..]);
#endif
        }

        if (dirInfo.Exists)
        {
            if (recursive)
            {
#if NETSTANDARD2_0
                var fileInfos = dirInfo.EnumerateFiles("*.*", SearchOption.AllDirectories);
#else
                var options = new EnumerationOptions
                {
                    MatchType = MatchType.Simple,
                    RecurseSubdirectories = true
                };

                var fileInfos = dirInfo.EnumerateFiles("*", options);
#endif
                foreach (var x in fileInfos)
                {
                    var p = P(x.FullName);
                    yield return new(
                        Path: p,
                        IsDirectory: false,
                        SizeInBytes: x.Length,
                        Created: x.CreationTimeUtc,
                        LastModified: x.LastWriteTimeUtc
                        );
                }
            }
            else
            {
                var fileInfos = dirInfo.EnumerateFiles();
                foreach (var x in fileInfos)
                {
                    var p = P(x.FullName);
                    yield return new(
                        Path: p,
                        IsDirectory: false,
                        SizeInBytes: x.Length,
                        Created: x.CreationTimeUtc,
                        LastModified: x.LastWriteTimeUtc
                        );
                }

                var dirInfos = dirInfo.EnumerateDirectories();
                foreach (var x in dirInfos)
                {
                    var p = P(x.FullName);
                    yield return new(
                        Path: p,
                        IsDirectory: true,
                        SizeInBytes: null,
                        Created: x.CreationTimeUtc,
                        LastModified: x.LastWriteTimeUtc
                        );
                }
            }
        }
        else
        {
            // path is a file?
            var fileInfo = GenFileInfo(path);
            if (fileInfo.Exists)
            {
                yield return new(
                    Path: path,
                    IsDirectory: false,
                    SizeInBytes: fileInfo.Length,
                    Created: fileInfo.CreationTimeUtc,
                    LastModified: fileInfo.LastWriteTimeUtc
                    );
            }
        }
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> ChangeDirAsync(BucketPath directory, Ct ct = default)
    {
        directory = directory.ToRelative();
        var dir = GetOrCreateDir(directory);
        var result = new FileSystemStorageBucket(dir, Permissions);
        return Task.FromResult<IStorageBucket>(result);
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> WithPermissionsAsync(Permissions newPermissions)
    {
        if (newPermissions.HasFlag(Permissions.Read) && !Permissions.HasFlag(Permissions.Read)) throw new Exception(
            $"Can't add Read permission. Error 4801b420-e13f-4cdb-8ace-d6530fd524b7."
            );

        if (newPermissions.HasFlag(Permissions.Write) && !Permissions.HasFlag(Permissions.Write)) throw new Exception(
            $"Can't add Write permission. Error 5f6b4fe7-cbf8-4bb7-8127-bbc1201953ac."
            );

        if (newPermissions.HasFlag(Permissions.Delete) && !Permissions.HasFlag(Permissions.Delete)) throw new Exception(
            $"Can't add Delete permission. Error 607fbe29-793b-4dae-b487-897b343c308f."
            );

        if (newPermissions.HasFlag(Permissions.List) && !Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Can't add List permission. Error 2a0b42bc-f4a1-4533-80d3-097772766878."
            );

        return Task.FromResult<IStorageBucket>(new FileSystemStorageBucket(_root, newPermissions));
    }

#endregion

    #region helpers

    /// <summary>
    /// Gets an existing dir, or creates a new dir (including all dirs along the path).
    /// Throws if any segment along the path is an existing file.
    /// </summary>
    private DirectoryInfo GetOrCreateDir(BucketPath path)
    {
        if (path.Segments.Length == 0) return _root;

        var result = _root.CreateSubdirectory(path.ToRelative().ToString());
        return result;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private FileInfo GenFileInfo(BucketPath filePath)
        => new (Path.Combine(_root.FullName, filePath.ToRelative().ToString()));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DirectoryInfo GenDirInfo(BucketPath filePath)
        => new(Path.Combine(_root.FullName, filePath.ToRelative().ToString()));

    #endregion
}
