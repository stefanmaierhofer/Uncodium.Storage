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

using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable IDE0290 // Use primary constructor

namespace Uncodium.Storage;

public class InMemoryStorageProvider : IStorageProvider
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

public class InMemoryStorageBucket : IStorageBucket
{
    private readonly EntryDir Root;

    public InMemoryStorageBucket(Permissions permissions)
    {
        Root = new(BucketPath.Root, []);
        Permissions = permissions;
    }

    private InMemoryStorageBucket(Permissions permissions, EntryDir root)
    {
        Root = root;
        Permissions = permissions;
    }

    #region IStorageBucket

    /// <inheritdoc/>
    public Permissions Permissions { get; }

    /// <inheritdoc/>
    public ValueTask WriteAsync(BucketPath file, byte[] buffer, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Write}. Error 34cdb7b9-34fc-41d6-b5fe-45016ba47b56.");

        file = file.ToAbsolute();

        var dir = GetOrCreateDir(file.GetPathWithoutLastSegment());

#if NETSTANDARD2_0
        var key = file.Segments[file.Segments.Length - 1];
#else
        var key = file.Segments[^1];
#endif

        lock (dir.Items)
        {
            if (dir.Items.TryGetValue(key, out var entry))
            {
                // file already exists ...

                if (entry is not EntryFile existingFile) throw new Exception(
                    $"Expected file {file} but found directory instead. Error 14921f95-f0a4-488b-ae39-1276eca50a04."
                    );

                switch (writeMode)
                {
                    case WriteMode.CreateOrReplace:
                        {
                            var e = new EntryFile(Path: file, Content: buffer, Created: DateTimeOffset.UtcNow, LastModified: DateTimeOffset.UtcNow);
                            dir.Items[key] = e;
                            break;
                        }

                    case WriteMode.CreateOrFail:
                        throw new Exception($"File {file} already exists. Error 1d00d0af-a669-4bfc-9a62-39abee1e1073.");


                    case WriteMode.AppendOrCreate:
                        {
                            var newContent = new byte[existingFile.Content.Length + buffer.Length];
                            Array.Copy(existingFile.Content, newContent, existingFile.Content.Length);
                            Array.Copy(buffer, 0, newContent, existingFile.Content.Length, buffer.Length);
                            var e = existingFile with { Content = newContent, LastModified = DateTimeOffset.Now };
                            dir.Items[key] = e;
                            break;
                        }

                    //case WriteMode.AppendOrFail:
                    //    throw new Exception($"File {file} already exists. Error b52ff003-758f-450d-8b4b-9c8742a8ada3.");

                    default:
                        throw new Exception($"Unknown write mode {writeMode}. Error b4f60284-5b8e-4031-8924-80ff50fcc7c6.");
                }
            }
            else
            {
                // file does not exist ...

                var e = new EntryFile(Path: file, Content: buffer, Created: DateTimeOffset.UtcNow, LastModified: DateTimeOffset.UtcNow);
                dir.Items[key] = writeMode switch
                {
                    WriteMode.CreateOrReplace or WriteMode.CreateOrFail or WriteMode.AppendOrCreate => e,
                    _ => throw new Exception($"Expected write mode CreateOrReplace, or CreateOrFail, or AppendOrCreate, but found {writeMode}. Error 09e9f484-b8f0-4245-964b-e8e52bc5c2a1."),
                };
            }
        }

#if NETSTANDARD2_0
        return new(Task.CompletedTask);
#else
        return ValueTask.CompletedTask;
#endif
    }

    /// <inheritdoc/>
    public Task<Stream> GetWriteStreamAsync(BucketPath file, WriteMode writeMode, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Write)) throw new Exception($"Missing permission {Permissions.Write}. Error f2998007-18c5-4f9b-ae25-ac2db6999f93.");

        file = file.ToAbsolute();

        var stream = new WriteStream(async buffer =>
        {
            await WriteAsync(file, buffer, writeMode, ct);
        });

        return Task.FromResult<Stream>(stream);
    }

    /// <inheritdoc/>
    public Task<bool> ExistsAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error f7b1ffe4-b91c-4258-a64b-9fd44c69235f.");

        file = file.ToAbsolute();

        return Task.FromResult(TryGetEntry(file, out _));
    }

    /// <inheritdoc/>
    public Task<byte[]> ReadAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 1cbbb6ad-d4cb-4ea3-8099-29ac5170dadb.");

        file = file.ToAbsolute();

        if (TryGetDir(file.GetPathWithoutLastSegment(), out var dir))
        {
            var key = file.LastSegmentAsString;

            lock (dir.Items)
            {
                if (dir.Items.TryGetValue(key, out var e0))
                {
                    if (e0 is EntryFile e)
                    {
                        return Task.FromResult(e.Content);
                    }
                    else
                    {
                        throw new Exception($"Expected file at {file}, but found {e0.GetType().Name}. Error fb6bc784-b23d-46fe-881c-33c3d8439ace.");
                    }
                }
                else
                {
                    throw new Exception($"File does not exist ({file}). Error e05c2d14-0ae5-4211-93c0-b9f554e9ad6a.");
                }
            }
        }
        else
        {
            throw new Exception($"File not found ({file}). Error ce7280b6-4612-40db-91f5-967c96fba24d.");
        }
    }

    /// <inheritdoc/>
    public async Task<byte[]> ReadSliceAsync(BucketPath file, long offset, long size, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error 17800a8b-58b0-4cd8-aee7-a4bc57693cf4.");

        file = file.ToAbsolute();

        var fullBuffer = await ReadAsync(file, ct);
        var sliceBuffer = new byte[size];
        Array.Copy(fullBuffer, offset, sliceBuffer, 0, size);
        return sliceBuffer;
    }

    /// <inheritdoc/>
    public async Task<Stream> GetReadStreamAsync(BucketPath file, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Read)) throw new Exception($"Missing permission {Permissions.Read}. Error d6b4b89d-334a-4160-900e-096fc04927eb.");

        file = file.ToAbsolute();

        var buffer = await ReadAsync(file, ct);
        return new MemoryStream(buffer);
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(BucketPath path, bool recursive, Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.Delete)) throw new Exception($"Missing permission {Permissions.Delete}. Error d2109751-a037-4a2e-a593-48f4c0549dbe.");

        path = path.ToAbsolute();

        if (TryGetDir(path.GetPathWithoutLastSegment(), out var dir))
        {
            var key = path.LastSegmentAsString;

            lock (dir.Items)
            {
                if (dir.Items.TryGetValue(key, out var e0))
                {
                    switch (e0)
                    {
                        case EntryFile /*eFile*/:
                            {
                                dir.Items.Remove(key);
                                break;
                            }

                        case EntryDir eDir:
                            {
                                if (eDir.Items.Count == 0)
                                {
                                    // empty dir -> can delete
                                    dir.Items.Remove(key);
                                }
                                else
                                {
                                    // non-empty dir
                                    if (recursive == true)
                                    {
                                        dir.Items.Remove(key);
                                    }
                                    else
                                    {
                                        throw new Exception(
                                            $"Directory is not empty ({path}) and parameter 'recursive' is set to false. " +
                                            $"Error 35b9016a-1a5c-4813-bd04-2cc4e4ea30c1."
                                            );
                                    }
                                }
                                break;
                            }
                    }
                }
            }
        }

#if NETSTANDARD2_0
        return new(Task.CompletedTask);
#else
        return ValueTask.CompletedTask;
#endif
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<FileSystemItem> ListAsync(BucketPath path, bool recursive, [EnumeratorCancellation] Ct ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!Permissions.HasFlag(Permissions.List)) throw new Exception(
            $"Missing permission {Permissions.List}. " +
            $"Error db4663e3-4ddc-4f5f-bd84-a73c6a783e35."
            );

        path = path.ToAbsolute();

        if (TryGetEntry(path, out var entry))
        {
            if (entry is not EntryDir dir) throw new Exception(
                $"Expected directory ({path}), but found {entry.GetType().Name}. " +
                $"Error e588afd5-a5fa-4e58-8a37-34e541512201."
                );

            var n = Root.Path.Segments.Length;
            IEntry[] items;
            lock (dir.Items) items = [.. dir.Items.Values];
            foreach (var item in items)
            {
                switch (item)
                {
                    case EntryFile f:
                        {
                            yield return new FileSystemItem(
                                Path: f.Path.SkipSegments(n),
                                IsDirectory: false,
                                SizeInBytes: f.LengthInBytes,
                                Created: f.Created,
                                LastModified: f.LastModified
                                );
                            break;
                        }

                    case EntryDir d:
                        {
                            if (recursive)
                            {
                                var xs = ListAsync(d.Path, recursive: true, ct);
                                await foreach (var x in xs) yield return x;
                            }
                            else
                            {
                                yield return new FileSystemItem(
                                    Path: d.Path.SkipSegments(n),
                                    IsDirectory: true,
                                    SizeInBytes: null,
                                    Created: null,
                                    LastModified: null
                                    );
                            }

                            break;
                        }

                    default:
                        throw new Exception($"Unknown item type {item.GetType().Name}. Error fc29911c-bc25-4a21-8845-70b112ea1742.");
                }

            }
        }
        else
        {
            throw new Exception($"Directory does not exist ({path}). Error 39b5da47-f9e0-448e-8dd8-d0f4ac3d5912.");
        }
    }

    /// <inheritdoc/>
    public Task<IStorageBucket> ChangeDirAsync(BucketPath directory, Ct ct = default)
    {
        var d = GetOrCreateDir(directory);
        var newFs = new InMemoryStorageBucket(Permissions, d);
        return Task.FromResult<IStorageBucket>(newFs);
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

        return Task.FromResult<IStorageBucket>(new InMemoryStorageBucket(newPermissions, Root));
    }

    #endregion

    #region helpers

    private class WriteStream : MemoryStream
    {
        private readonly Action<byte[]> _onClose;
        public WriteStream(Action<byte[]> onClose)
        {
            _onClose = onClose;
        }

        public override void Close()
        {
            base.Close();
            var buffer = ToArray();
            //Console.WriteLine($"buffer.Length = {buffer.Length}");
            _onClose(buffer);
        }

#if NETSTANDARD2_0
#else
        public override ValueTask DisposeAsync()
        {
            var result = base.DisposeAsync();
            _onClose(ToArray());
            return result;
        }
#endif
    }

    private interface IEntry
    {
    }

    private record EntryFile(BucketPath Path, byte[] Content, DateTimeOffset Created, DateTimeOffset LastModified) : IEntry
    {
        public long LengthInBytes => Content.Length;
    }

    private record EntryDir(BucketPath Path, Dictionary<string, IEntry> Items) : IEntry
    {
    }

    /// <summary>
    /// Returns span without first element.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<T> Tail<T>(ReadOnlySpan<T> xs)
    {
#if NETSTANDARD2_0
        return xs.Slice(1);
#else
        return xs[1..];
#endif
    }

    /// <summary>
    /// Gets an existing dir, or creates a new dir (including all dirs along the path).
    /// Throws if any segment along the path is an existing file.
    /// </summary>
    private bool TryGetEntry(BucketPath path, [NotNullWhen(true)] out IEntry? result)
    {
        if (path.Segments.Length == 0)
        {
            result = Root;
            return true;
        }

        var currentDir = Root;

        var remainingPath = (ReadOnlySpan<string>)[.. path.Segments];
        while (remainingPath.Length > 1)
        {
            var k = remainingPath[0];
            lock (currentDir.Items)
            {
                if (currentDir.Items.TryGetValue(k, out var e))
                {
                    // entry exists ...

                    if (e is EntryDir x)
                    {
                        currentDir = x;
                        remainingPath = Tail(remainingPath);
                    }
                    else
                    {
                        throw new Exception($"Invalid path. Expected a directory, but found {e.GetType().Name}. Error 02f00fd7-01b0-4292-ab58-74084ad6a30c.");
                    }
                }
                else
                {
                    // entry does not exist ...

                    result = null;
                    return false;
                }
            }
        }

        var key = path.LastSegmentAsString;
        lock (currentDir.Items)
        {
            return currentDir.Items.TryGetValue(key, out result);
        }
    }

    /// <summary>
    /// Gets an existing dir, or creates a new dir (including all dirs along the path).
    /// Throws if any segment along the path is an existing file.
    /// </summary>
    private EntryDir GetOrCreateDir(BucketPath path)
    {
        if (path.Segments.Length == 0) return Root;

        var d = Root;
        var p = (ReadOnlySpan<string>)[.. path.Segments];
        while (p.Length > 0)
        {
            var k = p[0];
            lock (d.Items)
            {
                if (d.Items.TryGetValue(k, out var e))
                {
                    // entry exists ...
                    if (e is EntryDir x)
                    {
                        d = x;
                        p = Tail(p);
                    }
                    else
                    {
                        throw new Exception($"Expected a directory, but found {e.GetType().Name}. Error b40f8a43-c9b1-421f-a3ff-51367634f58d.");
                    }
                }
                else
                {
                    // entry does not exist ...
                    var newDir = new EntryDir(d.Path + k, []);
                    d.Items[k] = newDir;
                    d = newDir;
                    p = Tail(p);
                }
            }
        }

        return d;
    }

    /// <summary>
    /// Gets an existing dir, or creates a new dir (including all dirs along the path).
    /// Throws if any segment along the path is an existing file.
    /// </summary>
    private bool TryGetDir(BucketPath path, [NotNullWhen(true)] out EntryDir? dir)
    {
        if (path.Segments.Length == 0)
        {
            dir = Root;
            return true;
        }

        var d = Root;
        var p = (ReadOnlySpan<string>)[.. path.Segments];
        while (p.Length > 0)
        {
            var k = p[0];
            lock (d.Items)
            {
                if (d.Items.TryGetValue(k, out var e))
                {
                    // entry exists ...
                    if (e is EntryDir x)
                    {
                        d = x;
                        p = Tail(p);
                    }
                    else
                    {
                        throw new Exception($"Expected directory but found {e.GetType().Name}. Error 0c05173c-d23d-4091-b3dc-bd9f7b598a8e.");
                    }
                }
                else
                {
                    dir = null;
                    return false;
                }
            }
        }

        dir = d;
        return true;
    }

    #endregion
}
