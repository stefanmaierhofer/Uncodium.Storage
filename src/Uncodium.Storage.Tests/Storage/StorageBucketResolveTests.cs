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

namespace Uncodium.Storage.Tests;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable CA1822 // Mark members as static

internal class StorageBucketResolveTests
#pragma warning restore IDE0079 // Remove unnecessary suppression
{
    [Test]
    public async Task CanParsePermissionsFromUri()
    {
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=a")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=d")).Permissions == Permissions.Delete);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=l")).Permissions == Permissions.List);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=r")).Permissions == Permissions.Read);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=w")).Permissions == Permissions.Write);


        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=rw")).Permissions == (Permissions.Read | Permissions.Write));
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=rwdl")).Permissions == Permissions.All);
    }

    [Test]
    public async Task CanResolveIStorageBucket()
    {
        var o = new InMemoryStorageBucket(Permissions.All);
        var bucket = await StorageBucket.ResolveAsync(o);

        Assert.That(bucket is InMemoryStorageBucket);
        Assert.That(bucket.Permissions == Permissions.All);
    }

    [Test]
    public async Task CanResolveUri_InMemory()
    {
        var o = "inmemory:?p=rl";
        var bucket = await StorageBucket.ResolveAsync(o);

        Assert.That(bucket is InMemoryStorageBucket);
        Assert.That(bucket.Permissions == (Permissions.Read | Permissions.List));
    }

    [Test]
    public async Task CanResolveUri_InMemory_Permissions()
    {
        Assert.That((await StorageBucket.ResolveAsync("inmemory")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory://")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=")).Permissions == Permissions.None);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=r")).Permissions == Permissions.Read);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=a")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory:?p=rwl")).Permissions == (Permissions.Read | Permissions.Write | Permissions.List));
        Assert.That((await StorageBucket.ResolveAsync("inmemory://?p=")).Permissions == Permissions.None);
        Assert.That((await StorageBucket.ResolveAsync("inmemory://?p=r")).Permissions == Permissions.Read);
        Assert.That((await StorageBucket.ResolveAsync("inmemory://?p=a")).Permissions == Permissions.All);
        Assert.That((await StorageBucket.ResolveAsync("inmemory://?p=rwl")).Permissions == (Permissions.Read | Permissions.Write | Permissions.List));
    }

    [Test]
    public async Task CanResolveUri_FileSystem()
    {
        var path = Path.GetFullPath(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
        try
        {
            var o = $"file://{path}?p=rwl";
            var bucket = await StorageBucket.ResolveAsync(o);

            Assert.That(bucket is FileSystemStorageBucket);
            Assert.That(bucket.Permissions == (Permissions.Read | Permissions.Write | Permissions.List));
        }
        finally
        {
            Directory.Delete(path);
        }
    }

    [Test]
    public async Task CanResolveUri_FileSystem_DefaultPermissions()
    {
        var path = Path.GetFullPath(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
        try
        {
            var o = $"file://{path}";
            var bucket = await StorageBucket.ResolveAsync(o);

            Assert.That(bucket is FileSystemStorageBucket);
            Assert.That(bucket.Permissions == Permissions.All);
        }
        finally
        {
            Directory.Delete(path);
        }
    }

    private record Foo(object Bucket);

    [Test]
    public async Task CanResolveJsonElement()
    {
        var o = new Foo(Bucket: "inmemory:?p=rl").ToJsonString().FromJson<Foo>();
        var bucket = await StorageBucket.ResolveAsync(o!.Bucket);

        Assert.That(bucket is InMemoryStorageBucket);
        Assert.That(bucket.Permissions == (Permissions.Read | Permissions.List));
    }
}