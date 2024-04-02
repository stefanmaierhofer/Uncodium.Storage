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

using System.Text;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable CA1822 // Mark members as static

namespace Uncodium.Storage.Tests;

internal class IStorageBucketTests
{
    #region test data sources

    private const string TMP_PATH = "tmp/IStorageBucketTests";
    private static readonly string PATH_PREFIX =
            $"{DateTimeOffset.UtcNow.Year:0000}{DateTimeOffset.UtcNow.Month:00}{DateTimeOffset.UtcNow.Day:00}/" +
            $"{DateTimeOffset.UtcNow.Hour:00}{DateTimeOffset.UtcNow.Minute:00}{DateTimeOffset.UtcNow.Second:00}";
    
    private static readonly Uri AZURE_SAS = new(
        "https://scratchsm.blob.core.windows.net/storagebuckettests" +
        "?sv=2023-01-03&st=2024-01-29T18%3A46%3A58Z&se=2025-01-30T18%3A46%3A00Z&sr=c" +
        "&sp=racwdxltf&sig=0luD7xRWqKySUcsKYy62tOAm7vhNB3mR7CjF3vmds6U%3D"
        );
    private static long AZURE_TEST_COUNT = 0L;

    public static IEnumerable<IStorageBucket> Buckets()
    {
        {
            yield return new InMemoryStorageBucket(Permissions.All);
        }

        {
            var basedir = Path.GetFullPath(Path.Combine(TMP_PATH, PATH_PREFIX, Path.GetRandomFileName()));
            var bucket = new FileSystemStorageBucket(basedir, Permissions.All);
            yield return bucket;
        }

        {
            var i = Interlocked.Increment(ref AZURE_TEST_COUNT);
            yield return new AzureStorageBucket(AZURE_SAS, new BucketPath(PATH_PREFIX) + $"{i}");
        }
    }

    #endregion

    #region create

    [Test]
    public void CreateInMemory()
    {
        _ = new InMemoryStorageBucket(Permissions.None);
        _ = new InMemoryStorageBucket(Permissions.All);
        _ = new InMemoryStorageBucket(Permissions.Read | Permissions.List);
    }

    #endregion

    #region permissions

    [Test, DataSource(nameof(Buckets))]
    public async Task ReadFailsIfNoReadPermission(IStorageBucket fs)
    {
        fs = await fs.WithPermissionsAsync(Permissions.All & ~Permissions.Read);

        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadAsync(file: "foo");
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task WriteFailsIfNoWritePermission(IStorageBucket fs)
    {
        fs = await fs.WithPermissionsAsync(Permissions.All & ~Permissions.Write);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);
        });
    }

    #endregion

    #region WriteAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrReplace_FileDoesNotExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_Cancelled(IStorageBucket fs)
    {
        var cts = new CancellationTokenSource(); cts.Cancel();
        await Assert.ThrowsAsync(async () =>
        {
            await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace, cts.Token);
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrReplace_FileDoesNotExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrReplace_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "foo", data: "replacement", writeMode: WriteMode.CreateOrReplace);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrReplace_FileDoesExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "foo/bar", data: "replacement", writeMode: WriteMode.CreateOrReplace);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrFail_FileDoesNotExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrFail);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrFail_FileDoesNotExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrFail);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrFail_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrFail);
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_CreateOrFail_FileDoesExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrFail);
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrCreate_FileDoesNotExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrCreate_FileDoesNotExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrCreate_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.AppendOrCreate);
        await fs.WriteAsync(file: "foo", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrCreate_FileDoesExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.AppendOrCreate);
        await fs.WriteAsync(file: "foo/bar", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrFail_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.AppendOrCreate);
        await fs.WriteAsync(file: "foo", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Write_AppendOrFail_FileDoesExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.AppendOrCreate);
        await fs.WriteAsync(file: "foo/bar", data: "append", writeMode: WriteMode.AppendOrCreate);
    }

    //[Test, DataSource(nameof(Buckets))]
    //public async Task Write_AppendOrFail_FileDoesNotExist(IStorageBucket fs)
    //{
    //    await Assert.ThrowsAsync(async () =>
    //    {
    //        await fs.WriteAsync(file: "foo", data: "append", writeMode: WriteMode.AppendOrFail);
    //    });
    //}

    //[Test, DataSource(nameof(Buckets))]
    //public async Task Write_AppendOrFail_FileDoesNotExist_Recursive(IStorageBucket fs)
    //{
    //    await Assert.ThrowsAsync(async () =>
    //    {
    //        await fs.WriteAsync(file: "foo/bar", data: "append", writeMode: WriteMode.AppendOrFail);
    //    });
    //}

    #endregion

    #region ExistsAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task ExistsAsync_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);
        Assert.True(await fs.ExistsAsync(file: "foo"));
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ExistsAsync_FileDoesExist_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);
        Assert.True(await fs.ExistsAsync(file: "foo/bar"));
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ExistsAsync_FileDoesNotExist(IStorageBucket fs)
    {
        Assert.False(await fs.ExistsAsync(file: "foo"));
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ExistsAsync_FileDoesNotExist_Recursive(IStorageBucket fs)
    {
        Assert.False(await fs.ExistsAsync(file: "foo/bar"));
    }

    #endregion

    #region ReadAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task Read_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);

        var dataReadBack = await fs.ReadAsync(file: "foo");
        Assert.True(Encoding.UTF8.GetString(dataReadBack) == "data");
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task Read_FileDoesNotExist(IStorageBucket fs)
    {
        await Assert.ThrowsAsync(async () =>
        {
            var dataReadBack = await fs.ReadAsync(file: "foo");
        });
    }

    #endregion

    #region ReadSliceAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task ReadSliceAsync_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "0123456789ABCDEF", writeMode: WriteMode.CreateOrReplace);

        Assert.True(Encoding.UTF8.GetString(await fs.ReadSliceAsync(file: "foo", offset: 0, size: 16)) == "0123456789ABCDEF");
        Assert.True(Encoding.UTF8.GetString(await fs.ReadSliceAsync(file: "foo", offset: 0, size: 4)) == "0123");
        Assert.True(Encoding.UTF8.GetString(await fs.ReadSliceAsync(file: "foo", offset: 8, size: 4)) == "89AB");
        Assert.True(Encoding.UTF8.GetString(await fs.ReadSliceAsync(file: "foo", offset: 10, size: 6)) == "ABCDEF");
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ReadSliceAsync_FileDoesNotExist(IStorageBucket fs)
    {
        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadSliceAsync(file: "foo", offset: 0, size: 16);
        });
    }

    #endregion

    #region DeleteAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_FileDoesExist(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo", data: "data", writeMode: WriteMode.CreateOrReplace);

        await fs.DeleteAsync(path: "foo", recursive: false);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadAsync(file: "foo");
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_Dir_NotEmpty_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);

        await fs.DeleteAsync(path: "foo", recursive: true);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadAsync(file: "foo/bar");
        });
    }


    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_Dir_NotEmpty_NotRecursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);

        await Assert.ThrowsAsync(async () =>
        {
            await fs.DeleteAsync(path: "foo", recursive: false); // delete non-empty dir with recursive=false will fail
        });

        await fs.ReadAsync(file: "foo/bar"); // can still read file, because delete failed
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_Dir_Empty_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);

        await fs.DeleteAsync(path: "foo/bar", recursive: true); // delete file -> leaves directory "foo" empty
        await fs.DeleteAsync(path: "foo", recursive: true); // can delete empty dir with recursive = true

        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadAsync(file: "foo/bar");
        });
    }


    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_Dir_Empty_NotRecursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "foo/bar", data: "data", writeMode: WriteMode.CreateOrReplace);

        await fs.DeleteAsync(path: "foo/bar", recursive: true); // delete file -> leaves directory "foo" empty
        await fs.DeleteAsync(path: "foo", recursive: false); // can delete empty dir with recursive=false

        await Assert.ThrowsAsync(async () =>
        {
            await fs.ReadAsync(file: "foo/bar");
        });
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task DeleteAsync_PathDoesNotExist(IStorageBucket fs)
    {
        await fs.DeleteAsync(path: "foo", recursive: false);
    }

    #endregion

    #region ListAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_EmptyRoot_NonRecursive(IStorageBucket fs)
    {
        var xs = await fs.ListAsync("/", recursive: false).ToListAsync();
        Assert.IsEmpty(xs);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_EmptyRoot_Recursive(IStorageBucket fs)
    {
        var xs = await fs.ListAsync("/", recursive: true).ToListAsync();
        Assert.IsEmpty(xs);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_RootWithSingleFile_NonRecursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "a", data: "data a", writeMode: WriteMode.CreateOrReplace);

        var xs = await fs.ListAsync("/", recursive: false).ToListAsync();
        Assert.True(xs.Count == 1);

        var x = xs.Single();
        Assert.True(x.IsDirectory == false);
        Assert.True(x.Path == "/a");
        Assert.True(x.SizeInBytes == 6);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_RootWithSingleFile_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "a", data: "data a", writeMode: WriteMode.CreateOrReplace);

        var xs = await fs.ListAsync("/", recursive: true).ToListAsync();
        Assert.True(xs.Count == 1);

        var x = xs.Single();
        Assert.True(x.IsDirectory == false);
        Assert.True(x.Path == "/a");
        Assert.True(x.SizeInBytes == 6);
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_RootWithManyFiles_NonRecursive(IStorageBucket fs)
    {
        for (var i = 0; i < 10; i++)
        {
            await fs.WriteAsync(file: $"{i}", data: $"data {1}", writeMode: WriteMode.CreateOrReplace);
        }

        var xs = await fs.ListAsync("/", recursive: false).ToListAsync();
        Assert.True(xs.Count == 10);

        for (var i = 0; i < xs.Count; i++)
        {
            var x = xs[i];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == $"/{i}");
            Assert.True(x.SizeInBytes == 6);
        }
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_RootWithManyFiles_Recursive(IStorageBucket fs)
    {
        for (var i = 0; i < 10; i++)
        {
            await fs.WriteAsync(file: $"{i}", data: $"data {1}", writeMode: WriteMode.CreateOrReplace);
        }

        var xs = await fs.ListAsync("/", recursive: true).ToListAsync();
        Assert.True(xs.Count == 10);

        for (var i = 0; i < xs.Count; i++)
        {
            var x = xs[i];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == $"/{i}");
            Assert.True(x.SizeInBytes == 6);
        }
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_Hierarchy_NonRecursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "a", data: "data a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/a", data: "data d/a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/b", data: "data d/b", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/d/a", data: "data d/d/a", writeMode: WriteMode.CreateOrReplace);

        var xs = (await fs.ListAsync("/", recursive: false).ToListAsync()).ToDictionary(x => x.Path.ToString());
        Assert.True(xs.Count == 2);

        {
            var x = xs["/a"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/a");
            Assert.True(x.SizeInBytes == 6);
        }
        {
            var x = xs["/d"];
            Assert.True(x.IsDirectory == true);
            Assert.True(x.Path == "/d");
            Assert.True(x.SizeInBytes == null);
        }
    }

    [Test, DataSource(nameof(Buckets))]
    public async Task ListAsync_Hierarchy_Recursive(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "a", data: "data a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/a", data: "data d/a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/b", data: "data d/b", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/d/a", data: "data d/d/a", writeMode: WriteMode.CreateOrReplace);

        var xs = (await fs.ListAsync("/", recursive: true).ToListAsync()).ToDictionary(x => x.Path.ToString());
        Assert.True(xs.Count == 4);

        {
            var x = xs["/a"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/a");
            Assert.True(x.SizeInBytes == 6);
        }
        //{
        //    var x = xs["/d"];
        //    Assert.True(x.IsDirectory == true);
        //    Assert.True(x.Path == "/d");
        //    Assert.True(x.SizeInBytes == null);
        //}
        {
            var x = xs["/d/a"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/d/a");
            Assert.True(x.SizeInBytes == 8);
        }
        {
            var x = xs["/d/b"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/d/b");
            Assert.True(x.SizeInBytes == 8);
        }
        //{
        //    var x = xs["/d/d"];
        //    Assert.True(x.IsDirectory == true);
        //    Assert.True(x.Path == "/d/d");
        //    Assert.True(x.SizeInBytes == null);
        //}
        {
            var x = xs["/d/d/a"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/d/d/a");
            Assert.True(x.SizeInBytes == 10);
        }
    }

    #endregion

    #region ChangeDirAsync

    [Test, DataSource(nameof(Buckets))]
    public async Task ChangeDirAsync(IStorageBucket fs)
    {
        await fs.WriteAsync(file: "a", data: "data a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/a", data: "data d/a", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/b", data: "data d/b", writeMode: WriteMode.CreateOrReplace);
        await fs.WriteAsync(file: "d/d/a", data: "data d/d/a", writeMode: WriteMode.CreateOrReplace);

        var cd = await fs.ChangeDirAsync("d");

        var xs = (await cd.ListAsync("/", recursive: false).ToListAsync()).ToDictionary(x => x.Path.ToString()); ;
        Assert.True(xs.Count == 3);

        {
            var x = xs["/a"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/a");
            Assert.True(x.SizeInBytes == 8);
        }
        {
            var x = xs["/b"];
            Assert.True(x.IsDirectory == false);
            Assert.True(x.Path == "/b");
            Assert.True(x.SizeInBytes == 8);
        }
        {
            var x = xs["/d"];
            Assert.True(x.IsDirectory == true);
            Assert.True(x.Path == "/d");
            Assert.True(x.SizeInBytes == null);
        }
    }

    #endregion
}