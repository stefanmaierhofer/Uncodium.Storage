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

internal class PathTests
{
    private static BucketPath P(string s) => new(s);

    [Test]
    public void Empty()
    {
        var e0 = BucketPath.Empty;
        Assert.True(e0.IsEmpty);
        Assert.True(e0.ToString() == string.Empty);

        var e1 = P(null!);
        Assert.True(e1.IsEmpty);
        Assert.True(e1.ToString() == string.Empty);

        var e2 = P(string.Empty);
        Assert.True(e2.IsEmpty);
        Assert.True(e2.ToString() == string.Empty);
    }

    [Test]
    public void Empty_Whitespace()
    {
        var e0 = P(" ");
        Assert.False(e0.IsEmpty);
        Assert.True(e0.ToString() == " ");

        var e1 = P("   \t  \t\t  \n   \r    ");
        Assert.False(e1.IsEmpty);
        Assert.True(e1.ToString() == "   \t  \t\t  \n   \r    ");
    }

    [Test]
    public void SingleSegment()
    {
        var p = P("foo");
        Assert.False(p.IsEmpty);
        Assert.True(p.ToString() == "foo");
    }

    [Test]
    public void SingleSegment_WithPreOrPostWhitespace()
    {
        var p0 = P(" \t \n \r foo");
        Assert.False(p0.IsEmpty);
        Assert.True(p0.ToString() == " \t \n \r foo");

        var p1 = P("foo   \t \n\r ");
        Assert.False(p1.IsEmpty);
        Assert.True(p1.ToString() == "foo   \t \n\r ");

        var p2 = P(" \t \n \r foo   \t \n\r ");
        Assert.False(p2.IsEmpty);
        Assert.True(p2.ToString() == " \t \n \r foo   \t \n\r ");
    }

    [Test]
    public void TwoSegments()
    {
        Assert.True(P(@"foo/bar") == "foo/bar");
        Assert.True(P(@"foo\bar") == "foo/bar");
        Assert.True(P(@"foo\bar") == "foo/bar");
        Assert.True(P(@"/foo/bar") == "/foo/bar");
        Assert.True(P(@"\foo/bar") == "/foo/bar");
        Assert.True(P(@"/foo\bar") == "/foo/bar");
        Assert.True(P(@"\foo\bar") == "/foo/bar");
    }

    [Test]
    public void TrailingSlashesAreIgnored()
    {
        Assert.True(P(@"foo/bar/") == "foo/bar");
        Assert.True(P(@"foo\bar/") == "foo/bar");
        Assert.True(P(@"foo\bar/") == "foo/bar");
        Assert.True(P(@"/foo/bar/") == "/foo/bar");
        Assert.True(P(@"\foo/bar/") == "/foo/bar");
        Assert.True(P(@"/foo\bar/") == "/foo/bar");
        Assert.True(P(@"\foo\bar/") == "/foo/bar");


        Assert.True(P(@"foo/bar\") == "foo/bar");
        Assert.True(P(@"foo\bar\") == "foo/bar");
        Assert.True(P(@"foo\bar\") == "foo/bar");
        Assert.True(P(@"/foo/bar\") == "/foo/bar");
        Assert.True(P(@"\foo/bar\") == "/foo/bar");
        Assert.True(P(@"/foo\bar\") == "/foo/bar");
        Assert.True(P(@"\foo\bar\") == "/foo/bar");
    }

    [Test]
    public void ConsecutiveSlashesAreRemoved()
    {
        Assert.True(P(@"foo//bar") == "foo/bar");
        Assert.True(P(@"foo///bar") == "foo/bar");
        Assert.True(P(@"foo\\bar") == "foo/bar");
        Assert.True(P(@"foo\\\bar") == "foo/bar");

        Assert.True(P(@"f//b") == "f/b");
        Assert.True(P(@"f///b") == "f/b");
        Assert.True(P(@"f\\b") == "f/b");
        Assert.True(P(@"f\\\b") == "f/b");

        Assert.True(P(@"////f//b/////").ToString() == "/f/b");
        Assert.True(P(@"\/\/f//b\/\/\").ToString() == "/f/b");

        Assert.True(P(@"a////////////////////b").ToString() == "a/b");
        Assert.True(P(@"////a////////////////////b/////").ToString() == "/a/b");
        Assert.True(P(@"\/\/a////////////////////b\/\/\").ToString() == "/a/b");
    }

    [Test]
    public void Comparisons()
    {
        var empty = BucketPath.Empty;

        Assert.True(P(@"foo") == new BucketPath(@"foo"));
        Assert.True(P(@"foo") != new BucketPath(@"bar"));

        Assert.True(P(@"foo") == @"foo");
        Assert.True(P(@"foo") != @"bar");

        Assert.True("foo" == P(@"foo"));
        Assert.True("foo" != P(@"bar"));

#pragma warning disable CS1718 // Comparison made to same variable
        Assert.True(empty == empty);
        Assert.False(empty != empty);
#pragma warning restore CS1718 // Comparison made to same variable


        Assert.True(empty == (string?)null);
        Assert.True(empty == "");
        Assert.True((string?)null == empty);
        Assert.True("" == empty);

        Assert.False(empty != (string?)null);
        Assert.False(empty != "");
        Assert.False((string?)null != empty);
        Assert.False("" != empty);
    }

    [Test]
    public void OperatorPlus()
    {
        var empty = BucketPath.Empty;

        Assert.True(P(@"foo") + P(@"bar") == "foo/bar");
        Assert.True(P(@"foo") + empty == "foo");
        Assert.True(empty + P(@"bar") == "bar");
        Assert.True(empty + empty == empty);
    }

    [Test]
    public void SegmentCount()
    {
        var empty = BucketPath.Empty;

        Assert.True(empty.Segments.Length == 0);
        Assert.True(P("").Segments.Length == 0);
        Assert.True(P("foo").Segments.Length == 1);
        Assert.True(P("foo/bar").Segments.Length == 2);
        Assert.True(P("foo/bar/haha").Segments.Length == 3);

        Assert.True((P("foo") + P("bar")).Segments.Length == 2);
        Assert.True((P("foo") + P("bar") + P("haha")).Segments.Length == 3);
    }

    [Test]
    public void LastSegment()
    {
        var empty = BucketPath.Empty;

        Assert.True(empty.LastSegment == empty);
        Assert.True(P("").LastSegment == empty);
        Assert.True(P("foo").LastSegment == "foo");
        Assert.True(P("foo/bar").LastSegment == "bar");
        Assert.True(P("foo/bar/haha").LastSegment == "haha");

        Assert.True((P("foo") + P("bar")).LastSegment == "bar");
        Assert.True((P("foo") + P("bar") + P("haha")).LastSegment == "haha");
    }

    [Test]
    public void LastSegmentWithoutExtension()
    {
        var empty = BucketPath.Empty;

        Assert.True(empty.LastSegmentWithoutExtension == empty);
        Assert.True(P("").LastSegmentWithoutExtension == empty);
        Assert.True(P("foo").LastSegmentWithoutExtension == "foo");
        Assert.True(P("foo/bar").LastSegmentWithoutExtension == "bar");
        Assert.True(P("foo/bar/haha").LastSegmentWithoutExtension == "haha");

        Assert.True((P("foo") + P("bar")).LastSegmentWithoutExtension == "bar");
        Assert.True((P("foo") + P("bar") + P("haha")).LastSegmentWithoutExtension == "haha");


        //var debug = p("foo.json").LastSegmentWithoutExtension;
        Assert.True(P("foo.json").LastSegmentWithoutExtension == "foo");
        Assert.True(P("foo/bar.json").LastSegmentWithoutExtension == "bar");
        Assert.True(P("foo/bar/haha").LastSegmentWithoutExtension == "haha");

        Assert.True((P("foo") + P("bar.json")).LastSegmentWithoutExtension == "bar");
        Assert.True((P("foo") + P("bar") + P("haha.json")).LastSegmentWithoutExtension == "haha");
    }

    [Test]
    public void GetPathWithoutLastSegment()
    {
        Assert.True(P("foo/bar/woohoo").GetPathWithoutLastSegment() == "foo/bar");
        Assert.True(P("/foo/bar/woohoo").GetPathWithoutLastSegment() == "/foo/bar");
    }

    [Test]
    public void CastFromString()
    {
        BucketPath p = (BucketPath)"foo/bar";
        Assert.True(p.ToString() == "foo/bar");
    }

    [Test]
    public void CastFromSpan()
    {
        ReadOnlySpan<char> span = "foo/bar".AsSpan();
        BucketPath p = (BucketPath)span;
        Assert.True(p.ToString() == "foo/bar");
    }

    [Test]
    public void CastToString()
    {
        BucketPath p = (BucketPath)"foo/bar";
        var s = p.ToString();
        Assert.True(s == "foo/bar");
    }

    [Test]
    public void Length()
    {
        string s = "foo/bar";
        BucketPath p = (BucketPath)s;
        Assert.True(p.ToString().Length == s.Length);
    }

    [Test]
    public void SegmentNamesConistingOnlyOfDotsAreInvalid()
    {
        Assert.Throws(() => P("."));
        Assert.Throws(() => P(".."));
        Assert.Throws(() => P("..."));
        Assert.Throws(() => P("foo/."));
        Assert.Throws(() => P("foo/.."));
        Assert.Throws(() => P("./foo"));
        Assert.Throws(() => P("../foo"));
        Assert.Throws(() => P("foo/./bar"));
        Assert.Throws(() => P("foo/../bar"));
    }
}

