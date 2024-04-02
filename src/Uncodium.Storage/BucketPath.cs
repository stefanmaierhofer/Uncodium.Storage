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
using System.Text.Json.Serialization;

namespace Uncodium.Storage;

public enum PathKind
{
    Relative = 0,
    Absolute = 1,
}

public class BucketPath : IEquatable<BucketPath>, IEquatable<string>
{
    public static readonly BucketPath Empty = new([], PathKind.Relative);
    public static readonly BucketPath Root = new([], PathKind.Absolute);

    public string[] Segments { get; }
    public PathKind Kind { get; }

    [JsonIgnore]
    public bool IsEmpty => Kind == PathKind.Relative && Segments.Length == 0;
    [JsonIgnore]
    public bool IsRoot => Kind == PathKind.Absolute && Segments.Length == 0;
    [JsonIgnore]
    public bool IsAbsolute => Kind == PathKind.Absolute;
    [JsonIgnore]
    public bool IsRelative => Kind == PathKind.Relative;

    [JsonConstructor]
    public BucketPath(string[] segments, PathKind kind)
    {
        Segments = segments;
        Kind = kind;

        Check(Segments);
    }

    public BucketPath(string? s) : this(s != null ? s.AsSpan() : []) { }

    public BucketPath(ReadOnlySpan<char> s)
    {
        if (s.Length == 0)
        {
            Kind = PathKind.Relative;
            Segments = [];
        }
        else
        {
            if (s[0] == '/' || s[0] == '\\')
            {
                Kind = PathKind.Absolute;
#if NETSTANDARD2_0
                Segments = ReplaceBackslashesWithSlashes(s.Slice(1)).Split(['/'], StringSplitOptions.RemoveEmptyEntries);
#else
                Segments = new string(s[1..]).Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
#endif
            }
            else
            {
                Kind = PathKind.Relative;
#if NETSTANDARD2_0
                Segments = ReplaceBackslashesWithSlashes(s).Split(['/'], StringSplitOptions.RemoveEmptyEntries);
#else
                Segments = new string(s).Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
#endif
            }

            Check(Segments);
        }

#if NETSTANDARD2_0
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static string ReplaceBackslashesWithSlashes(ReadOnlySpan<char> s)
        {
            unsafe
            {
                fixed (char* p = s)
                {
                    var pEnd = p + s.Length;
                    var q = p;
                    while (q < pEnd)
                    {
                        if (*q == '\\') *q = '/';
                        ++q;
                    }

                    return new string(p, 0, s.Length);
                }
            }
        }
#endif
    }

    [JsonIgnore]
    public BucketPath LastSegment => Segments.Length > 0
#if NETSTANDARD2_0
        ? new([Segments[Segments.Length - 1]], PathKind.Relative)
#else
        ? new([Segments[^1]], PathKind.Relative)
#endif
        : Empty
        ;

    [JsonIgnore]
    public string LastSegmentAsString => Segments.Length > 0
#if NETSTANDARD2_0
        ? Segments[Segments.Length - 1].ToString()
#else
        ? Segments[^1].ToString()
#endif
        : string.Empty
        ;

    [JsonIgnore]
    public BucketPath LastSegmentWithoutExtension => Segments.Length > 0
#if NETSTANDARD2_0
        ? new([System.IO.Path.GetFileNameWithoutExtension(Segments[Segments.Length - 1])], PathKind.Relative)
#else
        ? new([System.IO.Path.GetFileNameWithoutExtension(Segments[^1])], PathKind.Relative)
#endif
        : Empty
        ;

    /// <summary>
    /// Returns new path with the first n segments skipped.
    /// Throws if path has less than n segments.
    /// </summary>
    public BucketPath SkipSegments(int n)
    {
        if (Segments.Length < n) throw new Exception($"Failed to skip {n} segments in path with {Segments.Length} segments. Error 3984c531-4357-421e-911a-537d4d0f62ce.");
        if (n == 0) return this;
#if NETSTANDARD2_0
        var newSegments = new string[Segments.Length - n];
        Array.Copy(Segments, n, newSegments, 0, newSegments.Length);
        return new(newSegments, Kind);
#else
        return new(Segments[n..], Kind);
#endif
    }

    /// <summary>
    /// Converts a relative path to an absolute path.
    /// If the path is already absolute, returns itself.
    /// </summary>
    public BucketPath ToAbsolute() => IsAbsolute ? this : new(Segments, PathKind.Absolute);

    /// <summary>
    /// Converts an absolute path to a relative path.
    /// If the path is already relative, returns itself.
    /// </summary>
    public BucketPath ToRelative() => IsRelative ? this : new(Segments, PathKind.Relative);

    /// <summary>
    /// Gets path without the last segment.
    /// Throws if path has no segments (root or empty).
    /// </summary>
    public BucketPath GetPathWithoutLastSegment()
    {
        if (Segments.Length == 0) throw new Exception($"Expected at least one path segment. Error 164a838d-b1bc-4231-bc54-321878547950.");
        if (Segments.Length == 1) return new([], Kind);

#if NETSTANDARD2_0
        var segments = new string[Segments.Length - 1];
        Array.Copy(Segments, segments, segments.Length);
        return new(segments, Kind);
#else
        return new(Segments[..^1], Kind);
#endif
    }

    public Uri ToUri()
    {
        if (Kind == PathKind.Relative)
        {
            return new Uri(ToString(), UriKind.Relative);
        }

        throw new NotImplementedException("TODO: needs authority/host");
    }

    public override string ToString() => Kind switch
    {
#if NETSTANDARD2_0
        PathKind.Relative => string.Join("/", Segments),
        PathKind.Absolute => '/' + string.Join("/", Segments),
#else
        PathKind.Relative => string.Join('/', Segments),
        PathKind.Absolute => '/' + string.Join('/', Segments),
#endif
        _ => throw new Exception($"Unknown PathKind.{Kind}. Error 0f1053dd-6908-4c0a-8a12-e5c31e61cb3d.")
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static explicit operator BucketPath(ReadOnlySpan<char> s)
        => new(s);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static explicit operator BucketPath(string? s)
        => s != null ? new(s.AsSpan()) : Empty;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static explicit operator string(BucketPath? p)
        => p?.ToString() ?? string.Empty;

    public static bool operator ==(BucketPath? x, BucketPath? y) => (x, y) switch
    {
        (not null, not null) => BucketPath.Equals(x, y),
        (not null, null) => x.IsEmpty,
        (null, not null) => y.IsEmpty,
        (null, null) => true
    };
    public static bool operator !=(BucketPath? x, BucketPath? y) => (x, y) switch
    {
        (not null, not null) => !BucketPath.Equals(x, y),
        (not null, null) => !x.IsEmpty,
        (null, not null) => !y.IsEmpty,
        (null, null) => false
    };

    public static bool operator ==(BucketPath? x, string? y) => BucketPath.Equals(x, new BucketPath(y));
    public static bool operator !=(BucketPath? x, string? y) => !BucketPath.Equals(x, new BucketPath(y));

    public static bool operator ==(string? x, BucketPath? y) => BucketPath.Equals(new BucketPath(x), y);
    public static bool operator !=(string? x, BucketPath? y) => !BucketPath.Equals(new BucketPath(x), y);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BucketPath operator +(BucketPath? x, BucketPath? y) => BucketPath.Concat(x, y);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BucketPath operator +(BucketPath? x, string? y) => x + new BucketPath(y);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BucketPath operator +(string? x, BucketPath? y) => new BucketPath(x) + y;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(BucketPath? other) => BucketPath.Equals(this, other);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(string? other) => BucketPath.Equals(this, new BucketPath(other) ?? Empty);

    public override bool Equals(object? obj) => obj switch
    {
        null => IsEmpty,
        BucketPath other => BucketPath.Equals(this, other),
        string other => BucketPath.Equals(this, new BucketPath(other)),
        _ => false
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode() => ToString().GetHashCode();

    public static bool Equals(BucketPath? a, BucketPath? b)
    {
        if (a is null) return b is null || b.IsEmpty;
        if (b is null) return a.IsEmpty;

        if (a.Kind != b.Kind) return false;
        if (a.Segments.Length != b.Segments.Length) return false;

        var imax = a.Segments.Length;
        for (var i = 0; i < imax; i++)
        {
            if (a.Segments[i] != b.Segments[i]) return false;
        }

        return true;
    }

    public static BucketPath Concat(BucketPath? a, BucketPath? b) => (a, b) switch
    {
        (not null, not null) => (a.IsEmpty, b.IsEmpty) switch
        {
            (false, false) => (a.Kind, b.Kind) switch
            {
                (PathKind.Absolute, PathKind.Relative) => new([.. a.Segments, .. b.Segments], PathKind.Absolute),

                (PathKind.Relative, PathKind.Relative) => new([.. a.Segments, .. b.Segments], PathKind.Relative),

                (PathKind.Absolute, PathKind.Absolute) => throw new Exception(
                    $"Can't concatenate absolute path \"{a}\" with absolute path \"{b}\". Error 6027a28d-b30e-4793-80b2-1d847f816cc1."
                    ),

                (PathKind.Relative, PathKind.Absolute) => throw new Exception(
                    $"Can't concatenate relative path \"{a}\" with absolute path \"{b}\". Error 28b6c84c-82e5-4a99-8fe3-a6243b737e15."
                    ),

                _ => throw new Exception($"Unknown kind PathKind.{a.Kind} or PathKind.{b.Kind}. Error a58a763c-6f9a-49ba-a7ee-053cf56104a0.")
            },
            (false, true) => a,
            (true, false) => b,
            (true, true) => Empty
        },
        (not null, null) => a,
        (null, not null) => b,
        (null, null) => Empty
    };

    private static void Check(in string[] segments)
    {
        foreach (var s in segments)
        {
            if (s.Length == 0) throw new Exception($"A path segment must not be empty. Error b942e6a4-292b-4a09-97f7-29fee5e82035.");
            if (OnlyDots(s)) throw new Exception($"A path segment must not contain only '.' characters. Error c4f0db84-c8fa-43cd-abe6-f7b8b6531ae3.");
        }

        static bool OnlyDots(in string s)
        {
            unsafe
            {
                fixed (char* p0 = s)
                {
                    var p = p0; var pmax = p0 + s.Length;
                    while (p < pmax) if (*p++ != '.') return false;
                    return true;
                }
            }
        }
    }
}
