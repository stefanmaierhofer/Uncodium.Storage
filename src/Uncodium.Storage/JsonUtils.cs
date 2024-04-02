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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Uncodium.Storage;

public static class JsonUtils
{
    #region ToJson

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static JsonElement ToJsonElement(this object o)
        => JsonSerializer.SerializeToElement(o, JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static JsonElement ToJsonElement(this object o, JsonSerializerOptions options)
#if NETSTANDARD2_0
    {
        var s = JsonSerializer.Serialize(o, options);
        var j = JsonSerializer.Deserialize<JsonElement>(s, options);
        return j;
    }
#else
        => JsonSerializer.SerializeToElement(o, options);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToJsonString(this object o)
        => JsonSerializer.Serialize(o, JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToJsonString(this object o, JsonSerializerOptions options)
        => JsonSerializer.Serialize(o, options);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task ToJsonStreamAsync(this object o, Stream stream, Ct ct = default)
        => JsonSerializer.SerializeAsync(stream, o, JsonOptions, cancellationToken: ct);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task ToJsonFileAsync(this object o, string filename)
    {
        var fullPath = Path.GetFullPath(filename);
        var dir = Path.GetDirectoryName(fullPath)!;
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
        using var f = File.Open(filename, FileMode.Create, FileAccess.Write, FileShare.None);
        await JsonSerializer.SerializeAsync(f, o, JsonOptions);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task ToJsonFileAsync<T>(this T o, string filename)
    {
        var fullPath = Path.GetFullPath(filename);
        var dir = Path.GetDirectoryName(fullPath)!;
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
        using var f = File.Open(filename, FileMode.Create, FileAccess.Write, FileShare.None);
        await JsonSerializer.SerializeAsync<T>(f, o, JsonOptions);
    }

    #endregion

    #region FromJson

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? FromJson<T>(this string s)
        => JsonSerializer.Deserialize<T>(s, JsonOptions);

    /// <summary>
    /// 
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? TryFromJson<T>(this string s)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(s, JsonOptions);
        }
        catch
        {
            return default;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? FromJson(this string s, Type type)
        => JsonSerializer.Deserialize(s, type, JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? FromJson<T>(this JsonElement self)
        => self.Deserialize<T>(JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? FromJson(this JsonElement self, Type type)
        => self.Deserialize(type, JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> FromJsonStreamAsync<T>(this Stream? stream, Ct ct = default)
        => stream != null ? JsonSerializer.DeserializeAsync<T>(stream, JsonOptions, cancellationToken: ct) : default;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<object?> FromJsonStreamAsync(this Stream? stream, Type type, Ct ct = default)
        => stream != null ? JsonSerializer.DeserializeAsync(stream, type, JsonOptions, cancellationToken: ct) : default;

    #endregion

    #region Deserialize

    /// <summary>
    /// Makes a T from the given object, throws if not possible.
    /// Object may be a T, a JsonElement or a JSON string.
    /// </summary>
    public static T Deserialize<T>(this object o)
    {
        switch (o)
        {
            case T c: return c;
            case JsonElement json:
                {
                    var c = json.Deserialize<T>(JsonOptions);
                    return c ?? throw new Exception($"Failed to deserialize to {typeof(T).FullName}: {o}. Error 8ef1b60c-9738-4e53-ac0c-4fe5758d758c.");
                }
            case string json:
                {
                    return FromJson<T>(json) ?? throw new($"Failed to deserialize to {typeof(T).FullName}: {json}. Error 4c7cb28d-a093-4156-a7c7-ff1393018f00.");
                }
            default:
                throw new Exception($"Failed to convert type \"{o?.GetType()}\" to \"{typeof(T)}\". Error 1c12abcc-b78a-47d4-b9ef-4364a5871329.");
        }
    }

    #endregion

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        AllowTrailingCommas = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals | JsonNumberHandling.AllowReadingFromString,
        Converters =
        {
            new JsonStringEnumConverter(),
        },
    };
}