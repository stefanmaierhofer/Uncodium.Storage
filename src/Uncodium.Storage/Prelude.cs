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

global using System;
global using System.Collections.Generic;
global using System.Threading;
global using System.Threading.Tasks;

global using Ct = System.Threading.CancellationToken;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable IDE0060 // Remove unused parameter
#pragma warning disable IDE0290 // Use primary constructor

#if NETSTANDARD2_0

namespace System.Runtime.CompilerServices
{
    internal class IsExternalInit { }
}

namespace System.Diagnostics.CodeAnalysis
{
    //
    // Summary:
    //     Specifies that when a method returns System.Diagnostics.CodeAnalysis.NotNullWhenAttribute.ReturnValue,
    //     the parameter will not be null even if the corresponding type allows it.
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    internal sealed class NotNullWhenAttribute : Attribute
    {
        //
        // Summary:
        //     Initializes the attribute with the specified return value condition.
        //
        // Parameters:
        //   returnValue:
        //     The return value condition. If the method returns this value, the associated
        //     parameter will not be null.
        public NotNullWhenAttribute(bool returnValue) { }

        //
        // Summary:
        //     Gets the return value condition.
        //
        // Returns:
        //     The return value condition. If the method returns this value, the associated
        //     parameter will not be null.
        public bool ReturnValue { get; }
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Property | AttributeTargets.ReturnValue, AllowMultiple = true, Inherited = false)]
#if SYSTEM_PRIVATE_CORELIB
    public
#else
    internal
#endif
        sealed class NotNullIfNotNullAttribute : Attribute
    {
        /// <summary>Initializes the attribute with the associated parameter name.</summary>
        /// <param name="parameterName">
        /// The associated parameter name.  The output will be non-null if the argument to the parameter specified is non-null.
        /// </param>
        public NotNullIfNotNullAttribute(string parameterName) => ParameterName = parameterName;

        /// <summary>Gets the associated parameter name.</summary>
        public string ParameterName { get; }
    }

    /// <summary>Specifies that the method or property will ensure that the listed field and property members have not-null values when returning with the specified return value condition.</summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property, Inherited = false, AllowMultiple = true)]
#if SYSTEM_PRIVATE_CORELIB
    public
#else
    internal
#endif
        sealed class MemberNotNullWhenAttribute : Attribute
    {
        /// <summary>Initializes the attribute with the specified return value condition and a field or property member.</summary>
        /// <param name="returnValue">
        /// The return value condition. If the method returns this value, the associated parameter will not be null.
        /// </param>
        /// <param name="member">
        /// The field or property member that is promised to be not-null.
        /// </param>
        public MemberNotNullWhenAttribute(bool returnValue, string member)
        {
            ReturnValue = returnValue;
            Members = [member];
        }

        /// <summary>Initializes the attribute with the specified return value condition and list of field and property members.</summary>
        /// <param name="returnValue">
        /// The return value condition. If the method returns this value, the associated parameter will not be null.
        /// </param>
        /// <param name="members">
        /// The list of field and property members that are promised to be not-null.
        /// </param>
        public MemberNotNullWhenAttribute(bool returnValue, params string[] members)
        {
            ReturnValue = returnValue;
            Members = members;
        }

        /// <summary>Gets the return value condition.</summary>
        public bool ReturnValue { get; }

        /// <summary>Gets field or property member names.</summary>
        public string[] Members { get; }
    }
}

#endif

namespace Uncodium.Storage
{
    public static class IAsyncEnumerableExtensions
    {
        public static async Task<long> CountAsync<T>(this IAsyncEnumerable<T> xs, Ct ct = default)
        {
            var count = 0L;
            await foreach (var x in xs.WithCancellation(ct))
            {
                count++;
            }
            return count;
        }

        public static async Task<bool> AnyAsync<T>(this IAsyncEnumerable<T> xs, Ct ct = default)
        {
            await foreach (var _ in xs.WithCancellation(ct)) return true;
            return false;
        }

        public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> xs, Ct ct = default)
        {
            var result = new List<T>();
            await foreach (var x in xs.WithCancellation(ct)) result.Add(x);
            return result;
        }
    }
}