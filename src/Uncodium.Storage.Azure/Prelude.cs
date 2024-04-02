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