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

using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using System;
using System.Linq;
using System.Security;
using System.Web;

namespace Uncodium.Storage;

public static class AzureSasUtils
{
    /// <summary>
    /// E.g. ....?sp=rl
    /// </summary>
    public static AzureSasPermissions ParseSasPermissions(Uri sas)
    {
        var queryParams = HttpUtility.ParseQueryString(sas.Query);
        return ParseSasPermissions(queryParams["sp"]);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    public static AzureSasPermissions ParseSasPermissionsFromConnectionString(string connectionString)
    {
#if NET
        var qs = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries)
            .Single(x => x.StartsWith("SharedAccessSignature="))
            ["SharedAccessSignature=".Length..]
            ;
#else
        var qs = connectionString
            .Split([';'], StringSplitOptions.RemoveEmptyEntries)
            .Single(x => x.StartsWith("SharedAccessSignature="))
            .Substring("SharedAccessSignature=".Length)
            ;
#endif

        var queryParams = HttpUtility.ParseQueryString(qs);
        return ParseSasPermissions(queryParams["sp"]);
    }

    /// <summary>
    /// E.g. "rl"
    /// </summary>
    public static AzureSasPermissions ParseSasPermissions(string? s)
    {
        if (s == null) return AzureSasPermissions.None;

        var p = AzureSasPermissions.None;
        foreach (var c in s) p |= c switch
        {
            'r' => AzureSasPermissions.Read,
            'a' => AzureSasPermissions.Add,
            'c' => AzureSasPermissions.Create,
            'w' => AzureSasPermissions.Write,
            'd' => AzureSasPermissions.Delete,
            'x' => AzureSasPermissions.DeleteVersion,
            'y' => AzureSasPermissions.PermantDelete,
            'l' => AzureSasPermissions.List,
            't' => AzureSasPermissions.Tags,
            'f' => AzureSasPermissions.Find,
            'm' => AzureSasPermissions.Move,
            'e' => AzureSasPermissions.Execute,
            'o' => AzureSasPermissions.Ownership,
            'p' => AzureSasPermissions.Permissions,
            'i' => AzureSasPermissions.SetImmutabilityPolicy,
            _ => throw new Exception($"Unknown Azure SAS permission sp={c}. Error c5825a0e-3fef-4b40-8c6f-48f074947fea.")
        };

        return p;
    }

    /// <summary>
    /// Get uncodium permissions from AzureSasPermissions.
    /// </summary>
    public static Permissions ToUncodiumPermissions(this AzureSasPermissions x)
    {
        var p = Permissions.None;

        if (x.HasFlag(AzureSasPermissions.Read))    p |= Permissions.Read;
        if (x.HasFlag(AzureSasPermissions.Write))   p |= Permissions.Write;
        if (x.HasFlag(AzureSasPermissions.Delete))  p |= Permissions.Delete;
        if (x.HasFlag(AzureSasPermissions.List))    p |= Permissions.List;

        return p;
    }

    /// <summary>
    /// Get azure permissions from uncodium permissions.
    /// </summary>
    public static AzureSasPermissions ToAzurePermissions(this Permissions x)
    {
        var p = AzureSasPermissions.None;

        if (x.HasFlag(Permissions.Read))    p |= AzureSasPermissions.Read;
        if (x.HasFlag(Permissions.Write))   p |= AzureSasPermissions.Write;
        if (x.HasFlag(Permissions.Delete))  p |= AzureSasPermissions.Delete;
        if (x.HasFlag(Permissions.List))    p |= AzureSasPermissions.List;

        return p;
    }

    public static BlobContainerSasPermissions ToAzureBlobContainerSasPermissions(this Permissions x)
    {
        var p = default(BlobContainerSasPermissions);

        if (x.HasFlag(Permissions.Read))    p |= BlobContainerSasPermissions.Read;
        if (x.HasFlag(Permissions.Write))   p |= BlobContainerSasPermissions.Write;
        if (x.HasFlag(Permissions.Delete))  p |= BlobContainerSasPermissions.Delete;
        if (x.HasFlag(Permissions.List))    p |= BlobContainerSasPermissions.List;

        return p;
    }

    /// <summary>
    /// 
    /// </summary>
    public static Uri GetServiceSasUriForContainer(
        BlobContainerClient containerClient,
        BlobContainerSasPermissions permissions,
        DateTimeOffset validUntil,
        string? storedPolicyName = null
        )
    {
        // check whether this BlobContainerClient object has been authorized with shared key.
        if (containerClient.CanGenerateSasUri)
        {
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = containerClient.Name,
                Resource = "c" // resource is a container
                               // https://learn.microsoft.com/en-us/dotnet/api/azure.storage.sas.blobsasbuilder.resource?view=azure-dotnet
            };

            if (storedPolicyName == null)
            {
                sasBuilder.ExpiresOn = validUntil;
                sasBuilder.SetPermissions(permissions);
            }
            else
            {
                sasBuilder.Identifier = storedPolicyName;
            }

            var sasUri = containerClient.GenerateSasUri(sasBuilder);
            return sasUri;
        }
        else
        {
            throw new Exception(
                "BlobContainerClient must be authorized with Shared Key credentials to create a service SAS. " +
                "Error b41722a3-e311-49b3-9f4a-79247a6b1d38."
                );
        }
    }
}
