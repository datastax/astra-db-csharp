/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// using System;
// using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Specifies the cloud provider on which an Astra DB database is deployed.
/// </summary>
// [JsonConverter(typeof(CloudProviderTypeConverter))]
public enum CloudProviderType
{
    /// <summary>Amazon Web Services.</summary>
    [JsonStringEnumMemberName("aws")]
    AWS,
    /// <summary>Google Cloud Platform.</summary>
    [JsonStringEnumMemberName("gcp")]
    GCP,
    /// <summary>Microsoft Azure.</summary>
    [JsonStringEnumMemberName("azure")]
    AZURE
}

// internal sealed class CloudProviderTypeConverter : JsonConverter<CloudProviderType>
// {
//     public override CloudProviderType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
//     {
//         var value = reader.GetString();
//         if (string.Equals(value, "aws", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.AWS;
//         if (string.Equals(value, "gcp", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.GCP;
//         if (string.Equals(value, "azure", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.AZURE;
//         throw new JsonException($"Unknown CloudProviderType value: '{value}'");
//     }

//     public override void Write(Utf8JsonWriter writer, CloudProviderType value, JsonSerializerOptions options)
//     {
//         var serialized = value switch
//         {
//             CloudProviderType.AWS => "aws",
//             CloudProviderType.GCP => "gcp",
//             CloudProviderType.AZURE => "azure",
//             _ => throw new JsonException($"Unknown CloudProviderType value: '{value}'")
//         };
//         writer.WriteStringValue(serialized);
//     }
// }