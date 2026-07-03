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

using DataStax.AstraDB.DataApi.Admin;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;


/// <summary>
/// JSON converter for <see cref="CloudProviderType"/>, with case-insensitive deserialization logic.
/// </summary>
public class CloudProviderTypeConverter : JsonConverter<CloudProviderType>
{
    /// <summary>
    /// Reads and converts values into a <see cref="CloudProviderType"/>, case-insensitively.
    /// </summary>
    public override CloudProviderType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var value = reader.GetString();
        if (string.Equals(value, "aws", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.AWS;
        if (string.Equals(value, "gcp", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.GCP;
        if (string.Equals(value, "azure", StringComparison.OrdinalIgnoreCase)) return CloudProviderType.AZURE;
        throw new JsonException($"Unknown CloudProviderType value: '{value}'");
    }

    /// <summary>
    /// Writes an <see cref="CloudProviderType"/> value as string.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, CloudProviderType value, JsonSerializerOptions options)
    {
        var serialized = value switch
        {
            CloudProviderType.AWS => "aws",
            CloudProviderType.GCP => "gcp",
            CloudProviderType.AZURE => "azure",
            _ => throw new JsonException($"Unknown CloudProviderType value: '{value}'")
        };
        writer.WriteStringValue(serialized);
    }
}

/// <summary>
/// JSON converter for <see cref="CloudProviderType"/>?, with case-insensitive deserialization logic.
/// </summary>
public class CloudProviderTypeNullableConverter : JsonConverter<CloudProviderType?>
{
    private static readonly CloudProviderTypeConverter _inner = new();

    /// <inheritdoc/>
    public override CloudProviderType? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null) return null;
        return _inner.Read(ref reader, typeof(CloudProviderType), options);
    }

    /// <inheritdoc/>
    public override void Write(Utf8JsonWriter writer, CloudProviderType? value, JsonSerializerOptions options)
    {
        if (value is null) { writer.WriteNullValue(); return; }
        _inner.Write(writer, value.Value, options);
    }
}
