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

using DataStax.AstraDB.DataApi.Core.Commands;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// JSON converter that serializes and deserializes byte arrays using the Data API
/// binary format: <c>{ "$binary": "&lt;base64&gt;" }</c>.
/// </summary>
public class ByteArrayAsBinaryJsonConverter : JsonConverter<byte[]>
{
    /// <inheritdoc />
    public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException($"Expected start of object when reading byte array, but got {reader.TokenType}.");

        string base64String = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException($"Expected property name when reading byte array, but got {reader.TokenType}.");

            string propertyName = reader.GetString();

            if (propertyName == DataAPIKeywords.Binary)
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.String)
                    throw new JsonException($"Expected string value for '{DataAPIKeywords.Binary}', but got {reader.TokenType}.");
                base64String = reader.GetString();
            }
            else
            {
                reader.Skip();
            }
        }

        if (base64String == null)
            throw new JsonException($"Missing required property '{DataAPIKeywords.Binary}'.");

        try
        {
            return Convert.FromBase64String(base64String);
        }
        catch (FormatException ex)
        {
            throw new JsonException($"Invalid Base64 string: '{base64String}'.", ex);
        }
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(DataAPIKeywords.Binary, Convert.ToBase64String(value));
        writer.WriteEndObject();
    }
}

/// <summary>
/// JSON converter for float arrays. Serializes as a JSON array (<c>[1.0, 2.0]</c>)
/// and deserializes from either a JSON array or the Data API binary format
/// (<c>{ "$binary": "&lt;base64&gt;" }</c>). Use <see cref="FloatBinaryWriter"/> to serialize as binary instead.
/// </summary>
public class FloatArrayJsonConverterBase : JsonConverter<float[]>
{
    /// <inheritdoc />
    public override float[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.StartArray => ReadAsArray(ref reader),
            JsonTokenType.StartObject => ReadAsBinary(ref reader),
            _ => throw new JsonException($"Expected array or object when reading float array, but got {reader.TokenType}.")
        };
    }

    private static float[] ReadAsArray(ref Utf8JsonReader reader)
    {
        var list = new System.Collections.Generic.List<float>();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                break;
            if (reader.TokenType == JsonTokenType.Number)
            {
                list.Add(reader.GetSingle());
            }
            else if (reader.TokenType == JsonTokenType.String)
            {
                // AllowNamedFloatingPointLiterals encodes NaN/Infinity/-Infinity as strings
                list.Add(reader.GetString() switch
                {
                    "NaN" => float.NaN,
                    "Infinity" => float.PositiveInfinity,
                    "-Infinity" => float.NegativeInfinity,
                    var s => throw new JsonException($"Unexpected string value '{s}' in float array.")
                });
            }
            else
            {
                throw new JsonException($"Expected number in float array, but got {reader.TokenType}.");
            }
        }
        return list.ToArray();
    }

    private static float[] ReadAsBinary(ref Utf8JsonReader reader)
    {
        string base64String = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException($"Expected property name when reading float array, but got {reader.TokenType}.");

            string propertyName = reader.GetString();

            if (propertyName == DataAPIKeywords.Binary)
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.String)
                    throw new JsonException($"Expected string value for '{DataAPIKeywords.Binary}', but got {reader.TokenType}.");
                base64String = reader.GetString();
            }
            else
            {
                reader.Skip();
            }
        }

        if (base64String == null)
            throw new JsonException($"Missing required property '{DataAPIKeywords.Binary}'.");

        byte[] bytes;
        try
        {
            bytes = Convert.FromBase64String(base64String);
        }
        catch (FormatException ex)
        {
            throw new JsonException($"Invalid Base64 string: '{base64String}'.", ex);
        }

        if (bytes.Length % 4 != 0)
            throw new JsonException($"Binary data length {bytes.Length} is not a multiple of 4 (required for float array).");

        var floats = new float[bytes.Length / 4];
        for (int i = 0; i < floats.Length; i++)
        {
            int offset = i * 4;
            if (BitConverter.IsLittleEndian)
            {
                var chunk = new byte[] { bytes[offset + 3], bytes[offset + 2], bytes[offset + 1], bytes[offset] };
                floats[i] = BitConverter.ToSingle(chunk, 0);
            }
            else
            {
                floats[i] = BitConverter.ToSingle(bytes, offset);
            }
        }
        return floats;
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, float[] value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var f in value)
        {
            if (float.IsNaN(f))
                writer.WriteStringValue("NaN");
            else if (float.IsPositiveInfinity(f))
                writer.WriteStringValue("Infinity");
            else if (float.IsNegativeInfinity(f))
                writer.WriteStringValue("-Infinity");
            else
                writer.WriteNumberValue(f);
        }
        writer.WriteEndArray();
    }
}

/// <summary>
/// Serializes a float array to a JSON array: <c>[1.0, 2.0, 3.0]</c>.
/// Deserializes from either JSON array or binary format.
/// </summary>
public class FloatArrayWriter : FloatArrayJsonConverterBase { }

/// <summary>
/// Serializes a float array to the Data API binary format:
/// <c>{ "$binary": "&lt;base64&gt;" }</c> with big-endian IEEE 754 encoding.
/// Deserializes from either binary or JSON array format.
/// </summary>
public class FloatBinaryWriter : FloatArrayJsonConverterBase
{
    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, float[] value, JsonSerializerOptions options)
    {
        var bytes = new byte[value.Length * 4];
        for (int i = 0; i < value.Length; i++)
        {
            var chunk = BitConverter.GetBytes(value[i]);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(chunk);
            Buffer.BlockCopy(chunk, 0, bytes, i * 4, 4);
        }
        writer.WriteStartObject();
        writer.WriteString(DataAPIKeywords.Binary, Convert.ToBase64String(bytes));
        writer.WriteEndObject();
    }
}
